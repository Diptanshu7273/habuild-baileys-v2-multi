/**
 * =====================================================
 *  HABUILD COMMUNITY TRACKER v2.0 — Multi-Phone Server
 *  Powered by Baileys (FREE WhatsApp Web API)
 *  Supports 21+ phones / unlimited communities
 * =====================================================
 */
require('dotenv').config();
const express=require('express'),cors=require('cors'),fs=require('fs'),path=require('path');
const multer=require('multer'),pino=require('pino'),NodeCache=require('node-cache');
const{Boom}=require('@hapi/boom');
const{default:makeWASocket,useMultiFileAuthState,fetchLatestBaileysVersion,DisconnectReason,delay}=require('baileys');

const CONFIG={PORT:process.env.PORT||3000,ALERT_NUMBERS:(process.env.ALERT_NUMBERS||process.env.ALERT_NUMBER||'917273021959').split(',').map(n=>n.trim()).filter(Boolean),WARN_LIMIT:parseInt(process.env.WARN_LIMIT||'1600'),MAX_LIMIT:parseInt(process.env.MAX_LIMIT||'1800'),SYNC_INTERVAL:parseInt(process.env.SYNC_INTERVAL_MINUTES||'10')*60*1000,MIN_MEMBERS:parseInt(process.env.MIN_MEMBERS||'1'),DATA_FILE:path.join(__dirname,'data','communities.json'),SCHEDULED_FILE:path.join(__dirname,'data','scheduled.json'),SESSIONS_DIR:path.join(__dirname,'sessions'),UPLOADS_DIR:path.join(__dirname,'uploads')};

[path.dirname(CONFIG.DATA_FILE),CONFIG.SESSIONS_DIR,CONFIG.UPLOADS_DIR,path.join(__dirname,'public')].forEach(d=>{if(!fs.existsSync(d))fs.mkdirSync(d,{recursive:true});});

let db={communities:{},history:[],phones:{}};
let scheduled=[];
const groupCache=new NodeCache({stdTTL:5*60,useClones:false});

function loadData(){try{if(fs.existsSync(CONFIG.DATA_FILE)){db=JSON.parse(fs.readFileSync(CONFIG.DATA_FILE,'utf8'));if(!db.communities)db.communities={};if(!db.history)db.history=[];if(!db.phones)db.phones={};}}catch(e){}}
function saveData(){try{fs.writeFileSync(CONFIG.DATA_FILE,JSON.stringify(db,null,2));}catch(e){}}
function loadScheduled(){try{if(fs.existsSync(CONFIG.SCHEDULED_FILE))scheduled=JSON.parse(fs.readFileSync(CONFIG.SCHEDULED_FILE,'utf8'));}catch(e){scheduled=[];}}
function saveScheduled(){try{fs.writeFileSync(CONFIG.SCHEDULED_FILE,JSON.stringify(scheduled,null,2));}catch(e){}}
loadData();loadScheduled();

const sessions={};

async function createSession(sessionId){
  if(sessions[sessionId]?.sock){console.log(`⚠️ Session ${sessionId} exists`);return;}
  const authFolder=path.join(CONFIG.SESSIONS_DIR,sessionId);
  if(!fs.existsSync(authFolder))fs.mkdirSync(authFolder,{recursive:true});
  sessions[sessionId]={sock:null,status:'connecting',qr:null,phoneNumber:null,phoneName:db.phones[sessionId]?.name||sessionId,groupCount:0};
  console.log(`\n🔌 Starting session: ${sessionId}`);
  try{
    const{state,saveCreds}=await useMultiFileAuthState(authFolder);
    const{version}=await fetchLatestBaileysVersion();
    const sock=makeWASocket({version,auth:state,logger:pino({level:'silent'}),markOnlineOnConnect:false,browser:[`Habuild-${sessionId}`,'Chrome','120.0.0'],generateHighQualityLinkPreview:false,syncFullHistory:false,cachedGroupMetadata:async(jid)=>groupCache.get(jid)});
    sessions[sessionId].sock=sock;
    sock.ev.on('creds.update',saveCreds);

    sock.ev.on('group-participants.update',async(event)=>{
      try{await delay(1000);const metadata=await sock.groupMetadata(event.id);groupCache.set(event.id,metadata);
      const count=metadata.participants.length,action=event.action;
      const nums=event.participants.map(p=>p.replace('@s.whatsapp.net','').replace('@lid',''));
      console.log(`👥 [${sessionId}][${action.toUpperCase()}] ${metadata.subject}: ${nums.join(', ')} → ${count}`);
      if(db.communities[event.id]){db.communities[event.id].count=count;db.communities[event.id].updatedAt=new Date().toISOString();
      const change=action==='add'?nums.length:action==='remove'?-nums.length:0;
      addHistory(event.id,metadata.subject,count,change,`${action}: ${nums.join(', ')}`);saveData();checkAlerts(event.id,metadata.subject,count,sessionId);}}catch(e){}});

    sock.ev.on('groups.update',async([event])=>{try{const m=await sock.groupMetadata(event.id);groupCache.set(event.id,m);if(db.communities[event.id]){db.communities[event.id].name=m.subject;db.communities[event.id].count=m.participants.length;db.communities[event.id].updatedAt=new Date().toISOString();saveData();}}catch(e){}});

    sock.ev.on('connection.update',async({connection,lastDisconnect,qr})=>{
      if(qr){sessions[sessionId].qr=qr;sessions[sessionId].status='qr_pending';console.log(`📱 [${sessionId}] QR ready`);}
      if(connection==='open'){
        if(!sessions[sessionId])return;
        sessions[sessionId].status='connected';sessions[sessionId].qr=null;
        const me=sock.user;if(me){const num=me.id.replace('@s.whatsapp.net','').replace(':*','').split(':')[0];
        sessions[sessionId].phoneNumber=num;sessions[sessionId].phoneName=db.phones[sessionId]?.name||me.name||`Phone ${num}`;
        db.phones[sessionId]={id:sessionId,number:num,name:me.name||sessionId,status:'connected',connectedAt:new Date().toISOString()};saveData();}
        console.log(`✅ [${sessionId}] Connected! Phone: ${sessions[sessionId].phoneNumber}`);
        await delay(5000);await syncSessionGroups(sessionId);}
      if(connection==='close'){const sc=(lastDisconnect?.error instanceof Boom)?lastDisconnect.error.output.statusCode:null;
        if(!sessions[sessionId]){console.log(`🗑️ [${sessionId}] Session was removed, skipping reconnect`);return;}
        if(sc===DisconnectReason.loggedOut){sessions[sessionId].status='logged_out';console.log(`❌ [${sessionId}] Logged out`);if(db.phones[sessionId])db.phones[sessionId].status='logged_out';saveData();}
        else{sessions[sessionId].status='reconnecting';console.log(`⚠️ [${sessionId}] Disconnected. Reconnecting...`);if(db.phones[sessionId])db.phones[sessionId].status='reconnecting';saveData();await delay(15000);if(!sessions[sessionId]){console.log(`🗑️ [${sessionId}] Removed during wait, skip reconnect`);return;}sessions[sessionId].sock=null;createSession(sessionId);}}});
  }catch(err){console.error(`[${sessionId}] Fatal:`,err.message);if(sessions[sessionId])sessions[sessionId].status='error';}
}

async function syncSessionGroups(sessionId){
  const s=sessions[sessionId];if(!s?.sock||s.status!=='connected')return 0;
  try{const groups=await s.sock.groupFetchAllParticipating();const gl=Object.values(groups);let synced=0;
  for(const g of gl){const count=g.participants?.length||0;if(count<CONFIG.MIN_MEMBERS)continue;
    const ex=db.communities[g.id],old=ex?.count||0;
    db.communities[g.id]={...(ex||{}),id:g.id,name:g.subject||'Unnamed',count,phoneId:sessionId,phoneNumber:s.phoneNumber,updatedAt:new Date().toISOString(),description:g.desc||''};
    groupCache.set(g.id,g);if(ex&&old!==count)addHistory(g.id,g.subject,count,count-old);checkAlerts(g.id,g.subject,count,sessionId);synced++;}
  sessions[sessionId].groupCount=synced;console.log(`   [${sessionId}] Synced ${synced} groups`);return synced;}catch(e){console.error(`   [${sessionId}] Sync error:`,e.message);return 0;}}

async function syncAllGroups(){console.log('\n🔄 Syncing all sessions...');let t=0;
  for(const sid of Object.keys(sessions)){if(sessions[sid].status==='connected'){t+=await syncSessionGroups(sid);await delay(2000);}}
  saveData();console.log(`✅ Total: ${t} communities\n`);return{synced:t};}

function findSessionForGroup(gid){const c=db.communities[gid];if(c?.phoneId&&sessions[c.phoneId]?.status==='connected')return sessions[c.phoneId];for(const sid of Object.keys(sessions))if(sessions[sid].status==='connected')return sessions[sid];return null;}
function getAnyConnectedSession(){for(const sid of Object.keys(sessions))if(sessions[sid].status==='connected'&&sessions[sid].sock)return sessions[sid];return null;}

async function checkAlerts(gid,name,count,sid){const s=sessions[sid]||getAnyConnectedSession();if(!s?.sock)return;
  for(const num of CONFIG.ALERT_NUMBERS){
    const jid=num.replace(/[^0-9]/g,'')+'@s.whatsapp.net';
    try{
      if(count>=CONFIG.MAX_LIMIT)await s.sock.sendMessage(jid,{text:`🚨 *FULL* ${name}\n👥 ${count}/${CONFIG.MAX_LIMIT}\n⛔ New link needed!\n— Habuild`});
      else if(count>=CONFIG.WARN_LIMIT)await s.sock.sendMessage(jid,{text:`⚠️ *WARNING* ${name}\n👥 ${count}/${CONFIG.MAX_LIMIT}\n📊 ${CONFIG.MAX_LIMIT-count} slots left\n— Habuild`});
    }catch(e){console.error(`Alert to ${num} failed:`,e.message);}
    await delay(1000);
  }}

function addHistory(gid,name,count,change,action){db.history.unshift({groupId:gid,groupName:name,count,change:change||0,date:new Date().toISOString(),action:action||''});if(db.history.length>500)db.history=db.history.slice(0,500);}

async function checkScheduledMessages(){const now=Date.now();let upd=false;
  for(const msg of scheduled){if(msg.status!=='pending')continue;if(new Date(msg.scheduledAt).getTime()>now)continue;msg.status='sending';upd=true;
  try{const tgs=msg.targetGroup==='all'?Object.keys(db.communities):[msg.targetGroup];let sent=0;
  for(const gid of tgs){const s=findSessionForGroup(gid);if(!s?.sock)continue;
  try{let c;if(msg.type==='text')c={text:msg.text};else if(msg.type==='image'&&msg.filePath)c={image:fs.readFileSync(msg.filePath),caption:msg.caption||'',mimetype:msg.mimetype||'image/jpeg'};else if(msg.type==='video'&&msg.filePath)c={video:fs.readFileSync(msg.filePath),caption:msg.caption||'',mimetype:msg.mimetype||'video/mp4'};else if(msg.type==='document'&&msg.filePath)c={document:fs.readFileSync(msg.filePath),fileName:msg.fileName||'doc',caption:msg.caption||'',mimetype:msg.mimetype||'application/pdf'};else c={text:msg.text||msg.caption||''};
  await s.sock.sendMessage(gid,c);sent++;await delay(2000);}catch(e){}}
  msg.status='sent';msg.sentAt=new Date().toISOString();msg.sentCount=sent;}catch(e){msg.status='failed';msg.error=e.message;}}if(upd)saveScheduled();}

// ── Express ──────────────────────────────────────────
const app=express();app.use(cors());app.use(express.json({limit:'50mb'}));app.use(express.urlencoded({extended:true,limit:'50mb'}));app.use(express.static(path.join(__dirname,'public')));
const upload=multer({dest:CONFIG.UPLOADS_DIR});
app.get('/',(req,res)=>res.sendFile(path.join(__dirname,'public','dashboard.html')));

// Sessions API
app.get('/api/sessions',(req,res)=>{const list=Object.entries(sessions).map(([id,s])=>({id,status:s.status,phoneNumber:s.phoneNumber,phoneName:s.phoneName,groupCount:s.groupCount,hasQR:!!s.qr}));res.json({sessions:list});});
app.post('/api/sessions',(req,res)=>{const{name}=req.body;const sid=name?name.replace(/[^a-zA-Z0-9_-]/g,'_').toLowerCase():`phone_${Object.keys(sessions).length+1}`;if(sessions[sid])return res.json({success:false,error:'Exists'});createSession(sid);res.json({success:true,sessionId:sid});});
app.get('/api/sessions/:id/qr',(req,res)=>{const s=sessions[req.params.id];if(!s)return res.json({qr:null,status:'not_found'});res.json({qr:s.qr,status:s.status,phoneNumber:s.phoneNumber});});

// Serve QR as PNG image
app.get('/api/sessions/:id/qr.png',async(req,res)=>{
  const s=sessions[req.params.id];
  if(!s||!s.qr){res.status(404).send('No QR');return;}
  try{
    const QRCode=require('qrcode');
    const buffer=await QRCode.toBuffer(s.qr,{width:300,margin:2,color:{dark:'#1a1a18',light:'#ffffff'}});
    res.set('Content-Type','image/png');
    res.set('Cache-Control','no-cache');
    res.send(buffer);
  }catch(e){res.status(500).send('QR generate error: '+e.message);}
});

app.delete('/api/sessions/:id',async(req,res)=>{const sid=req.params.id,s=sessions[sid];
  try{
    if(s&&s.sock)try{s.sock.end(undefined);}catch(e){}
    delete sessions[sid];
    delete db.phones[sid];
    let removed=0;
    for(const cid of Object.keys(db.communities)){
      if(db.communities[cid].phoneId===sid){delete db.communities[cid];removed++;}
    }
    const authFolder=path.join(CONFIG.SESSIONS_DIR,sid);
    if(fs.existsSync(authFolder))fs.rmSync(authFolder,{recursive:true,force:true});
    saveData();
    console.log(`🗑️ Session ${sid} removed completely (${removed} communities cleaned)`);
    res.json({success:true,communitiesRemoved:removed});
  }catch(e){res.json({success:false,error:e.message});}
});

app.post('/api/sessions/:id/logout',async(req,res)=>{const sid=req.params.id,s=sessions[sid];if(!s)return res.json({success:false});try{if(s.sock)await s.sock.logout();fs.rmSync(path.join(CONFIG.SESSIONS_DIR,sid),{recursive:true,force:true});delete sessions[sid];if(db.phones[sid])db.phones[sid].status='logged_out';saveData();res.json({success:true});}catch(e){res.json({success:false,error:e.message});}});

// Rename a phone session
app.patch('/api/sessions/:id',async(req,res)=>{
  const sid=req.params.id;
  const{name}=req.body;
  if(!name||!name.trim())return res.json({success:false,error:'Name required'});
  const s=sessions[sid];
  if(!s)return res.json({success:false,error:'Session not found'});
  s.phoneName=name.trim();
  if(!db.phones[sid])db.phones[sid]={id:sid,name:name.trim(),status:s.status};
  else db.phones[sid].name=name.trim();
  saveData();
  console.log(`✏️ Renamed ${sid} → "${name.trim()}"`);
  res.json({success:true,newName:name.trim()});
});

// Communities
app.get('/api/communities',(req,res)=>{
  const c=Object.values(db.communities).sort((a,b)=>b.count-a.count);
  const phoneMap={};
  Object.entries(db.phones).forEach(([id,p])=>{phoneMap[id]={...p};});
  Object.entries(sessions).forEach(([id,s])=>{
    phoneMap[id]={id,number:s.phoneNumber,name:s.phoneName||id,status:s.status,groupCount:s.groupCount||0,...(phoneMap[id]||{})};
  });
  const phones=Object.values(phoneMap);
  res.json({communities:c,phones,totalMembers:c.reduce((a,x)=>a+(x.count||0),0),connectedPhones:Object.values(sessions).filter(s=>s.status==='connected').length,totalPhones:Object.keys(sessions).length});
});

app.get('/api/history',(req,res)=>res.json({history:db.history.slice(0,100)}));
app.post('/api/sync',async(req,res)=>{const r=await syncAllGroups();res.json(r);});
app.get('/api/status',(req,res)=>{const cn=Object.values(sessions).filter(s=>s.status==='connected').length,t=Object.keys(sessions).length,pq=Object.values(sessions).filter(s=>s.status==='qr_pending').length;res.json({status:cn>0?'connected':pq>0?'qr_pending':t>0?'disconnected':'no_sessions',connectedPhones:cn,totalPhones:t,pendingQR:pq,communities:Object.keys(db.communities).length,totalMembers:Object.values(db.communities).reduce((a,c)=>a+(c.count||0),0)});});

// Members
app.get('/api/group/:groupId/members',async(req,res)=>{const{groupId}=req.params;const s=findSessionForGroup(groupId);if(!s?.sock)return res.status(503).json({error:'No session'});
  try{
    const m=await s.sock.groupMetadata(groupId);
    if(m.participants.length>0){
      console.log('🔍 Sample participant keys:', JSON.stringify(Object.keys(m.participants[0])));
      console.log('🔍 Sample participant:', JSON.stringify(m.participants[0]));
    }
    const lidStore=s.sock.signalRepository?.lidMapping||null;
    const members=await Promise.all(m.participants.map(async(p)=>{
      const raw=p.id||'';
      const isLid=raw.includes('@lid');
      const lidNum=raw.replace('@s.whatsapp.net','').replace('@lid','').replace(':*','').split(':')[0].trim();
      let realNumber=null;
      if(p.phoneNumber){realNumber=p.phoneNumber.replace('@s.whatsapp.net','').replace('+','').trim();}
      else if(p.pn){realNumber=p.pn.replace('@s.whatsapp.net','').replace('+','').trim();}
      else if(p.jid&&p.jid.includes('@s.whatsapp.net')){realNumber=p.jid.replace('@s.whatsapp.net','').trim();}
      else if(isLid&&lidStore){try{const pn=await lidStore.getPNForLID(raw);if(pn)realNumber=pn.replace('@s.whatsapp.net','').trim();}catch(e){}}
      if(!isLid){realNumber=lidNum;}
      const displayNum=realNumber||lidNum;
      const showAsLid=!realNumber&&isLid;
      let role='member';
      if(p.admin==='superadmin'||p.admin==='admin')role='admin';
      return{number:realNumber||lidNum,displayNumber:showAsLid?`LID:${lidNum}`:displayNum,role,isLid:showAsLid,lid:isLid?lidNum:null};
    }));
    res.json({groupId,groupName:m.subject,description:m.desc||'',totalMembers:members.length,admins:members.filter(x=>x.role==='admin').length,regularMembers:members.filter(x=>x.role==='member').length,members});
  }catch(e){console.error('Members error:',e);res.status(500).json({error:e.message});}});

// Add/Remove members
app.post('/api/group/:groupId/members',async(req,res)=>{const{groupId}=req.params,{number}=req.body;const s=findSessionForGroup(groupId);if(!s?.sock)return res.status(503).json({error:'No session'});if(!number)return res.status(400).json({error:'Number required'});
  try{const jid=number.replace(/[^0-9]/g,'')+'@s.whatsapp.net';const r=await s.sock.groupParticipantsUpdate(groupId,[jid],'add');if(db.communities[groupId]){db.communities[groupId].count+=1;db.communities[groupId].updatedAt=new Date().toISOString();addHistory(groupId,db.communities[groupId].name,db.communities[groupId].count,1,`Added ${number}`);saveData();}res.json({success:true,message:`Added +${number}`,result:r});}catch(e){res.json({success:false,error:e.message});}});

app.delete('/api/group/:groupId/members/:number',async(req,res)=>{const{groupId,number}=req.params;const s=findSessionForGroup(groupId);if(!s?.sock)return res.status(503).json({error:'No session'});
  try{const jid=number.replace(/[^0-9]/g,'')+'@s.whatsapp.net';const r=await s.sock.groupParticipantsUpdate(groupId,[jid],'remove');if(db.communities[groupId]){db.communities[groupId].count=Math.max(0,(db.communities[groupId].count||1)-1);db.communities[groupId].updatedAt=new Date().toISOString();addHistory(groupId,db.communities[groupId].name,db.communities[groupId].count,-1,`Removed ${number}`);saveData();}res.json({success:true,message:`Removed +${number}`,result:r});}catch(e){res.json({success:false,error:e.message});}});

// ── Excel Export — deduplicated, sorted A-Z ──────────
app.get('/api/export-excel',async(req,res)=>{
  try{
    const ExcelJS=require('exceljs');
    const wb=new ExcelJS.Workbook();
    wb.creator='Habuild Tracker';
    const ws=wb.addWorksheet('Communities',{views:[{state:'frozen',ySplit:1}]});

    ws.columns=[
      {header:'Community Name',key:'name',width:50},
      {header:'Members',key:'members',width:15},
    ];

    // Style header row
    ws.getRow(1).eachCell(cell=>{
      cell.font={bold:true,color:{argb:'FFFFFFFF'},size:11};
      cell.fill={type:'pattern',pattern:'solid',fgColor:{argb:'FF2D7A4F'}};
      cell.alignment={horizontal:'center',vertical:'middle'};
    });
    ws.getRow(1).height=24;

    // Deduplicate by name — keep only the entry with highest member count
    const nameMap={};
    Object.values(db.communities).forEach(c=>{
      const key=(c.name||'').trim().toLowerCase();
      if(!nameMap[key]||c.count>nameMap[key].count) nameMap[key]=c;
    });
    const sorted=Object.values(nameMap).sort((a,b)=>(a.name||'').localeCompare(b.name||''));

    // Add data rows using array format (reliable)
    for(const c of sorted){
      const row=ws.addRow([c.name||'', c.count||0]);
      row.getCell(2).alignment={horizontal:'center'};
    }

    // Blank row then bold TOTAL
    ws.addRow([]);
    const total=sorted.reduce((a,c)=>a+(c.count||0),0);
    const totalRow=ws.addRow(['TOTAL', total]);
    totalRow.getCell(1).font={bold:true,size:11};
    totalRow.getCell(2).font={bold:true,size:11};
    totalRow.getCell(2).alignment={horizontal:'center'};

    res.setHeader('Content-Type','application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition',`attachment; filename="Habuild-Communities-${new Date().toISOString().slice(0,10)}.xlsx"`);
    await wb.xlsx.write(res);
    res.end();
  }catch(e){
    console.error('Excel export error:',e.message);
    res.status(500).json({error:e.message});
  }
});

// Invite link
app.get('/api/group/:groupId/invite',async(req,res)=>{const s=findSessionForGroup(req.params.groupId);if(!s?.sock)return res.status(503).json({error:'No session'});try{const c=await s.sock.groupInviteCode(req.params.groupId);res.json({success:true,inviteLink:`https://chat.whatsapp.com/${c}`});}catch(e){res.json({success:false,error:'Need admin'});}});

// Send message
app.post('/api/send-message',upload.single('file'),async(req,res)=>{const{type,text,caption,target}=req.body;const file=req.file;const tgs=target==='all'?Object.keys(db.communities):[target];let sent=0;
  for(const gid of tgs){const s=findSessionForGroup(gid);if(!s?.sock)continue;try{let c;if(type==='text')c={text:text||''};else if(type==='image'&&file)c={image:fs.readFileSync(file.path),caption:caption||'',mimetype:file.mimetype};else if(type==='video'&&file)c={video:fs.readFileSync(file.path),caption:caption||'',mimetype:file.mimetype};else if(type==='document'&&file)c={document:fs.readFileSync(file.path),fileName:file.originalname,caption:caption||'',mimetype:file.mimetype};else c={text:text||caption||''};await s.sock.sendMessage(gid,c);sent++;await delay(2000);}catch(e){}}
  if(file)try{fs.unlinkSync(file.path);}catch(e){}res.json({success:true,sent,total:tgs.length});});

// Scheduler
app.get('/api/scheduled',(req,res)=>res.json({messages:scheduled}));
app.post('/api/schedule',upload.single('file'),(req,res)=>{const{type,text,caption,scheduledAt,target}=req.body;const file=req.file;const msg={id:Date.now().toString(36)+Math.random().toString(36).substr(2,5),type:type||'text',text:text||'',caption:caption||'',filePath:file?file.path:null,fileName:file?file.originalname:null,mimetype:file?file.mimetype:null,scheduledAt:scheduledAt||new Date().toISOString(),targetGroup:target||'all',status:'pending',createdAt:new Date().toISOString()};scheduled.push(msg);saveScheduled();res.json({success:true,id:msg.id});});
app.delete('/api/schedule/:id',(req,res)=>{const m=scheduled.find(x=>x.id===req.params.id);if(m&&m.status==='pending'){m.status='cancelled';saveScheduled();res.json({success:true});}else res.json({success:false});});

// Config / Test alert
app.post('/api/config',(req,res)=>{
  if(req.body.alertNumbers){CONFIG.ALERT_NUMBERS=req.body.alertNumbers.split(',').map(n=>n.trim().replace(/[^0-9]/g,'')).filter(Boolean);}
  else if(req.body.waNumber){const num=req.body.waNumber.replace(/[^0-9]/g,'');if(!CONFIG.ALERT_NUMBERS.includes(num))CONFIG.ALERT_NUMBERS.push(num);}
  if(req.body.addNumber){const num=req.body.addNumber.replace(/[^0-9]/g,'');if(num&&!CONFIG.ALERT_NUMBERS.includes(num))CONFIG.ALERT_NUMBERS.push(num);}
  if(req.body.removeNumber){const num=req.body.removeNumber.replace(/[^0-9]/g,'');CONFIG.ALERT_NUMBERS=CONFIG.ALERT_NUMBERS.filter(n=>n!==num);}
  res.json({success:true,alertNumbers:CONFIG.ALERT_NUMBERS});
});
app.get('/api/config',(req,res)=>{res.json({alertNumbers:CONFIG.ALERT_NUMBERS,warnLimit:CONFIG.WARN_LIMIT,maxLimit:CONFIG.MAX_LIMIT});});
app.get('/api/test-alert',async(req,res)=>{const s=getAnyConnectedSession();if(!s?.sock)return res.status(503).json({error:'No session'});
  let sent=0;
  for(const num of CONFIG.ALERT_NUMBERS){
    try{const jid=num.replace(/[^0-9]/g,'')+'@s.whatsapp.net';await s.sock.sendMessage(jid,{text:`✅ *Test Alert*\nPhones: ${Object.values(sessions).filter(x=>x.status==='connected').length}\nGroups: ${Object.keys(db.communities).length}\nAlert recipients: ${CONFIG.ALERT_NUMBERS.length}\n— Habuild`});sent++;await delay(1000);}catch(e){console.error(`Test alert to ${num} failed:`,e.message);}
  }
  res.json({success:true,sent,total:CONFIG.ALERT_NUMBERS.length});});

// ── Start ────────────────────────────────────────────
app.listen(CONFIG.PORT,()=>{
  console.log('\n'+'═'.repeat(55));
  console.log('  🌿 HABUILD COMMUNITY TRACKER v2.0');
  console.log('  💚 Powered by Baileys (FREE — Multi-Phone)');
  console.log(`  🌐 Dashboard: http://localhost:${CONFIG.PORT}`);
  console.log(`  📱 Phones: http://localhost:${CONFIG.PORT}/phones.html`);
  console.log(`  🔔 Alerts: ${CONFIG.ALERT_NUMBERS.map(n=>'+'+n).join(', ')}`);
  console.log('═'.repeat(55)+'\n');
  const existing=fs.readdirSync(CONFIG.SESSIONS_DIR).filter(f=>fs.statSync(path.join(CONFIG.SESSIONS_DIR,f)).isDirectory());
  if(existing.length>0){console.log(`📂 Found ${existing.length} saved sessions. Reconnecting...`);existing.forEach((sid,i)=>{setTimeout(()=>createSession(sid),i*5000);});}
  else console.log('📱 No sessions. Go to /phones.html to add phones.');
  setInterval(syncAllGroups,CONFIG.SYNC_INTERVAL);
  setInterval(checkScheduledMessages,30*1000);
});