/* ============================================================
   DHURTA SYNC — app.js  (Premium redesign)
   P2P engine: MQTT mesh · WebRTC DataChannel · Reed-Solomon QR
   UI layer:   Radar mode · Chat mode · Ambient light
   ============================================================ */
'use strict';

const app = (() => {

  /* ─────────────── CONSTANTS ─────────────── */
  const VERSION     = 'v10';
  const CHUNK_SIZE  = 256 * 1024;   // 256 KB — 4× faster than 64 KB
  const BUFFER_HIGH = 4 * 1024 * 1024; // 4 MB buffer threshold
  const HB_MS       = 1800;
  const PEER_TTL    = 7000;
  const WARP_BPS    = 50 * 1024 * 1024; // 50 MB/s target on LAN
  const NTFY_BASE   = 'https://ntfy.sh';

  const BROKERS = [
    'wss://broker.emqx.io:8084/mqtt',
    'wss://broker.hivemq.com:8884/mqtt',
    'wss://test.mosquitto.org:8081',
  ];

  const COLORS = [
    '#6366f1','#8b5cf6','#ec4899','#f43f5e','#f97316',
    '#eab308','#22c55e','#14b8a6','#3b82f6','#06b6d4',
    '#a78bfa','#fb923c',
  ];
  const EMOJIS = ['😎','🦊','🐼','🦁','🐸','🦋','🦄','🐲','🤖','👾','🧸','🎯'];

  /* ─────────────── STATE ─────────────── */
  let myId = localStorage.getItem('dhurta_id') || (() => {
    const b = crypto.getRandomValues(new Uint8Array(6));
    const s = Array.from(b, x => x.toString(16).padStart(2, '0')).join('');
    localStorage.setItem('dhurta_id', s); return s;
  })();
  let myName  = localStorage.getItem('dhurta_name')  || 'User_' + myId.slice(0, 4);
  let myColor = localStorage.getItem('dhurta_color') || COLORS[0];
  let myEmoji = localStorage.getItem('dhurta_emoji') || EMOJIS[0];
  let currentPin = localStorage.getItem('dhurta_pin') || _genPin();

  let currentBrokerIdx = 0, mqttClient = null;
  let peers = {}, pcs = {}, dcs = {};
  let selectedPeerId = null;
  let hbTimer = null, pruneTimer = null;
  let outTransfers = {}, inTransfers = {};
  const _msgDedup = new Set();

  let localStream = null, screenStream = null;
  let _scanStream = null, _scanRAF = null, _currentQRTab = 'show';
  let callState = 'idle', callPeerId = null, pendingOffer = null;
  let isMuted = false, isCamOff = false;
  let _callMinimized = false, _callTimer = null, _callSeconds = 0;
  let _ringAudioCtx = null, _ringTimer = null;
  let _bleDevices = {};          // id → { name, rssi, isDhurta }
  let _historyDB = null;         // IndexedDB
  let _batteryMode = false;
  let _callBgIdx = 0;
  const CALL_BACKGROUNDS = ['none','blur','#0d1224','linear-gradient(135deg,#1e1b4b,#0f172a)','linear-gradient(135deg,#0c1a2e,#0d3349)'];

  // LAN signalling fallback (same-device multi-tab)
  let _bc = null;
  try { _bc = new BroadcastChannel('dhurta_v10_' + (localStorage.getItem('dhurta_pin') || '0000')); } catch {}

  // ntfy.sh topic for push (personal, per-device)
  const _ntfyTopic = 'dhurta_' + myId.slice(0, 10);

  let currentMode = 'transfer';

  /* ─────────────── QR GENERATOR (ISO/IEC 18004) ─────────────── */
  const QR = (() => {
    const EXP = new Uint8Array(512), LOG = new Uint8Array(256);
    let x = 1;
    for (let i = 0; i < 255; i++) {
      EXP[i] = x; LOG[x] = i;
      x <<= 1; if (x & 0x100) x ^= 0x11d;
    }
    for (let i = 255; i < 512; i++) EXP[i] = EXP[i - 255];
    const mul = (a, b) => (!a || !b) ? 0 : EXP[LOG[a] + LOG[b]];
    const polyMul = (p, q) => {
      const r = new Uint8Array(p.length + q.length - 1);
      for (let i = 0; i < p.length; i++)
        for (let j = 0; j < q.length; j++) r[i+j] ^= mul(p[i], q[j]);
      return r;
    };
    const genPoly = n => { let p = new Uint8Array([1]); for (let i = 0; i < n; i++) p = polyMul(p, new Uint8Array([1, EXP[i]])); return p; };
    const rsEncode = (data, n) => {
      const gen = genPoly(n), msg = new Uint8Array(data.length + n); msg.set(data);
      for (let i = 0; i < data.length; i++) { const c = msg[i]; if (c) for (let j = 0; j < gen.length; j++) msg[i+j] ^= mul(gen[j], c); }
      return msg.slice(data.length);
    };
    const encode = text => {
      const bytes = new TextEncoder().encode(text), L = bytes.length;
      const VS = [{v:2,size:25,data:28,blocks:1,ec:16},{v:3,size:29,data:44,blocks:1,ec:26},{v:4,size:33,data:64,blocks:2,ec:18},{v:5,size:37,data:86,blocks:2,ec:24}];
      const vi = VS.find(v => v.data >= L + 2) || VS[VS.length-1];
      const {v, size, data:dW, blocks, ec:ecPB} = vi;
      const bits = [], addBits = (val, len) => { for (let i = len-1; i >= 0; i--) bits.push((val>>i)&1); };
      addBits(0b0100,4); addBits(L,8); for (const b of bytes) addBits(b,8); addBits(0,4);
      while (bits.length % 8) bits.push(0);
      const dB = new Uint8Array(dW);
      for (let i = 0; i < bits.length/8 && i < dW; i++) for (let b = 0; b < 8; b++) dB[i] = (dB[i]<<1)|(bits[i*8+b]||0);
      const pads = [0xec,0x11]; for (let i = Math.ceil(bits.length/8); i < dW; i++) dB[i] = pads[i%2];
      const bSz = Math.floor(dW/blocks), dBlk = [], ecBlk = [];
      for (let b = 0; b < blocks; b++) { const s = b*bSz, e = b===blocks-1?dW:s+bSz, bl = dB.slice(s,e); dBlk.push(bl); ecBlk.push(rsEncode(bl,ecPB)); }
      const cw = [], mx = Math.max(...dBlk.map(b=>b.length));
      for (let i = 0; i < mx; i++) for (const b of dBlk) if (i < b.length) cw.push(b[i]);
      for (let i = 0; i < ecPB; i++) for (const e of ecBlk) cw.push(e[i]);
      const mat = Array.from({length:size},()=>new Int8Array(size).fill(-1));
      const sf = (r,c,val) => { if (r>=0&&r<size&&c>=0&&c<size) mat[r][c]=val; };
      const finder = (r,c) => { for (let i=0;i<7;i++) for (let j=0;j<7;j++) sf(r+i,c+j,(i===0||i===6||j===0||j===6)?1:(i>=2&&i<=4&&j>=2&&j<=4)?1:0); };
      finder(0,0); finder(0,size-7); finder(size-7,0);
      for (let i=0;i<8;i++){sf(7,i,0);sf(i,7,0);sf(7,size-1-i,0);sf(i,size-8,0);sf(size-8,i,0);sf(size-1-i,7,0);}
      for (let i=8;i<size-8;i++){sf(6,i,i%2===0?1:0);sf(i,6,i%2===0?1:0);}
      const AL={2:[6,18],3:[6,22],4:[6,26],5:[6,30]};
      if (AL[v]) { const pos=AL[v]; for (const ar of pos) for (const ac of pos) { if(mat[ar][ac]!==-1)continue; for(let i=-2;i<=2;i++) for(let j=-2;j<=2;j++) sf(ar+i,ac+j,(Math.abs(i)===2||Math.abs(j)===2)?1:(i===0&&j===0)?1:0); } }
      sf(size-8,8,1);
      for(let i=0;i<=8;i++){if(mat[8][i]===-1)mat[8][i]=-2;if(mat[i][8]===-1)mat[i][8]=-2;if(mat[size-1-i][8]===-1)mat[size-1-i][8]=-2;if(mat[8][size-1-i]===-1)mat[8][size-1-i]=-2;}
      let cIdx=0, goingUp=true;
      const MASK=(r,c)=>(r+c)%2===0;
      for(let col=size-1;col>=1;col-=2){if(col===6)col--;for(let ri=0;ri<size;ri++){const r=goingUp?size-1-ri:ri;for(let dc=0;dc<=1;dc++){const c=col-dc;if(mat[r][c]===-1){const byte=cIdx<cw.length*8?cw[Math.floor(cIdx/8)]:0;const bit=(byte>>(7-(cIdx%8)))&1;cIdx++;mat[r][c]=MASK(r,c)?bit^1:bit;}}}goingUp=!goingUp;}
      const fmt=[1,1,1,0,1,1,1,1,1,0,0,0,1,0,0],fXor=[1,0,1,0,1,0,0,0,0,0,1,0,0,1,0];
      const f=fmt.map((b,i)=>b^fXor[i]);
      const fp1=[[8,0],[8,1],[8,2],[8,3],[8,4],[8,5],[8,7],[8,8],[7,8],[5,8],[4,8],[3,8],[2,8],[1,8],[0,8]];
      const fp2=Array.from({length:7},(_,i)=>[size-1-i,8]).concat([[8,size-8]],Array.from({length:7},(_,i)=>[8,size-7+i]));
      f.forEach((b,i)=>{sf(...fp1[i],b);if(i<fp2.length)sf(...fp2[i],b);});
      return {mat,size};
    };
    return {
      render(canvas, text, scale=7) {
        const {mat,size}=encode(text), q=4, total=(size+q*2)*scale;
        canvas.width=total; canvas.height=total;
        const ctx=canvas.getContext('2d');
        ctx.fillStyle='#fff'; ctx.fillRect(0,0,total,total);
        ctx.fillStyle='#000';
        for(let r=0;r<size;r++) for(let c=0;c<size;c++) if(mat[r][c]===1) ctx.fillRect((c+q)*scale,(r+q)*scale,scale,scale);
      }
    };
  })();

  /* ─────────────── AVATAR DRAWING ─────────────── */
  function drawAvatar(canvas, color, emoji, size) {
    canvas.width = size; canvas.height = size;
    const ctx = canvas.getContext('2d');
    ctx.clearRect(0, 0, size, size);
    ctx.fillStyle = color;
    ctx.beginPath(); ctx.arc(size/2, size/2, size/2, 0, Math.PI*2); ctx.fill();
    ctx.font = `${size*.52}px sans-serif`;
    ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
    ctx.fillText(emoji, size/2, size/2 + size*.04);
  }

  function avatarDataURL(color, emoji, size=40) {
    const c = document.createElement('canvas');
    drawAvatar(c, color, emoji, size); return c.toDataURL();
  }

  function createAvatarCanvas(color, emoji, size) {
    const c = document.createElement('canvas');
    drawAvatar(c, color, emoji, size); return c;
  }

  /* ─────────────── RADAR ANIMATION ─────────────── */
  class RadarAnimation {
    constructor(canvas) {
      this.canvas = canvas;
      this.ctx    = canvas.getContext('2d');
      this.angle  = 0;
      this.running = false;
      this._resize();
      window.addEventListener('resize', () => { this._resize(); });
    }
    _resize() {
      const stage = this.canvas.parentElement;
      if (!stage) return;
      this.canvas.width  = stage.offsetWidth;
      this.canvas.height = stage.offsetHeight;
    }
    start() { if (this.running) return; this.running = true; this._loop(); }
    stop()  { this.running = false; }
    _loop() {
      if (!this.running) return;
      this._draw();
      requestAnimationFrame(() => this._loop());
    }
    _draw() {
      const { canvas, ctx } = this;
      const w = canvas.width, h = canvas.height;
      const cx = w / 2, cy = h / 2;
      const maxR = Math.min(w, h) * 0.42;
      ctx.clearRect(0, 0, w, h);

      // Concentric rings
      for (let i = 1; i <= 4; i++) {
        const r = maxR * (i / 4);
        const alpha = 0.04 + (4-i) * 0.015;
        ctx.beginPath(); ctx.arc(cx, cy, r, 0, Math.PI*2);
        ctx.strokeStyle = `rgba(129,140,248,${alpha})`;
        ctx.lineWidth = 1; ctx.stroke();
      }

      // Cross-hair lines
      ctx.save();
      ctx.setLineDash([4, 8]);
      ctx.strokeStyle = 'rgba(129,140,248,0.06)';
      ctx.lineWidth = 1;
      ctx.beginPath(); ctx.moveTo(cx - maxR, cy); ctx.lineTo(cx + maxR, cy); ctx.stroke();
      ctx.beginPath(); ctx.moveTo(cx, cy - maxR); ctx.lineTo(cx, cy + maxR); ctx.stroke();
      ctx.setLineDash([]);
      ctx.restore();

      // Sweep sector
      const a = this.angle;
      const sweep = Math.PI / 2.5;
      ctx.save();
      ctx.beginPath(); ctx.moveTo(cx, cy);
      ctx.arc(cx, cy, maxR, a - sweep, a); ctx.closePath();
      const grad = ctx.createLinearGradient(
        cx + maxR * 0.3 * Math.cos(a - sweep), cy + maxR * 0.3 * Math.sin(a - sweep),
        cx + maxR * Math.cos(a), cy + maxR * Math.sin(a)
      );
      grad.addColorStop(0, 'rgba(129,140,248,0)');
      grad.addColorStop(1, 'rgba(129,140,248,0.12)');
      ctx.fillStyle = grad;
      ctx.fill();

      // Sweep line leading edge
      ctx.beginPath();
      ctx.moveTo(cx, cy);
      ctx.lineTo(cx + maxR * Math.cos(a), cy + maxR * Math.sin(a));
      ctx.strokeStyle = 'rgba(129,140,248,0.5)';
      ctx.lineWidth = 1.5; ctx.stroke();
      ctx.restore();

      // Connection lines to peers
      const peerIds = Object.keys(peers);
      peerIds.forEach((id, i) => {
        const n = peerIds.length;
        const peerAngle = (i / n) * Math.PI * 2 - Math.PI / 2;
        const r = maxR * 0.62;
        const px = cx + r * Math.cos(peerAngle);
        const py = cy + r * Math.sin(peerAngle);
        const hasChannel = dcs[id]?.readyState === 'open';
        ctx.save();
        ctx.setLineDash(hasChannel ? [] : [4, 6]);
        ctx.beginPath(); ctx.moveTo(cx, cy); ctx.lineTo(px, py);
        ctx.strokeStyle = hasChannel ? 'rgba(45,212,191,0.3)' : 'rgba(129,140,248,0.15)';
        ctx.lineWidth = hasChannel ? 1.5 : 1;
        ctx.stroke();
        ctx.setLineDash([]);
        ctx.restore();
      });

      this.angle = (this.angle + 0.012) % (Math.PI * 2);
    }
  }

  let _radar = null;

  /* ─────────────── PARTICLE SYSTEM ─────────────── */
  class ParticleSystem {
    constructor(canvas) {
      this.canvas = canvas; this.ctx = canvas.getContext('2d');
      this.particles = []; this.running = false; this.speedMBs = 0;
      this._resize();
      window.addEventListener('resize', () => this._resize());
    }
    _resize() {
      const dpr = window.devicePixelRatio || 1;
      const r = this.canvas.parentElement?.getBoundingClientRect();
      if (!r) return;
      this.canvas.width = r.width * dpr; this.canvas.height = r.height * dpr;
      this.canvas.style.width = r.width + 'px'; this.canvas.style.height = r.height + 'px';
      this.ctx.scale(dpr, dpr); this._w = r.width; this._h = r.height;
    }
    start() { if (this.running) return; this.running = true; this._loop(); }
    stop()  { this.running = false; }
    _loop() {
      if (!this.running && !this.particles.length) return;
      const w = this._w || 300, h = this._h || 60;
      this.ctx.clearRect(0, 0, w, h);
      const density = Math.min(1, this.speedMBs / 50);
      if (this.running && Math.random() < 0.2 + density * 0.5)
        this.particles.push({ x:0, y:h*(0.1+Math.random()*0.8), vx:2+density*5+Math.random()*3, vy:(Math.random()-.5)*1.2, life:1, size:1.5+Math.random()*(2+density*3), hue:160+Math.random()*40, trail:[] });
      for (let i = this.particles.length-1; i >= 0; i--) {
        const p = this.particles[i];
        p.trail.push({x:p.x,y:p.y}); if(p.trail.length>10) p.trail.shift();
        p.x += p.vx; p.y += p.vy; p.life -= this.running ? 0.01 : 0.03;
        if (p.life <= 0 || p.x > w+20) { this.particles.splice(i,1); continue; }
        for (let t=0; t<p.trail.length-1; t++) {
          this.ctx.globalAlpha = (t/p.trail.length)*p.life*0.5;
          this.ctx.strokeStyle = `hsl(${p.hue},90%,65%)`; this.ctx.lineWidth = p.size*.6;
          this.ctx.beginPath(); this.ctx.moveTo(p.trail[t].x,p.trail[t].y); this.ctx.lineTo(p.trail[t+1].x,p.trail[t+1].y); this.ctx.stroke();
        }
        this.ctx.globalAlpha = p.life; this.ctx.shadowColor = `hsl(${p.hue},90%,65%)`; this.ctx.shadowBlur = 6;
        this.ctx.fillStyle = `hsl(${p.hue},90%,78%)`; this.ctx.beginPath(); this.ctx.arc(p.x,p.y,p.size,0,Math.PI*2); this.ctx.fill();
        this.ctx.shadowBlur = 0; this.ctx.globalAlpha = 1;
      }
      requestAnimationFrame(() => this._loop());
    }
  }

  let _particles = null;
  let _activeXfers = 0;

  /* ─────────────── UTILITIES ─────────────── */
  function _genPin() {
    const p = String(Math.floor(1000 + Math.random() * 9000));
    localStorage.setItem('dhurta_pin', p); return p;
  }
  function roomTopic() { return `dhurta_mesh_${VERSION}/room_${currentPin}`; }
  function uid() { return Date.now().toString(36) + Math.random().toString(36).slice(2,7); }
  function formatBytes(b) {
    if (b < 1024) return b + ' B';
    if (b < 1048576) return (b/1024).toFixed(1) + ' KB';
    if (b < 1073741824) return (b/1048576).toFixed(1) + ' MB';
    return (b/1073741824).toFixed(2) + ' GB';
  }
  function fileType(mime) {
    if (!mime) return 'file';
    if (mime.startsWith('image/')) return 'image';
    if (mime.startsWith('video/')) return 'video';
    if (mime.startsWith('audio/')) return 'audio';
    if (mime === 'application/pdf') return 'pdf';
    if (mime.includes('zip')||mime.includes('tar')) return 'archive';
    return 'file';
  }
  function fileIcon(mime) {
    const icons = {
      image: `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="18" height="18" rx="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21 15 16 10 5 21"/></svg>`,
      video: `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="23 7 16 12 23 17 23 7"/><rect x="1" y="5" width="15" height="14" rx="2"/></svg>`,
      audio: `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M9 18V5l12-2v13"/><circle cx="6" cy="18" r="3"/><circle cx="18" cy="16" r="3"/></svg>`,
      pdf:   `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>`,
      archive:`<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="21 8 21 21 3 21 3 8"/><rect x="1" y="3" width="22" height="5"/><line x1="10" y1="12" x2="14" y2="12"/></svg>`,
      file:  `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"/><polyline points="13 2 13 9 20 9"/></svg>`,
    };
    return icons[fileType(mime)] || icons.file;
  }
  function nowTime() { return new Date().toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'}); }
  function scrollBottom() { const s=document.getElementById('chat-stream'); requestAnimationFrame(()=>s&&(s.scrollTop=s.scrollHeight)); }

  /* Toast */
  function toast(msg, ms=2800) {
    const layer = document.getElementById('toast-layer');
    const el = document.createElement('div'); el.className='toast-msg'; el.textContent=msg;
    layer.appendChild(el);
    setTimeout(() => { el.style.opacity='0'; el.style.transform='translateY(6px)'; el.style.transition='opacity .3s, transform .3s'; setTimeout(()=>el.remove(),350); }, ms);
  }

  /* Ambient light mouse tracking */
  function _setupAmbient() {
    document.addEventListener('mousemove', e => {
      document.documentElement.style.setProperty('--mx', e.clientX + 'px');
      document.documentElement.style.setProperty('--my', e.clientY + 'px');
    }, { passive: true });
  }

  /* ─────────────── MQTT ─────────────── */
  function connectMQTT() {
    // Wire BroadcastChannel for LAN/offline same-device signalling
    if (_bc) {
      _bc.onmessage = e => { if(e.data?.from !== myId) _handleSignal(e.data); };
      _setStatus('online', 'LAN Mode');
    }
    const url = BROKERS[currentBrokerIdx % BROKERS.length];
    if (!_batteryMode) _setStatus('warn', 'Connecting…');
    try { mqttClient = mqtt.connect(url, { clientId:'dhurta_'+myId, keepalive:30, reconnectPeriod:0, connectTimeout:8000, clean:true }); }
    catch { _rotateAndReconnect(); return; }
    mqttClient.on('connect', () => {
      mqttClient.subscribe(roomTopic(), {qos:0});
      _setStatus('online', 'Online');
      broadcastPresence('DISCOVER_PING'); _startHB();
      _subscribeNtfy();
    });
    mqttClient.on('message', (_t, raw) => {
      let msg; try { msg = JSON.parse(raw.toString()); } catch { return; }
      if (msg.from === myId) return; _handleSignal(msg);
    });
    mqttClient.on('error', ()=>_rotateAndReconnect());
    mqttClient.on('close', ()=>_rotateAndReconnect());
  }
  function _rotateAndReconnect() {
    _setStatus('error', 'Reconnecting…');
    if (mqttClient) { try { mqttClient.end(true); } catch {} mqttClient=null; }
    currentBrokerIdx++;
    setTimeout(connectMQTT, 1200);
  }
  function publish(payload) {
    const msg = {from:myId,...payload};
    const raw = JSON.stringify(msg);
    if (mqttClient?.connected) mqttClient.publish(roomTopic(), raw, {qos:0});
    // LAN fallback: BroadcastChannel works same-device multi-tab even without internet
    if (_bc) try { _bc.postMessage(msg); } catch {}
  }
  function broadcastPresence(type='HEARTBEAT') {
    publish({type, name:myName, color:myColor, emoji:myEmoji, ts:Date.now(), ntfy:_ntfyTopic});
  }
  function _startHB() {
    clearInterval(hbTimer); clearInterval(pruneTimer);
    hbTimer    = setInterval(()=>broadcastPresence('HEARTBEAT'), HB_MS);
    pruneTimer = setInterval(_prunePeers, 2000);
  }
  function _prunePeers() {
    const now=Date.now(); let changed=false;
    for (const id in peers) if (now-peers[id].lastSeen>PEER_TTL) { delete peers[id]; _closePc(id); changed=true; }
    if (changed) { renderRoster(); _renderRadarPeers(); }
  }

  /* ─────────────── SIGNALING ─────────────── */
  function _handleSignal(msg) {
    const {from,type}=msg;
    if (type==='DISCOVER_PING'||type==='HEARTBEAT') {
      const isNew=!peers[from];
      peers[from]={name:msg.name,color:msg.color,emoji:msg.emoji,lastSeen:Date.now(),ntfy:msg.ntfy};
      if (type==='DISCOVER_PING') publish({type:'DISCOVER_PONG',name:myName,color:myColor,emoji:myEmoji,ts:Date.now(),ntfy:_ntfyTopic});
      if (isNew) {
        renderRoster(); _renderRadarPeers();
        toast('🔵 '+msg.name+' joined');
        // auto-establish secure channel so chat/files work immediately
        setTimeout(()=>_ensureDC(from).catch(()=>{}), 800);
      } else { _updatePeerItem(from); _updatePeerOrb(from); peers[from].lastSeen=Date.now(); }
    }
    if (type==='DISCOVER_PONG') {
      const isNew=!peers[from];
      peers[from]={name:msg.name,color:msg.color,emoji:msg.emoji,lastSeen:Date.now(),ntfy:msg.ntfy};
      if (isNew) {
        renderRoster(); _renderRadarPeers();
        setTimeout(()=>_ensureDC(from).catch(()=>{}), 800);
      }
    }
    // MQTT chat — works even before WebRTC DC is ready
    if (type==='CHAT') {
      if (!peers[from]) peers[from]={name:msg.name||from.slice(0,6),color:msg.color||'#888',emoji:msg.emoji||'😊',lastSeen:Date.now()};
      _appendMessage({from, text:msg.text, mqttName:msg.name, mqttColor:msg.color});
      // Push notification if page not focused
      _showChatNotification(from, msg.name||peers[from]?.name||'Peer', msg.text);
      // Also push via ntfy to peer so they get locked-screen notification
    }
    if (type==='TYPING_MQTT') _handleTyping(from,msg);
    if (type==='OFFER')        _handleOffer(from,msg);
    if (type==='ANSWER')       _handleAnswer(from,msg);
    if (type==='ICE')          _handleIce(from,msg);
    if (type==='CALL_REQUEST') _handleCallRequest(from,msg);
    if (type==='CALL_ACCEPT')  _handleCallAccept(from,msg);
    if (type==='CALL_REJECT')  _handleCallReject(from);
    if (type==='CALL_END')     endCall(true);
    if (type==='CURSOR')       _handleCursor(from,msg);
    if (type==='CLIPBOARD_SYNC') _receiveClipboard(from,msg);
    if (type==='NOTIFY_RELAY') _showRelayedNotification(msg);
    if (type==='TYPING')       _handleTyping(from,msg);
  }

  /* ─────────────── WebRTC ─────────────── */
  // iceCandidatePoolSize pre-gathers host (LAN) candidates before signalling starts
  // No TURN server → pure P2P; on same WiFi/hotspot the host candidates resolve locally
  const ICE = {
    iceServers: [
      {urls:'stun:stun.l.google.com:19302'},
      {urls:'stun:stun1.l.google.com:19302'},
    ],
    iceCandidatePoolSize: 10,
    bundlePolicy: 'max-bundle',
    rtcpMuxPolicy: 'require',
  };

  function _getOrCreatePc(id) {
    if (pcs[id]) return pcs[id];
    const pc = new RTCPeerConnection(ICE); pcs[id]=pc;
    pc.onicecandidate = e => {
      if (!e.candidate) return;
      // Always send — browser already picks fastest path (host > srflx > relay)
      publish({type:'ICE',to:id,candidate:e.candidate});
    };
    pc.ontrack = e => {
      const v=document.getElementById('call-vid-remote'); if(v) v.srcObject=e.streams[0];
      const st=document.getElementById('call-overlay-status'); if(st) st.textContent='Connected';
      _startCallTimer(); _showCallOverlay();
    };
    pc.ondatachannel = e => _setupDC(id, e.channel);
    pc.onconnectionstatechange = () => { if(['failed','disconnected','closed'].includes(pc.connectionState)) _closePc(id); };
    return pc;
  }
  function _closePc(id) {
    if(pcs[id]){try{pcs[id].close();}catch{}delete pcs[id];}
    if(dcs[id]){try{dcs[id].close();}catch{}delete dcs[id];}
    renderRoster(); _renderRadarPeers();
  }
  function _setupDC(id, dc) {
    dcs[id]=dc; dc.binaryType='arraybuffer';
    dc.onopen    = ()=>{ toast('🔒 Secure channel with '+(peers[id]?.name||id.slice(0,6))); renderRoster(); _renderRadarPeers(); };
    dc.onmessage = e=>_handleDCMsg(id,e.data);
    dc.onerror   = ()=>_closePc(id);
    dc.onclose   = ()=>{ delete dcs[id]; renderRoster(); _renderRadarPeers(); };
  }
  async function _ensureDC(id) {
    if (dcs[id]?.readyState==='open') return dcs[id];
    const pc=_getOrCreatePc(id);
    const dc=pc.createDataChannel('dhurta',{ordered:true});
    _setupDC(id,dc);
    const offer=await pc.createOffer(); await pc.setLocalDescription(offer);
    publish({type:'OFFER',to:id,sdp:pc.localDescription.sdp,sdpType:'offer'});
    return new Promise((res,rej)=>{
      dc.onopen=()=>{ _setupDC(id,dc); res(dc); };
      dc.onerror=rej; setTimeout(()=>rej(new Error('DC timeout')),15000);
    });
  }
  async function _handleOffer(from,msg) {
    if(msg.to&&msg.to!==myId)return;
    const pc=_getOrCreatePc(from);
    const offerSdp = typeof msg.sdp==='object' ? msg.sdp.sdp : msg.sdp;
    await pc.setRemoteDescription(new RTCSessionDescription({type:'offer',sdp:offerSdp}));
    const ans=await pc.createAnswer(); await pc.setLocalDescription(ans);
    publish({type:'ANSWER',to:from,sdp:pc.localDescription.sdp,sdpType:'answer'});
  }
  async function _handleAnswer(from,msg) {
    if(msg.to&&msg.to!==myId)return;
    const pc=pcs[from]; if(!pc)return;
    const ansSdp = typeof msg.sdp==='object' ? msg.sdp.sdp : msg.sdp;
    await pc.setRemoteDescription(new RTCSessionDescription({type:'answer',sdp:ansSdp}));
  }
  async function _handleIce(from,msg) {
    if(msg.to&&msg.to!==myId)return;
    const pc=pcs[from]; if(!pc)return;
    try{await pc.addIceCandidate(new RTCIceCandidate(msg.candidate));}catch{}
  }

  /* ─────────────── DATA CHANNEL MESSAGES ─────────────── */
  function _handleDCMsg(id, data) {
    if (typeof data==='string') {
      let msg; try{msg=JSON.parse(data);}catch{return;}
      if(msg.type==='CHAT')         _appendMessage({from:id,text:msg.text,mqttName:msg.name,mqttColor:msg.color});
      if(msg.type==='FILE_META')    { inTransfers[msg.transferId]={meta:msg,chunks:[],received:0,startTime:Date.now()}; _appendIncomingFile(id,msg); }
      if(msg.type==='FILE_DONE')    { const t=inTransfers[msg.transferId]; if(!t)return; _finalizeFile(msg.transferId,new Blob(t.chunks,{type:t.meta.mimeType}),t.meta); delete inTransfers[msg.transferId]; _activeXfers=Math.max(0,_activeXfers-1); if(!_activeXfers&&_particles)_particles.stop(); }
      if(msg.type==='CLIPBOARD_DATA') _receiveClipboard(id,msg);
    } else {
      const view=new DataView(data), tIdLen=view.getUint8(0);
      const tId=new TextDecoder().decode(new Uint8Array(data,1,tIdLen));
      const chunk=data.slice(1+tIdLen);
      const t=inTransfers[tId]; if(!t)return;
      t.chunks.push(chunk); t.received+=chunk.byteLength;
      const pct=(t.received/t.meta.size)*100;
      const elapsed=(Date.now()-t.startTime)/1000||.001;
      const speedBs=t.received/elapsed;
      _updateFileProgress(tId,pct,t.received,t.meta.size,speedBs);
      if(_particles) _particles.speedMBs=speedBs/1048576;
    }
  }

  /* ─────────────── TRANSFER ANIMATION ─────────────── */
  function _startXferAnim() {
    _activeXfers++;
    const parent=document.getElementById('chat-stream')?.parentElement;
    if(!parent) return;
    if(!_particles) {
      let cv=document.getElementById('_particle_canvas');
      if(!cv){cv=document.createElement('canvas');cv.id='_particle_canvas';cv.style.cssText='position:absolute;top:0;left:0;width:100%;height:56px;pointer-events:none;z-index:4;';parent.insertBefore(cv,parent.firstChild);}
      _particles=new ParticleSystem(cv);
    }
    _particles.start();
  }

  /* ─────────────── CHAT ─────────────── */
  let _typingTimers = {};
  function _onTyping() {
    publish({type:'TYPING_MQTT',active:true,name:myName});
    clearTimeout(_typingTimers._out);
    _typingTimers._out=setTimeout(()=>publish({type:'TYPING_MQTT',active:false}),2000);
  }
  function _handleTyping(from,msg) {
    const el=document.getElementById('typing_'+from);
    if(msg.active){
      if(!el){const r=document.createElement('div');r.className='msg-row in';r.id='typing_'+from;r.innerHTML='<div class="typing-indicator"><div class="typing-dot"></div><div class="typing-dot"></div><div class="typing-dot"></div></div>';document.getElementById('chat-stream')?.appendChild(r);scrollBottom();}
      clearTimeout(_typingTimers[from]); _typingTimers[from]=setTimeout(()=>{document.getElementById('typing_'+from)?.remove();delete _typingTimers[from];},3000);
    } else { el?.remove(); clearTimeout(_typingTimers[from]); delete _typingTimers[from]; }
  }

  function sendMessage() {
    const input=document.getElementById('msg-input');
    const text=input.value.trim(); if(!text)return;
    input.value='';
    _appendMessage({from:myId,text});
    // Broadcast over MQTT (works immediately, no DC needed)
    publish({type:'CHAT', text, name:myName, color:myColor, emoji:myEmoji, ts:Date.now()});
    // Also send via DC for peers who may be in different MQTT rooms
    const p=JSON.stringify({type:'CHAT',text,name:myName,color:myColor,ts:Date.now()});
    for(const id in dcs) if(dcs[id].readyState==='open') dcs[id].send(p);
  }

  function _appendMessage({from,text,mqttName,mqttColor}) {
    // dedupe: skip if same message arrived via both DC and MQTT
    const dedupKey = from+'|'+text.slice(0,20);
    if(_msgDedup.has(dedupKey)){return;} _msgDedup.add(dedupKey);
    setTimeout(()=>_msgDedup.delete(dedupKey), 3000);

    _hideEmpty();
    document.getElementById('typing_'+from)?.remove();
    const isMe=from===myId; const peer=peers[from];
    const name=isMe?myName:(peer?.name||mqttName||from.slice(0,8));
    const color=isMe?myColor:(peer?.color||mqttColor||'#888');
    const row=document.createElement('div'); row.className='msg-row '+(isMe?'out':'in');
    const bub=document.createElement('div'); bub.className='bubble';
    if(!isMe){const s=document.createElement('div');s.className='bubble-sender';s.style.color=color;s.textContent=name;bub.appendChild(s);}
    const txt=document.createElement('div'); txt.className='bubble-text'; txt.textContent=text; bub.appendChild(txt);
    const meta=document.createElement('div'); meta.className='bubble-meta';
    const t=document.createElement('span'); t.className='bubble-time'; t.textContent=nowTime(); meta.appendChild(t);
    if(isMe){const tk=document.createElement('span');tk.className='bubble-ticks';tk.textContent='✓✓';meta.appendChild(tk);}
    bub.appendChild(meta); row.appendChild(bub);
    document.getElementById('chat-stream')?.appendChild(row); scrollBottom();
  }

  /* ─────────────── CLIPBOARD SYNC ─────────────── */
  async function syncClipboard() {
    const btns=[document.getElementById('clip-btn-transfer'),document.querySelector('.clipboard-btn')];
    btns.forEach(b=>b?.classList.add('syncing'));
    try {
      let text=''; try{text=await navigator.clipboard.readText();}catch{}
      if(!text){toast('Clipboard empty or permission denied');return;}
      const p=JSON.stringify({type:'CLIPBOARD_DATA',text,ts:Date.now()});
      let sent=0; for(const id in dcs) if(dcs[id].readyState==='open'){dcs[id].send(p);sent++;}
      publish({type:'CLIPBOARD_SYNC',text,ts:Date.now()});
      toast(`📋 Clipboard synced to ${sent} peer(s)`);
    } finally { setTimeout(()=>btns.forEach(b=>b?.classList.remove('syncing')),800); }
  }

  function _receiveClipboard(from,msg) {
    const peer=peers[from]; const name=peer?.name||from.slice(0,8);
    _hideEmpty();
    const row=document.createElement('div'); row.className='msg-row in';
    const card=document.createElement('div'); card.className='clip-card';
    card.innerHTML=`<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="var(--accent)" stroke-width="2"><rect x="9" y="2" width="6" height="4" rx="1"/><path d="M8 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V4a2 2 0 0 0-2-2h-2"/></svg>
    <div style="flex:1;min-width:0"><div style="font-size:10px;color:var(--t3);margin-bottom:2px">${name} shared clipboard</div><div class="clip-preview">${msg.text.slice(0,80)}</div></div>
    <button class="clip-paste" onclick="navigator.clipboard.writeText(${JSON.stringify(msg.text)}).then(()=>app._toast('Pasted'))">Paste</button>`;
    row.appendChild(card);
    document.getElementById('chat-stream')?.appendChild(row); scrollBottom();
    toast('📋 Clipboard received from '+name);
  }

  /* ─────────────── NOTIFICATION RELAY ─────────────── */
  function _reqNotifPerm() { if('Notification'in window&&Notification.permission==='default') Notification.requestPermission(); }
  function _showRelayedNotification(msg) {
    if(Notification.permission==='granted') new Notification(msg.title||'Dhurta notification',{body:msg.body||'',icon:avatarDataURL(peers[msg.from]?.color||'#555',peers[msg.from]?.emoji||'🔔',40)});
    toast('🔔 '+(msg.title||'Notification')+': '+(msg.body||''));
  }

  /* ─────────────── FILE TRANSFER ─────────────── */
  async function sendFiles(files) {
    if(!files?.length)return;
    let targets=Object.keys(dcs).filter(id=>dcs[id].readyState==='open');
    if(!targets.length){
      for(const id of Object.keys(peers)){try{await _ensureDC(id);}catch{}}
      targets=Object.keys(dcs).filter(id=>dcs[id].readyState==='open');
    }
    if(!targets.length){toast('No peers connected. Ask them to join room '+currentPin);return;}
    for(const file of files){
      AI.smartToast(file);
      if(file.size > 10*1024*1024) AI.getTransferTip(file);
      const tId=uid();
      const meta={type:'FILE_META',transferId:tId,name:file.name,size:file.size,mimeType:file.type};
      _appendOutgoingFile(myId,meta,tId);
      _startXferAnim();
      for(const pid of targets){
        const dc=dcs[pid]; if(!dc||dc.readyState!=='open')continue;
        dc.send(JSON.stringify(meta));
        await _streamFile(file,tId,dc);
        dc.send(JSON.stringify({type:'FILE_DONE',transferId:tId}));
      }
      _updateFileProgress(tId,100,file.size,file.size,0);
      _activeXfers=Math.max(0,_activeXfers-1); if(!_activeXfers&&_particles)_particles.stop();
    }
    document.querySelectorAll('#file-input,#file-input-transfer').forEach(el=>el.value='');
  }

  async function _streamFile(file,tId,dc) {
    const tIdBytes = new TextEncoder().encode(tId);
    const hdr = new Uint8Array(1 + tIdBytes.length);
    hdr[0] = tIdBytes.length; hdr.set(tIdBytes, 1);
    let offset = 0; const t0 = Date.now();
    while (offset < file.size) {
      if (dc.readyState !== 'open') break;
      // Yield only when buffer is full — this removes artificial per-chunk delay
      // and lets WebRTC saturate the channel at full LAN speed (~30-50 MB/s)
      if (dc.bufferedAmount > BUFFER_HIGH) {
        await new Promise(r => {
          dc.bufferedAmountLowThreshold = BUFFER_HIGH / 2;
          dc.onbufferedamountlow = r;
        });
      }
      const ab = await file.slice(offset, offset + CHUNK_SIZE).arrayBuffer();
      const pkt = new Uint8Array(hdr.length + ab.byteLength);
      pkt.set(hdr); pkt.set(new Uint8Array(ab), hdr.length);
      dc.send(pkt.buffer);
      offset += ab.byteLength;
      const elapsed = (Date.now() - t0) / 1000 || 0.001;
      const speedBs = offset / elapsed;
      _updateFileProgress(tId, (offset / file.size) * 100, offset, file.size, speedBs);
      if (_particles) _particles.speedMBs = speedBs / 1048576;
      _saveTransferHistory(tId, file.name, file.size, 'sending', speedBs);
    }
  }

  /* ── File card builders ── */
  function _appendOutgoingFile(from,meta,tId) {
    _hideEmpty(); _buildFileCard(from,meta,tId,true);
  }
  function _appendIncomingFile(from,meta) {
    _hideEmpty();
    if(currentMode==='transfer') _buildBeamCard(from,meta);
    else _buildFileCard(from,meta,meta.transferId,false);
    _startXferAnim();
  }

  function _buildFileCard(from,meta,tId,isOut) {
    const isMe=isOut;
    const peer=peers[from]; const name=isMe?myName:(peer?.name||from.slice(0,8));
    const color=isMe?myColor:(peer?.color||'#888');
    const CIRC=2*Math.PI*20;
    const row=document.createElement('div'); row.className='msg-row '+(isMe?'out':'in');
    const bub=document.createElement('div'); bub.className='bubble'; bub.style.padding='8px';
    if(!isMe){const s=document.createElement('div');s.className='bubble-sender';s.style.color=color;s.textContent=name;bub.appendChild(s);}
    const card=document.createElement('div'); card.className='file-card'+(isOut?'':' receiving'); card.id='fc_'+tId;
    const inner=document.createElement('div'); inner.className='file-card-inner';
    const wrap=document.createElement('div'); wrap.className='file-icon-wrap';
    const icon=document.createElement('div'); icon.className='file-icon'; icon.innerHTML=fileIcon(meta.mimeType);
    const svgNS='http://www.w3.org/2000/svg'; const svg=document.createElementNS(svgNS,'svg');
    svg.setAttribute('class','file-ring'); svg.setAttribute('viewBox','0 0 52 52');
    const trk=document.createElementNS(svgNS,'circle'); trk.setAttribute('class','track'); trk.setAttribute('cx',26);trk.setAttribute('cy',26);trk.setAttribute('r','20');
    const fill=document.createElementNS(svgNS,'circle'); fill.setAttribute('class','fill'); fill.setAttribute('id','ring_'+tId); fill.setAttribute('cx',26);fill.setAttribute('cy',26);fill.setAttribute('r','20');fill.setAttribute('stroke-dasharray',CIRC.toFixed(1));fill.setAttribute('stroke-dashoffset',CIRC.toFixed(1));fill.setAttribute('transform','rotate(-90 26 26)');
    svg.appendChild(trk);svg.appendChild(fill);wrap.appendChild(icon);wrap.appendChild(svg);
    const fm=document.createElement('div'); fm.className='file-meta';
    const fn=document.createElement('div');fn.className='file-name';fn.textContent=meta.name;
    const fs=document.createElement('div');fs.className='file-size';fs.textContent=formatBytes(meta.size);
    const fsp=document.createElement('div');fsp.className='file-speed';fsp.id='fspeed_'+tId;fsp.textContent=isOut?'Sending…':'Awaiting…';
    fm.appendChild(fn);fm.appendChild(fs);fm.appendChild(fsp);
    inner.appendChild(wrap);inner.appendChild(fm);card.appendChild(inner);
    const prog=document.createElement('div');prog.className='file-progress';
    const bar=document.createElement('div');bar.className='file-progress-bar';bar.id='fp_'+tId;prog.appendChild(bar);card.appendChild(prog);
    const btn=document.createElement('button');btn.className='btn-dl';btn.id='fd_'+tId;btn.disabled=true;btn.textContent=isOut?'Sending…':'Receiving…';card.appendChild(btn);
    const meta2=document.createElement('div');meta2.className='bubble-meta';
    const t=document.createElement('span');t.className='bubble-time';t.textContent=nowTime();meta2.appendChild(t);
    if(isMe){const tk=document.createElement('span');tk.className='bubble-ticks';tk.textContent='✓';meta2.appendChild(tk);}
    bub.appendChild(card);bub.appendChild(meta2);row.appendChild(bub);
    document.getElementById('chat-stream')?.appendChild(row); scrollBottom();
  }

  function _buildBeamCard(from,meta) {
    const peer=peers[from];
    const strip=document.getElementById('beam-notifications'); if(!strip)return;
    const card=document.createElement('div');card.className='beam-card';card.id='bc_'+meta.transferId;
    const icon=document.createElement('div');icon.className='beam-card-icon';icon.innerHTML=fileIcon(meta.mimeType);
    const m=document.createElement('div');m.className='beam-card-meta';
    const nm=document.createElement('div');nm.className='beam-card-name';nm.textContent=meta.name;
    const sz=document.createElement('div');sz.className='beam-card-size';sz.textContent=formatBytes(meta.size)+' from '+(peer?.name||from.slice(0,6));
    const bar=document.createElement('div');bar.className='beam-card-bar';
    const fill=document.createElement('div');fill.className='beam-card-fill';fill.id='bcfp_'+meta.transferId;bar.appendChild(fill);
    m.appendChild(nm);m.appendChild(sz);m.appendChild(bar);
    const btn=document.createElement('button');btn.className='beam-dl-btn';btn.id='bcdl_'+meta.transferId;btn.disabled=true;btn.textContent='Receiving…';
    card.appendChild(icon);card.appendChild(m);card.appendChild(btn);strip.appendChild(card);
  }

  function _updateFileProgress(tId,pct,received,total,speedBs) {
    const CIRC=2*Math.PI*20;
    const bar=document.getElementById('fp_'+tId); if(bar) bar.style.width=pct.toFixed(1)+'%';
    const ring=document.getElementById('ring_'+tId); if(ring) ring.setAttribute('stroke-dashoffset',(CIRC*(1-pct/100)).toFixed(2));
    const sp=document.getElementById('fspeed_'+tId); if(sp&&pct<100) sp.textContent=formatBytes(speedBs)+'/s · '+pct.toFixed(0)+'%';
    const bfill=document.getElementById('bcfp_'+tId); if(bfill) bfill.style.width=pct.toFixed(1)+'%';
    const btn=document.getElementById('fd_'+tId); if(btn&&pct<100&&!btn.disabled) btn.textContent=pct.toFixed(0)+'%';
    if(currentMode==='beam') _updateBeamProgress(pct, speedBs);
  }

  function _finalizeFile(tId,blob,meta) {
    const bar=document.getElementById('fp_'+tId); if(bar) bar.style.width='100%';
    const ring=document.getElementById('ring_'+tId); if(ring) ring.setAttribute('stroke-dashoffset','0');
    const sp=document.getElementById('fspeed_'+tId); if(sp) sp.textContent='Complete ✓';
    document.getElementById('fc_'+tId)?.classList.remove('receiving');

    const dl=(id,label)=>{
      const btn=document.getElementById(id); if(!btn)return;
      btn.disabled=false; btn.textContent=label; btn.classList.add('done');
      btn.onclick=()=>{ const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download=meta.name;a.click();setTimeout(()=>URL.revokeObjectURL(a.href),30000); };
    };
    dl('fd_'+tId,'💾 Save'); dl('bcdl_'+tId,'💾 Save');

    if(meta.mimeType?.startsWith('image/')){
      const fc=document.getElementById('fc_'+tId);
      if(fc){const img=document.createElement('img');img.src=URL.createObjectURL(blob);img.style.cssText='max-width:100%;margin-top:8px;border-radius:7px;cursor:pointer;display:block;';img.onclick=()=>openLightbox(img.src);fc.appendChild(img);}
    }
    if(currentMode==='beam') { _addToBeamGrid(blob, meta); _updateBeamProgress(100, 0); }
    toast('📥 '+meta.name+' received');
  }

  /* Transfer tray entry */
  function _addTrayItem(tId,name,size) {
    const tray=document.getElementById('transfer-tray');
    const list=document.getElementById('transfers-list');
    if(tray) tray.classList.add('visible');
    const item=document.createElement('div');item.className='transfer-item';
    item.innerHTML=`<div class="ti-name">${name}</div><div class="ti-bar"><div class="ti-fill" id="tif_${tId}"></div></div><div class="ti-meta"><span id="tis_${tId}">Starting…</span><span id="tib_${tId}">0 / ${formatBytes(size)}</span></div>`;
    list?.appendChild(item);
    setTimeout(()=>{item.style.transition='opacity .8s';item.style.opacity='0';setTimeout(()=>item.remove(),900);},12000);
    return {fill:document.getElementById('tif_'+tId),speed:document.getElementById('tis_'+tId),bytes:document.getElementById('tib_'+tId)};
  }

  /* ─────────────── DRAG & DROP ─────────────── */
  function _setupDragDrop() {
    let counter=0;
    const overlay=document.getElementById('drag-overlay');
    document.addEventListener('dragenter',e=>{if(e.dataTransfer?.types?.includes('Files')){counter++;overlay?.classList.add('active');}});
    document.addEventListener('dragleave',()=>{counter--;if(counter<=0){counter=0;overlay?.classList.remove('active');}});
    document.addEventListener('dragover',e=>e.preventDefault());
    document.addEventListener('drop',e=>{e.preventDefault();counter=0;overlay?.classList.remove('active');const files=Array.from(e.dataTransfer?.files||[]);if(files.length)sendFiles(files);});
  }

  /* ─────────────── CALLS ─────────────── */
  async function startCall(callType) {
    if(!selectedPeerId){toast('Select a peer first');return;}
    if(callState!=='idle'){toast('Call in progress');return;}
    callPeerId=selectedPeerId; callState='calling';
    publish({type:'CALL_REQUEST',to:callPeerId,callType});
    toast('Calling '+(peers[callPeerId]?.name||'peer')+'…');
    await _acquireMedia(callType);
  }
  async function _acquireMedia(callType) {
    if(callType==='screen'){
      try{screenStream=await navigator.mediaDevices.getDisplayMedia({video:true,audio:true});localStream=screenStream;_trackCursor();}
      catch(e){toast('Screen share failed: '+e.message);callState='idle';}
    } else {
      try{localStream=await navigator.mediaDevices.getUserMedia({audio:true,video:callType==='video'});}
      catch(e){
        if(e.name==='NotReadableError'&&callType==='video'){
          toast('Camera busy — audio only');
          try{localStream=await navigator.mediaDevices.getUserMedia({audio:true,video:false});}
          catch(e2){toast('Mic error: '+e2.message);callState='idle';return;}
        } else {toast('Media error: '+e.message);callState='idle';return;}
      }
    }
    const vid=document.getElementById('call-vid-local'); if(vid) vid.srcObject=localStream;
    _showCallOverlay();
  }
  async function _initiateWebRTCCall(id) {
    const pc=_getOrCreatePc(id);
    if(localStream) for(const t of localStream.getTracks()) pc.addTrack(t,localStream);
    const dc=pc.createDataChannel('dhurta',{ordered:true}); _setupDC(id,dc);
    const offer=await pc.createOffer(); await pc.setLocalDescription(offer);
    publish({type:'OFFER',to:id,sdp:pc.localDescription.sdp,sdpType:'offer'});
  }
  /* ─── Call overlay helpers ─── */
  function _showCallOverlay() {
    document.getElementById('call-overlay')?.classList.remove('hidden');
  }
  function _hideCallOverlay() {
    document.getElementById('call-overlay')?.classList.add('hidden');
    document.getElementById('call-overlay')?.classList.remove('minimized');
    _callMinimized = false;
  }
  function _showIncomingOverlay(from, callType) {
    const peer = peers[from]||{};
    const ov = document.getElementById('incoming-overlay');
    if (!ov) return;
    const av = document.getElementById('inc-ov-avatar');
    if (av) { av.innerHTML=''; av.appendChild(createAvatarCanvas(peer.color||'#6366f1',peer.emoji||'📞',64)); }
    const nm = document.getElementById('inc-ov-name'); if(nm) nm.textContent = peer.name||from.slice(0,8);
    const tp = document.getElementById('inc-ov-type'); if(tp) tp.textContent = {video:'📹 Video Call',screen:'🖥️ Screen Share',voice:'📞 Voice Call'}[callType]||'📞 Call';
    ov.classList.remove('hidden');
    _playRingtone();
    // System notification for locked screen
    _showCallNotification(peer.name||'Unknown', callType);
  }
  function _hideIncomingOverlay() {
    document.getElementById('incoming-overlay')?.classList.add('hidden');
    _stopRingtone();
  }
  function _startCallTimer() {
    _callSeconds = 0;
    clearInterval(_callTimer);
    _callTimer = setInterval(() => {
      _callSeconds++;
      const m = Math.floor(_callSeconds/60), s = _callSeconds%60;
      const txt = m+':'+(s<10?'0':'')+s;
      const el = document.getElementById('call-overlay-timer');
      if (el) el.textContent = txt;
      const mb = document.getElementById('call-mini-time');
      if (mb) mb.textContent = txt;
    }, 1000);
  }
  function _stopCallTimer() { clearInterval(_callTimer); _callTimer = null; }
  function _updateCallOverlayPeer(from) {
    const peer = peers[from]||{};
    const av = document.getElementById('call-overlay-avatar');
    if (av) { av.innerHTML=''; av.appendChild(createAvatarCanvas(peer.color||'#6366f1',peer.emoji||'📞',72)); }
    const nm = document.getElementById('call-overlay-name'); if(nm) nm.textContent = peer.name||from.slice(0,8);
  }
  function _showVideoDock() {
    // Route video elements into call overlay instead of old video-dock
    const vr = document.getElementById('call-vid-remote');
    if (vr && localStream) { /* handled by stream assignment */ }
    document.getElementById('call-overlay')?.classList.remove('hidden');
  }

  /* ─── Ringtone (Web Audio API) ─── */
  function _playRingtone() {
    _stopRingtone();
    try {
      _ringAudioCtx = new (window.AudioContext||window.webkitAudioContext)();
      const freqs = [880, 1100, 880, 0, 880, 1100];
      let step = 0;
      function ring() {
        if (callState !== 'ringing') { _stopRingtone(); return; }
        const f = freqs[step % freqs.length];
        if (f > 0) {
          const osc = _ringAudioCtx.createOscillator();
          const g = _ringAudioCtx.createGain();
          osc.type = 'sine'; osc.frequency.value = f;
          g.gain.setValueAtTime(0, _ringAudioCtx.currentTime);
          g.gain.linearRampToValueAtTime(0.25, _ringAudioCtx.currentTime + 0.02);
          g.gain.linearRampToValueAtTime(0, _ringAudioCtx.currentTime + 0.18);
          osc.connect(g); g.connect(_ringAudioCtx.destination);
          osc.start(); osc.stop(_ringAudioCtx.currentTime + 0.2);
        }
        step++; _ringTimer = setTimeout(ring, 220);
      }
      ring();
    } catch {}
    if (navigator.vibrate) navigator.vibrate([600,300,600,300,600]);
  }
  function _stopRingtone() {
    clearTimeout(_ringTimer); _ringTimer = null;
    if (_ringAudioCtx) { try { _ringAudioCtx.close(); } catch {} _ringAudioCtx = null; }
    if (navigator.vibrate) navigator.vibrate(0);
  }

  function _handleCallRequest(from,msg) {
    if(msg.to&&msg.to!==myId)return;
    if(callState!=='idle'){publish({type:'CALL_REJECT',to:from});return;}
    callState='ringing'; callPeerId=from; pendingOffer={peerId:from,callType:msg.callType};
    _showIncomingOverlay(from, msg.callType);
  }
  async function acceptCall() {
    if(!pendingOffer)return;
    _hideIncomingOverlay();
    callState='active'; const{peerId,callType}=pendingOffer; pendingOffer=null;
    _updateCallOverlayPeer(peerId); _showCallOverlay();
    const st = document.getElementById('call-overlay-status'); if(st) st.textContent='Connecting…';
    await _acquireMedia(callType); await _initiateWebRTCCall(peerId);
    publish({type:'CALL_ACCEPT',to:peerId});
  }
  function rejectCall() {
    if(!pendingOffer)return;
    _hideIncomingOverlay();
    publish({type:'CALL_REJECT',to:pendingOffer.peerId}); pendingOffer=null; callState='idle';
  }
  function _handleCallAccept(from,msg) {
    if(msg.to&&msg.to!==myId)return;
    callState='active';
    _updateCallOverlayPeer(from); _showCallOverlay();
    const st = document.getElementById('call-overlay-status'); if(st) st.textContent='Connected';
    _startCallTimer();
    _initiateWebRTCCall(from);
  }
  function _handleCallReject(from) { toast((peers[from]?.name||'Peer')+' declined'); endCall(true); }
  function endCall(remote=false) {
    if(!remote&&callPeerId) publish({type:'CALL_END',to:callPeerId});
    localStream?.getTracks().forEach(t=>t.stop()); localStream=null;
    screenStream?.getTracks().forEach(t=>t.stop()); screenStream=null;
    for(const id in pcs) _closePc(id);
    const vr=document.getElementById('call-vid-remote'); if(vr) vr.srcObject=null;
    const vl=document.getElementById('call-vid-local');  if(vl) vl.srcObject=null;
    _hideCallOverlay(); _hideIncomingOverlay(); _stopCallTimer();
    callState='idle'; callPeerId=null; isMuted=false; isCamOff=false;
  }
  function minimizeCall() {
    _callMinimized = !_callMinimized;
    document.getElementById('call-overlay')?.classList.toggle('minimized', _callMinimized);
  }
  function toggleCallBg() {
    _callBgIdx = (_callBgIdx + 1) % CALL_BACKGROUNDS.length;
    const bg = document.getElementById('call-overlay-bg');
    if (!bg) return;
    const v = CALL_BACKGROUNDS[_callBgIdx];
    if (v === 'none') { bg.style.background=''; bg.style.backdropFilter=''; }
    else if (v === 'blur') { bg.style.background='rgba(9,9,11,.6)'; bg.style.backdropFilter='blur(20px)'; }
    else { bg.style.background=v; bg.style.backdropFilter=''; }
  }
  function toggleMute() {
    if(!localStream)return; isMuted=!isMuted;
    localStream.getAudioTracks().forEach(t=>t.enabled=!isMuted);
    document.getElementById('cc-mute')?.classList.toggle('active',isMuted);
    const ic = document.getElementById('cc-mute-ic');
    if (ic) ic.textContent = isMuted ? '🔇' : '🎤';
  }
  function toggleCamera() {
    if(!localStream)return; isCamOff=!isCamOff;
    localStream.getVideoTracks().forEach(t=>t.enabled=!isCamOff);
    document.getElementById('cc-cam')?.classList.toggle('active',isCamOff);
  }
  async function toggleScreen() {
    if(screenStream){screenStream.getTracks().forEach(t=>t.stop());screenStream=null;document.getElementById('btn-screen')?.classList.remove('active');}
    else {
      try{screenStream=await navigator.mediaDevices.getDisplayMedia({video:true});
      const s=pcs[callPeerId]?.getSenders().find(s=>s.track?.kind==='video');if(s)s.replaceTrack(screenStream.getVideoTracks()[0]);
      document.getElementById('btn-screen')?.classList.add('active');_trackCursor();}
      catch(e){toast('Screen share failed: '+e.message);}
    }
  }
  function _trackCursor() { document.addEventListener('mousemove',e=>{if(!screenStream||!callPeerId)return;publish({type:'CURSOR',to:callPeerId,x:e.screenX/window.screen.width,y:e.screenY/window.screen.height});},{passive:true}); }
  function _handleCursor(from,msg) { if(from!==callPeerId)return; const p=document.getElementById('screen-pointer'),w=p?.parentElement;if(!p||!w)return;p.style.display='block';p.style.left=(msg.x*w.offsetWidth)+'px';p.style.top=(msg.y*w.offsetHeight)+'px'; }
  function _showVideoDock() { document.getElementById('video-dock')?.classList.add('active'); if(currentMode==='chat') switchTab('chat'); }

  /* ─────────────── ROSTER ─────────────── */
  function renderRoster() {
    const list=document.getElementById('peer-list'); if(!list)return;
    list.innerHTML='';
    list.appendChild(_makePeerItem(myId,myName,myColor,myEmoji,true));
    for(const[id,p] of Object.entries(peers)) list.appendChild(_makePeerItem(id,p.name,p.color,p.emoji,false));
    const count=Object.keys(peers).length+1;
    const el=document.getElementById('online-count'); if(el) el.textContent=count;
    if(currentMode==='beam') _renderBeamPeers();
  }
  function _makePeerItem(id,name,color,emoji,isMe) {
    const item=document.createElement('div');
    item.className='peer-item'+(isMe?' you':'')+(id===selectedPeerId?' selected':'');
    item.id='pi_'+id;
    const avWrap=document.createElement('div');avWrap.className='pi-avatar';
    avWrap.appendChild(createAvatarCanvas(color,emoji,36));
    const dot=document.createElement('div');dot.className='pi-dot'+(!isMe&&dcs[id]?.readyState!=='open'?' away':'');avWrap.appendChild(dot);
    const info=document.createElement('div');info.className='pi-info';
    const nm=document.createElement('div');nm.className='pi-name';nm.textContent=name;info.appendChild(nm);
    const sub=document.createElement('div');
    if(isMe){sub.className='pi-you';sub.textContent='Host · You';}
    else{sub.className='pi-sub';sub.textContent=dcs[id]?.readyState==='open'?'Secure channel':'In room';}
    info.appendChild(sub);
    item.appendChild(avWrap);item.appendChild(info);
    if(!isMe) item.onclick=()=>selectPeer(id);
    return item;
  }
  function _updatePeerItem(id) {
    const el=document.getElementById('pi_'+id); if(!el){renderRoster();return;}
    const p=peers[id]; peers[id].lastSeen=Date.now();
    const av=el.querySelector('.pi-avatar canvas'); if(av) drawAvatar(av,p.color,p.emoji,36);
    const nm=el.querySelector('.pi-name'); if(nm) nm.textContent=p.name;
  }

  function selectPeer(id) {
    selectedPeerId=id;
    document.querySelectorAll('.peer-item').forEach(e=>e.classList.remove('selected'));
    document.getElementById('pi_'+id)?.classList.add('selected');
    document.querySelectorAll('.peer-orb').forEach(e=>e.classList.toggle('selected',e.dataset.peerId===id));
    const p=peers[id];
    const nm=document.getElementById('call-target-name'); if(nm) nm.textContent=p?.name||id.slice(0,8);
    const av=document.getElementById('call-target-avatar'); if(av){av.innerHTML='';av.appendChild(createAvatarCanvas(p?.color||'#555',p?.emoji||'👤',46));}
    document.getElementById('call-peer-display')?.classList.add('has-peer');
    _ensureDC(id).catch(()=>{});
    if(currentMode==='chat') switchTab('chat');
  }

  /* ─────────────── RADAR PEERS ─────────────── */
  function _renderRadarPeers() {
    const layer=document.getElementById('peers-layer'); if(!layer)return;
    const peerIds=Object.keys(peers);
    const hint=document.getElementById('radar-hint');
    if(hint) hint.style.display=peerIds.length?'none':'flex';

    // Remove orbs for gone peers
    layer.querySelectorAll('[data-peer-id]').forEach(el=>{if(!peers[el.dataset.peerId])el.remove();});

    const n=peerIds.length; if(!n)return;
    peerIds.forEach((id,i)=>{
      const angle=(i/n)*Math.PI*2-Math.PI/2;
      const R=38; // % from center
      const x=50+R*Math.cos(angle);
      const y=50+R*Math.sin(angle);
      let orb=document.getElementById('orb_'+id);
      if(!orb){
        orb=_createPeerOrb(id);
        layer.appendChild(orb);
      }
      orb.style.left=x+'%'; orb.style.top=y+'%';
      const p=peers[id];
      orb.querySelector('.orb-label').textContent=p?.name?.split(' ')[0]||id.slice(0,6);
      const cv=orb.querySelector('canvas'); if(cv) drawAvatar(cv,p?.color||'#555',p?.emoji||'👤',44);
      orb.classList.toggle('selected',id===selectedPeerId);
      orb.classList.toggle('connected',dcs[id]?.readyState==='open');
    });
  }
  function _createPeerOrb(id) {
    const orb=document.createElement('div');
    orb.className='peer-orb device-orb'; orb.id='orb_'+id; orb.dataset.peerId=id;
    const cv=createAvatarCanvas(peers[id]?.color||'#555',peers[id]?.emoji||'👤',44); cv.title=peers[id]?.name||id;
    const label=document.createElement('div');label.className='orb-label';label.textContent=peers[id]?.name?.split(' ')[0]||id.slice(0,6);
    orb.appendChild(cv);orb.appendChild(label);
    orb.onclick=()=>selectPeer(id);
    return orb;
  }
  function _updatePeerOrb(id) {
    const orb=document.getElementById('orb_'+id); if(!orb)return;
    const p=peers[id]; const cv=orb.querySelector('canvas'); if(cv) drawAvatar(cv,p?.color||'#555',p?.emoji||'👤',44);
    const lb=orb.querySelector('.orb-label'); if(lb) lb.textContent=p?.name?.split(' ')[0]||id.slice(0,6);
  }

  /* ─────────────── ROOM ─────────────── */
  function joinRoom(pinOverride) {
    const v1=document.getElementById('room-pin-input')?.value.trim();
    const v2=document.getElementById('dock-pin-input')?.value.trim();
    const v3=document.getElementById('beam-pin-input')?.value.trim();
    const pin=pinOverride||(v1?.length===4?v1:null)||(v2?.length===4?v2:null)||(v3?.length===4?v3:null);
    if(!pin||!/^\d{4}$/.test(pin)){toast('Enter a valid 4-digit PIN');return;}
    if(pin===currentPin){toast('Already in room '+pin);return;}
    if(mqttClient?.connected) mqttClient.unsubscribe(roomTopic());
    peers={}; for(const id in pcs) _closePc(id);
    _clearChat();
    renderRoster(); _renderRadarPeers();
    currentPin=pin; localStorage.setItem('dhurta_pin',pin);
    _updatePinUI();
    if(mqttClient?.connected){mqttClient.subscribe(roomTopic(),{qos:0});broadcastPresence('DISCOVER_PING');}
    toast('Joined room '+pin);
    document.querySelectorAll('#room-pin-input,#dock-pin-input,#beam-pin-input').forEach(el=>el&&(el.value=''));
  }

  function _clearChat() {
    const s=document.getElementById('chat-stream');
    if(!s) return;
    s.innerHTML=`<div class="chat-empty" id="chat-welcome">
      <div class="empty-icon"><svg width="52" height="52" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg></div>
      <h3>Start a conversation</h3>
      <p>Join a room with a 4-digit PIN, or scan a QR code to connect with nearby devices.</p>
    </div>`;
    _msgDedup.clear();
  }
  function _updatePinUI() {
    const chip=document.getElementById('room-chip-pin'); if(chip) chip.textContent=currentPin;
    const qpin=document.getElementById('qr-pin-text'); if(qpin) qpin.textContent=currentPin;
  }

  /* ─────────────── UI ─────────────── */
  function _hideEmpty() { const e=document.getElementById('chat-welcome'); if(e) e.style.display='none'; }
  function _setStatus(state, text) {
    const dot=document.getElementById('status-dot'); const txt=document.getElementById('status-text');
    if(dot) dot.className='status-dot '+(state==='online'?'online':state==='error'?'error':'warn');
    if(txt) txt.textContent=text;
  }

  /* Mode switch */
  function setMode(mode) {
    currentMode=mode;
    ['transfer','beam','chat'].forEach(m=>{
      document.getElementById('panel-'+m)?.classList.toggle('hidden',mode!==m);
    });
    document.querySelectorAll('.mode-pill-btn').forEach(b=>b.classList.toggle('active',b.dataset.mode===mode));
    if(mode==='transfer'){ if(_radar)_radar.start(); _renderRadarPeers(); }
    else { if(_radar)_radar.stop(); }
    if(mode==='beam') _renderBeamPeers();
  }

  /* Beam peers bar */
  function _renderBeamPeers() {
    const bar=document.getElementById('beam-peers-bar'); if(!bar) return;
    const hint=document.getElementById('beam-peer-hint');
    const peerIds=Object.keys(peers);
    // remove old chips, keep hint
    bar.querySelectorAll('.beam-peer-chip').forEach(e=>e.remove());
    if(!peerIds.length){ if(hint) hint.style.display=''; return; }
    if(hint) hint.style.display='none';
    peerIds.forEach(id=>{
      const p=peers[id];
      const chip=document.createElement('div'); chip.className='beam-peer-chip'+(id===selectedPeerId?' selected':'');
      chip.dataset.peerId=id;
      const cv=createAvatarCanvas(p.color||'#555',p.emoji||'👤',28);
      const nm=document.createElement('span');nm.className='bpc-name';nm.textContent=p.name?.split(' ')[0]||id.slice(0,6);
      const dot=document.createElement('div');dot.className='bpc-dot';
      chip.appendChild(cv);chip.appendChild(nm);chip.appendChild(dot);
      chip.onclick=()=>{
        selectPeer(id);
        bar.querySelectorAll('.beam-peer-chip').forEach(c=>c.classList.toggle('selected',c.dataset.peerId===id));
      };
      bar.appendChild(chip);
    });
  }

  /* Beam progress */
  function _updateBeamProgress(pct, speedBs) {
    const CIRC=2*Math.PI*52;
    const arc=document.getElementById('bpr-arc');
    if(arc) arc.style.strokeDashoffset=((1-pct/100)*CIRC).toFixed(2);
    const pp=document.getElementById('bpr-pct'); if(pp) pp.textContent=pct.toFixed(0)+'%';
    const sp=document.getElementById('bpr-spd'); if(sp) sp.textContent=speedBs>0?formatBytes(speedBs)+'/s':'';
    const ring=document.getElementById('beam-progress-ring');
    const orb=document.getElementById('beam-send-orb');
    if(pct>0&&pct<100){ ring&&(ring.style.display='flex'); orb&&(orb.style.display='none'); }
    else { ring&&(ring.style.display='none'); orb&&(orb.style.display='flex'); }
  }

  /* Add file to beam grid */
  function _addToBeamGrid(blob, meta) {
    const grid=document.getElementById('beam-grid');
    document.getElementById('br-empty')?.remove();
    if(!grid) return;
    const item=document.createElement('div');item.className='beam-grid-item';
    if(meta.mimeType?.startsWith('image/')){
      const img=document.createElement('img');img.src=URL.createObjectURL(blob);item.appendChild(img);
    } else if(meta.mimeType?.startsWith('video/')){
      const vid=document.createElement('video');vid.src=URL.createObjectURL(blob);vid.style.cssText='width:100%;height:100%;object-fit:cover;';item.appendChild(vid);
    } else {
      const ic=document.createElement('div');ic.className='bgi-icon';ic.innerHTML=fileIcon(meta.mimeType);item.appendChild(ic);
    }
    const nm=document.createElement('div');nm.className='bgi-name';nm.textContent=meta.name;item.appendChild(nm);
    const dl=document.createElement('button');dl.className='bgi-dl';dl.innerHTML='<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="8 17 12 21 16 17"/><line x1="12" y1="3" x2="12" y2="21"/></svg>';
    dl.onclick=e=>{e.stopPropagation();const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download=meta.name;a.click();};
    item.appendChild(dl);
    item.onclick=()=>{ if(meta.mimeType?.startsWith('image/')) openLightbox(URL.createObjectURL(blob)); else dl.click(); };
    grid.prepend(item);
  }

  /* Chat tab switching (mobile) */
  function switchTab(tab) {
    document.querySelectorAll('.mtab').forEach(b=>b.classList.toggle('active',b.dataset.tab===tab));
    document.getElementById('chat-sidebar')?.classList.toggle('active',tab==='team');
    document.getElementById('chat-main-pane')?.classList.toggle('active',tab==='chat');
    document.getElementById('calls-panel')?.classList.toggle('active',tab==='calls');
  }

  /* Modals */
  function switchQRTab(tab) {
    _currentQRTab = tab;
    document.getElementById('qr-tab-show')?.classList.toggle('active', tab==='show');
    document.getElementById('qr-tab-scan')?.classList.toggle('active', tab==='scan');
    document.getElementById('qr-show-panel')?.classList.toggle('hidden', tab!=='show');
    document.getElementById('qr-scan-panel')?.classList.toggle('hidden', tab!=='scan');
    if (tab==='scan') _startQRScan();
    else _stopQRScan();
  }

  function openQR() {
    document.getElementById('qr-modal')?.classList.add('open');
    const url = (location.href.split('?')[0]) + '?pin=' + currentPin;
    const el = document.getElementById('qr-url-text'); if(el) el.textContent = url;
    const pin = document.getElementById('qr-pin-text'); if(pin) pin.textContent = currentPin;
    const cv = document.getElementById('qr-canvas'); if(cv) QR.render(cv, url, 7);
    // apply current tab
    switchQRTab(_currentQRTab);
  }

  function closeQR() {
    document.getElementById('qr-modal')?.classList.remove('open');
    _stopQRScan();
    _currentQRTab = 'show';
  }

  /* ── QR Camera Scanner ── */
  async function _startQRScan() {
    const status = document.getElementById('scan-status');
    if (status) status.textContent = 'Starting camera…';
    try {
      _scanStream = await navigator.mediaDevices.getUserMedia({
        video: { facingMode: { ideal: 'environment' }, width: { ideal: 640 }, height: { ideal: 640 } }
      });
      const vid = document.getElementById('scan-video');
      if (!vid) { _stopQRScan(); return; }
      vid.srcObject = _scanStream;
      await vid.play();
      if (status) status.textContent = 'Scanning…';
      _scanRAF = requestAnimationFrame(_scanFrame);
    } catch(e) {
      if (status) { status.textContent = 'Camera denied: ' + e.message; status.classList.add('found'); }
    }
  }

  function _scanFrame() {
    if (!_scanStream) return;
    const vid = document.getElementById('scan-video');
    const cv  = document.getElementById('scan-canvas');
    if (!vid || !cv || vid.readyState < vid.HAVE_ENOUGH_DATA) {
      _scanRAF = requestAnimationFrame(_scanFrame); return;
    }
    cv.width = vid.videoWidth; cv.height = vid.videoHeight;
    const ctx = cv.getContext('2d');
    ctx.drawImage(vid, 0, 0);
    const img = ctx.getImageData(0, 0, cv.width, cv.height);
    const code = (typeof jsQR !== 'undefined') && jsQR(img.data, img.width, img.height);
    if (code) {
      const raw = code.data;
      const match = raw.match(/[?&]pin=(\d{4})/);
      const pin = match ? match[1] : (/^\d{4}$/.test(raw) ? raw : null);
      if (pin) {
        const st = document.getElementById('scan-status');
        if (st) { st.textContent = '✓ Found room ' + pin + ' — joining…'; st.classList.add('found'); }
        _stopQRScan();
        setTimeout(() => {
          closeQR();
          joinRoom(pin);
          toast('Joined room ' + pin + ' via QR scan');
        }, 600);
        return;
      }
    }
    _scanRAF = requestAnimationFrame(_scanFrame);
  }

  function _stopQRScan() {
    if (_scanRAF) { cancelAnimationFrame(_scanRAF); _scanRAF = null; }
    if (_scanStream) { _scanStream.getTracks().forEach(t => t.stop()); _scanStream = null; }
    const vid = document.getElementById('scan-video'); if(vid) vid.srcObject = null;
  }

  let _selColor=myColor, _selEmoji=myEmoji;
  function openProfile() {
    document.getElementById('profile-modal')?.classList.add('open');
    const ni=document.getElementById('profile-name-input'); if(ni) ni.value=myName;
    _selColor=myColor; _selEmoji=myEmoji;
    const cp=document.getElementById('avatar-palette'); if(cp){cp.innerHTML='';COLORS.forEach(c=>{const b=document.createElement('div');b.className='av-color'+(c===_selColor?' selected':'');b.style.background=c;b.onclick=()=>{_selColor=c;cp.querySelectorAll('.av-color').forEach(x=>x.classList.remove('selected'));b.classList.add('selected');_previewAv();};cp.appendChild(b);});}
    const ep=document.getElementById('emoji-palette'); if(ep){ep.innerHTML='';EMOJIS.forEach(em=>{const b=document.createElement('div');b.className='av-emoji'+(em===_selEmoji?' selected':'');b.textContent=em;b.onclick=()=>{_selEmoji=em;ep.querySelectorAll('.av-emoji').forEach(x=>x.classList.remove('selected'));b.classList.add('selected');_previewAv();};ep.appendChild(b);});}
    _previewAv();
  }
  function _previewAv() { const cv=document.getElementById('profile-avatar-preview'); if(cv) drawAvatar(cv,_selColor,_selEmoji,80); }
  function saveProfile() {
    const nm=document.getElementById('profile-name-input')?.value.trim(); if(!nm){toast('Name required');return;}
    myName=nm;myColor=_selColor;myEmoji=_selEmoji;
    localStorage.setItem('dhurta_name',myName);localStorage.setItem('dhurta_color',myColor);localStorage.setItem('dhurta_emoji',myEmoji);
    const av=document.getElementById('my-avatar'); if(av) drawAvatar(av,myColor,myEmoji,32);
    const ov=document.getElementById('orb-avatar'); if(ov) drawAvatar(ov,myColor,myEmoji,52);
    const on=document.getElementById('orb-name'); if(on) on.textContent=myName.split(' ')[0];
    broadcastPresence('DISCOVER_PING'); renderRoster(); _renderRadarPeers();
    closeProfile(); toast('Profile updated');
  }
  function closeProfile() { document.getElementById('profile-modal')?.classList.remove('open'); }

  function openLightbox(src) { const lb=document.getElementById('lightbox');if(lb){document.getElementById('lightbox-img').src=src;lb.classList.add('open');} }
  function closeLightbox() { document.getElementById('lightbox')?.classList.remove('open'); }

  function selfRepair() {
    toast('Re-syncing…');
    clearInterval(hbTimer);clearInterval(pruneTimer);
    if(mqttClient){try{mqttClient.end(true);}catch{}mqttClient=null;}
    for(const id in pcs) _closePc(id);
    currentBrokerIdx++;
    setTimeout(connectMQTT,400);
  }

  /* ─────────────── PWA ─────────────── */
  function _registerSW() { if('serviceWorker'in navigator) navigator.serviceWorker.register('sw.js').catch(()=>{}); }

  /* ─────────────── INIT ─────────────── */
  function init() {
    // URL PIN
    const urlPin=new URLSearchParams(location.search).get('pin');
    if(urlPin&&/^\d{4}$/.test(urlPin)&&urlPin!==currentPin){currentPin=urlPin;localStorage.setItem('dhurta_pin',urlPin);toast('Joining room '+urlPin);}

    _setupAmbient();
    _registerSW();
    _reqNotifPerm();
    _setupDragDrop();

    // Draw avatars
    const myAv=document.getElementById('my-avatar'); if(myAv) drawAvatar(myAv,myColor,myEmoji,32);
    const orbAv=document.getElementById('orb-avatar'); if(orbAv) drawAvatar(orbAv,myColor,myEmoji,52);
    const orbNm=document.getElementById('orb-name'); if(orbNm) orbNm.textContent=myName.split(' ')[0];

    _updatePinUI();
    renderRoster();

    // Radar
    const rc=document.getElementById('radar-canvas');
    if(rc){_radar=new RadarAnimation(rc);_radar.start();}
    window.addEventListener('resize',()=>{if(_radar)_radar._resize();});

    // Input events
    const mi=document.getElementById('msg-input');
    if(mi){mi.addEventListener('input',_onTyping);mi.addEventListener('keydown',e=>{if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();sendMessage();}});}

    // Room PIN from URL opens correct mode
    if(urlPin) setMode('transfer');

    connectMQTT();
    _initDB();
    _initBLE();
    _initNFC();
    _initBattery();

    // Nav 3D connection animation
    _startNavCanvas();
    window.addEventListener('resize', _startNavCanvas);

    // Chat futuristic background
    _startChatBgCanvas();
  }

  /* ─────────────── NAV CANVAS — 3D DEVICE CONNECTION ─────────────── */
  function _startNavCanvas() {
    const cv = document.getElementById('nav-connection-canvas');
    if (!cv) return;
    const ctx = cv.getContext('2d');
    const nav = cv.parentElement;
    cv.width = nav.offsetWidth;
    cv.height = nav.offsetHeight;
    const W = cv.width, H = cv.height;
    const CY = H / 2;

    // Two device positions: left anchor (laptop) and right anchor (phone)
    const devL = { x: W * 0.12, y: CY };
    const devR = { x: W * 0.88, y: CY };

    // Travelling data dots
    const dots = Array.from({length: 7}, (_, i) => ({
      t: i / 7,          // 0..1 progress along path
      speed: 0.0008 + Math.random() * 0.0006,
      dir: i % 2 === 0 ? 1 : -1,  // alternating directions
      size: 2.5 + Math.random() * 2,
      color: i % 3 === 0 ? '#06b6d4' : i % 3 === 1 ? '#818cf8' : '#f97316',
    }));

    // Bezier control points (gentle S-curve in nav space)
    const cp1 = { x: W * 0.35, y: CY - H * 0.6 };
    const cp2 = { x: W * 0.65, y: CY + H * 0.6 };

    function bezier(t) {
      const u = 1 - t;
      return {
        x: u*u*u*devL.x + 3*u*u*t*cp1.x + 3*u*t*t*cp2.x + t*t*t*devR.x,
        y: u*u*u*devL.y + 3*u*u*t*cp1.y + 3*u*t*t*cp2.y + t*t*t*devR.y,
      };
    }

    let raf;
    function draw() {
      ctx.clearRect(0, 0, W, H);

      // Draw the curved path
      const grad = ctx.createLinearGradient(devL.x, CY, devR.x, CY);
      grad.addColorStop(0,   'rgba(6,182,212,.6)');
      grad.addColorStop(0.5, 'rgba(129,140,248,.4)');
      grad.addColorStop(1,   'rgba(249,115,22,.6)');
      ctx.beginPath();
      ctx.moveTo(devL.x, devL.y);
      ctx.bezierCurveTo(cp1.x, cp1.y, cp2.x, cp2.y, devR.x, devR.y);
      ctx.strokeStyle = grad;
      ctx.lineWidth = 1.2;
      ctx.setLineDash([5, 6]);
      ctx.globalAlpha = 0.7;
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.globalAlpha = 1;

      // Laptop device node (cyan)
      const lgr = ctx.createRadialGradient(devL.x, devL.y, 0, devL.x, devL.y, 8);
      lgr.addColorStop(0, 'rgba(6,182,212,1)');
      lgr.addColorStop(1, 'rgba(6,182,212,0)');
      ctx.beginPath(); ctx.arc(devL.x, devL.y, 5, 0, Math.PI*2);
      ctx.fillStyle = '#06b6d4'; ctx.fill();
      ctx.beginPath(); ctx.arc(devL.x, devL.y, 9, 0, Math.PI*2);
      ctx.fillStyle = lgr; ctx.fill();

      // Phone device node (orange)
      const rgr = ctx.createRadialGradient(devR.x, devR.y, 0, devR.x, devR.y, 8);
      rgr.addColorStop(0, 'rgba(249,115,22,1)');
      rgr.addColorStop(1, 'rgba(249,115,22,0)');
      ctx.beginPath(); ctx.arc(devR.x, devR.y, 5, 0, Math.PI*2);
      ctx.fillStyle = '#f97316'; ctx.fill();
      ctx.beginPath(); ctx.arc(devR.x, devR.y, 9, 0, Math.PI*2);
      ctx.fillStyle = rgr; ctx.fill();

      // Travelling dots along the bezier
      const now = performance.now();
      dots.forEach(d => {
        d.t += d.speed * d.dir;
        if (d.t > 1) d.t = 0;
        if (d.t < 0) d.t = 1;
        const pos = bezier(d.t);
        const gr = ctx.createRadialGradient(pos.x, pos.y, 0, pos.x, pos.y, d.size*2);
        gr.addColorStop(0, d.color);
        gr.addColorStop(1, 'transparent');
        ctx.beginPath(); ctx.arc(pos.x, pos.y, d.size, 0, Math.PI*2);
        ctx.fillStyle = gr; ctx.fill();
        // Trail
        const prev = bezier(Math.max(0, d.t - d.dir * 0.04));
        ctx.beginPath();
        ctx.moveTo(pos.x, pos.y); ctx.lineTo(prev.x, prev.y);
        ctx.strokeStyle = d.color; ctx.globalAlpha = 0.3;
        ctx.lineWidth = 1.5; ctx.stroke(); ctx.globalAlpha = 1;
      });

      raf = requestAnimationFrame(draw);
    }

    if (cv._navRaf) cancelAnimationFrame(cv._navRaf);
    draw();
    cv._navRaf = raf;
  }

  /* ─────────────── CHAT BG CANVAS — FUTURISTIC NEURAL GRID ─────────────── */
  function _startChatBgCanvas() {
    const cv = document.getElementById('chat-bg-canvas');
    if (!cv) return;

    const ctx = cv.getContext('2d');
    let W, H, nodes, raf;

    function resize() {
      const parent = cv.parentElement;
      W = cv.width  = parent.offsetWidth;
      H = cv.height = parent.offsetHeight;
      _buildNodes();
    }

    function _buildNodes() {
      const count = Math.min(60, Math.floor(W * H / 14000));
      nodes = Array.from({length: count}, () => ({
        x: Math.random() * W,
        y: Math.random() * H,
        vx: (Math.random() - .5) * 0.25,
        vy: (Math.random() - .5) * 0.25,
        r:  1.5 + Math.random() * 2,
        hue: Math.random() > .5 ? 190 : 250,  // cyan or indigo
        pulse: Math.random() * Math.PI * 2,
      }));
    }

    const MAX_DIST = 140;

    function draw() {
      ctx.clearRect(0, 0, W, H);

      // Move nodes
      nodes.forEach(n => {
        n.x += n.vx; n.y += n.vy;
        n.pulse += 0.025;
        if (n.x < 0 || n.x > W) n.vx *= -1;
        if (n.y < 0 || n.y > H) n.vy *= -1;
      });

      // Draw connections
      for (let i = 0; i < nodes.length; i++) {
        for (let j = i + 1; j < nodes.length; j++) {
          const a = nodes[i], b = nodes[j];
          const dx = a.x - b.x, dy = a.y - b.y;
          const dist = Math.sqrt(dx*dx + dy*dy);
          if (dist > MAX_DIST) continue;
          const alpha = (1 - dist / MAX_DIST) * 0.55;
          const hue = (a.hue + b.hue) / 2;
          ctx.beginPath();
          ctx.moveTo(a.x, a.y); ctx.lineTo(b.x, b.y);
          ctx.strokeStyle = `hsla(${hue},90%,65%,${alpha})`;
          ctx.lineWidth = 0.6;
          ctx.stroke();
        }
      }

      // Draw nodes
      nodes.forEach(n => {
        const glow = 0.7 + 0.3 * Math.sin(n.pulse);
        const gr = ctx.createRadialGradient(n.x, n.y, 0, n.x, n.y, n.r * 3);
        gr.addColorStop(0, `hsla(${n.hue},90%,70%,${glow})`);
        gr.addColorStop(1, `hsla(${n.hue},90%,70%,0)`);
        ctx.beginPath(); ctx.arc(n.x, n.y, n.r * 3, 0, Math.PI*2);
        ctx.fillStyle = gr; ctx.fill();
        ctx.beginPath(); ctx.arc(n.x, n.y, n.r, 0, Math.PI*2);
        ctx.fillStyle = `hsl(${n.hue},90%,75%)`; ctx.fill();
      });

      // Hex grid overlay (very subtle)
      ctx.strokeStyle = 'rgba(129,140,248,.04)';
      ctx.lineWidth = 0.5;
      const gs = 56;
      for (let x = 0; x < W + gs; x += gs) {
        ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, H); ctx.stroke();
      }
      for (let y = 0; y < H + gs; y += gs) {
        ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(W, y); ctx.stroke();
      }

      raf = requestAnimationFrame(draw);
    }

    if (cv._bgRaf) cancelAnimationFrame(cv._bgRaf);
    resize();
    draw();
    cv._bgRaf = raf;
    window.addEventListener('resize', resize);
  }

  document.addEventListener('DOMContentLoaded', init);

  /* ─────────────── NOTIFICATIONS ─────────────── */
  async function _reqNotifPerm() {
    if ('Notification' in window && Notification.permission === 'default') {
      await Notification.requestPermission();
    }
  }
  function _showChatNotification(from, name, text) {
    if (document.visibilityState === 'visible') return; // don't spam if looking at app
    if (Notification.permission !== 'granted') return;
    const n = new Notification('💬 ' + name + ' @ Dhurta Sync', {
      body: text.slice(0, 120),
      icon: 'sync.png',
      badge: 'sync.png',
      tag: 'chat_' + from,
      renotify: true,
    });
    n.onclick = () => { window.focus(); app.setMode('chat'); n.close(); };
  }
  function _showCallNotification(name, callType) {
    if (Notification.permission !== 'granted') return;
    const label = {video:'📹 Video Call',voice:'📞 Voice Call',screen:'🖥️ Screen Share'}[callType]||'Call';
    const n = new Notification('📲 Incoming call — ' + name, {
      body: label + ' · Tap to answer',
      icon: 'sync.png',
      badge: 'sync.png',
      tag: 'call_incoming',
      requireInteraction: true,
      silent: false,
    });
    n.onclick = () => { window.focus(); n.close(); };
  }

  /* ntfy.sh push subscription — so peers can wake locked screen */
  function _subscribeNtfy() {
    if (!('serviceWorker' in navigator)) return;
    // Register this device's ntfy topic so peers know how to reach us
    localStorage.setItem('dhurta_ntfy', _ntfyTopic);
  }
  function _pushToNtfy(peerId, title, body) {
    const topic = peers[peerId]?.ntfy;
    if (!topic) return;
    fetch(NTFY_BASE + '/' + topic, {
      method: 'POST',
      headers: { 'Title': title, 'Tags': 'dhurta,sync', 'Priority': '4', 'Actions': 'view, Open App, https://sync.dhurta.com' },
      body,
    }).catch(()=>{});
  }

  /* ─────────────── TRANSFER HISTORY (IndexedDB) ─────────────── */
  function _initDB() {
    const req = indexedDB.open('dhurta_history', 1);
    req.onupgradeneeded = e => {
      const db = e.target.result;
      if (!db.objectStoreNames.contains('transfers')) {
        const s = db.createObjectStore('transfers', {keyPath:'id', autoIncrement:true});
        s.createIndex('ts','ts'); s.createIndex('name','name');
      }
    };
    req.onsuccess = e => { _historyDB = e.target.result; };
  }
  function _saveTransferHistory(tId, name, size, direction, speedBs) {
    if (!_historyDB) return;
    try {
      const tx = _historyDB.transaction('transfers','readwrite');
      tx.objectStore('transfers').put({ id: tId, name, size, direction, speedBs, ts: Date.now() });
    } catch {}
  }
  function openHistory() {
    if (!_historyDB) { toast('History not available'); return; }
    const tx = _historyDB.transaction('transfers','readonly');
    const req = tx.objectStore('transfers').index('ts').getAll();
    req.onsuccess = () => {
      const rows = (req.result||[]).reverse().slice(0,50);
      _renderHistoryModal(rows);
    };
  }
  function _renderHistoryModal(rows) {
    let el = document.getElementById('history-modal');
    if (!el) {
      el = document.createElement('div');
      el.className = 'modal-back'; el.id = 'history-modal';
      el.innerHTML = `<div class="modal-sheet">
        <div class="sheet-head"><h3>Transfer History</h3><button class="sheet-x" onclick="document.getElementById('history-modal').classList.add('hidden')">✕</button></div>
        <div class="sheet-body" id="history-list"></div></div>`;
      document.body.appendChild(el);
      el.addEventListener('click', ev => { if(ev.target===el) el.classList.add('hidden'); });
    }
    el.classList.remove('hidden');
    const list = document.getElementById('history-list');
    if (!rows.length) { list.innerHTML = '<p style="color:var(--t3);padding:20px;text-align:center">No transfers yet</p>'; return; }
    list.innerHTML = rows.map(r => {
      const mb = (r.size/1048576).toFixed(1);
      const spd = r.speedBs ? ((r.speedBs/1048576).toFixed(1)+' MB/s') : '';
      const dt = new Date(r.ts).toLocaleString();
      const icon = r.direction==='sending'?'↑':'↓';
      return `<div class="history-item"><span class="hi-dir">${icon}</span><div class="hi-info"><div class="hi-name">${r.name}</div><div class="hi-meta">${mb} MB · ${spd} · ${dt}</div></div></div>`;
    }).join('');
  }

  /* ─────────────── BLE NEARBY DISCOVERY ─────────────── */
  function _initBLE() {
    if (!navigator.bluetooth) return;
    // Auto-scan on load (passive — just update state if already requested)
    document.getElementById('ble-scan-btn')?.addEventListener('click', scanBLE);
  }
  async function scanBLE() {
    if (!navigator.bluetooth) { toast('Bluetooth not supported in this browser'); return; }
    toast('🔵 Scanning for nearby devices…');
    try {
      // requestDevice shows system picker — user selects devices they want to add
      const device = await navigator.bluetooth.requestDevice({
        acceptAllDevices: true,
        optionalServices: [],
      });
      const id = device.id || device.name || Math.random().toString(36).slice(2);
      _bleDevices[id] = { name: device.name||'Unknown Device', isDhurta: false };
      toast('📡 Found: ' + (device.name||'Unknown Device'));
      _renderBLEList();
    } catch (e) {
      if (e.name !== 'NotFoundError') toast('BLE: ' + e.message);
    }
  }
  function _renderBLEList() {
    const list = document.getElementById('ble-device-list');
    if (!list) return;
    const items = Object.entries(_bleDevices);
    if (!items.length) { list.innerHTML = '<p class="ble-empty">No BLE devices found yet</p>'; return; }
    list.innerHTML = items.map(([id, d]) =>
      `<div class="ble-item">
        <span class="ble-icon">${d.isDhurta?'⚡':'📱'}</span>
        <span class="ble-name">${d.name}</span>
        ${d.isDhurta ? '<span class="ble-badge">Sync</span>' : `<button class="ble-invite" onclick="app._sendBLEInvite('${id}')">Invite</button>`}
      </div>`
    ).join('');
  }
  function _sendBLEInvite(id) {
    // Can't push BLE from browser — copy invite link instead
    const url = `https://sync.dhurta.com/?pin=${currentPin}`;
    if (navigator.share) {
      navigator.share({ title:'Join me on Dhurta Sync', url }).catch(()=>{});
    } else {
      navigator.clipboard?.writeText(url);
      toast('📋 Room link copied — share it with the other device');
    }
  }

  /* ─────────────── WEB NFC PAIRING ─────────────── */
  function _initNFC() {
    if (!('NDEFReader' in window)) return;
    document.getElementById('nfc-pair-btn')?.classList.remove('hidden');
  }
  async function nfcPair() {
    if (!('NDEFReader' in window)) { toast('NFC not supported on this device'); return; }
    try {
      const ndef = new NDEFReader();
      toast('📡 Hold devices together to pair via NFC…');
      // Write our room URL to NFC tag
      await ndef.write({ records: [{ recordType:'url', data:`https://sync.dhurta.com/?pin=${currentPin}` }] });
      toast('✅ NFC written — tap other device to pair');
      // Also read incoming NFC
      await ndef.scan();
      ndef.onreading = e => {
        for (const r of e.message.records) {
          if (r.recordType === 'url') {
            const url = new TextDecoder().decode(r.data);
            const pin = url.match(/pin=(\d{4})/)?.[1];
            if (pin) { joinRoom(pin); toast('📡 NFC paired to room '+pin); }
          }
        }
      };
    } catch (e) { toast('NFC: ' + e.message); }
  }

  /* ─────────────── CLIPBOARD SYNC ─────────────── */
  function syncClipboard() {
    navigator.clipboard?.readText().then(text => {
      if (!text) { toast('Clipboard is empty'); return; }
      publish({type:'CHAT', text:'📋 Clipboard: ' + text, name:myName, color:myColor, emoji:myEmoji, ts:Date.now()});
      toast('📋 Clipboard shared with peers');
      for(const id in dcs) if(dcs[id].readyState==='open') dcs[id].send(JSON.stringify({type:'CLIPBOARD_DATA',content:text,name:myName}));
    }).catch(() => toast('Clipboard read permission denied'));
  }

  /* ─────────────── SCREENSHOT SHARE ─────────────── */
  async function shareScreenshot() {
    try {
      const stream = await navigator.mediaDevices.getDisplayMedia({video:{cursor:'always'},audio:false});
      const track = stream.getVideoTracks()[0];
      const cap = new ImageCapture(track);
      const bmp = await cap.grabFrame();
      track.stop();
      const cv = document.createElement('canvas');
      cv.width=bmp.width; cv.height=bmp.height;
      cv.getContext('2d').drawImage(bmp,0,0);
      cv.toBlob(blob => {
        if (!blob) return;
        const file = new File([blob],'screenshot.png',{type:'image/png'});
        sendFiles([file]);
      },'image/png');
    } catch(e) { toast('Screenshot: '+e.message); }
  }

  /* ─────────────── NETWORK QUALITY ─────────────── */
  function _updateNetworkQuality() {
    const conn = navigator.connection||navigator.mozConnection||navigator.webkitConnection;
    if (!conn) return;
    const el = document.getElementById('net-quality');
    if (!el) return;
    const mbps = conn.downlink||0;
    const type = conn.effectiveType||'';
    let label='', color='';
    if (mbps > 10 || type==='4g') { label='🟢 Fast'; color='#22c55e'; }
    else if (mbps > 1 || type==='3g') { label='🟡 OK'; color='#eab308'; }
    else { label='🔴 Slow'; color='#ef4444'; }
    el.textContent = label + (mbps?' · '+mbps+'Mbps':'');
    el.style.color = color;
  }

  /* ─────────────── BATTERY SAVER ─────────────── */
  function _initBattery() {
    if (!navigator.getBattery) return;
    navigator.getBattery().then(b => {
      function check() {
        const low = b.level < 0.2 && !b.charging;
        if (low !== _batteryMode) {
          _batteryMode = low;
          document.body.classList.toggle('battery-saver', _batteryMode);
          if (_batteryMode) toast('🔋 Battery saver ON — animations paused');
        }
      }
      b.addEventListener('levelchange', check);
      b.addEventListener('chargingchange', check);
      check();
    });
  }

  /* ─────────────── THEME TOGGLE ─────────────── */
  function toggleTheme() {
    const cur = document.documentElement.getAttribute('data-theme');
    const next = cur === 'light' ? 'dark' : 'light';
    document.documentElement.setAttribute('data-theme', next);
    localStorage.setItem('dhurta_theme', next);
    toast(next === 'light' ? '☀️ Light mode' : '🌙 Dark mode');
  }

  /* ─────────────── CUSTOM ROOM NAME ─────────────── */
  function setCustomRoom(name) {
    const pin = name.trim().slice(0,20);
    currentPin = pin;
    localStorage.setItem('dhurta_pin', pin);
    _updatePinUI();
    if(mqttClient?.connected){mqttClient.subscribe(roomTopic(),{qos:0}); broadcastPresence('DISCOVER_PING');}
    if(_bc){try{_bc.close();}catch{} _bc=new BroadcastChannel('dhurta_v10_'+pin);}
    toast('Room: '+pin);
  }

  /* ─────────────── AI MODULE ─────────────── */
  const AI = (() => {
    let _session = null, _ready = false, _checking = false;

    async function _init() {
      if (_ready || _checking) return _ready;
      _checking = true;
      try {
        // Chrome built-in AI (Prompt API / Gemini Nano)
        if (window.ai?.languageModel) {
          const cap = await window.ai.languageModel.capabilities();
          if (cap.available !== 'no') {
            _session = await window.ai.languageModel.create({
              systemPrompt: 'You are a helpful assistant built into Dhurta Sync, a peer-to-peer file sharing app. Be concise. Help users with file transfers, device pairing, and general questions. Keep replies under 3 sentences unless more detail is clearly needed.',
            });
            _ready = true;
          }
        }
      } catch {}
      _checking = false;
      return _ready;
    }

    async function ask(prompt) {
      if (!await _init()) return null;
      try { return await _session.prompt(prompt); } catch { return null; }
    }

    function analyzeFile(file) {
      const ext = file.name.split('.').pop().toLowerCase();
      const size = file.size;
      const mb = (size / 1048576).toFixed(1);
      const eta = size / (10 * 1024 * 1024); // ~10 MB/s estimate
      const etaStr = eta < 1 ? `${(eta*1000).toFixed(0)} ms` : `${eta.toFixed(1)} s`;
      const categories = {
        image: ['jpg','jpeg','png','gif','webp','svg','avif','heic'],
        video: ['mp4','mkv','mov','avi','webm','m4v'],
        audio: ['mp3','aac','flac','wav','ogg','m4a'],
        doc:   ['pdf','docx','doc','pptx','xlsx','txt','md','csv'],
        code:  ['js','ts','py','rs','go','java','cpp','c','html','css','json'],
        archive:['zip','rar','7z','tar','gz'],
      };
      let cat = 'file';
      for (const [k, exts] of Object.entries(categories)) {
        if (exts.includes(ext)) { cat = k; break; }
      }
      const icons = {image:'🖼️',video:'🎬',audio:'🎵',doc:'📄',code:'💻',archive:'📦',file:'📁'};
      return { mb, etaStr, cat, icon: icons[cat] || '📁', ext };
    }

    function smartToast(file) {
      const { icon, mb, etaStr, cat } = analyzeFile(file);
      toast(`${icon} Sending ${cat} · ${mb} MB · est. ${etaStr}`);
    }

    async function chatSuggest(userText) {
      if (!userText.trim()) return;
      const reply = await ask(userText);
      if (!reply) return;
      _appendMessage({ from: '__ai__', text: reply, mqttName: '✨ Dhurta AI', mqttColor: '#818cf8' });
    }

    async function getTransferTip(file) {
      const { mb, cat } = analyzeFile(file);
      const reply = await ask(`I'm sending a ${mb} MB ${cat} file called "${file.name}" via P2P WebRTC. Give me one short tip to make this transfer smoother.`);
      if (reply) toast('💡 ' + reply.slice(0, 120));
    }

    return { ask, analyzeFile, smartToast, chatSuggest, getTransferTip, get ready() { return _ready; } };
  })();

  /* AI chat input hook — prefix message with "/" to trigger AI */
  const _origSendMessage = sendMessage;
  function sendMessageAI() {
    const input = document.getElementById('msg-input');
    const txt = input?.value?.trim() || '';
    if (txt.startsWith('/ai ') || txt.startsWith('/ask ')) {
      const q = txt.replace(/^\/(ai|ask)\s+/, '');
      input.value = '';
      AI.chatSuggest(q);
    } else {
      _origSendMessage();
    }
  }

  /* ─────────────── PUBLIC API ─────────────── */
  return {
    sendMessage: sendMessageAI, sendFiles, syncClipboard,
    setMode, switchTab, joinRoom, selectPeer,
    openQR, closeQR, switchQRTab,
    openProfile, closeProfile, saveProfile,
    startCall, acceptCall, rejectCall, endCall,
    minimizeCall, toggleCallBg,
    toggleMute, toggleCamera, toggleScreen,
    selfRepair, openLightbox, closeLightbox,
    scanBLE, _sendBLEInvite,
    nfcPair, shareScreenshot, openHistory,
    toggleTheme, setCustomRoom,
    AI,
    _toast: toast,
  };

})();
