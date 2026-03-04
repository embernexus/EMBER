import { useState, useEffect, useRef } from "react";

/* ─── STYLES ─────────────────────────────────────────────────────────────── */
const CSS = `
  @import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=JetBrains+Mono:wght@400;600&display=swap');
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  html, body { background: #05020a; color: #fff; font-family: 'Syne', sans-serif; overflow-x: hidden; }
  ::-webkit-scrollbar { width: 3px; height: 3px; }
  ::-webkit-scrollbar-thumb { background: rgba(255,106,0,0.3); border-radius: 2px; }
  ::-webkit-scrollbar-track { background: transparent; }
  input, select, button, textarea { font-family: 'Syne', sans-serif; }

  @keyframes pulse-dot { 0%,100%{box-shadow:0 0 0 0 rgba(255,106,0,.7)} 50%{box-shadow:0 0 0 6px rgba(255,106,0,0)} }
  @keyframes slideUp { from{opacity:0;transform:translateY(16px)} to{opacity:1;transform:none} }
  @keyframes fadeIn { from{opacity:0} to{opacity:1} }
  @keyframes spin { to{transform:rotate(360deg)} }
  @keyframes flicker { 0%,100%{opacity:1} 93%{opacity:.4} 94%{opacity:1} 96%{opacity:.7} 97%{opacity:1} }
  @keyframes gradShift { 0%,100%{background-position:0% 50%} 50%{background-position:100% 50%} }
  @keyframes ringExpand { 0%{transform:translate(-50%,-50%) scale(.8);opacity:.7} 100%{transform:translate(-50%,-50%) scale(2.6);opacity:0} }
  @keyframes floatUp { 0%{transform:translateY(0) scale(.8);opacity:0} 10%{opacity:1} 90%{opacity:.6} 100%{transform:translateY(-18vh) scale(1.3);opacity:0} }
  @keyframes textGlow { 0%,100%{text-shadow:0 0 20px rgba(255,106,0,.3)} 50%{text-shadow:0 0 40px rgba(255,106,0,.7),0 0 80px rgba(255,69,0,.2)} }
  @keyframes borderPulse { 0%,100%{border-color:rgba(255,106,0,.15)} 50%{border-color:rgba(255,106,0,.5);box-shadow:0 0 20px rgba(255,106,0,.1)} }

  .btn-fire {
    background: linear-gradient(135deg,#ff6a00,#ee2200);
    border:none; color:#fff; cursor:pointer; border-radius:10px;
    font-weight:700; letter-spacing:.3px; position:relative; overflow:hidden;
    transition:transform .15s, box-shadow .15s;
    box-shadow:0 4px 20px rgba(255,106,0,.3);
  }
  .btn-fire::after { content:''; position:absolute; inset:0; background:linear-gradient(135deg,rgba(255,255,255,.12),transparent); pointer-events:none; }
  .btn-fire:hover:not(:disabled) { transform:translateY(-1px); box-shadow:0 8px 28px rgba(255,106,0,.45); }
  .btn-fire:active:not(:disabled) { transform:none; }
  .btn-fire:disabled { opacity:.5; cursor:not-allowed; }

  .btn-ghost {
    background:rgba(255,255,255,.04); border:1px solid rgba(255,255,255,.08);
    color:rgba(255,255,255,.6); cursor:pointer; border-radius:8px; font-weight:600;
    transition:all .15s;
  }
  .btn-ghost:hover { background:rgba(255,106,0,.1); border-color:rgba(255,106,0,.3); color:#ff8c42; }

  .glass { background:rgba(255,255,255,.03); border:1px solid rgba(255,255,255,.07); border-radius:16px; position:relative; overflow:hidden; }
  .glass::before { content:''; position:absolute; inset:0; background:linear-gradient(135deg,rgba(255,255,255,.04) 0%,transparent 60%); pointer-events:none; }

  .input-f {
    width:100%; background:rgba(255,255,255,.05); border:1px solid rgba(255,255,255,.1);
    border-radius:10px; color:#fff; padding:11px 14px; font-size:14px; outline:none;
    transition:border-color .2s, box-shadow .2s;
  }
  .input-f:focus { border-color:rgba(255,106,0,.5); box-shadow:0 0 0 3px rgba(255,106,0,.1); }
  .input-f::placeholder { color:rgba(255,255,255,.2); }
  .input-f:disabled { opacity:.5; cursor:not-allowed; }

  .split-btn {
    width:30px; height:30px; border-radius:6px; font-size:12px; font-weight:700;
    cursor:pointer; transition:all .15s; border:1px solid rgba(255,255,255,.08);
    background:rgba(255,255,255,.03); color:rgba(255,255,255,.35);
  }
  .split-btn.on { background:rgba(255,106,0,.2); border-color:rgba(255,106,0,.5); color:#ff8c42; box-shadow:0 0 8px rgba(255,106,0,.2); }
  .split-btn:hover:not(.on) { background:rgba(255,255,255,.08); color:rgba(255,255,255,.7); }

  .tx-link { color:inherit; text-decoration:none; opacity:.6; transition:opacity .15s; font-family:'JetBrains Mono',monospace; }
  .tx-link:hover { opacity:1; text-decoration:underline; color:#ff8c42; }

  .tag-on { background:rgba(255,106,0,.12); border:1px solid rgba(255,106,0,.28); color:#ff8c42; padding:2px 10px; border-radius:20px; font-size:11px; font-weight:700; display:flex; align-items:center; gap:5px; }
  .tag-off { background:rgba(255,255,255,.05); border:1px solid rgba(255,255,255,.1); color:rgba(255,255,255,.3); padding:2px 10px; border-radius:20px; font-size:11px; font-weight:600; }

  .nav-blur { backdrop-filter:blur(20px) saturate(180%); -webkit-backdrop-filter:blur(20px); background:rgba(5,2,10,.75); border-bottom:1px solid rgba(255,255,255,.06); }

  .mono { font-family:'JetBrains Mono',monospace; }

  .log-scroll { max-height:220px; overflow-y:auto; }
  .log-row:not(:last-child) { border-bottom:1px solid rgba(255,255,255,.04); }
  .feed-row:not(:last-child) { border-bottom:1px solid rgba(255,255,255,.04); }

  @keyframes ticker { 0%{transform:translateX(0)} 100%{transform:translateX(-50%)} }
  .ticker-track { display:flex; animation:ticker 28s linear infinite; width:max-content; }
  .ticker-track:hover { animation-play-state:paused; }
  .ticker-wrap { overflow:hidden; width:100%; }
`;

/* ─── FIRE CANVAS ────────────────────────────────────────────────────────── */
function FireBg() {
  const ref = useRef(null);
  useEffect(() => {
    const canvas = ref.current; if (!canvas) return;
    const ctx = canvas.getContext("2d");
    let raf;
    const resize = () => { canvas.width = window.innerWidth; canvas.height = window.innerHeight; };
    resize(); window.addEventListener("resize", resize);
    const COLS = [[255,255,180],[255,155,20],[255,75,0],[200,25,0],[100,8,0]];
    let pts = Array.from({length:32}, () => {
      const p = { x:Math.random()*window.innerWidth, y:window.innerHeight+10, vx:(Math.random()-.5)*.5, vy:-(Math.random()*1.6+.5), life:Math.random(), sz:Math.random()*90+25, w:Math.random()*Math.PI*2 };
      p.y = window.innerHeight - Math.random()*window.innerHeight*.35;
      return p;
    });
    const spawn = () => ({ x:Math.random()*canvas.width, y:canvas.height+10, vx:(Math.random()-.5)*.5, vy:-(Math.random()*1.6+.5), life:1, sz:Math.random()*90+25, w:Math.random()*Math.PI*2 });
    const draw = () => {
      ctx.clearRect(0,0,canvas.width,canvas.height);
      const t = Date.now()*.001;
      pts.forEach((p,i) => {
        p.x += p.vx + Math.sin(t*1.1+p.w+i*.25)*.45;
        p.y += p.vy; p.life -= .0035; p.sz *= .998;
        if (p.life <= 0 || p.y < -p.sz) { pts[i] = spawn(); return; }
        const ci = Math.min(Math.floor((1-p.life)*COLS.length), COLS.length-1);
        const [r,g,b] = COLS[ci];
        const g2 = ctx.createRadialGradient(p.x,p.y,0,p.x,p.y,p.sz);
        g2.addColorStop(0,`rgba(${r},${g},${b},${p.life*.16})`);
        g2.addColorStop(.5,`rgba(${r},${Math.floor(g*.5)},0,${p.life*.08})`);
        g2.addColorStop(1,"rgba(0,0,0,0)");
        ctx.beginPath(); ctx.arc(p.x,p.y,p.sz,0,Math.PI*2); ctx.fillStyle=g2; ctx.fill();
      });
      raf = requestAnimationFrame(draw);
    };
    draw();
    return () => { cancelAnimationFrame(raf); window.removeEventListener("resize",resize); };
  },[]);
  return <canvas ref={ref} style={{position:"fixed",inset:0,width:"100%",height:"100%",pointerEvents:"none",zIndex:0,opacity:.85}} />;
}

/* ─── FLOATING EMBERS ────────────────────────────────────────────────────── */
function Embers() {
  const items = useRef(Array.from({length:16},(_,i)=>({ id:i, left:`${5+Math.random()*90}%`, delay:`${Math.random()*14}s`, dur:`${7+Math.random()*9}s`, sz:Math.random()>.5?3:2, col:Math.random()>.5?"#ff8c42":"#ff4500" }))).current;
  return (
    <div style={{position:"fixed",inset:0,pointerEvents:"none",zIndex:1,overflow:"hidden"}}>
      {items.map(e=>(
        <div key={e.id} style={{position:"absolute",bottom:0,left:e.left,width:e.sz,height:e.sz,borderRadius:"50%",background:e.col,boxShadow:`0 0 ${e.sz*3}px ${e.col}`,animation:`floatUp ${e.dur} ${e.delay} infinite ease-out`,opacity:0}} />
      ))}
    </div>
  );
}

/* ─── UTILS ──────────────────────────────────────────────────────────────── */
const fmt = n => n>=1e6?`${(n/1e6).toFixed(2)}M`:n>=1e3?`${(n/1e3).toFixed(1)}K`:String(n??0);
const fmtFull = n => (n??0).toLocaleString();
const fmtSec = s => s<60?`${s}s`:s<3600?`${Math.floor(s/60)}m ${s%60}s`:`${Math.floor(s/3600)}h ${Math.floor((s%3600)/60)}m`;
const fmtAge = s => s===0?"just now":s<60?`${s}s ago`:`${Math.floor(s/60)}m ago`;
const solscanTx   = sig  => `https://solscan.io/tx/${sig}`;
const solscanAddr = addr => `https://solscan.io/account/${addr}`;

const EVT_META = {
  burn:   {icon:"🔥",color:"#ff6a00",label:"BURN"},
  claim:  {icon:"⚡",color:"#ffd700",label:"CLAIM"},
  split:  {icon:"🔀",color:"#ff8c42",label:"SPLIT"},
  error:  {icon:"⚠", color:"#ff4060",label:"ERROR"},
  buyback:{icon:"🔄",color:"#ff9500",label:"BUY"},
};

/* ─── INITIAL DATA ───────────────────────────────────────────────────────── */
const mkLog = (tokenId, type, msg, tx) => ({id:Math.random(), tokenId, type, msg, tx: tx||null, age:Math.floor(Math.random()*600)+10});

const BASE_LOGS = [
  mkLog("1","burn","Burned 48,200 BONK — sent to the incinerator","5xK2mNpQrst7AbCdEfGh"),
  mkLog("1","claim","Claimed 48,200 BONK creator rewards","9mP1KqZrWxYvUt3SrQ2P"),
  mkLog("1","split","Split buy 3/3 complete — 16,100 BONK","3nQ7VwXyZaBcDeFgHiJk"),
  mkLog("1","split","Split buy 2/3 complete — 16,050 BONK","7rJ4NmLkPqRsTuVwXyZa"),
  mkLog("1","split","Split buy 1/3 initiated — 16,050 BONK","2wL9BcDeFgHiJkLmNoPq"),
  mkLog("1","burn","Burned 32,800 BONK — incinerated","8nM1RsTuVwXyZaBcDeF"),
  mkLog("2","burn","Burned 21,000 WIF — sent to incinerator","7rJ4AbCdEfGhIjKlMnOp"),
  mkLog("2","claim","Claimed 5,800 WIF creator rewards","4pR6QrStUvWxYzAbCdEf"),
  mkLog("2","split","Split buy 5/5 — 1,160 WIF","1aB2CdEfGhIjKlMnOpQr"),
  mkLog("2","split","Split buy 4/5 — 1,160 WIF","6cD3EfGhIjKlMnOpQrSt"),
  mkLog("3","error","Insufficient balance — job paused",null),
  mkLog("3","burn","Burned 8,900 MYRO — incinerated","5eF4GhIjKlMnOpQrStUv"),
  mkLog("3","claim","Claimed 8,900 MYRO creator rewards","9gH5IjKlMnOpQrStUvWx"),
];

const TOKENS_INIT = [
  {id:"1",symbol:"BONK",name:"Bonk",mint:"DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",deposit:"EMBRBonkDep4xK2mNpQrst7uVwXyz1A2B3C",claimSec:120,burnSec:300,splits:3,active:true,burned:4820000,pending:12400,txCount:342},
  {id:"2",symbol:"WIF", name:"dogwifhat",mint:"EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm",deposit:"EMBRWifDep9rJ4nLmKqPwYzAb2Cd3E4F5G",claimSec:60,burnSec:180,splits:5,active:true,burned:2100000,pending:5800,txCount:218},
  {id:"3",symbol:"MYRO",name:"Myro",mint:"HhJpBhRRn4g85VZfzpmAn1CzCGCp4MQzR7Sn5dLNbwN",deposit:"EMBRMyroDep7hG3fEcBaDxWvUtSrQ2P3Q4R",claimSec:300,burnSec:600,splits:1,active:false,burned:890000,pending:0,txCount:87},
];

const FEED_INIT = [
  {id:1,type:"burn",  token:"BONK",msg:"Buyback + burn complete — 48,200 BONK eliminated",tx:"5xK2mNpQrst7AbCdEfGh",age:0},
  {id:2,type:"claim", token:"WIF", msg:"Creator rewards claimed — 5,800 WIF collected",tx:"9mP1KqZrWxYvUt3SrQ2P",age:34},
  {id:3,type:"split", token:"BONK",msg:"Split buy 2/3 executed — 16,100 BONK acquired",tx:"3nQ7VwXyZaBcDeFgHiJk",age:78},
  {id:4,type:"burn",  token:"WIF", msg:"21,000 WIF sent to the incinerator — permanently gone",tx:"7rJ4AbCdEfGhIjKlMnOp",age:142},
  {id:5,type:"error", token:"MYRO",msg:"Insufficient balance — job paused automatically",tx:null,age:310},
  {id:6,type:"claim", token:"BONK",msg:"Claimed 48,200 BONK creator rewards",tx:"2wL9BcDeFgHiJkLmNoPq",age:380},
  {id:7,type:"burn",  token:"BONK",msg:"Buyback cycle initiated — 3-split strategy",tx:"8nM1RsTuVwXyZaBcDeF",age:520},
];

const CHART_DATA = [
  {d:"M",v:310000},{d:"T",v:480000},{d:"W",v:290000},
  {d:"T",v:620000},{d:"F",v:750000},{d:"S",v:530000},{d:"S",v:890000},
];

/* ─── KNOWN MINTS (mock lookup) ──────────────────────────────────────────── */
const KNOWN_MINTS = {
  "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263": {symbol:"BONK",name:"Bonk"},
  "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm": {symbol:"WIF", name:"dogwifhat"},
  "HhJpBhRRn4g85VZfzpmAn1CzCGCp4MQzR7Sn5dLNbwN": {symbol:"MYRO",name:"Myro"},
  "MEW1gQWJ3nEXg2qgERiKu7FAFj79PHvQVREQUzScPP5": {symbol:"MEW", name:"cat in a dogs world"},
  "A3eME5CetyZPBoWbRUwY3tSe25S6tb18ba9ZPbWk9eFJ": {symbol:"PENG",name:"Peng"},
};

/* ─── QR MOCK ────────────────────────────────────────────────────────────── */
function QR({seed=""}) {
  const sz=72,cells=9,cs=sz/cells;
  const h=(seed||"x").split("").reduce((a,c,i)=>(a+c.charCodeAt(0)*(i+1))|0,0);
  const grid=Array.from({length:cells},(_,r)=>Array.from({length:cells},(_,c)=>((h+r*11+c*17+r*c)*31%5)<2));
  return (
    <div style={{background:"#fff",padding:5,borderRadius:8,display:"inline-flex",flexShrink:0}}>
      <svg width={sz} height={sz}>
        {grid.map((row,r)=>row.map((on,c)=>on?<rect key={`${r}-${c}`} x={c*cs} y={r*cs} width={cs-.5} height={cs-.5} rx={.5} fill="#0a0205"/>:null))}
      </svg>
    </div>
  );
}

/* ─── COUNTDOWN RING ─────────────────────────────────────────────────────── */
function Ring({total}) {
  const [rem,setRem]=useState(total);
  useEffect(()=>{setRem(total);const id=setInterval(()=>setRem(r=>r>0?r-1:total),1000);return()=>clearInterval(id);},[total]);
  const r=17,circ=2*Math.PI*r,prog=((total-rem)/total)*circ;
  return (
    <div style={{position:"relative",width:46,height:46,flexShrink:0}}>
      <svg width={46} height={46} style={{transform:"rotate(-90deg)"}}>
        <circle cx={23} cy={23} r={r} fill="none" stroke="rgba(255,106,0,.1)" strokeWidth={2.5}/>
        <circle cx={23} cy={23} r={r} fill="none" stroke="#ff6a00" strokeWidth={2.5}
          strokeLinecap="round" strokeDasharray={circ} strokeDashoffset={circ-prog}
          style={{transition:"stroke-dashoffset .9s linear",filter:"drop-shadow(0 0 4px #ff6a00)"}}/>
      </svg>
      <div style={{position:"absolute",inset:0,display:"flex",alignItems:"center",justifyContent:"center",fontSize:9,fontWeight:700,color:"#ff8c42",fontFamily:"'JetBrains Mono',monospace"}}>{rem}</div>
    </div>
  );
}

/* ─── BURN CHART ─────────────────────────────────────────────────────────── */
function BurnChart({data}) {
  const max=Math.max(...data.map(d=>d.v));
  return (
    <div style={{display:"flex",alignItems:"flex-end",gap:7,height:100,paddingBottom:22,position:"relative"}}>
      {[.25,.5,.75,1].map(f=><div key={f} style={{position:"absolute",left:0,right:0,bottom:`${f*76+22}px`,height:1,background:"rgba(255,255,255,.04)"}}/>)}
      {data.map((d,i)=>{
        const h=Math.max(4,(d.v/max)*76);
        return (
          <div key={i} style={{flex:1,display:"flex",flexDirection:"column",alignItems:"center",gap:5}}>
            <div style={{width:"100%",height:h,borderRadius:"4px 4px 0 0",position:"relative",overflow:"hidden",cursor:"default"}} title={fmtFull(d.v)+" burned"}>
              <div style={{position:"absolute",inset:0,background:"linear-gradient(180deg,#ff8c42 0%,#ff4500 55%,#bb1500 100%)",borderRadius:"4px 4px 0 0"}}/>
              <div style={{position:"absolute",inset:0,background:"linear-gradient(180deg,rgba(255,255,200,.2) 0%,transparent 40%)",borderRadius:"4px 4px 0 0"}}/>
            </div>
            <span style={{fontSize:10,color:"rgba(255,255,255,.3)",fontWeight:600}}>{d.d}</span>
          </div>
        );
      })}
    </div>
  );
}

/* ─── TX LINK COMPONENT ──────────────────────────────────────────────────── */
function TxLink({tx, short=true}) {
  if (!tx) return null;
  const display = short ? `${tx.slice(0,6)}...${tx.slice(-4)}` : tx;
  return (
    <a href={solscanTx(tx)} target="_blank" rel="noopener noreferrer" className="tx-link mono"
      style={{fontSize:10}} onClick={e=>e.stopPropagation()}>
      {display} ↗
    </a>
  );
}

function AddrLink({addr, label}) {
  if (!addr) return null;
  const display = label || `${addr.slice(0,6)}...${addr.slice(-4)}`;
  return (
    <a href={solscanAddr(addr)} target="_blank" rel="noopener noreferrer" className="tx-link mono"
      style={{fontSize:11}} onClick={e=>e.stopPropagation()}>
      {display} ↗
    </a>
  );
}

/* ─── PER-TOKEN LOG PANEL ────────────────────────────────────────────────── */
function TokenLogs({tokenId, logs}) {
  const tokenLogs = logs.filter(l=>l.tokenId===tokenId);
  if (!tokenLogs.length) return <div style={{fontSize:12,color:"rgba(255,255,255,.25)",padding:"10px 0"}}>No activity yet.</div>;
  return (
    <div className="log-scroll">
      {tokenLogs.map((e,i)=>{
        const meta = EVT_META[e.type]||EVT_META.claim;
        return (
          <div key={e.id} className="log-row" style={{display:"flex",gap:10,alignItems:"flex-start",padding:"9px 0",animation:i===0?"slideUp .25s ease":"none"}}>
            <div style={{width:26,height:26,borderRadius:7,background:`${meta.color}18`,border:`1px solid ${meta.color}30`,display:"flex",alignItems:"center",justifyContent:"center",fontSize:12,flexShrink:0}}>{meta.icon}</div>
            <div style={{flex:1,minWidth:0}}>
              <div style={{fontSize:12,color:"rgba(255,255,255,.75)",lineHeight:1.5,marginBottom:e.tx?3:0}}>{e.msg}</div>
              {e.tx && <TxLink tx={e.tx}/>}
            </div>
            <div style={{fontSize:10,color:"rgba(255,255,255,.2)",whiteSpace:"nowrap",flexShrink:0,marginTop:2}}>{fmtAge(e.age)}</div>
          </div>
        );
      })}
    </div>
  );
}

/* ─── TOKEN CARD ─────────────────────────────────────────────────────────── */
function TokenCard({token, onUpdate, logs, allLogs}) {
  const [open,setOpen]=useState(false);
  const [tab,setTab]=useState("overview"); // overview | logs | settings
  const [editing,setEditing]=useState(false);
  const [local,setLocal]=useState({...token});
  const [copied,setCopied]=useState(false);

  const copy=()=>{navigator.clipboard.writeText(token.deposit).catch(()=>{});setCopied(true);setTimeout(()=>setCopied(false),1500);};
  const save=()=>{onUpdate(local);setEditing(false);};
  const toggle=(e)=>{e.stopPropagation();onUpdate({...token,active:!token.active});};
  const isActive=token.active;

  return (
    <div className="glass" style={{cursor:"pointer",transition:"all .25s",border:isActive?"1px solid rgba(255,106,0,.2)":"1px solid rgba(255,255,255,.07)",
      ...(isActive?{animation:"borderPulse 3s infinite"}:{})}}
      onClick={()=>setOpen(o=>!o)}>
      {isActive&&<div style={{position:"absolute",top:0,left:0,right:0,height:1,background:"linear-gradient(90deg,transparent,#ff6a00,#ff4500,transparent)",zIndex:1}}/>}

      {/* ── HEADER ROW ── */}
      <div style={{padding:"18px 20px",display:"flex",gap:14,alignItems:"center"}}>
        <div style={{width:44,height:44,borderRadius:12,background:"linear-gradient(135deg,#ff6a00,#cc2200)",display:"flex",alignItems:"center",justifyContent:"center",fontSize:18,fontWeight:800,flexShrink:0,
          boxShadow:isActive?"0 0 18px rgba(255,106,0,.4)":"none",transition:"box-shadow .3s"}}>
          {token.symbol[0]}
        </div>

        <div style={{flex:1,minWidth:0}}>
          <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:4,flexWrap:"wrap"}}>
            <span style={{fontWeight:800,fontSize:16,color:"#fff"}}>${token.symbol}</span>
            <span style={{fontSize:12,color:"rgba(255,255,255,.3)"}}>{token.name}</span>
            <span className={isActive?"tag-on":"tag-off"}>
              {isActive&&<span style={{width:5,height:5,borderRadius:"50%",background:"#ff6a00",animation:"pulse-dot 2s infinite",display:"inline-block"}}/>}
              {isActive?"BURNING":"PAUSED"}
            </span>
          </div>
          <div style={{display:"flex",gap:16,flexWrap:"wrap"}}>
            {[["BURNED",fmt(token.burned),"#ff8c42"],["TXS",fmtFull(token.txCount),"rgba(255,255,255,.6)"],["PENDING",fmt(token.pending),token.pending>0?"#ffd700":"rgba(255,255,255,.3)"]].map(([l,v,c])=>(
              <div key={l}><span style={{fontSize:10,color:"rgba(255,255,255,.25)",fontWeight:600,letterSpacing:.4}}>{l} </span><span style={{fontSize:13,fontWeight:700,color:c}}>{v}</span></div>
            ))}
          </div>
        </div>

        <div style={{display:"flex",alignItems:"center",gap:10,flexShrink:0}}>
          {isActive&&<Ring total={token.burnSec}/>}
          <button className="btn-ghost" onClick={toggle} style={{padding:"6px 14px",fontSize:12}}>{isActive?"Pause":"Start"}</button>
          <span style={{color:"rgba(255,255,255,.2)",fontSize:18,transform:open?"rotate(180deg)":"none",transition:"transform .2s",userSelect:"none"}}>⌄</span>
        </div>
      </div>

      {/* ── EXPANDED ── */}
      {open&&(
        <div style={{borderTop:"1px solid rgba(255,255,255,.05)"}} onClick={e=>e.stopPropagation()}>
          {/* tabs */}
          <div style={{display:"flex",gap:0,padding:"0 20px",borderBottom:"1px solid rgba(255,255,255,.05)"}}>
            {[["overview","Overview"],["logs","Burn Logs"],["settings","Settings"]].map(([v,l])=>(
              <button key={v} onClick={()=>setTab(v)}
                style={{background:"none",border:"none",borderBottom:`2px solid ${tab===v?"#ff6a00":"transparent"}`,color:tab===v?"#ff8c42":"rgba(255,255,255,.35)",padding:"10px 16px",cursor:"pointer",fontSize:12,fontWeight:700,transition:"all .15s",letterSpacing:.3}}>
                {l}
              </button>
            ))}
          </div>

          <div style={{padding:"18px 20px",animation:"slideUp .2s ease"}}>

            {/* OVERVIEW TAB */}
            {tab==="overview"&&(
              <div style={{display:"flex",flexDirection:"column",gap:14}}>
                <div style={{background:"rgba(255,106,0,.05)",border:"1px solid rgba(255,106,0,.1)",borderRadius:10,padding:"14px 16px"}}>
                  <div style={{fontSize:10,color:"rgba(255,255,255,.3)",fontWeight:600,letterSpacing:1,marginBottom:10}}>DEPOSIT ADDRESS</div>
                  <div style={{display:"flex",gap:14,alignItems:"center"}}>
                    <QR seed={token.deposit}/>
                    <div style={{flex:1,minWidth:0}}>
                      <div style={{marginBottom:8}}>
                        <AddrLink addr={token.deposit}/>
                      </div>
                      <div className="mono" style={{fontSize:10,color:"#ff8c42",wordBreak:"break-all",lineHeight:1.6,marginBottom:8,background:"rgba(0,0,0,.3)",padding:"8px 10px",borderRadius:6,border:"1px solid rgba(255,106,0,.1)"}}>
                        {token.deposit}
                      </div>
                      <button className="btn-ghost" onClick={copy} style={{padding:"5px 12px",fontSize:11}}>{copied?"✓ Copied!":"⎘ Copy"}</button>
                    </div>
                  </div>
                </div>
                <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:10}}>
                  {[["Claim Every",fmtSec(token.claimSec)],["Burn Every",fmtSec(token.burnSec)],["Split Buys",`${token.splits}× per cycle`],["Mint",<AddrLink addr={token.mint} label={`${token.mint.slice(0,8)}...`}/>]].map(([k,v])=>(
                    <div key={k} style={{background:"rgba(255,255,255,.03)",borderRadius:8,padding:"10px 12px"}}>
                      <div style={{fontSize:10,color:"rgba(255,255,255,.3)",marginBottom:4,fontWeight:600,letterSpacing:.5}}>{k.toUpperCase()}</div>
                      <div style={{fontSize:13,color:"#fff",fontWeight:700}}>{v}</div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* LOGS TAB */}
            {tab==="logs"&&(
              <div>
                <div style={{fontSize:11,color:"rgba(255,255,255,.25)",marginBottom:10}}>Showing last {allLogs.filter(l=>l.tokenId===token.id).length} events</div>
                <TokenLogs tokenId={token.id} logs={allLogs}/>
              </div>
            )}

            {/* SETTINGS TAB */}
            {tab==="settings"&&(
              <div style={{display:"flex",flexDirection:"column",gap:16}}>
                {editing?(
                  <>
                    <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:14}}>
                      {[{label:"Claim Interval (s)",key:"claimSec",min:60},{label:"Burn Interval (s)",key:"burnSec",min:60}].map(fd=>(
                        <div key={fd.key}>
                          <label style={{display:"block",fontSize:10,color:"rgba(255,255,255,.35)",letterSpacing:1,marginBottom:7,fontWeight:600}}>{fd.label.toUpperCase()}</label>
                          <input type="number" min={fd.min} className="input-f" style={{padding:"9px 12px",fontSize:13}}
                            value={local[fd.key]} onChange={e=>setLocal({...local,[fd.key]:+e.target.value})}/>
                        </div>
                      ))}
                    </div>
                    <div>
                      <label style={{display:"block",fontSize:10,color:"rgba(255,255,255,.35)",letterSpacing:1,marginBottom:8,fontWeight:600}}>SPLIT BUYS PER CYCLE</label>
                      <div style={{display:"flex",gap:5,flexWrap:"wrap"}}>
                        {[1,2,3,4,5,6,7,8,9,10].map(n=>(
                          <button key={n} className={`split-btn${local.splits===n?" on":""}`} onClick={()=>setLocal({...local,splits:n})}>{n}</button>
                        ))}
                      </div>
                    </div>
                    <div style={{display:"flex",gap:8}}>
                      <button className="btn-fire" onClick={save} style={{padding:"9px 20px",fontSize:13}}>Save</button>
                      <button className="btn-ghost" onClick={()=>{setEditing(false);setLocal({...token});}} style={{padding:"9px 16px",fontSize:13}}>Cancel</button>
                    </div>
                  </>
                ):(
                  <>
                    <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:10}}>
                      {[["Claim Every",fmtSec(token.claimSec)],["Burn Every",fmtSec(token.burnSec)],["Split Buys",`${token.splits}× per cycle`],["Status",token.active?"🔥 Active":"⏸ Paused"]].map(([k,v])=>(
                        <div key={k} style={{background:"rgba(255,255,255,.03)",borderRadius:8,padding:"10px 12px"}}>
                          <div style={{fontSize:10,color:"rgba(255,255,255,.3)",marginBottom:4,fontWeight:600,letterSpacing:.5}}>{k.toUpperCase()}</div>
                          <div style={{fontSize:13,color:"#fff",fontWeight:700}}>{v}</div>
                        </div>
                      ))}
                    </div>
                    <button className="btn-ghost" onClick={()=>setEditing(true)} style={{padding:"8px 18px",fontSize:12,width:"fit-content"}}>✎ Edit Settings</button>
                  </>
                )}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

/* ─── LOGIN MODAL ────────────────────────────────────────────────────────── */
function LoginModal({onClose,onLogin}) {
  const [tab,setTab]=useState("login");
  const [f,setF]=useState({user:"",pass:"",confirm:""});
  const [err,setErr]=useState("");
  const [loading,setLoading]=useState(false);
  const submit=()=>{
    setErr("");
    if(!f.user.trim()||!f.pass.trim()) return setErr("All fields required.");
    if(tab==="register"&&f.pass!==f.confirm) return setErr("Passwords do not match.");
    if(f.pass.length<6) return setErr("Password must be 6+ characters.");
    setLoading(true);
    setTimeout(()=>{setLoading(false);onLogin(f.user);},900);
  };
  return (
    <div style={{position:"fixed",inset:0,zIndex:999,display:"flex",alignItems:"center",justifyContent:"center",animation:"fadeIn .2s ease"}} onClick={onClose}>
      <div style={{position:"absolute",inset:0,background:"rgba(0,0,0,.8)",backdropFilter:"blur(10px)"}}/>
      <div className="glass" onClick={e=>e.stopPropagation()} style={{position:"relative",zIndex:1,width:"min(420px,94vw)",padding:36,animation:"slideUp .25s ease",boxShadow:"0 32px 80px rgba(0,0,0,.6),0 0 60px rgba(255,106,0,.07)"}}>
        <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:28}}>
          <div style={{display:"flex",alignItems:"center",gap:10}}>
            <span style={{fontSize:26,animation:"flicker 4s infinite"}}>🔥</span>
            <div><div style={{fontWeight:800,fontSize:19,color:"#fff",letterSpacing:1}}>EMBER</div><div style={{fontSize:10,color:"rgba(255,255,255,.3)",letterSpacing:2}}>BURN PROTOCOL</div></div>
          </div>
          <button onClick={onClose} style={{background:"none",border:"none",color:"rgba(255,255,255,.3)",fontSize:22,cursor:"pointer",lineHeight:1}}>✕</button>
        </div>
        <div style={{display:"flex",background:"rgba(255,255,255,.04)",borderRadius:10,padding:3,marginBottom:24}}>
          {[["login","Sign In"],["register","Create Account"]].map(([v,l])=>(
            <button key={v} onClick={()=>{setTab(v);setErr("");}}
              style={{flex:1,padding:"9px 0",border:"none",borderRadius:8,cursor:"pointer",fontSize:13,fontWeight:700,transition:"all .2s",
                background:tab===v?"linear-gradient(135deg,#ff6a00,#ff4500)":"none",
                color:tab===v?"#fff":"rgba(255,255,255,.3)",
                boxShadow:tab===v?"0 2px 12px rgba(255,106,0,.3)":"none"}}>{l}</button>
          ))}
        </div>
        <div style={{display:"flex",flexDirection:"column",gap:14}}>
          {[{label:"Username",key:"user",type:"text"},{label:"Password",key:"pass",type:"password"},...(tab==="register"?[{label:"Confirm Password",key:"confirm",type:"password"}]:[])].map(fd=>(
            <div key={fd.key}>
              <label style={{display:"block",fontSize:11,color:"rgba(255,255,255,.35)",letterSpacing:1,marginBottom:7,fontWeight:600}}>{fd.label.toUpperCase()}</label>
              <input type={fd.type} className="input-f" value={f[fd.key]} onChange={e=>setF({...f,[fd.key]:e.target.value})}
                onKeyDown={e=>e.key==="Enter"&&submit()} placeholder={fd.type==="password"?"••••••••":"username"}/>
            </div>
          ))}
          {err&&<div style={{background:"rgba(255,64,96,.1)",border:"1px solid rgba(255,64,96,.2)",borderRadius:8,padding:"10px 14px",fontSize:13,color:"#ff8080"}}>{err}</div>}
          <button className="btn-fire" onClick={submit} disabled={loading} style={{padding:"13px",fontSize:14,marginTop:4}}>
            {loading?<span style={{display:"flex",alignItems:"center",justifyContent:"center",gap:8}}><span style={{width:14,height:14,border:"2px solid rgba(255,255,255,.3)",borderTopColor:"#fff",borderRadius:"50%",animation:"spin .7s linear infinite",display:"inline-block"}}/>Authenticating...</span>:tab==="login"?"🔥 Enter the Burn":"🔥 Ignite Account"}
          </button>
        </div>
        <p style={{textAlign:"center",marginTop:16,fontSize:11,color:"rgba(255,255,255,.18)"}}>Demo — any credentials work</p>
      </div>
    </div>
  );
}

/* ─── ATTACH MODAL ───────────────────────────────────────────────────────── */
function AttachModal({onClose,onAttach}) {
  const [step,setStep]=useState(1);
  const [mint,setMint]=useState("");
  const [mintStatus,setMintStatus]=useState("idle"); // idle | looking | found | notfound
  const [resolved,setResolved]=useState(null); // {symbol,name}
  const [f,setF]=useState({claimSec:120,burnSec:300,splits:1});
  const [err,setErr]=useState("");
  const [dep,setDep]=useState("");

  // Auto-lookup on mint input change
  useEffect(()=>{
    if(mint.length<32){setMintStatus("idle");setResolved(null);return;}
    setMintStatus("looking");
    const timer=setTimeout(()=>{
      const known=KNOWN_MINTS[mint.trim()];
      if(known){setResolved(known);setMintStatus("found");}
      else{
        // Simulate a generic lookup for unknown mints
        if(mint.length>=32){
          const fake={symbol:mint.slice(0,4).toUpperCase(),name:`Token (${mint.slice(0,6)}...)`};
          setResolved(fake);setMintStatus("found");
        } else {setMintStatus("notfound");}
      }
    },600);
    return()=>clearTimeout(timer);
  },[mint]);

  const next=()=>{
    setErr("");
    if(!mint.trim()||!resolved) return setErr("Enter a valid mint address first.");
    if(f.claimSec<60||f.burnSec<60) return setErr("Intervals must be ≥ 60 seconds.");
    const d=`EMBR${Math.random().toString(36).slice(2,8).toUpperCase()}${Date.now().toString(36).toUpperCase()}`;
    setDep(d); setStep(2);
  };

  const finish=()=>{
    onAttach({...resolved,id:String(Date.now()),mint:mint.trim(),deposit:dep,...f,active:false,burned:0,pending:0,txCount:0});
    onClose();
  };

  return (
    <div style={{position:"fixed",inset:0,zIndex:999,display:"flex",alignItems:"center",justifyContent:"center",animation:"fadeIn .2s ease"}} onClick={onClose}>
      <div style={{position:"absolute",inset:0,background:"rgba(0,0,0,.82)",backdropFilter:"blur(10px)"}}/>
      <div className="glass" onClick={e=>e.stopPropagation()} style={{position:"relative",zIndex:1,width:"min(520px,95vw)",padding:36,animation:"slideUp .25s ease",maxHeight:"90vh",overflowY:"auto",boxShadow:"0 32px 80px rgba(0,0,0,.6),0 0 60px rgba(255,106,0,.07)"}}>

        <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:24}}>
          <div>
            <div style={{fontWeight:800,fontSize:21,color:"#fff",marginBottom:8}}>{step===1?"⚙️ Configure Token":"📍 Your Deposit Address"}</div>
            <div style={{display:"flex",gap:6}}>
              {[1,2].map(s=><div key={s} style={{height:3,width:48,borderRadius:2,background:s<=step?"linear-gradient(90deg,#ff6a00,#ff4500)":"rgba(255,255,255,.1)",transition:"background .3s"}}/>)}
            </div>
          </div>
          <button onClick={onClose} style={{background:"none",border:"none",color:"rgba(255,255,255,.3)",fontSize:22,cursor:"pointer"}}>✕</button>
        </div>

        {step===1&&(
          <div style={{display:"flex",flexDirection:"column",gap:18}}>

            {/* MINT ADDRESS with auto-lookup */}
            <div>
              <label style={{display:"block",fontSize:11,color:"rgba(255,255,255,.35)",letterSpacing:1,marginBottom:7,fontWeight:600}}>TOKEN MINT ADDRESS</label>
              <input className="input-f" value={mint} onChange={e=>setMint(e.target.value)} placeholder="Paste mint address — symbol auto-fills"/>
              {/* status */}
              {mintStatus==="looking"&&(
                <div style={{marginTop:8,display:"flex",alignItems:"center",gap:6,fontSize:12,color:"rgba(255,255,255,.4)"}}>
                  <span style={{width:10,height:10,border:"2px solid rgba(255,255,255,.2)",borderTopColor:"#ff8c42",borderRadius:"50%",animation:"spin .6s linear infinite",display:"inline-block"}}/>
                  Looking up token...
                </div>
              )}
              {mintStatus==="found"&&resolved&&(
                <div style={{marginTop:8,background:"rgba(255,106,0,.08)",border:"1px solid rgba(255,106,0,.2)",borderRadius:8,padding:"10px 14px",display:"flex",alignItems:"center",gap:10}}>
                  <span style={{fontSize:16}}>✅</span>
                  <div>
                    <div style={{fontWeight:700,color:"#fff",fontSize:14}}>${resolved.symbol} <span style={{fontWeight:400,color:"rgba(255,255,255,.5)",fontSize:12}}>— {resolved.name}</span></div>
                    <div style={{fontSize:11,color:"rgba(255,255,255,.3)",marginTop:2}}>Token found — symbol and name locked in</div>
                  </div>
                </div>
              )}
              {mintStatus==="notfound"&&(
                <div style={{marginTop:8,background:"rgba(255,64,64,.08)",border:"1px solid rgba(255,64,64,.2)",borderRadius:8,padding:"8px 12px",fontSize:12,color:"#ff8080"}}>
                  ⚠ Token not found. Check the mint address.
                </div>
              )}
            </div>

            {/* Read-only symbol / name */}
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:14}}>
              {[{label:"Token Symbol",val:resolved?.symbol},{label:"Token Name",val:resolved?.name}].map(fd=>(
                <div key={fd.label}>
                  <label style={{display:"block",fontSize:11,color:"rgba(255,255,255,.35)",letterSpacing:1,marginBottom:7,fontWeight:600}}>{fd.label.toUpperCase()}</label>
                  <input className="input-f" value={fd.val||""} readOnly disabled placeholder="Auto-filled from mint"
                    style={{opacity:fd.val?1:.4,cursor:"not-allowed"}}/>
                </div>
              ))}
            </div>

            {/* intervals */}
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:14}}>
              {[{label:"Claim Interval",key:"claimSec"},{label:"Burn Interval",key:"burnSec"}].map(fd=>(
                <div key={fd.key}>
                  <label style={{display:"block",fontSize:11,color:"rgba(255,255,255,.35)",letterSpacing:1,marginBottom:7,fontWeight:600}}>{fd.label.toUpperCase()}</label>
                  <div style={{position:"relative"}}>
                    <input type="number" min={60} className="input-f" value={f[fd.key]} onChange={e=>setF({...f,[fd.key]:+e.target.value})} style={{paddingRight:30}}/>
                    <span style={{position:"absolute",right:12,top:"50%",transform:"translateY(-50%)",fontSize:11,color:"rgba(255,255,255,.25)"}}>s</span>
                  </div>
                  <div style={{fontSize:10,color:"rgba(255,255,255,.2)",marginTop:4}}>min 60s</div>
                </div>
              ))}
            </div>

            {/* split buys */}
            <div>
              <label style={{display:"block",fontSize:11,color:"rgba(255,255,255,.35)",letterSpacing:1,marginBottom:8,fontWeight:600}}>SPLIT BUYS PER CYCLE</label>
              <div style={{display:"flex",gap:5,flexWrap:"wrap"}}>
                {[1,2,3,4,5,6,7,8,9,10].map(n=>(
                  <button key={n} className={`split-btn${f.splits===n?" on":""}`} onClick={()=>setF({...f,splits:n})}>{n}</button>
                ))}
              </div>
              <div style={{fontSize:11,color:"rgba(255,255,255,.2)",marginTop:7}}>
                {f.splits===1?"One buyback executed per burn cycle":`${f.splits} separate buys spread across the burn cycle`}
              </div>
            </div>

            {err&&<div style={{background:"rgba(255,64,96,.1)",border:"1px solid rgba(255,64,96,.2)",borderRadius:8,padding:"10px 14px",fontSize:13,color:"#ff8080"}}>{err}</div>}

            <button className="btn-fire" onClick={next} style={{padding:"13px",fontSize:14}}>
              Generate Deposit Address →
            </button>
          </div>
        )}

        {step===2&&(
          <div style={{display:"flex",flexDirection:"column",gap:20}}>
            <div style={{background:"rgba(255,106,0,.06)",border:"1px solid rgba(255,106,0,.15)",borderRadius:12,padding:20}}>
              <div style={{fontSize:11,color:"rgba(255,255,255,.3)",fontWeight:600,letterSpacing:1,marginBottom:12}}>DEPOSIT ADDRESS</div>
              <div style={{display:"flex",gap:14,alignItems:"center"}}>
                <QR seed={dep}/>
                <div style={{flex:1,minWidth:0}}>
                  <div style={{marginBottom:6}}><AddrLink addr={dep}/></div>
                  <div className="mono" style={{fontSize:10,color:"#ff8c42",wordBreak:"break-all",lineHeight:1.6,background:"rgba(0,0,0,.3)",padding:"8px 10px",borderRadius:6,border:"1px solid rgba(255,106,0,.1)",marginBottom:8}}>{dep}</div>
                  <button className="btn-ghost" onClick={()=>navigator.clipboard.writeText(dep).catch(()=>{})} style={{padding:"5px 14px",fontSize:11}}>⎘ Copy Address</button>
                </div>
              </div>
            </div>

            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:10}}>
              {[["Token",`$${resolved?.symbol} — ${resolved?.name}`],["Claim Every",fmtSec(f.claimSec)],["Burn Every",fmtSec(f.burnSec)],["Split Buys",`${f.splits}× per cycle`]].map(([k,v])=>(
                <div key={k} style={{background:"rgba(255,255,255,.03)",borderRadius:8,padding:"10px 12px"}}>
                  <div style={{fontSize:10,color:"rgba(255,255,255,.3)",marginBottom:4,fontWeight:600,letterSpacing:.5}}>{k.toUpperCase()}</div>
                  <div style={{fontSize:13,color:"#fff",fontWeight:700}}>{v}</div>
                </div>
              ))}
            </div>

            <div style={{background:"rgba(255,200,0,.05)",border:"1px solid rgba(255,200,0,.12)",borderRadius:8,padding:"10px 14px",fontSize:12,color:"rgba(255,200,80,.65)",display:"flex",gap:8}}>
              <span>⚠</span><span>Simulated mode — bot integration pending. Events will be mocked.</span>
            </div>

            <div style={{display:"flex",gap:10}}>
              <button className="btn-ghost" onClick={()=>setStep(1)} style={{padding:"12px 18px",fontSize:13}}>← Back</button>
              <button className="btn-fire" onClick={finish} style={{padding:"12px",fontSize:14,flex:1}}>🔥 Ignite Token</button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

/* ─── LIVE FEED ──────────────────────────────────────────────────────────── */
function LiveFeed({events}) {
  return (
    <div>
      {events.slice(0,12).map((e,i)=>{
        const meta=EVT_META[e.type]||EVT_META.claim;
        return (
          <div key={e.id} className="feed-row" style={{display:"flex",gap:10,alignItems:"flex-start",padding:"10px 0",animation:i===0?"slideUp .3s ease":"none"}}>
            <div style={{width:28,height:28,borderRadius:8,background:`${meta.color}18`,border:`1px solid ${meta.color}28`,display:"flex",alignItems:"center",justifyContent:"center",fontSize:13,flexShrink:0}}>{meta.icon}</div>
            <div style={{flex:1,minWidth:0}}>
              <div style={{display:"flex",alignItems:"center",gap:6,marginBottom:3,flexWrap:"wrap"}}>
                <span style={{fontSize:10,fontWeight:700,color:meta.color,letterSpacing:.5}}>{meta.label}</span>
                <span style={{fontSize:10,color:"rgba(255,255,255,.3)",fontWeight:600}}>${e.token}</span>
              </div>
              <div style={{fontSize:12,color:"rgba(255,255,255,.7)",lineHeight:1.4,marginBottom:e.tx?4:0}}>{e.msg}</div>
              {e.tx&&<TxLink tx={e.tx}/>}
            </div>
            <div style={{fontSize:10,color:"rgba(255,255,255,.2)",whiteSpace:"nowrap",flexShrink:0,marginTop:2}}>{fmtAge(e.age)}</div>
          </div>
        );
      })}
    </div>
  );
}

/* ─── TOKEN TICKER BAR ───────────────────────────────────────────────────── */
function TokenTicker({tokens}) {
  // Double the array so the seamless loop works
  const items = [...tokens, ...tokens];
  return (
    <div style={{position:"relative",zIndex:50,background:"rgba(0,0,0,.55)",borderBottom:"1px solid rgba(255,106,0,.12)",backdropFilter:"blur(10px)",height:38,display:"flex",alignItems:"center",overflow:"hidden"}}>
      {/* fade edges */}
      <div style={{position:"absolute",left:0,top:0,bottom:0,width:60,background:"linear-gradient(90deg,rgba(0,0,0,.8),transparent)",zIndex:2,pointerEvents:"none"}}/>
      <div style={{position:"absolute",right:0,top:0,bottom:0,width:60,background:"linear-gradient(270deg,rgba(0,0,0,.8),transparent)",zIndex:2,pointerEvents:"none"}}/>
      <div className="ticker-wrap">
        <div className="ticker-track">
          {items.map((t,i)=>(
            <div key={i} style={{display:"flex",alignItems:"center",gap:10,padding:"0 28px",borderRight:"1px solid rgba(255,255,255,.05)",height:38,whiteSpace:"nowrap"}}>
              <div style={{width:20,height:20,borderRadius:6,background:"linear-gradient(135deg,#ff6a00,#cc2200)",display:"flex",alignItems:"center",justifyContent:"center",fontSize:10,fontWeight:800,flexShrink:0}}>
                {t.symbol[0]}
              </div>
              <span style={{fontWeight:700,fontSize:12,color:"#fff"}}>${t.symbol}</span>
              <span style={{fontSize:11,color:"rgba(255,255,255,.3)"}}>{t.name}</span>
              <span style={{fontSize:10,fontWeight:700,color:t.active?"#ff8c42":"rgba(255,255,255,.25)",display:"flex",alignItems:"center",gap:4}}>
                {t.active&&<span style={{width:5,height:5,borderRadius:"50%",background:"#ff6a00",display:"inline-block",animation:"pulse-dot 2s infinite"}}/>}
                {t.active?"BURNING":"PAUSED"}
              </span>
              <span style={{fontSize:11,color:"rgba(255,255,255,.4)"}}>🔥 {fmt(t.burned)}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

/* ─── HOW IT WORKS ───────────────────────────────────────────────────────── */
function HowItWorks() {
  const steps = [
    {
      n:"01", icon:"🪙", title:"Attach Your Token",
      body:"Connect any Solana memecoin by pasting its mint address. EMBER looks up the token automatically and generates a unique deposit address assigned exclusively to your token."
    },
    {
      n:"02", icon:"📍", title:"Set Your Deposit Address",
      body:"In your token's creator dashboard, set your creator rewards destination to the deposit address EMBER gave you. Every reward that hits that address belongs to your burn cycle."
    },
    {
      n:"03", icon:"⚡", title:"Auto-Claim Fires",
      body:"On your configured interval (minimum 60 seconds), EMBER's bot automatically claims whatever creator rewards have accumulated at your deposit address. No manual action needed."
    },
    {
      n:"04", icon:"🔄", title:"Buyback Executes",
      body:"The claimed rewards are used to buy back your token from the open market. You control how many separate buys happen per cycle — anywhere from 1 to 10 — giving you flexibility on timing and execution."
    },
    {
      n:"05", icon:"🔥", title:"Tokens Are Incinerated",
      body:"Bought tokens are sent directly to the EMBER incinerator — a program-controlled burn address. They are permanently removed from the circulating supply. No recovery, no reversal."
    },
    {
      n:"06", icon:"📊", title:"Track Everything On-Chain",
      body:"Every claim, buyback, and burn is logged with a real transaction signature you can verify on Solscan. Your dashboard updates in real time so you always know exactly what's happening."
    },
  ];
  return (
    <section id="how-it-works" style={{position:"relative",zIndex:2,maxWidth:1100,margin:"0 auto",padding:"100px 24px 80px"}}>
      <div style={{textAlign:"center",marginBottom:64}}>
        <div style={{fontSize:11,fontWeight:700,letterSpacing:3,color:"#ff8c42",marginBottom:14,textTransform:"uppercase"}}>How It Works</div>
        <h2 style={{fontSize:"clamp(32px,5vw,54px)",fontWeight:800,color:"#fff",lineHeight:1.1,marginBottom:16}}>
          From rewards to ash.<br/><span style={{background:"linear-gradient(135deg,#ff8c42,#ff4500)",WebkitBackgroundClip:"text",WebkitTextFillColor:"transparent"}}>Fully automated.</span>
        </h2>
        <p style={{fontSize:16,color:"rgba(255,255,255,.35)",maxWidth:520,margin:"0 auto",lineHeight:1.7}}>
          EMBER handles the entire lifecycle — from collecting your creator rewards to permanently incinerating your token supply — without you lifting a finger.
        </p>
      </div>

      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(300px,1fr))",gap:20}}>
        {steps.map((s,i)=>(
          <div key={s.n} className="glass" style={{padding:"28px 26px",animation:`slideUp .5s ease ${i*.08}s both`}}>
            <div style={{display:"flex",alignItems:"flex-start",gap:14,marginBottom:16}}>
              <div style={{fontSize:10,fontWeight:800,color:"rgba(255,106,0,.4)",fontFamily:"'JetBrains Mono',monospace",letterSpacing:1,paddingTop:2,flexShrink:0}}>{s.n}</div>
              <div style={{width:40,height:40,borderRadius:10,background:"rgba(255,106,0,.1)",border:"1px solid rgba(255,106,0,.2)",display:"flex",alignItems:"center",justifyContent:"center",fontSize:20,flexShrink:0}}>{s.icon}</div>
            </div>
            <div style={{fontWeight:800,fontSize:16,color:"#fff",marginBottom:10}}>{s.title}</div>
            <div style={{fontSize:13,color:"rgba(255,255,255,.38)",lineHeight:1.75}}>{s.body}</div>
          </div>
        ))}
      </div>

      {/* incinerator callout */}
      <div style={{marginTop:48,background:"linear-gradient(135deg,rgba(255,80,0,.1),rgba(200,20,0,.08))",border:"1px solid rgba(255,80,0,.2)",borderRadius:16,padding:"28px 32px",display:"flex",gap:20,alignItems:"center",flexWrap:"wrap"}}>
        <div style={{fontSize:36}}>🔥</div>
        <div style={{flex:1,minWidth:200}}>
          <div style={{fontWeight:800,fontSize:17,color:"#fff",marginBottom:6}}>The EMBER Incinerator</div>
          <div style={{fontSize:13,color:"rgba(255,255,255,.4)",lineHeight:1.7}}>
            Burned tokens are sent to the EMBER incinerator address — a purpose-built burn destination on Solana. Every transaction is public, verifiable, and permanent. There is no undo.
          </div>
        </div>
        <div style={{fontSize:11,color:"rgba(255,255,255,.2)",fontFamily:"'JetBrains Mono',monospace",background:"rgba(0,0,0,.3)",padding:"8px 14px",borderRadius:8,border:"1px solid rgba(255,255,255,.07)",whiteSpace:"nowrap"}}>
          EMBR1nc1n3rat0r...
        </div>
      </div>
    </section>
  );
}

/* ─── ROOT APP ───────────────────────────────────────────────────────────── */
export default function App() {
  const [user,setUser]=useState(null);
  const [showLogin,setShowLogin]=useState(false);
  const [showAttach,setShowAttach]=useState(false);
  const [tokens,setTokens]=useState(TOKENS_INIT);
  const [feed,setFeed]=useState(FEED_INIT);
  const [allLogs,setAllLogs]=useState(BASE_LOGS);
  const [menuOpen,setMenuOpen]=useState(false);

  // Simulate live events
  useEffect(()=>{
    if(!user) return;
    const pool=[
      {type:"burn",token:"BONK",msg:"Buyback + burn complete — 24,100 BONK eliminated",genTx:true},
      {type:"claim",token:"WIF",msg:"Creator rewards claimed — 3,200 WIF collected",genTx:true},
      {type:"split",token:"BONK",msg:"Split buy 1/3 executed — 16,100 BONK acquired",genTx:true},
      {type:"burn",token:"WIF",msg:"10,500 WIF sent to the incinerator — gone forever",genTx:true},
      {type:"buyback",token:"MYRO",msg:"Buyback triggered — 900 MYRO acquired",genTx:true},
    ];
    const id=setInterval(()=>{
      const e=pool[Math.floor(Math.random()*pool.length)];
      const tx=`${Math.random().toString(36).slice(2,10)}${Math.random().toString(36).slice(2,10)}`;
      const newEvent={...e,id:Date.now(),tx,age:0};
      setFeed(p=>[newEvent,...p.slice(0,28)]);
      const tokenId=tokens.find(t=>t.symbol===e.token)?.id;
      if(tokenId){
        setAllLogs(p=>[{...newEvent,tokenId,age:0},...p.slice(0,99)]);
        if(e.type==="burn") setTokens(p=>p.map(t=>t.symbol===e.token?{...t,burned:t.burned+Math.floor(Math.random()*20000+5000),txCount:t.txCount+1}:t));
      }
    },7000);
    return()=>clearInterval(id);
  },[user]);

  const totalBurned=tokens.reduce((a,t)=>a+t.burned,0);
  const activeCount=tokens.filter(t=>t.active).length;
  const totalPending=tokens.reduce((a,t)=>a+t.pending,0);

  return (
    <>
      <style>{CSS}</style>
      <FireBg/>
      <Embers/>

      {/* NAV */}
      <nav className="nav-blur" style={{position:"sticky",top:0,zIndex:100,height:64,display:"flex",alignItems:"center",padding:"0 28px",justifyContent:"space-between"}}>
        <div style={{display:"flex",alignItems:"center",gap:12}}>
          <span style={{fontSize:26,animation:"flicker 5s infinite"}}>🔥</span>
          <div>
            <span style={{fontSize:20,fontWeight:800,letterSpacing:2,color:"#fff",animation:"textGlow 4s infinite"}}>EMBER</span>
            <span style={{fontSize:10,color:"rgba(255,255,255,.22)",letterSpacing:3,marginLeft:10}}>BURN PROTOCOL</span>
          </div>
        </div>
        <div style={{display:"flex",alignItems:"center",gap:10}}>
          <div style={{display:"flex",alignItems:"center",gap:6,background:"rgba(255,106,0,.07)",border:"1px solid rgba(255,106,0,.14)",borderRadius:20,padding:"5px 13px"}}>
            <div style={{width:6,height:6,borderRadius:"50%",background:"#ff6a00",animation:"pulse-dot 1.8s infinite"}}/>
            <span style={{fontSize:11,color:"#ff8c42",fontWeight:700}}>LIVE</span>
          </div>
          <div style={{background:"rgba(255,180,0,.06)",border:"1px solid rgba(255,180,0,.12)",borderRadius:8,padding:"5px 11px",fontSize:10,color:"rgba(255,200,80,.55)",fontWeight:600}}>⚠ SIM</div>
          {user?(
            <div style={{position:"relative"}}>
              <button onClick={()=>setMenuOpen(m=>!m)} style={{background:"rgba(255,106,0,.1)",border:"1px solid rgba(255,106,0,.18)",borderRadius:10,padding:"7px 14px",color:"#fff",cursor:"pointer",fontSize:13,fontWeight:600,display:"flex",alignItems:"center",gap:8}}>
                <div style={{width:22,height:22,borderRadius:"50%",background:"linear-gradient(135deg,#ff6a00,#cc2200)",display:"flex",alignItems:"center",justifyContent:"center",fontSize:11,fontWeight:800}}>{user[0].toUpperCase()}</div>
                {user}
              </button>
              {menuOpen&&(
                <div style={{position:"absolute",top:"calc(100% + 8px)",right:0,background:"#0f0407",border:"1px solid rgba(255,255,255,.08)",borderRadius:10,overflow:"hidden",minWidth:160,boxShadow:"0 16px 40px rgba(0,0,0,.5)",animation:"slideUp .15s ease"}}>
                  <div style={{padding:"10px 16px",fontSize:11,color:"rgba(255,255,255,.25)",borderBottom:"1px solid rgba(255,255,255,.06)"}}>Signed in as {user}</div>
                  <button onClick={()=>{setUser(null);setMenuOpen(false);}} style={{width:"100%",background:"none",border:"none",color:"rgba(255,255,255,.65)",padding:"11px 16px",textAlign:"left",cursor:"pointer",fontSize:13,fontWeight:600}}>Sign Out</button>
                </div>
              )}
            </div>
          ):(
            <button className="btn-fire" onClick={()=>setShowLogin(true)} style={{padding:"9px 20px",fontSize:13}}>🔥 Sign In</button>
          )}
        </div>
      </nav>

      {/* TICKER */}
      <TokenTicker tokens={tokens}/>

      {/* HERO */}
      {!user&&(
        <div style={{position:"relative",zIndex:2,minHeight:"88vh",display:"flex",flexDirection:"column",alignItems:"center",justifyContent:"center",textAlign:"center",padding:"80px 24px"}}>
          <div style={{position:"absolute",width:640,height:640,borderRadius:"50%",border:"1px solid rgba(255,106,0,.05)",left:"50%",top:"42%",transform:"translate(-50%,-50%)",animation:"ringExpand 5s linear infinite",pointerEvents:"none"}}/>
          <div style={{position:"absolute",width:640,height:640,borderRadius:"50%",border:"1px solid rgba(255,106,0,.04)",left:"50%",top:"42%",transform:"translate(-50%,-50%)",animation:"ringExpand 5s linear 2.5s infinite",pointerEvents:"none"}}/>
          <div style={{fontSize:72,marginBottom:20,animation:"flicker 3s infinite",filter:"drop-shadow(0 0 40px rgba(255,106,0,.55))"}}>🔥</div>
          <h1 style={{fontSize:"clamp(44px,8vw,90px)",fontWeight:800,color:"#fff",lineHeight:1.05,marginBottom:16,letterSpacing:-1,
            background:"linear-gradient(135deg,#ffffff 0%,#ffd0a0 35%,#ff6a00 80%)",WebkitBackgroundClip:"text",WebkitTextFillColor:"transparent",
            backgroundSize:"200% 200%",animation:"gradShift 5s ease infinite"}}>
            BURN EVERYTHING.
          </h1>
          <p style={{fontSize:"clamp(15px,2.2vw,20px)",color:"rgba(255,255,255,.38)",maxWidth:520,lineHeight:1.65,marginBottom:38,fontWeight:500}}>
            Attach any Solana memecoin. EMBER automatically converts your creator rewards into buybacks and permanent incineration.
          </p>
          <div style={{display:"flex",gap:12,flexWrap:"wrap",justifyContent:"center"}}>
            <button className="btn-fire" onClick={()=>setShowLogin(true)} style={{padding:"15px 38px",fontSize:15}}>🔥 Get Started</button>
            <button className="btn-ghost" onClick={()=>document.getElementById("how-it-works")?.scrollIntoView({behavior:"smooth"})} style={{padding:"15px 30px",fontSize:15}}>How It Works ↓</button>
          </div>
          <div style={{marginTop:72,display:"flex",gap:48,flexWrap:"wrap",justifyContent:"center"}}>
            {[["$4.8M+","Total Incinerated"],["1,247","Transactions"],["3","Active Tokens"]].map(([v,l])=>(
              <div key={l} style={{textAlign:"center"}}>
                <div style={{fontSize:30,fontWeight:800,color:"#ff8c42",animation:"textGlow 4s infinite"}}>{v}</div>
                <div style={{fontSize:11,color:"rgba(255,255,255,.27)",fontWeight:700,letterSpacing:1.2,marginTop:4}}>{l.toUpperCase()}</div>
              </div>
            ))}
          </div>
          <div style={{marginTop:72,display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(200px,1fr))",gap:14,maxWidth:760,width:"100%"}}>
            {[
              {icon:"⚡",t:"Auto Claim",d:"Your creator rewards are claimed automatically on your configured schedule — minimum 60 second intervals."},
              {icon:"🔥",t:"Buyback + Burn",d:"Claimed rewards buy back your token, which is then sent to the EMBER incinerator — permanently removed from supply."},
              {icon:"🔀",t:"Split Buys",d:"Choose 1 to 10 separate buys per burn cycle, giving you control over how and when each cycle executes."},
            ].map(c=>(
              <div key={c.t} className="glass" style={{padding:"20px 18px",textAlign:"left"}}>
                <div style={{fontSize:26,marginBottom:10}}>{c.icon}</div>
                <div style={{fontWeight:800,fontSize:14,color:"#fff",marginBottom:5}}>{c.t}</div>
                <div style={{fontSize:12,color:"rgba(255,255,255,.3)",lineHeight:1.6}}>{c.d}</div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* HOW IT WORKS (visible when logged out) */}
      {!user&&<HowItWorks/>}

      {/* DASHBOARD */}
      {user&&(
        <div style={{position:"relative",zIndex:2,maxWidth:1300,margin:"0 auto",padding:"32px 24px"}}>
          <div style={{marginBottom:24,animation:"slideUp .4s ease"}}>
            <h1 style={{fontSize:26,fontWeight:800,color:"#fff",marginBottom:4}}>Hey {user} 🔥</h1>
            <p style={{fontSize:13,color:"rgba(255,255,255,.3)"}}>Your burn dashboard — simulated mode active.</p>
          </div>

          {/* KPIs */}
          <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:14,marginBottom:28}}>
            {[
              {l:"TOKENS CONNECTED",v:tokens.length,s:"integrations",i:"🪙",c:"#ff8c42"},
              {l:"TOTAL INCINERATED",v:fmt(totalBurned),s:"tokens gone forever",i:"🔥",c:"#ff4500"},
              {l:"ACTIVE BURNS",   v:activeCount,s:"running now",i:"⚡",c:"#ffd700"},
              {l:"PENDING REWARDS",v:fmt(totalPending),s:"awaiting claim",i:"💰",c:"#80ff80"},
            ].map((k,i)=>(
              <div key={k.l} className="glass" style={{padding:"20px 20px",animation:`slideUp .4s ease ${i*.07}s both`,display:"flex",flexDirection:"column",alignItems:"center",textAlign:"center"}}>
                <div style={{fontSize:28,marginBottom:10,lineHeight:1}}>{k.i}</div>
                <div style={{fontSize:10,color:"rgba(255,255,255,.28)",fontWeight:700,letterSpacing:1,marginBottom:8}}>{k.l}</div>
                <div style={{fontSize:32,fontWeight:800,color:k.c,lineHeight:1,marginBottom:4}}>{k.v}</div>
                <div style={{fontSize:11,color:"rgba(255,255,255,.2)",fontWeight:600}}>{k.s}</div>
              </div>
            ))}
          </div>

          {/* 2-col */}
          <div style={{display:"grid",gridTemplateColumns:"1fr 350px",gap:20,alignItems:"start"}}>
            <div style={{display:"flex",flexDirection:"column",gap:18}}>
              {/* tokens header */}
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"center"}}>
                <div>
                  <div style={{fontSize:17,fontWeight:800,color:"#fff"}}>Your Tokens</div>
                  <div style={{fontSize:12,color:"rgba(255,255,255,.28)"}}>Click to expand — logs, settings, deposit address</div>
                </div>
                <button className="btn-fire" onClick={()=>setShowAttach(true)} style={{padding:"9px 18px",fontSize:13}}>+ Attach Token</button>
              </div>

              {tokens.map(t=>(
                <TokenCard key={t.id} token={t} logs={allLogs.filter(l=>l.tokenId===t.id)} allLogs={allLogs}
                  onUpdate={u=>setTokens(p=>p.map(x=>x.id===u.id?u:x))}/>
              ))}

              {/* chart */}
              <div className="glass" style={{padding:"22px 24px"}}>
                <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:18}}>
                  <div>
                    <div style={{fontSize:15,fontWeight:800,color:"#fff"}}>Burn Activity</div>
                    <div style={{fontSize:12,color:"rgba(255,255,255,.28)"}}>Last 7 days</div>
                  </div>
                  <div style={{fontSize:12,color:"#ff8c42",fontWeight:700}}>{fmt(CHART_DATA.reduce((a,d)=>a+d.v,0))} total</div>
                </div>
                <BurnChart data={CHART_DATA}/>
              </div>
            </div>

            {/* right col */}
            <div style={{display:"flex",flexDirection:"column",gap:16}}>
              <div className="glass" style={{padding:"20px 22px"}}>
                <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:16}}>
                  <div style={{fontSize:14,fontWeight:800,color:"#fff"}}>Live Feed</div>
                  <div style={{display:"flex",alignItems:"center",gap:5}}>
                    <div style={{width:5,height:5,borderRadius:"50%",background:"#ff6a00",animation:"pulse-dot 1.5s infinite"}}/>
                    <span style={{fontSize:10,color:"#ff8c42",fontWeight:700}}>REAL-TIME</span>
                  </div>
                </div>
                <LiveFeed events={feed}/>
              </div>

              <div className="glass" style={{padding:"20px 22px"}}>
                <div style={{fontSize:14,fontWeight:800,color:"#fff",marginBottom:14}}>Protocol Stats</div>
                {[
                  ["All-Time Burned",fmt(totalBurned)+" tokens"],
                  ["Total Transactions",fmtFull(tokens.reduce((a,t)=>a+t.txCount,0))],
                  ["Active Schedulers",`${activeCount} / ${tokens.length}`],
                  ["Avg Burn Interval","4m 12s"],
                ].map(([k,v])=>(
                  <div key={k} style={{display:"flex",justifyContent:"space-between",padding:"9px 0",borderBottom:"1px solid rgba(255,255,255,.04)",fontSize:13}}>
                    <span style={{color:"rgba(255,255,255,.32)",fontWeight:600}}>{k}</span>
                    <span style={{color:"#fff",fontWeight:700}}>{v}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      )}

      <footer style={{position:"relative",zIndex:2,padding:"28px 24px",borderTop:"1px solid rgba(255,255,255,.04)",marginTop:40,textAlign:"center"}}>
        <div style={{fontSize:12,color:"rgba(255,255,255,.13)"}}>🔥 EMBER Burn Protocol · Powered by $EMBER · Not financial advice</div>
      </footer>

      {showLogin&&<LoginModal onClose={()=>setShowLogin(false)} onLogin={u=>{setUser(u);setShowLogin(false);}}/>}
      {showAttach&&<AttachModal onClose={()=>setShowAttach(false)} onAttach={t=>setTokens(p=>[...p,t])}/>}
    </>
  );
}
