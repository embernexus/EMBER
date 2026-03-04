import { fmt } from "../../lib/format";

function normalizeImageUrl(url) {
  const raw = String(url || "").trim();
  if (!raw) return "";
  if (raw.startsWith("ipfs://")) {
    return `https://ipfs.io/ipfs/${raw.slice("ipfs://".length).replace(/^ipfs\//i, "")}`;
  }
  if (raw.startsWith("ar://")) {
    return `https://arweave.net/${raw.slice("ar://".length)}`;
  }
  return raw;
}

function activeStatus(token) {
  const bot = String(token?.selectedBot || token?.moduleType || "burn");
  if (!token?.active) return "PAUSED";
  if (bot === "volume") return "VOLUME";
  if (bot === "market_maker") return "MM";
  return "BURNING";
}

export default function TokenTicker({ tokens }) {
  if(!tokens.length){
    return (
      <div style={{position:"fixed",top:64,left:0,right:0,zIndex:110,background:"rgba(0,0,0,.55)",borderBottom:"1px solid rgba(255,106,0,.12)",backdropFilter:"blur(10px)",height:38,display:"flex",alignItems:"center",justifyContent:"center",overflow:"hidden"}}>
        <span style={{fontSize:11,color:"rgba(255,255,255,.46)",letterSpacing:.6,fontWeight:600}}>No active token feeds yet</span>
      </div>
    );
  }
  const items = [...tokens, ...tokens];
  return (
    <div style={{position:"fixed",top:64,left:0,right:0,zIndex:110,background:"rgba(0,0,0,.55)",borderBottom:"1px solid rgba(255,106,0,.12)",backdropFilter:"blur(10px)",height:38,display:"flex",alignItems:"center",overflow:"hidden"}}>
      <div style={{position:"absolute",left:0,top:0,bottom:0,width:60,background:"linear-gradient(90deg,rgba(0,0,0,.8),transparent)",zIndex:2,pointerEvents:"none"}}/>
      <div style={{position:"absolute",right:0,top:0,bottom:0,width:60,background:"linear-gradient(270deg,rgba(0,0,0,.8),transparent)",zIndex:2,pointerEvents:"none"}}/>
      <div className="ticker-wrap">
        <div className="ticker-track">
          {items.map((t,i)=>(
            <div key={i} style={{display:"flex",alignItems:"center",gap:10,padding:"0 28px",borderRight:"1px solid rgba(255,255,255,.05)",height:38,whiteSpace:"nowrap"}}>
              <div style={{position:"relative",width:20,height:20,borderRadius:6,background:"linear-gradient(135deg,#ff6a00,#cc2200)",display:"flex",alignItems:"center",justifyContent:"center",fontSize:10,fontWeight:800,flexShrink:0,overflow:"hidden"}}>
                <span style={{position:"relative",zIndex:0,color:"#fff"}}>{t.symbol[0]}</span>
                {normalizeImageUrl(t.pictureUrl) && (
                  <img
                    src={normalizeImageUrl(t.pictureUrl)}
                    alt={`${t.symbol} token`}
                    style={{position:"absolute",inset:0,width:20,height:20,objectFit:"cover",zIndex:1,border:"1px solid rgba(255,255,255,.2)"}}
                    onError={(e)=>{ e.currentTarget.remove(); }}
                  />
                )}
              </div>
              <span style={{fontWeight:700,fontSize:12,color:"#fff"}}>${t.symbol}</span>
              <span style={{fontSize:11,color:"rgba(255,255,255,.3)"}}>{t.name}</span>
              <span style={{fontSize:10,fontWeight:700,color:t.active?"#ff8c42":"rgba(255,255,255,.25)",display:"flex",alignItems:"center",gap:4}}>
                {t.active&&<span style={{width:5,height:5,borderRadius:"50%",background:"#ff6a00",display:"inline-block",animation:"pulse-dot 2s infinite"}}/>}
                {activeStatus(t)}
              </span>
              <span style={{fontSize:11,color:"rgba(255,255,255,.4)"}}>{"\u{1F525}"} {fmt(t.burned)}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
