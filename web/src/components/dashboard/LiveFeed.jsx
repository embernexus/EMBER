import { EVT_META } from "../../config/appConstants";
import { fmtAge } from "../../lib/format";
import { TxLink } from "./TokenCard";

export default function LiveFeed({ events }) {
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
