import { fmtFull } from "../lib/format";

export default function BurnChart({ data }) {
  const max = Math.max(...data.map(d => d.v));
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
