import { ROADMAP_PHASES } from "../config/content";
import { useI18n } from "../i18n/I18nProvider";

export default function RoadmapPage() {
  const { t } = useI18n();
  const phases = ROADMAP_PHASES;

  return (
    <section style={{position:"relative",zIndex:2,maxWidth:1200,margin:"0 auto",padding:"84px 24px 80px"}}>
      <div style={{textAlign:"center",marginBottom:42}}>
        <div style={{display:"inline-flex",alignItems:"center",gap:8,padding:"6px 12px",borderRadius:999,border:"1px solid rgba(255,106,0,.24)",background:"rgba(255,106,0,.08)",fontSize:11,fontWeight:700,letterSpacing:.8,color:"#ff9f5a",textTransform:"uppercase",marginBottom:14}}>
          <span>{"\u{1F5FA}\uFE0F"}</span>
          <span>{t("roadmap.roadmap")}</span>
        </div>
        <p style={{fontSize:16,color:"rgba(255,255,255,.5)",maxWidth:760,margin:"0 auto",lineHeight:1.7}}>
          {t("roadmap.subtitle")}
        </p>
        <div style={{display:"flex",justifyContent:"center",flexWrap:"wrap",gap:8,marginTop:14}}>
          {phases.map(p=>(
            <div key={p.status} style={{fontSize:11,fontWeight:700,letterSpacing:.7,padding:"5px 10px",borderRadius:999,background:p.tone,border:`1px solid ${p.border}`,color:p.text,textTransform:"uppercase"}}>
              {p.status}
            </div>
          ))}
        </div>
      </div>

      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(260px,1fr))",gap:16}}>
        {phases.map((p,i)=>(
          <div key={p.phase} className="glass" style={{padding:"22px 20px",animation:`slideUp .45s ease ${i*.08}s both`,border:`1px solid ${p.border}`,textAlign:"center"}}>
            <div style={{display:"flex",alignItems:"center",justifyContent:"center",gap:10,marginBottom:14,flexWrap:"wrap",flexDirection:"column"}}>
              <div style={{fontSize:18,fontWeight:800,color:"#fff"}}>{p.phase}</div>
              <div style={{fontSize:11,fontWeight:700,letterSpacing:.7,padding:"5px 10px",borderRadius:999,background:p.tone,border:`1px solid ${p.border}`,color:p.text}}>
                {p.status}
              </div>
            </div>
            <div style={{display:"flex",flexDirection:"column",gap:8}}>
              {p.items.map(it=>(
                <div key={it} style={{display:"flex",gap:8,alignItems:"flex-start",justifyContent:"center",fontSize:13,color:"rgba(255,255,255,.7)",lineHeight:1.55,textAlign:"center"}}>
                  <span style={{color:"#ff8c42",marginTop:1}}>{"\u2022"}</span>
                  <span>{it}</span>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}
