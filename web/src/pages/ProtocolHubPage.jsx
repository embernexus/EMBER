import { DOC_SECTIONS, DOC_QUICK_LINKS, DOC_FAQ } from "../config/content";
import { useI18n } from "../i18n/I18nProvider";

export default function ProtocolHubPage() {
  const { t } = useI18n();
  const sections = DOC_SECTIONS;
  const quickLinks = DOC_QUICK_LINKS;
  const faq = DOC_FAQ;

  return (
    <section style={{position:"relative",zIndex:2,maxWidth:1260,margin:"0 auto",padding:"84px 24px 80px"}}>
      <div style={{textAlign:"center",marginBottom:26}}>
        <div style={{display:"inline-flex",alignItems:"center",gap:8,padding:"6px 12px",borderRadius:999,border:"1px solid rgba(255,106,0,.24)",background:"rgba(255,106,0,.08)",fontSize:11,fontWeight:700,letterSpacing:.8,color:"#ff9f5a",textTransform:"uppercase",marginBottom:14}}>
          <span>{"\u{1F4DA}"}</span>
          <span>{t("docu.knowledgeBase")}</span>
        </div>
        <p style={{fontSize:15,color:"rgba(255,255,255,.45)",maxWidth:780,margin:"0 auto",lineHeight:1.7}}>
          {t("docu.subtitle")}
        </p>
      </div>

      <div className="hub-layout">
        <aside className="glass hub-aside" style={{position:"sticky",top:118,padding:"14px 12px",textAlign:"center"}}>
          <div style={{fontSize:10,fontWeight:800,letterSpacing:.9,color:"#ff9f5a",textTransform:"uppercase",marginBottom:8,textAlign:"center"}}>{t("docu.sections")}</div>
          <div style={{display:"flex",flexDirection:"column",gap:6,marginBottom:12}}>
            {sections.map(s=>(
              <a key={s.id} href={`#hub-${s.id}`} style={{textDecoration:"none",fontSize:12,color:"rgba(255,255,255,.66)",padding:"8px 9px",borderRadius:8,border:"1px solid rgba(255,255,255,.06)",background:"rgba(255,255,255,.02)",textAlign:"center"}}>
                {s.title}
              </a>
            ))}
          </div>
          <div style={{fontSize:10,fontWeight:800,letterSpacing:.9,color:"#ff9f5a",textTransform:"uppercase",marginBottom:8,textAlign:"center"}}>{t("docu.quickApi")}</div>
          <div style={{display:"flex",flexDirection:"column",gap:6}}>
            {quickLinks.map(item=>(
              <div key={item.label} style={{padding:"8px 9px",borderRadius:8,border:"1px solid rgba(255,255,255,.06)",background:"rgba(255,255,255,.02)",textAlign:"center"}}>
                <div style={{fontSize:10,color:"rgba(255,255,255,.45)",fontWeight:700,marginBottom:4,textAlign:"center"}}>{item.label}</div>
                <div className="mono" style={{fontSize:11,color:"rgba(255,255,255,.8)",textAlign:"center"}}>{item.value}</div>
              </div>
            ))}
          </div>
        </aside>

        <div style={{display:"flex",flexDirection:"column",gap:12}}>
          {sections.map((s,i)=>(
            <article key={s.id} id={`hub-${s.id}`} className="glass" style={{padding:"16px 16px 14px",border:"1px solid rgba(255,255,255,.09)",scrollMarginTop:126,animation:`slideUp .35s ease ${i*.05}s both`,textAlign:"center"}}>
              <div style={{fontSize:18,color:"#fff",fontWeight:800,marginBottom:8,textAlign:"center"}}>{s.title}</div>
              <div style={{fontSize:13,color:"rgba(255,255,255,.68)",lineHeight:1.7,marginBottom:10,textAlign:"center"}}>{s.text}</div>
              <div style={{display:"flex",flexDirection:"column",gap:7}}>
                {s.points.map(point=>(
                  <div key={point} style={{display:"flex",gap:8,alignItems:"flex-start",justifyContent:"center",fontSize:13,color:"rgba(255,255,255,.72)",lineHeight:1.55,textAlign:"center"}}>
                    <span style={{color:"#ff8c42",marginTop:1}}>{"\u2022"}</span>
                    <span>{point}</span>
                  </div>
                ))}
              </div>
            </article>
          ))}

          <div className="glass" style={{padding:"16px 16px 12px",border:"1px solid rgba(255,255,255,.09)",textAlign:"center"}}>
            <div style={{fontSize:11,color:"#ff9f5a",fontWeight:800,letterSpacing:.9,textTransform:"uppercase",marginBottom:10,textAlign:"center"}}>{t("docu.frequentlyAsked")}</div>
            <div style={{display:"flex",flexDirection:"column",gap:10}}>
              {faq.map(item=>(
                <div key={item.q} style={{padding:"10px 10px",borderRadius:10,border:"1px solid rgba(255,255,255,.07)",background:"rgba(255,255,255,.02)",textAlign:"center"}}>
                  <div style={{fontSize:13,color:"#fff",fontWeight:700,marginBottom:5,textAlign:"center"}}>{item.q}</div>
                  <div style={{fontSize:12,color:"rgba(255,255,255,.62)",lineHeight:1.65,textAlign:"center"}}>{item.a}</div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
