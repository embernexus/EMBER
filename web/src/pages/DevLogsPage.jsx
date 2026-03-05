import { DEV_LOGS } from "../config/content";
import { useI18n } from "../i18n/I18nProvider";

export default function DevLogsPage() {
  const { t } = useI18n();
  const logs = DEV_LOGS;

  return (
    <section style={{position:"relative",zIndex:2,maxWidth:1240,margin:"0 auto",padding:"84px 24px 80px"}}>
      <div style={{textAlign:"center",marginBottom:26}}>
        <div style={{display:"inline-flex",alignItems:"center",gap:8,padding:"6px 12px",borderRadius:999,border:"1px solid rgba(255,106,0,.24)",background:"rgba(255,106,0,.08)",fontSize:11,fontWeight:700,letterSpacing:.8,color:"#ff9f5a",textTransform:"uppercase",marginBottom:14}}>
          <span>{"\u{1F4DD}"}</span>
          <span>{t("devlogs.protocolChangelog")}</span>
        </div>
        <p style={{fontSize:15,color:"rgba(255,255,255,.45)",maxWidth:760,margin:"0 auto",lineHeight:1.7}}>
          {t("devlogs.subtitle")}
        </p>
      </div>

      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(220px,1fr))",gap:12,marginBottom:16}}>
        {[
          [t("devlogs.latestBuild"), logs[0]?.version || t("devlogs.na")],
          [t("devlogs.releaseEntries"), String(logs.length)],
          [t("devlogs.currentChannel"), t("devlogs.production")],
          [t("devlogs.status"), t("devlogs.shipping")],
        ].map(([k,v])=>(
          <div key={k} className="glass" style={{padding:"12px 14px",border:"1px solid rgba(255,255,255,.08)",textAlign:"center"}}>
            <div style={{fontSize:10,color:"rgba(255,255,255,.42)",fontWeight:700,letterSpacing:.9,textTransform:"uppercase",marginBottom:6}}>{k}</div>
            <div style={{fontSize:18,color:"#fff",fontWeight:800,lineHeight:1.1}}>{v}</div>
          </div>
        ))}
      </div>

      <div style={{display:"grid",gridTemplateColumns:"minmax(0,1.6fr) minmax(0,1fr)",gap:14,alignItems:"start"}}>
        <div className="glass" style={{padding:"12px 12px 8px",border:"1px solid rgba(255,255,255,.09)"}}>
          {logs.map((log, i)=>(
            <article key={log.version} style={{padding:"14px 14px 16px",borderBottom:i===logs.length-1?"none":"1px solid rgba(255,255,255,.05)"}}>
              <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",gap:10,flexWrap:"wrap",marginBottom:8}}>
                <div style={{display:"inline-flex",alignItems:"center",gap:8}}>
                  <span style={{fontSize:12,padding:"4px 8px",borderRadius:999,border:"1px solid rgba(255,106,0,.28)",background:"rgba(255,106,0,.1)",color:"#ffae76",fontWeight:800,letterSpacing:.5}}>{log.version}</span>
                  <span style={{fontSize:11,color:"rgba(255,255,255,.45)",fontWeight:600}}>{log.date}</span>
                </div>
                <span style={{fontSize:10,padding:"4px 8px",borderRadius:999,border:"1px solid rgba(130,220,255,.25)",background:"rgba(130,220,255,.08)",color:"#8ad9ff",fontWeight:700,letterSpacing:.6,textTransform:"uppercase"}}>{log.channel}</span>
              </div>
              <div style={{fontSize:17,color:"#fff",fontWeight:800,marginBottom:7}}>{log.title}</div>
              <div style={{fontSize:13,color:"rgba(255,255,255,.66)",lineHeight:1.65,marginBottom:10}}>{log.summary}</div>
              <div style={{display:"flex",flexDirection:"column",gap:7}}>
                {log.changes.map(line=>(
                  <div key={line} style={{display:"flex",gap:8,alignItems:"flex-start",fontSize:13,color:"rgba(255,255,255,.72)",lineHeight:1.55}}>
                    <span style={{color:"#ff8c42",marginTop:1}}>{"\u2022"}</span>
                    <span>{line}</span>
                  </div>
                ))}
              </div>
            </article>
          ))}
        </div>

        <div style={{display:"flex",flexDirection:"column",gap:12}}>
          <div className="glass" style={{padding:"14px 14px 12px",border:"1px solid rgba(255,255,255,.09)",textAlign:"center"}}>
            <div style={{fontSize:11,color:"#ff9f5a",fontWeight:800,letterSpacing:.9,textTransform:"uppercase",marginBottom:8}}>{t("devlogs.releasePolicy")}</div>
            <div style={{fontSize:13,color:"rgba(255,255,255,.68)",lineHeight:1.7}}>
              {t("devlogs.releasePolicyBody")}
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
