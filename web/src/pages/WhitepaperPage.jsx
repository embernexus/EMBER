import { WHITEPAPER_SECTIONS } from "../config/content";
import {
  EMBER_DEV_WALLET,
  EMBER_TREASURY_WALLET,
  EMBER_TOKEN_CONTRACT,
  SOLANA_INCINERATOR,
} from "../config/site";
import { useI18n } from "../i18n/I18nProvider";
import { solscanAddr } from "../lib/format";

function resolveWhitepaperTokenBullets() {
  return [
    { label: "Token contract:", address: `${EMBER_TOKEN_CONTRACT} (to be published at launch).` },
    { label: "Creator wallet reference:", address: EMBER_DEV_WALLET, href: solscanAddr(EMBER_DEV_WALLET) },
    EMBER_TREASURY_WALLET.length >= 32 && EMBER_TREASURY_WALLET !== "TBD"
      ? { label: "Treasury wallet:", address: EMBER_TREASURY_WALLET, href: solscanAddr(EMBER_TREASURY_WALLET) }
      : { label: "Treasury wallet:", address: "TBD (will be published)." },
    { label: "Public burn destination:", address: SOLANA_INCINERATOR, href: solscanAddr(SOLANA_INCINERATOR) },
    { text: "Execution + deposit wallets use EMBR/EMBER vanity prefixes for transparent, branded on-chain attribution." },
  ];
}

export default function WhitepaperPage() {
  const { t } = useI18n();
  const sections = WHITEPAPER_SECTIONS.map((s) =>
    s.id === "token" ? { ...s, bullets: resolveWhitepaperTokenBullets() } : s
  );

  return (
    <section style={{position:"relative",zIndex:2,maxWidth:1260,margin:"0 auto",padding:"84px 24px 80px"}}>
      <div style={{textAlign:"center",marginBottom:28}}>
        <div style={{display:"inline-flex",alignItems:"center",gap:8,padding:"6px 12px",borderRadius:999,border:"1px solid rgba(255,106,0,.25)",background:"rgba(255,106,0,.08)",fontSize:11,fontWeight:700,letterSpacing:.8,color:"#ff9f5a",textTransform:"uppercase",marginBottom:14}}>
          <span>{"\u{1F4D8}"}</span>
          <span>{t("whitepaper.emberDocumentation")}</span>
        </div>
        <p style={{fontSize:14,color:"rgba(255,255,255,.45)",maxWidth:820,margin:"0 auto",lineHeight:1.7}}>
          {t("whitepaper.subtitle")}
        </p>
      </div>

      <div className="paper-layout">
        <aside className="glass paper-nav" style={{position:"sticky",top:118,padding:"14px 14px 10px",textAlign:"center"}}>
          <div style={{fontSize:10,fontWeight:800,letterSpacing:.9,color:"#ff9f5a",textTransform:"uppercase",marginBottom:10,textAlign:"center"}}>{t("whitepaper.sections")}</div>
          <div style={{display:"flex",flexDirection:"column",gap:6}}>
            {sections.map(s=>(
              <a key={s.id} href={`#wp-${s.id}`} style={{textDecoration:"none",fontSize:12,color:"rgba(255,255,255,.62)",padding:"8px 9px",borderRadius:8,border:"1px solid rgba(255,255,255,.06)",background:"rgba(255,255,255,.02)",textAlign:"center"}}>
                {s.title}
              </a>
            ))}
          </div>
        </aside>

        <div style={{display:"flex",flexDirection:"column",gap:14}}>
          {sections.map(s=>(
            <article key={s.id} id={`wp-${s.id}`} className="glass" style={{padding:"18px 18px 16px",scrollMarginTop:126,border:"1px solid rgba(255,255,255,.09)",textAlign:"center"}}>
              <h3 style={{fontSize:18,fontWeight:800,color:"#fff",marginBottom:10,textAlign:"center"}}>{s.title}</h3>
              {s.paragraphs?.map(p=>(
                <p key={p} style={{fontSize:13,color:"rgba(255,255,255,.68)",lineHeight:1.7,marginBottom:10,textAlign:"center"}}>{p}</p>
              ))}
              {s.bullets?.length ? (
                <div style={{display:"flex",flexDirection:"column",gap:8,marginBottom:s.paragraphs2?.length?10:0}}>
                  {s.bullets.map((b, idx)=>(
                    <div key={typeof b === "string" ? b : `${b.text}-${idx}`} style={{display:"flex",gap:8,alignItems:"flex-start",justifyContent:"center",fontSize:13,color:"rgba(255,255,255,.72)",lineHeight:1.6,textAlign:"center"}}>
                      <span style={{color:"#ff8c42",marginTop:1}}>{"\u2022"}</span>
                      {typeof b === "string" ? (
                        <span>{b}</span>
                      ) : b.href && b.address ? (
                        <span>
                          {b.label}{" "}
                          <a
                            href={b.href}
                            target="_blank"
                            rel="noopener noreferrer"
                            style={{color:"rgba(255,255,255,.85)",textDecoration:"underline",textUnderlineOffset:2}}
                          >
                            {b.address}
                          </a>
                        </span>
                      ) : b.href ? (
                        <a
                          href={b.href}
                          target="_blank"
                          rel="noopener noreferrer"
                          style={{color:"rgba(255,255,255,.85)",textDecoration:"underline",textUnderlineOffset:2}}
                        >
                          {b.text}
                        </a>
                      ) : b.address ? (
                        <span>{b.label} {b.address}</span>
                      ) : (
                        <span>{b.text}</span>
                      )}
                    </div>
                  ))}
                </div>
              ) : null}
              {s.paragraphs2?.map(p=>(
                <p key={p} style={{fontSize:13,color:"rgba(255,255,255,.68)",lineHeight:1.7,textAlign:"center"}}>{p}</p>
              ))}
            </article>
          ))}
        </div>
      </div>
    </section>
  );
}
