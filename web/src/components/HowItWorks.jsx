import { HOW_IT_WORKS_STEPS } from "../config/content";
import {
  EMBER_DEV_WALLET,
  EMBER_TREASURY_WALLET,
  EMBER_TOKEN_CONTRACT,
  SOLANA_WEBSITE_URL,
} from "../config/site";
import { useI18n } from "../i18n/I18nProvider";
import { solscanAddr } from "../lib/format";

export default function HowItWorks() {
  const { t } = useI18n();
  const steps = HOW_IT_WORKS_STEPS;

  return (
    <section id="how-it-works" style={{position:"relative",zIndex:2,maxWidth:1100,margin:"0 auto",padding:"100px 24px 80px"}}>
      <div style={{textAlign:"center",marginBottom:64}}>
        <div style={{fontSize:11,fontWeight:700,letterSpacing:3,color:"#ff8c42",marginBottom:14,textTransform:"uppercase"}}>{t("how.heading")}</div>
        <h2 style={{fontSize:"clamp(32px,5vw,54px)",fontWeight:800,color:"#fff",lineHeight:1.1,marginBottom:16}}>
          {t("how.title1")}<br/><span style={{background:"linear-gradient(135deg,#ff8c42,#ff4500)",WebkitBackgroundClip:"text",WebkitTextFillColor:"transparent"}}>{t("how.title2")}</span>
        </h2>
        <p style={{fontSize:16,color:"rgba(255,255,255,.35)",maxWidth:520,margin:"0 auto",lineHeight:1.7}}>
          {t("how.subtitle")}
        </p>
      </div>

      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(300px,1fr))",gap:20}}>
        {steps.map((s,i)=>(
          <div key={s.n} className="glass" style={{padding:"28px 26px",animation:`slideUp .5s ease ${i*.08}s both`,textAlign:"center"}}>
            <div style={{display:"flex",justifyContent:"center",marginBottom:16}}>
              <div style={{width:40,height:40,borderRadius:10,background:"rgba(255,106,0,.1)",border:"1px solid rgba(255,106,0,.2)",display:"flex",alignItems:"center",justifyContent:"center",fontSize:20,flexShrink:0}}>{s.icon}</div>
            </div>
            <div style={{fontWeight:800,fontSize:16,color:"#fff",marginBottom:10}}>{s.title}</div>
            <div style={{fontSize:13,color:"rgba(255,255,255,.38)",lineHeight:1.75}}>{s.body}</div>
          </div>
        ))}
      </div>

      <div className="glass" style={{marginTop:18,padding:"18px 20px",textAlign:"center",border:"1px solid rgba(255,255,255,.1)"}}>
        <div style={{fontSize:11,fontWeight:700,letterSpacing:1.4,color:"#ff9f5a",textTransform:"uppercase",marginBottom:8}}>
          EMBR / EMBER Wallet Transparency
        </div>
        <div style={{fontSize:13,color:"rgba(255,255,255,.68)",lineHeight:1.7,maxWidth:920,margin:"0 auto"}}>
          EMBER generates branded EMBR/EMBER bot and deposit wallets for transparent on-chain attribution. Keys are encrypted at rest with AES-256-GCM and only decrypted server-side during execution signing, so wallet custody stays protected while all activity remains publicly verifiable.
        </div>
      </div>

      <div className="glass" style={{marginTop:28,padding:"24px 26px",textAlign:"center"}}>
        <div style={{fontSize:11,fontWeight:700,letterSpacing:1.8,color:"#ff8c42",textTransform:"uppercase",marginBottom:8}}>
          {t("how.feeTransparency")}
        </div>
        <div style={{fontSize:14,color:"rgba(255,255,255,.72)",lineHeight:1.75,marginBottom:14,maxWidth:860,marginInline:"auto"}}>
          {t("how.feeBody")}
        </div>
        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(220px,1fr))",gap:10}}>
          <div style={{background:"rgba(255,255,255,.03)",border:"1px solid rgba(255,255,255,.08)",borderRadius:10,padding:"12px 14px",textAlign:"center"}}>
            <div style={{fontSize:18,fontWeight:800,color:"#fff",marginBottom:4}}>50% Treasury Allocation</div>
            <div style={{fontSize:12,color:"rgba(255,255,255,.55)",lineHeight:1.6}}>
              {t("how.treasuryBody")}
            </div>
          </div>
          <div style={{background:"rgba(255,106,0,.08)",border:"1px solid rgba(255,106,0,.2)",borderRadius:10,padding:"12px 14px",textAlign:"center"}}>
            <div style={{fontSize:18,fontWeight:800,color:"#ffb07a",marginBottom:4}}>50% EMBER Buyback + Burn</div>
            <div style={{fontSize:12,color:"rgba(255,255,255,.62)",lineHeight:1.6}}>
              {t("how.buybackBody")}
            </div>
          </div>
        </div>
        <div style={{fontSize:11,fontWeight:700,letterSpacing:1.2,color:"#ff9f5a",textTransform:"uppercase",marginTop:14,marginBottom:8}}>
          {t("how.claimFeeTitle")}
        </div>
        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(220px,1fr))",gap:10}}>
          <div style={{background:"rgba(255,255,255,.03)",border:"1px solid rgba(255,255,255,.08)",borderRadius:10,padding:"12px 14px",textAlign:"center"}}>
            <div style={{fontSize:18,fontWeight:800,color:"#fff",marginBottom:4}}>{t("how.claimTreasuryTitle")}</div>
            <div style={{fontSize:12,color:"rgba(255,255,255,.55)",lineHeight:1.6}}>
              {t("how.claimTreasuryBody")}
            </div>
          </div>
          <div style={{background:"rgba(255,106,0,.08)",border:"1px solid rgba(255,106,0,.2)",borderRadius:10,padding:"12px 14px",textAlign:"center"}}>
            <div style={{fontSize:18,fontWeight:800,color:"#ffb07a",marginBottom:4}}>{t("how.claimBuybackTitle")}</div>
            <div style={{fontSize:12,color:"rgba(255,255,255,.62)",lineHeight:1.6}}>
              {t("how.claimBuybackBody")}
            </div>
          </div>
        </div>
      </div>

      <div style={{marginTop:48,background:"linear-gradient(135deg,rgba(255,80,0,.1),rgba(200,20,0,.08))",border:"1px solid rgba(255,80,0,.2)",borderRadius:16,padding:"28px 32px"}}>
        <div style={{display:"flex",gap:10,alignItems:"center",justifyContent:"center",flexDirection:"column",marginBottom:16,textAlign:"center"}}>
          <div style={{fontSize:36}}>{"\u{1F525}"}</div>
          <div style={{maxWidth:860}}>
            <div style={{fontWeight:800,fontSize:17,color:"#fff",marginBottom:6}}>The EMBER Incinerator</div>
            <div style={{fontSize:13,color:"rgba(255,255,255,.4)",lineHeight:1.7}}>
              {t("how.incineratorBody")}
            </div>
          </div>
        </div>

        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(260px,1fr))",gap:10}}>
          <div style={{background:"rgba(0,0,0,.25)",border:"1px solid rgba(255,255,255,.12)",borderRadius:10,padding:"12px 14px",textAlign:"center"}}>
            <div style={{fontSize:11,fontWeight:700,letterSpacing:.8,color:"rgba(255,255,255,.72)",textTransform:"uppercase",marginBottom:6,textAlign:"center"}}>EMBER Creator Wallet</div>
            <div style={{fontSize:12,color:"rgba(255,255,255,.5)",lineHeight:1.6,marginBottom:8}}>
              Original dev wallet used to create the EMBER token and referenced for project transparency.
            </div>
            <a href={solscanAddr(EMBER_DEV_WALLET)} target="_blank" rel="noopener noreferrer" title="View EMBER creator wallet on Solscan" style={{fontSize:11,color:"rgba(255,255,255,.74)",fontFamily:"'JetBrains Mono',monospace",background:"rgba(0,0,0,.35)",padding:"8px 10px",borderRadius:8,border:"1px solid rgba(255,255,255,.15)",whiteSpace:"normal",overflowWrap:"anywhere",wordBreak:"break-word",textDecoration:"none",display:"block",width:"100%"}}>
              {EMBER_DEV_WALLET} {"\u2197"}
            </a>
          </div>

          <div style={{background:"rgba(0,0,0,.25)",border:"1px solid rgba(255,255,255,.12)",borderRadius:10,padding:"12px 14px",textAlign:"center"}}>
            <div style={{fontSize:11,fontWeight:700,letterSpacing:.8,color:"rgba(255,255,255,.72)",textTransform:"uppercase",marginBottom:6,textAlign:"center"}}>EMBER Treasury Wallet</div>
            <div style={{fontSize:12,color:"rgba(255,255,255,.5)",lineHeight:1.6,marginBottom:8}}>
              Treasury destination for 50% of creator rewards and the treasury-side 2.5% claim-fee allocation.
            </div>
            {EMBER_TREASURY_WALLET.length >= 32 && EMBER_TREASURY_WALLET !== "TBD" ? (
              <a href={solscanAddr(EMBER_TREASURY_WALLET)} target="_blank" rel="noopener noreferrer" title="View EMBER treasury wallet on Solscan" style={{fontSize:11,color:"rgba(255,255,255,.74)",fontFamily:"'JetBrains Mono',monospace",background:"rgba(0,0,0,.35)",padding:"8px 10px",borderRadius:8,border:"1px solid rgba(255,255,255,.15)",whiteSpace:"normal",overflowWrap:"anywhere",wordBreak:"break-word",textDecoration:"none",display:"block",width:"100%"}}>
                {EMBER_TREASURY_WALLET} {"\u2197"}
              </a>
            ) : (
              <div style={{fontSize:11,color:"rgba(255,255,255,.74)",fontFamily:"'JetBrains Mono',monospace",background:"rgba(0,0,0,.35)",padding:"8px 10px",borderRadius:8,border:"1px solid rgba(255,255,255,.15)",whiteSpace:"normal",overflowWrap:"anywhere",wordBreak:"break-word",display:"block",width:"100%"}}>
                Treasury address pending
              </div>
            )}
          </div>

          <div style={{background:"rgba(0,0,0,.25)",border:"1px solid rgba(255,255,255,.12)",borderRadius:10,padding:"12px 14px",textAlign:"center"}}>
            <div style={{fontSize:11,fontWeight:700,letterSpacing:.8,color:"rgba(255,255,255,.72)",textTransform:"uppercase",marginBottom:6,textAlign:"center"}}>EMBER Token Contract</div>
            <div style={{fontSize:12,color:"rgba(255,255,255,.5)",lineHeight:1.6,marginBottom:8}}>
              Official token contract address for $EMBER. Treasury destination for 50% of EMBER creator rewards.
            </div>
            <div style={{fontSize:11,color:"rgba(255,255,255,.74)",fontFamily:"'JetBrains Mono',monospace",background:"rgba(0,0,0,.35)",padding:"8px 10px",borderRadius:8,border:"1px solid rgba(255,255,255,.15)",whiteSpace:"normal",overflowWrap:"anywhere",wordBreak:"break-word",display:"block",width:"100%"}}>
              {EMBER_TOKEN_CONTRACT}
            </div>
          </div>
        </div>
      </div>

      <div className="glass" style={{marginTop:18,padding:"18px"}}>
        <div style={{borderRadius:12,overflow:"hidden",border:"1px solid rgba(255,255,255,.1)",background:"#0a0205"}}>
          <iframe
            title="Dexscreener Chart"
            src={`https://dexscreener.com/solana/${encodeURIComponent(
              EMBER_TOKEN_CONTRACT
            )}?embed=1&theme=dark&trades=0&info=0`}
            style={{width:"100%",height:"clamp(360px,55vh,520px)",border:"0"}}
            loading="lazy"
            allowFullScreen
          />
        </div>
      </div>
    </section>
  );
}
