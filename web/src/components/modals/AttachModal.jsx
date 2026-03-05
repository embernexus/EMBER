import { useEffect, useState } from "react";
import { apiResolveMint } from "../../api/client";
import { useI18n } from "../../i18n/I18nProvider";
import { fmtSec, solscanAddr } from "../../lib/format";

const BOT_MODE_OPTIONS = [
  { value: "burn", label: "Burn Bot" },
  { value: "volume", label: "Volume Bot" },
  { value: "market_maker", label: "Market Maker Bot (Coming Soon)", disabled: true },
];

const BOT_PRESETS = {
  burn: {
    claimSec: 120,
    burnSec: 300,
    splits: 1,
    claimEnabled: true,
    tradeWalletCount: 1,
    speed: 35,
    aggression: 35,
    minTradeSol: 0.01,
    maxTradeSol: 0.05,
  },
  volume: {
    claimSec: 90,
    burnSec: 240,
    splits: 3,
    claimEnabled: false,
    tradeWalletCount: 1,
    speed: 35,
    aggression: 35,
    minTradeSol: 0.01,
    maxTradeSol: 0.05,
  },
  market_maker: { claimSec: 60, burnSec: 180, splits: 4 },
};

function AddrLink({addr, label}) {
  if (!addr) return null;
  const display = label || `${addr.slice(0,6)}...${addr.slice(-4)}`;
  return (
    <a href={solscanAddr(addr)} target="_blank" rel="noopener noreferrer" className="tx-link mono"
      style={{fontSize:11}} onClick={e=>e.stopPropagation()}>
      {display} {"\u2197"}
    </a>
  );
}

function QR({seed=""}) {
  const [expanded, setExpanded] = useState(false);
  const src = `https://api.qrserver.com/v1/create-qr-code/?size=200x200&data=${encodeURIComponent(seed || "")}`;
  return (
    <div
      onClick={()=>setExpanded(v=>!v)}
      title="Click to resize"
      style={{background:"#fff",padding:5,borderRadius:8,display:"inline-flex",flexShrink:0,cursor:"pointer",transition:"transform .15s ease",transform:expanded?"scale(1.12)":"scale(1)"}}
    >
      <img src={src} alt="Deposit QR" width={72} height={72} style={{display:"block",borderRadius:4}} />
    </div>
  );
}

export default function AttachModal({onClose,onAttach,onGenerateDeposit}) {
  const { t } = useI18n();
  const [step,setStep]=useState(1);
  const [mint,setMint]=useState("");
  const [mintStatus,setMintStatus]=useState("idle");
  const [resolved,setResolved]=useState(null);
  const [f,setF]=useState({
    botMode:"burn",
    claimSec:120,
    burnSec:300,
    splits:1,
    claimEnabled:true,
    tradeWalletCount:1,
    speed:35,
    aggression:35,
    minTradeSol:0.01,
    maxTradeSol:0.05,
  });
  const [err,setErr]=useState("");
  const [dep,setDep]=useState("");
  const [pendingDepositId,setPendingDepositId]=useState("");
  const [submitting,setSubmitting]=useState(false);
  const [tokenImgVisible,setTokenImgVisible]=useState(true);

  const clamp01 = (v)=>Math.max(0.001, Number(v) || 0.001);
  const deriveMaxTradeSol = (minTrade, aggression) => {
    const min = clamp01(minTrade);
    const agg = Math.max(0, Math.min(100, Number(aggression) || 0));
    const cap = Math.max(0.2, min * 10);
    const out = min + (cap - min) * (agg / 100);
    return Number(out.toFixed(3));
  };
  const speedEverySec = (speed) => Math.max(3, 25 - Math.round((Math.max(0, Math.min(100, Number(speed) || 0)) / 100) * 20));

  useEffect(()=>{
    const cleanMint = mint.trim();
    if(cleanMint.length<32){setMintStatus("idle");setResolved(null);return;}
    let cancelled = false;
    setMintStatus("looking");
    const timer=setTimeout(async ()=>{
      try{
        const token = await apiResolveMint(cleanMint);
        if(cancelled) return;
        if(token?.symbol && token?.name){
          setResolved(token);
          setMintStatus("found");
        }else{
          setResolved(null);
          setMintStatus("notfound");
        }
      }catch{
        if(cancelled) return;
        setResolved(null);
        setMintStatus("notfound");
      }
    },450);
    return()=>{cancelled=true;clearTimeout(timer);};
  },[mint]);

  useEffect(()=>{
    setTokenImgVisible(true);
  },[resolved?.pictureUrl]);

  useEffect(()=>{
    if (f.botMode !== "volume") return;
    const derived = deriveMaxTradeSol(f.minTradeSol, f.aggression);
    if (Math.abs(Number(f.maxTradeSol || 0) - derived) < 0.0005) return;
    setF(prev=>({ ...prev, maxTradeSol: derived }));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  },[f.botMode, f.minTradeSol, f.aggression]);

  const next=async ()=>{
    setErr("");
    if(!mint.trim()||!resolved) return setErr(t("attach.errors.validMint"));
    if(f.claimSec<60) return setErr(t("attach.errors.intervalMin"));
    if(f.botMode==="burn"){
      if(f.burnSec<60) return setErr(t("attach.errors.intervalMin"));
      if(!Number.isFinite(f.splits)||f.splits<1) return setErr(t("attach.errors.splitMin"));
    }
    if(f.botMode==="volume"){
      const tw = Math.max(1, Math.min(5, Math.floor(Number(f.tradeWalletCount)||1)));
      const minSol = Number(f.minTradeSol || 0);
      const maxSol = Number(f.maxTradeSol || 0);
      if(tw<1||tw>5) return setErr("Trade wallets must be between 1 and 5.");
      if(!Number.isFinite(minSol) || !Number.isFinite(maxSol) || minSol<=0 || maxSol<=0){
        return setErr("Volume trade range must be valid SOL values.");
      }
      if(maxSol<minSol) return setErr("Max trade SOL must be greater than or equal to min trade SOL.");
    }
    if(typeof onGenerateDeposit !== "function") return setErr(t("attach.errors.generatorUnavailable"));
    setSubmitting(true);
    try {
      const result = await onGenerateDeposit();
      if(!result?.deposit || !result?.pendingDepositId){
        throw new Error(t("attach.errors.generatorInvalid"));
      }
      setDep(result.deposit);
      setPendingDepositId(result.pendingDepositId);
      setStep(2);
    } catch (e) {
      setErr(e?.message || t("attach.errors.generateFailed"));
    } finally {
      setSubmitting(false);
    }
  };

  const finish=async ()=>{
    const normalizedSplits = Math.max(1, Math.floor(Number(f.splits) || 1));
    const tradeWalletCount = Math.max(1, Math.min(5, Math.floor(Number(f.tradeWalletCount)||1)));
    const speed = Math.max(0, Math.min(100, Number(f.speed)||0));
    const aggression = Math.max(0, Math.min(100, Number(f.aggression)||0));
    const minTradeSol = Math.max(0.001, Number(f.minTradeSol)||0.01);
    const maxTradeSol = Math.max(minTradeSol, Number(f.maxTradeSol)||0.05);
    setErr("");
    if(!pendingDepositId || !dep) return setErr(t("attach.errors.generateFirst"));
    setSubmitting(true);
    try {
      await onAttach({
        ...resolved,
        mint:mint.trim(),
        ...f,
        selectedBot:f.botMode,
        pendingDepositId,
        splits:normalizedSplits,
        claimEnabled:Boolean(f.claimEnabled),
        tradeWalletCount,
        speed,
        aggression,
        minTradeSol,
        maxTradeSol,
      });
      onClose();
    } catch (e) {
      setErr(e?.message || t("attach.errors.attachFailed"));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div style={{position:"fixed",inset:0,zIndex:999,display:"flex",alignItems:"center",justifyContent:"center",animation:"fadeIn .2s ease"}} onClick={onClose}>
      <div style={{position:"absolute",inset:0,background:"rgba(0,0,0,.82)",backdropFilter:"blur(10px)"}}/>
      <div className="glass" onClick={e=>e.stopPropagation()} style={{position:"relative",zIndex:1,width:"min(520px,95vw)",padding:36,animation:"slideUp .25s ease",maxHeight:"90vh",overflowY:"auto",boxShadow:"0 32px 80px rgba(0,0,0,.6),0 0 60px rgba(255,106,0,.07)"}}>

        <div style={{position:"relative",display:"flex",justifyContent:"center",alignItems:"center",marginBottom:24}}>
            <div style={{display:"flex",flexDirection:"column",alignItems:"center"}}>
            <div style={{fontWeight:800,fontSize:21,color:"#fff",marginBottom:8,textAlign:"center"}}>{step===1?t("attach.configureToken"):t("attach.depositTitle")}</div>
            <div style={{display:"flex",gap:6,justifyContent:"center"}}>
              {[1,2].map(s=><div key={s} style={{height:3,width:48,borderRadius:2,background:s<=step?"linear-gradient(90deg,#ff6a00,#ff4500)":"rgba(255,255,255,.1)",transition:"background .3s"}}/>)}
            </div>
          </div>
          <button onClick={onClose} style={{position:"absolute",right:0,background:"none",border:"none",color:"rgba(255,255,255,.3)",fontSize:22,cursor:"pointer"}}>x</button>
        </div>

        {step===1&&(
          <div style={{display:"flex",flexDirection:"column",gap:18}}>
            <div>
              <label style={{display:"block",fontSize:11,color:"rgba(255,255,255,.35)",letterSpacing:1,marginBottom:7,fontWeight:600}}>TOKEN MINT ADDRESS</label>
              <input className="input-f" value={mint} onChange={e=>setMint(e.target.value)} placeholder={t("attach.placeholders.mint")}/>
              {mintStatus==="looking"&&(
                <div style={{marginTop:8,display:"flex",alignItems:"center",gap:6,fontSize:12,color:"rgba(255,255,255,.4)"}}>
                  <span style={{width:10,height:10,border:"2px solid rgba(255,255,255,.2)",borderTopColor:"#ff8c42",borderRadius:"50%",animation:"spin .6s linear infinite",display:"inline-block"}}/>
                  {t("attach.lookup")}
                </div>
              )}
              {mintStatus==="found"&&resolved&&(
                <div style={{marginTop:8,background:"rgba(255,106,0,.08)",border:"1px solid rgba(255,106,0,.2)",borderRadius:8,padding:"10px 14px",display:"flex",alignItems:"center",gap:10}}>
                    <span style={{fontSize:16}}>{t("attach.ok")}</span>
                  {resolved?.pictureUrl && tokenImgVisible && (
                    <img
                      src={resolved.pictureUrl}
                      alt={`${resolved.symbol || "Token"} logo`}
                      style={{width:28,height:28,borderRadius:8,objectFit:"cover",border:"1px solid rgba(255,255,255,.2)",flexShrink:0}}
                      onError={()=>setTokenImgVisible(false)}
                    />
                  )}
                  <div>
                    <div style={{fontWeight:700,color:"#fff",fontSize:14}}>${resolved.symbol} <span style={{fontWeight:400,color:"rgba(255,255,255,.5)",fontSize:12}}>- {resolved.name}</span></div>
                    <div style={{fontSize:11,color:"rgba(255,255,255,.3)",marginTop:2}}>{t("attach.tokenFound")}</div>
                  </div>
                </div>
              )}
              {mintStatus==="notfound"&&(
                <div style={{marginTop:8,fontSize:12,color:"#ff9f9f"}}>
                  Unable to resolve token metadata for this mint.
                </div>
              )}
            </div>

            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:14}}>
              {[{label:t("attach.tokenSymbol"),val:resolved?.symbol},{label:t("attach.tokenName"),val:resolved?.name}].map(fd=>(
                <div key={fd.label}>
                  <label style={{display:"block",fontSize:11,color:"rgba(255,255,255,.35)",letterSpacing:1,marginBottom:7,fontWeight:600}}>{fd.label.toUpperCase()}</label>
                  <input className="input-f" value={fd.val||""} readOnly disabled placeholder={t("attach.placeholders.autoMint")} style={{opacity:fd.val?1:.4,cursor:"not-allowed"}}/>
                </div>
              ))}
            </div>

            <div>
              <label style={{display:"block",fontSize:11,color:"rgba(255,255,255,.35)",letterSpacing:1,marginBottom:8,fontWeight:600}}>BOT MODE</label>
              <select
                className="input-f"
                value={f.botMode}
                onChange={e=>{
                  const botMode = e.target.value;
                  const preset = BOT_PRESETS[botMode] || BOT_PRESETS.burn;
                  setF(prev=>({
                    ...prev,
                    botMode,
                    claimSec:preset.claimSec,
                    burnSec:preset.burnSec,
                    splits:preset.splits,
                    claimEnabled:preset.claimEnabled ?? prev.claimEnabled,
                    tradeWalletCount:preset.tradeWalletCount ?? prev.tradeWalletCount,
                    speed:preset.speed ?? prev.speed,
                    aggression:preset.aggression ?? prev.aggression,
                    minTradeSol:preset.minTradeSol ?? prev.minTradeSol,
                    maxTradeSol:preset.maxTradeSol ?? prev.maxTradeSol,
                  }));
                }}
              >
                {BOT_MODE_OPTIONS.map(opt=>(
                  <option key={opt.value} value={opt.value} disabled={Boolean(opt.disabled)}>{opt.label}</option>
                ))}
              </select>
            </div>

            {f.botMode==="burn" ? (
              <>
                <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:14}}>
                  {[{label:t("attach.claimInterval"),key:"claimSec"},{label:t("attach.burnInterval"),key:"burnSec"}].map(fd=>(
                    <div key={fd.key}>
                      <label style={{display:"block",fontSize:11,color:"rgba(255,255,255,.35)",letterSpacing:1,marginBottom:7,fontWeight:600}}>{fd.label.toUpperCase()}</label>
                      <div style={{position:"relative"}}>
                        <input type="number" min={60} className="input-f" value={f[fd.key]} onChange={e=>setF({...f,[fd.key]:+e.target.value})} style={{paddingRight:30}}/>
                        <span style={{position:"absolute",right:12,top:"50%",transform:"translateY(-50%)",fontSize:11,color:"rgba(255,255,255,.25)"}}>s</span>
                      </div>
                      <div style={{fontSize:10,color:"rgba(255,255,255,.2)",marginTop:4}}>{t("attach.min60")}</div>
                    </div>
                  ))}
                </div>

                <div>
                  <label style={{display:"block",fontSize:11,color:"rgba(255,255,255,.35)",letterSpacing:1,marginBottom:8,fontWeight:600}}>SPLIT BUYS PER CYCLE</label>
                  <div style={{maxWidth:240}}>
                    <input type="number" min={1} step={1} className="input-f" value={f.splits} onChange={e=>setF({...f,splits:Math.max(1,Math.floor(Number(e.target.value)||1))})}/>
                  </div>
                  <div style={{fontSize:11,color:"rgba(255,255,255,.2)",marginTop:7}}>{f.splits===1?t("attach.oneExecution"):t("attach.multiExecution",{count:f.splits})}</div>
                  <div style={{fontSize:11,color:"rgba(255,255,255,.33)",marginTop:7}}>Burn Bot always keeps creator reward claiming enabled. External SOL deposits to the same wallet are also processed.</div>
                  <div style={{fontSize:11,color:"rgba(255,255,255,.33)",marginTop:7}}>If creator rewards are shared/designated from another wallet, claimable balance can appear with delay. It may show 0.00 for a few minutes before updating.</div>
                </div>
              </>
            ) : (
              <>
                <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:14}}>
                  <div>
                    <label style={{display:"block",fontSize:11,color:"rgba(255,255,255,.35)",letterSpacing:1,marginBottom:7,fontWeight:600}}>CLAIM INTERVAL</label>
                    <div style={{position:"relative"}}>
                      <input type="number" min={60} className="input-f" value={f.claimSec} onChange={e=>setF({...f,claimSec:+e.target.value})} style={{paddingRight:30}}/>
                      <span style={{position:"absolute",right:12,top:"50%",transform:"translateY(-50%)",fontSize:11,color:"rgba(255,255,255,.25)"}}>s</span>
                    </div>
                    <div style={{fontSize:10,color:"rgba(255,255,255,.2)",marginTop:4}}>{t("attach.min60")}</div>
                  </div>
                  <div>
                    <label style={{display:"block",fontSize:11,color:"rgba(255,255,255,.35)",letterSpacing:1,marginBottom:7,fontWeight:600}}>TRADE WALLETS</label>
                    <input type="number" min={1} max={5} step={1} className="input-f" value={f.tradeWalletCount} onChange={e=>setF({...f,tradeWalletCount:Math.max(1,Math.min(5,Math.floor(Number(e.target.value)||1)) )})}/>
                    <div style={{fontSize:10,color:"rgba(255,255,255,.2)",marginTop:4}}>Choose 1 to 5 wallets.</div>
                  </div>
                </div>

                <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:14}}>
                  <div>
                    <label style={{display:"block",fontSize:11,color:"rgba(255,255,255,.35)",letterSpacing:1,marginBottom:7,fontWeight:600}}>MIN TRADE SOL</label>
                    <input type="number" min={0.001} step={0.001} className="input-f" value={f.minTradeSol} onChange={e=>setF({...f,minTradeSol:e.target.value,maxTradeSol:deriveMaxTradeSol(e.target.value,f.aggression)})}/>
                  </div>
                  <div>
                    <label style={{display:"block",fontSize:11,color:"rgba(255,255,255,.35)",letterSpacing:1,marginBottom:7,fontWeight:600}}>MAX TRADE SOL (AUTO)</label>
                    <input type="number" readOnly className="input-f" value={Number(f.maxTradeSol||0).toFixed(3)} />
                  </div>
                </div>

                <div>
                  <label style={{display:"block",fontSize:11,color:"rgba(255,255,255,.35)",letterSpacing:1,marginBottom:7,fontWeight:600}}>SPEED ({Math.round(Number(f.speed)||0)})</label>
                  <input type="range" min={0} max={100} value={f.speed} className="input-f ember-range" onChange={e=>setF({...f,speed:Number(e.target.value)})}/>
                  <div style={{fontSize:11,color:"rgba(255,255,255,.35)",marginTop:6}}>Execution pace auto-updates to about every <span className="mono" style={{color:"#ff9f5a"}}>{speedEverySec(f.speed)}s</span>.</div>
                </div>

                <div>
                  <label style={{display:"block",fontSize:11,color:"rgba(255,255,255,.35)",letterSpacing:1,marginBottom:7,fontWeight:600}}>AGGRESSION ({Math.round(Number(f.aggression)||0)})</label>
                  <input type="range" min={0} max={100} value={f.aggression} className="input-f ember-range" onChange={e=>setF({...f,aggression:Number(e.target.value),maxTradeSol:deriveMaxTradeSol(f.minTradeSol,e.target.value)})}/>
                  <div style={{fontSize:11,color:"rgba(255,255,255,.35)",marginTop:6}}>Aggression auto-updates max trade size to <span className="mono" style={{color:"#ff9f5a"}}>{Number(f.maxTradeSol||0).toFixed(3)} SOL</span>.</div>
                </div>

                <label style={{display:"inline-flex",alignItems:"center",gap:10,fontSize:12,color:"rgba(255,255,255,.75)"}}>
                  <span className="ember-toggle">
                    <input type="checkbox" checked={Boolean(f.claimEnabled)} onChange={e=>setF({...f,claimEnabled:e.target.checked})}/>
                    <span className="ember-toggle-track" />
                  </span>
                  Enable creator reward claiming for this volume bot
                </label>
                <div style={{fontSize:11,color:"rgba(255,255,255,.33)",marginTop:6}}>If disabled, this module runs from external wallet funding only.</div>
                <div style={{fontSize:11,color:"rgba(255,255,255,.33)",marginTop:6}}>If creator rewards are shared/designated from another wallet, claimable balance can appear with delay. It may show 0.00 for a few minutes before updating.</div>
              </>
            )}

            {err&&<div style={{background:"rgba(255,64,96,.1)",border:"1px solid rgba(255,64,96,.2)",borderRadius:8,padding:"10px 14px",fontSize:13,color:"#ff8080"}}>{err}</div>}
            <button className="btn-fire" onClick={next} disabled={submitting} style={{padding:"13px",fontSize:14}}>{submitting?t("attach.generating"):t("attach.generateDeposit")}</button>
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
                  <button className="btn-ghost" onClick={()=>navigator.clipboard.writeText(dep).catch(()=>{})} style={{padding:"5px 14px",fontSize:11}}>Copy Address</button>
                </div>
              </div>
              <div style={{fontSize:11,color:"rgba(255,255,255,.4)",lineHeight:1.6,marginTop:10}}>
                Fund this address from creator rewards (when enabled), external SOL transfers, or both.
              </div>
              <div style={{fontSize:11,color:"rgba(255,255,255,.35)",lineHeight:1.6,marginTop:8}}>
                Shared/designated creator rewards are not always instant. It can take a few minutes before claimable rewards are visible.
              </div>
            </div>

            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:10}}>
              {[
                ["Token",`$${resolved?.symbol} - ${resolved?.name}`],
                ["Bot Mode",BOT_MODE_OPTIONS.find(opt=>opt.value===f.botMode)?.label || "Burn Bot"],
                ["Claim Every",fmtSec(f.claimSec)],
                ...(f.botMode==="volume"
                  ? [
                      ["Trade Wallets",`${Math.max(1, Math.min(5, Math.floor(Number(f.tradeWalletCount)||1)))}x`],
                      ["Trade Range",`${Number(f.minTradeSol||0).toFixed(3)} - ${Math.max(Number(f.minTradeSol||0), Number(f.maxTradeSol||0)).toFixed(3)} SOL`],
                    ]
                  : [
                      ["Burn Every",fmtSec(f.burnSec)],
                      ["Split Buys",`${f.splits}x per cycle`],
                    ]),
              ].map(([k,v])=>(
                <div key={k} style={{background:"rgba(255,255,255,.03)",borderRadius:8,padding:"10px 12px"}}>
                  <div style={{fontSize:10,color:"rgba(255,255,255,.3)",marginBottom:4,fontWeight:600,letterSpacing:.5}}>{k.toUpperCase()}</div>
                  <div style={{fontSize:13,color:"#fff",fontWeight:700}}>{v}</div>
                </div>
              ))}
            </div>
            {resolved?.pictureUrl && tokenImgVisible && (
              <div style={{display:"flex",justifyContent:"center"}}>
                <img
                  src={resolved.pictureUrl}
                  alt={`${resolved.symbol || "Token"} token`}
                  style={{width:72,height:72,borderRadius:14,objectFit:"cover",border:"1px solid rgba(255,255,255,.2)"}}
                  onError={()=>setTokenImgVisible(false)}
                />
              </div>
            )}

            <div style={{background:"rgba(56,189,248,.08)",border:"1px solid rgba(56,189,248,.2)",borderRadius:8,padding:"10px 14px",fontSize:12,color:"rgba(173,233,255,.85)",display:"flex",gap:8}}>
              <span>{"\u2713"}</span><span>{t("attach.ready")}</span>
            </div>
            {err&&<div style={{background:"rgba(255,64,96,.1)",border:"1px solid rgba(255,64,96,.2)",borderRadius:8,padding:"10px 14px",fontSize:13,color:"#ff8080"}}>{err}</div>}

            <div style={{display:"flex",gap:10}}>
              <button className="btn-ghost" onClick={()=>{setStep(1);setPendingDepositId("");setDep("");}} disabled={submitting} style={{padding:"12px 18px",fontSize:13}}>{t("attach.back")}</button>
              <button className="btn-fire" onClick={finish} disabled={submitting} style={{padding:"12px",fontSize:14,flex:1}}>{submitting?t("attach.saving"):t("attach.igniteToken")}</button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
