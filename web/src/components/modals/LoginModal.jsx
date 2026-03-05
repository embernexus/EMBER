import { useState } from "react";
import { apiAuthLogin, apiAuthRegister, isUpgradeRequiredError } from "../../api/client";
import { useI18n } from "../../i18n/I18nProvider";

export default function LoginModal({onClose,onLogin}) {
  const { t } = useI18n();
  const [tab,setTab]=useState("login");
  const [f,setF]=useState({user:"",pass:"",confirm:""});
  const [err,setErr]=useState("");
  const [loading,setLoading]=useState(false);
  const submit=async ()=>{
    setErr("");
    if(!f.user.trim()||!f.pass.trim()) return setErr(t("login.errors.allFields"));
    if(tab==="register"&&f.pass!==f.confirm) return setErr(t("login.errors.passwordMatch"));
    if(f.pass.length<6) return setErr(t("login.errors.passwordLength"));
    setLoading(true);
    try {
      const data = tab === "login"
        ? await apiAuthLogin(f.user.trim(), f.pass)
        : await apiAuthRegister(f.user.trim(), f.pass);
      onLogin(data?.user?.username || f.user.trim());
    } catch (e) {
      if (isUpgradeRequiredError(e)) {
        setErr(t("login.errors.upgradeRequired"));
      } else {
        setErr(e?.message || t("login.errors.authFailed"));
      }
    } finally {
      setLoading(false);
    }
  };
  return (
    <div style={{position:"fixed",inset:0,zIndex:999,display:"flex",alignItems:"center",justifyContent:"center",animation:"fadeIn .2s ease"}}>
      <div style={{position:"absolute",inset:0,background:"rgba(0,0,0,.8)",backdropFilter:"blur(10px)"}} onClick={onClose}/>
      <div className="glass" onClick={e=>e.stopPropagation()} style={{position:"relative",zIndex:1,width:"min(420px,94vw)",padding:36,animation:"slideUp .25s ease",boxShadow:"0 32px 80px rgba(0,0,0,.6),0 0 60px rgba(255,106,0,.07)"}}>
        <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:28}}>
          <div style={{display:"flex",alignItems:"center",gap:10}}>
            <span style={{fontSize:26,animation:"flicker 4s infinite"}}>{"\u{1F525}"}</span>
            <div><div style={{fontWeight:800,fontSize:19,color:"#fff",letterSpacing:1}}>EMBER</div><div style={{fontSize:10,color:"rgba(255,236,214,.88)",letterSpacing:2,fontWeight:700}}>NEXUS</div></div>
          </div>
          <button onClick={onClose} style={{background:"none",border:"none",color:"rgba(255,255,255,.3)",fontSize:22,cursor:"pointer",lineHeight:1}}>x</button>
        </div>
        <div style={{display:"flex",background:"rgba(255,255,255,.04)",borderRadius:10,padding:3,marginBottom:24}}>
          {[["login",t("login.signIn")],["register",t("login.createAccount")]].map(([v,l])=>(
            <button key={v} onClick={()=>{setTab(v);setErr("");}}
              style={{flex:1,padding:"9px 0",border:"none",borderRadius:8,cursor:"pointer",fontSize:13,fontWeight:700,transition:"all .2s",
                background:tab===v?"linear-gradient(135deg,#ff6a00,#ff4500)":"none",
                color:tab===v?"#fff":"rgba(255,255,255,.3)",
                boxShadow:tab===v?"0 2px 12px rgba(255,106,0,.3)":"none"}}>{l}</button>
          ))}
        </div>
        <div style={{display:"flex",flexDirection:"column",gap:14}}>
          {[{label:t("login.username"),key:"user",type:"text"},{label:t("login.password"),key:"pass",type:"password"},...(tab==="register"?[{label:t("login.confirmPassword"),key:"confirm",type:"password"}]:[])].map(fd=>(
            <div key={fd.key}>
              <label style={{display:"block",fontSize:11,color:"rgba(255,255,255,.35)",letterSpacing:1,marginBottom:7,fontWeight:600}}>{fd.label.toUpperCase()}</label>
              <input type={fd.type} className="input-f" value={f[fd.key]} onChange={e=>setF({...f,[fd.key]:e.target.value})}
                onKeyDown={e=>e.key==="Enter"&&submit()} placeholder={fd.type==="password"?"********":t("login.usernamePlaceholder")}/>
            </div>
          ))}
          {err&&<div style={{background:"rgba(255,64,96,.1)",border:"1px solid rgba(255,64,96,.2)",borderRadius:8,padding:"10px 14px",fontSize:13,color:"#ff8080"}}>{err}</div>}
          <button className="btn-fire" onClick={submit} disabled={loading} style={{padding:"13px",fontSize:14,marginTop:4}} data-no-auto-translate="true">
            {loading?<span style={{display:"flex",alignItems:"center",justifyContent:"center",gap:8}}><span style={{width:14,height:14,border:"2px solid rgba(255,255,255,.3)",borderTopColor:"#fff",borderRadius:"50%",animation:"spin .7s linear infinite",display:"inline-block"}}/>{t("login.authenticating")}</span>:tab==="login"?t("login.enter"):t("login.create")}
          </button>
        </div>
      </div>
    </div>
  );
}
