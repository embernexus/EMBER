import { useEffect, useState } from "react";
import { apiRecordDeploy } from "../../api/client";
import { useI18n } from "../../i18n/I18nProvider";
import {
  BOT_ATTACH_OPTIONS,
  DEPLOY_ALLOWED_BANNER_TYPES,
  DEPLOY_ALLOWED_IMAGE_TYPES,
  DEPLOY_ALLOWED_VIDEO_TYPES,
  DEPLOY_BANNER_MAX_BYTES,
  DEPLOY_IMAGE_MAX_BYTES,
  DEPLOY_PRIORITY_FEE,
  DEPLOY_RPC_URL,
  DEPLOY_SLIPPAGE,
  DEPLOY_VIDEO_MAX_BYTES,
  PUMP_GLOBAL_DEFAULT_RESERVES,
  PUMP_GLOBAL_SEED,
  PUMP_PROGRAM_ID,
  PUMP_RPC_FALLBACKS,
  PUMP_TOKEN_DECIMALS,
} from "../../config/deploy";

function readU64LE(bytes, offset) {
  let value = 0n;
  for (let i = 0; i < 8; i += 1) {
    value |= BigInt(bytes[offset + i] || 0) << (8n * BigInt(i));
  }
  return value;
}

function parseDecimalToScaledBigInt(raw, scale) {
  const input = String(raw ?? "").trim();
  if (!input) return 0n;
  if (!/^\d*\.?\d*$/.test(input)) return null;
  const [intPart = "0", fracPart = ""] = input.split(".");
  const normalizedInt = intPart === "" ? "0" : intPart;
  const paddedFrac = (fracPart + "0".repeat(scale)).slice(0, scale);
  const intVal = BigInt(normalizedInt || "0");
  const fracVal = BigInt(paddedFrac || "0");
  return intVal * (10n ** BigInt(scale)) + fracVal;
}

function formatScaledBigInt(value, scale, maxFractionDigits = scale) {
  const safe = value < 0n ? 0n : value;
  const base = 10n ** BigInt(scale);
  const whole = safe / base;
  const frac = safe % base;
  if (maxFractionDigits <= 0 || frac === 0n) return whole.toString();
  const rawFrac = frac.toString().padStart(scale, "0").slice(0, maxFractionDigits);
  const trimmedFrac = rawFrac.replace(/0+$/, "");
  return trimmedFrac ? `${whole.toString()}.${trimmedFrac}` : whole.toString();
}

function quoteInitialTokensFromSolLamports(solLamports, reserves) {
  if (!reserves || solLamports <= 0n) return 0n;
  const n = reserves.initialVirtualSolReserves * reserves.initialVirtualTokenReserves;
  const i = reserves.initialVirtualSolReserves + solLamports;
  const r = n / i + 1n;
  const s = reserves.initialVirtualTokenReserves > r ? reserves.initialVirtualTokenReserves - r : 0n;
  return s < reserves.initialRealTokenReserves ? s : reserves.initialRealTokenReserves;
}

function quoteInitialSolLamportsFromTokens(tokenAmountBase, reserves) {
  if (!reserves || tokenAmountBase <= 0n) return 0n;
  const capped = tokenAmountBase < reserves.initialRealTokenReserves
    ? tokenAmountBase
    : reserves.initialRealTokenReserves;
  if (capped <= 0n) return 0n;
  const newVirtualTokenReserves = reserves.initialVirtualTokenReserves > capped
    ? reserves.initialVirtualTokenReserves - capped
    : 1n;
  const n = reserves.initialVirtualSolReserves * reserves.initialVirtualTokenReserves;
  const newVirtualSolReserves = n / newVirtualTokenReserves + 1n;
  return newVirtualSolReserves > reserves.initialVirtualSolReserves
    ? newVirtualSolReserves - reserves.initialVirtualSolReserves
    : 0n;
}

async function fetchPumpGlobalReserves(rpcUrl) {
  const { Connection, PublicKey } = await import("@solana/web3.js");
  const rpcCandidates = Array.from(
    new Set([String(rpcUrl || "").trim(), ...PUMP_RPC_FALLBACKS].filter(Boolean))
  );
  const programId = new PublicKey(PUMP_PROGRAM_ID);
  const [globalPda] = PublicKey.findProgramAddressSync([new TextEncoder().encode(PUMP_GLOBAL_SEED)], programId);
  for (const rpc of rpcCandidates) {
    try {
      const connection = new Connection(rpc, "confirmed");
      const info = await connection.getAccountInfo(globalPda, "confirmed");
      if (!info?.data) continue;
      const bytes = info.data;
      if (bytes.length < 113) continue;
      return {
        initialVirtualTokenReserves: readU64LE(bytes, 73),
        initialVirtualSolReserves: readU64LE(bytes, 81),
        initialRealTokenReserves: readU64LE(bytes, 89),
      };
    } catch {}
  }
  return { ...PUMP_GLOBAL_DEFAULT_RESERVES };
}

function getInjectedWallet() {
  if (typeof window === "undefined") return null;
  const phantom = window?.phantom?.solana;
  if (phantom?.isPhantom) return { provider: phantom, label: "Phantom" };
  const solflare = window?.solflare;
  if (solflare?.isSolflare) return { provider: solflare, label: "Solflare" };
  const generic = window?.solana;
  if (generic?.isPhantom) return { provider: generic, label: "Phantom" };
  if (generic?.isSolflare) return { provider: generic, label: "Solflare" };
  return null;
}

function getDeployMediaKind(file) {
  const type = String(file?.type || "").toLowerCase();
  if (DEPLOY_ALLOWED_IMAGE_TYPES.has(type)) return "image";
  if (DEPLOY_ALLOWED_VIDEO_TYPES.has(type)) return "video";
  const name = String(file?.name || "").toLowerCase();
  if (name.endsWith(".jpg") || name.endsWith(".jpeg") || name.endsWith(".png") || name.endsWith(".gif")) return "image";
  if (name.endsWith(".mp4")) return "video";
  return "";
}

function validateDeployMediaFile(file) {
  const kind = getDeployMediaKind(file);
  if (!kind) {
    return "Unsupported file type. Use JPG, PNG, GIF, or MP4.";
  }
  const bytes = Number(file?.size || 0);
  if (!Number.isFinite(bytes) || bytes <= 0) {
    return "Selected media file is empty.";
  }
  if (kind === "image" && bytes > DEPLOY_IMAGE_MAX_BYTES) {
    return "Image must be 15MB or smaller.";
  }
  if (kind === "video" && bytes > DEPLOY_VIDEO_MAX_BYTES) {
    return "Video must be 30MB or smaller.";
  }
  return "";
}

function getDeployBannerKind(file) {
  const type = String(file?.type || "").toLowerCase();
  if (DEPLOY_ALLOWED_BANNER_TYPES.has(type)) return "image";
  const name = String(file?.name || "").toLowerCase();
  if (name.endsWith(".jpg") || name.endsWith(".jpeg") || name.endsWith(".png") || name.endsWith(".gif")) return "image";
  return "";
}

function validateDeployBannerFile(file) {
  const kind = getDeployBannerKind(file);
  if (!kind) {
    return "Unsupported banner type. Use JPG, PNG, or GIF.";
  }
  const bytes = Number(file?.size || 0);
  if (!Number.isFinite(bytes) || bytes <= 0) {
    return "Selected banner file is empty.";
  }
  if (bytes > DEPLOY_BANNER_MAX_BYTES) {
    return "Banner must be 4.3MB or smaller.";
  }
  return "";
}

export default function DeployModal({ onClose, user, onRequireLogin, onGoDashboard, onAutoAttach }) {
  const { t } = useI18n();
  const [form, setForm] = useState({
    name: "",
    symbol: "",
    description: "Deployed by EMBER.nexus",
    twitter: "",
    telegram: "",
    website: "",
    initialBuySol: "0.1",
    initialBuyTokens: "0",
    mayhemMode: false,
  });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState(null);
  const [selectedBot, setSelectedBot] = useState("");
  const [imageFile, setImageFile] = useState(null);
  const [imagePreviewUrl, setImagePreviewUrl] = useState("");
  const [bannerFile, setBannerFile] = useState(null);
  const [bannerPreviewUrl, setBannerPreviewUrl] = useState("");
  const [walletProvider, setWalletProvider] = useState(null);
  const [walletAddress, setWalletAddress] = useState("");
  const [walletConnecting, setWalletConnecting] = useState(false);
  const [globalReserves, setGlobalReserves] = useState(null);
  const [quoteLoading, setQuoteLoading] = useState(true);
  const [quoteError, setQuoteError] = useState("");

  const setField = (key, value) => setForm(prev => ({ ...prev, [key]: value }));
  const isDecimalInput = (raw) => /^(\d+|\d*\.\d*)$/.test(raw) || raw === "";

  const onInitialBuySolChange = (raw) => {
    if (!isDecimalInput(raw)) return;
    setForm(prev => {
      if (!globalReserves) return { ...prev, initialBuySol: raw };
      const solLamports = parseDecimalToScaledBigInt(raw, 9);
      if (solLamports === null) return { ...prev, initialBuySol: raw };
      const tokenBase = quoteInitialTokensFromSolLamports(solLamports, globalReserves);
      return {
        ...prev,
        initialBuySol: raw,
        initialBuyTokens: formatScaledBigInt(tokenBase, PUMP_TOKEN_DECIMALS, PUMP_TOKEN_DECIMALS),
      };
    });
  };

  const onInitialBuyTokensChange = (raw) => {
    if (!isDecimalInput(raw)) return;
    setForm(prev => {
      if (!globalReserves) return { ...prev, initialBuyTokens: raw };
      const tokenBase = parseDecimalToScaledBigInt(raw, PUMP_TOKEN_DECIMALS);
      if (tokenBase === null) return { ...prev, initialBuyTokens: raw };
      const solLamports = quoteInitialSolLamportsFromTokens(tokenBase, globalReserves);
      return {
        ...prev,
        initialBuyTokens: raw,
        initialBuySol: formatScaledBigInt(solLamports, 9, 9),
      };
    });
  };
  const autoAttach = Boolean(selectedBot);
  const requiresLoginForSubmit = autoAttach && !user;
  const noBuyIn = Number(form.initialBuySol || 0) === 0;

  useEffect(()=>{
    if(!imageFile){
      setImagePreviewUrl("");
      return;
    }
    const next = URL.createObjectURL(imageFile);
    setImagePreviewUrl(next);
    return ()=>URL.revokeObjectURL(next);
  },[imageFile]);

  useEffect(()=>{
    if(!bannerFile){
      setBannerPreviewUrl("");
      return;
    }
    const next = URL.createObjectURL(bannerFile);
    setBannerPreviewUrl(next);
    return ()=>URL.revokeObjectURL(next);
  },[bannerFile]);

  useEffect(() => {
    let mounted = true;
    const run = async () => {
      setQuoteLoading(true);
      setQuoteError("");
      try {
        const reserves = await fetchPumpGlobalReserves(DEPLOY_RPC_URL);
        if (!mounted) return;
        setGlobalReserves(reserves);
        setForm(prev => {
          const solLamports = parseDecimalToScaledBigInt(prev.initialBuySol, 9) ?? 0n;
          const tokenBase = quoteInitialTokensFromSolLamports(solLamports, reserves);
          return {
            ...prev,
            initialBuyTokens: formatScaledBigInt(tokenBase, PUMP_TOKEN_DECIMALS, PUMP_TOKEN_DECIMALS),
          };
        });
      } catch (e) {
        if (!mounted) return;
        setQuoteError(e?.message || "Unable to load Pump quote parameters.");
      } finally {
        if (mounted) setQuoteLoading(false);
      }
    };
    run();
    return () => { mounted = false; };
  }, []);

  const connectWallet = async () => {
    setError("");
    const found = getInjectedWallet();
    if (!found) {
      setError("No supported wallet found. Install Phantom or Solflare.");
      return null;
    }
    setWalletConnecting(true);
    try {
      const res = found.provider?.isConnected
        ? { publicKey: found.provider.publicKey }
        : await found.provider.connect();
      const address =
        found.provider?.publicKey?.toString?.() ||
        res?.publicKey?.toString?.() ||
        "";
      if (!address) {
        throw new Error("Wallet connected, but no public key was returned.");
      }
      setWalletProvider(found.provider);
      setWalletAddress(address);
      return { provider: found.provider, label: found.label, address };
    } catch (e) {
      setError(e?.message || "Wallet connection failed.");
      return null;
    } finally {
      setWalletConnecting(false);
    }
  };

  const submit = async () => {
    let activeProvider = walletProvider || null;
    let activeWalletAddress = walletAddress || "";
    setError("");
    setResult(null);
    if (requiresLoginForSubmit) {
      setError(t("deploy.errors.signInRequired"));
      return;
    }
    if (!form.name.trim() || !form.symbol.trim() || !form.description.trim()) {
      setError(t("deploy.errors.requiredFields"));
      return;
    }
    if (!imageFile) {
      setError(t("deploy.errors.mediaRequired"));
      return;
    }
    const mediaError = validateDeployMediaFile(imageFile);
    if (mediaError) {
      setError(mediaError);
      return;
    }
    if (bannerFile) {
      const bannerError = validateDeployBannerFile(bannerFile);
      if (bannerError) {
        setError(bannerError);
        return;
      }
    }
    const initialBuySolNum = Number(form.initialBuySol || 0);
    if (!Number.isFinite(initialBuySolNum) || initialBuySolNum < 0) {
      setError(t("deploy.errors.solNegative"));
      return;
    }
    if (Number(form.initialBuyTokens || 0) < 0) {
      setError(t("deploy.errors.tokensNegative"));
      return;
    }

    setLoading(true);
    try {
      if (!activeProvider || !activeWalletAddress) {
        const connected = await connectWallet();
        if (!connected) {
          throw new Error("Wallet connection is required to deploy.");
        }
        activeProvider = connected.provider;
        activeWalletAddress = connected.address;
      }
      const [{ default: bs58 }, { Connection, Keypair, VersionedTransaction }] = await Promise.all([
        import("bs58"),
        import("@solana/web3.js"),
      ]);
      if (!activeProvider) {
        throw new Error("Wallet provider not found.");
      }
      if (typeof activeProvider.signTransaction !== "function") {
        throw new Error("Connected wallet does not support transaction signing.");
      }

      const symbol = form.symbol.trim().toUpperCase().slice(0, 12);
      const name = form.name.trim();
      const description = form.description.trim();

      const metadataForm = new FormData();
      metadataForm.append("file", imageFile, imageFile.name || "token-media");
      if (bannerFile) {
        metadataForm.append("banner", bannerFile, bannerFile.name || "token-banner");
      }
      metadataForm.append("name", name);
      metadataForm.append("symbol", symbol);
      metadataForm.append("description", description);
      metadataForm.append("twitter", form.twitter.trim());
      metadataForm.append("telegram", form.telegram.trim());
      metadataForm.append("website", form.website.trim());
      metadataForm.append("showName", "true");

      const metadataRes = await fetch("https://pump.fun/api/ipfs", {
        method: "POST",
        body: metadataForm,
      });
      const metadataData = await metadataRes.json().catch(() => ({}));
      if (!metadataRes.ok) {
        throw new Error(metadataData?.error || "Metadata upload failed.");
      }
      const metadataUri = String(metadataData?.metadataUri || "").trim();
      if (!metadataUri) {
        throw new Error("Metadata URI was not returned.");
      }
      const metadataImage =
        String(
          metadataData?.metadata?.image ||
          metadataData?.image ||
          metadataData?.imageUri ||
          ""
        ).trim() || "";

      const mintKeypair = Keypair.generate();
      const amount = initialBuySolNum;

      const tradeRes = await fetch("https://pumpportal.fun/api/trade-local", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          publicKey: activeWalletAddress,
          action: "create",
          tokenMetadata: {
            name,
            symbol,
            uri: metadataUri,
          },
          mint: mintKeypair.publicKey.toBase58(),
          denominatedInSol: "true",
          amount,
          slippage: DEPLOY_SLIPPAGE,
          priorityFee: DEPLOY_PRIORITY_FEE,
          pool: "pump",
          isMayhemMode: form.mayhemMode ? "true" : "false",
        }),
      });
      if (!tradeRes.ok) {
        const txt = await tradeRes.text().catch(() => "");
        throw new Error(txt || "Failed to build deploy transaction.");
      }
      const txBytes = new Uint8Array(await tradeRes.arrayBuffer());
      const tx = VersionedTransaction.deserialize(txBytes);
      tx.sign([mintKeypair]);
      const signedTx = await activeProvider.signTransaction(tx);

      const connection = new Connection(DEPLOY_RPC_URL, "confirmed");
      const signature = await connection.sendRawTransaction(signedTx.serialize(), {
        maxRetries: 5,
        skipPreflight: false,
      });
      await connection.confirmTransaction(signature, "confirmed");

      const mint = mintKeypair.publicKey.toBase58();
      let recordData = {};
      let postDeployWarning = "";
      try {
        recordData = await apiRecordDeploy({
          mint,
          symbol,
          name,
          pictureUrl: metadataImage,
          signature,
          autoAttach,
          selectedBot: autoAttach ? selectedBot : "",
        });
      } catch (recordError) {
        postDeployWarning = recordError?.message || "Deployed on-chain, but post-deploy sync failed.";
      }

      if (recordData?.attachedToken && onAutoAttach) {
        await onAutoAttach(recordData.attachedToken);
      } else if (recordData?.autoAttached && onAutoAttach) {
        await onAutoAttach();
      }

      setResult({
        mint,
        signature,
        solscanTx: `https://solscan.io/tx/${signature}`,
        solscanMint: `https://solscan.io/token/${mint}`,
        pumpfunUrl: `https://pump.fun/coin/${mint}`,
        autoAttached: Boolean(recordData?.autoAttached),
        postDeployWarning,
      });
    } catch (e) {
      setError(e.message || "Deploy failed.");
    } finally {
      if (activeProvider && typeof activeProvider.disconnect === "function") {
        try {
          await activeProvider.disconnect();
        } catch {}
      }
      setWalletProvider(null);
      setWalletAddress("");
      setLoading(false);
    }
  };

  const labelStyle = {
    display: "block",
    fontSize: 11,
    color: "rgba(255,255,255,.42)",
    letterSpacing: 1,
    marginBottom: 6,
    fontWeight: 700,
  };

  return (
    <div style={{position:"fixed",inset:0,zIndex:999,display:"flex",alignItems:"center",justifyContent:"center",animation:"fadeIn .2s ease"}} onClick={onClose}>
      <div style={{position:"absolute",inset:0,background:"rgba(0,0,0,.82)",backdropFilter:"blur(10px)"}}/>
      <div className="glass deploy-panel" onClick={e=>e.stopPropagation()} style={{position:"relative",zIndex:1,animation:"slideUp .25s ease",boxShadow:"0 32px 80px rgba(0,0,0,.6),0 0 60px rgba(255,106,0,.07)"}}>
        <div className="deploy-header" style={{position:"relative",display:"flex",justifyContent:"center",alignItems:"center"}}>
          <div style={{textAlign:"center"}}>
            <div style={{display:"inline-flex",alignItems:"center",gap:8,padding:"6px 10px",borderRadius:999,background:"rgba(255,106,0,.12)",border:"1px solid rgba(255,106,0,.28)",fontSize:11,fontWeight:800,color:"#ff9f5a",letterSpacing:.8,marginBottom:10}}>
              <span style={{fontSize:13}}>{"\u{1F525}"}</span>
              <span>DEPLOY</span>
            </div>
            <div style={{fontWeight:800,fontSize:24,color:"#fff",marginBottom:4}}>{t("deploy.launchToken")}</div>
            <div style={{fontSize:12,color:"rgba(255,255,255,.45)"}}>{t("deploy.subtitle")}</div>
          </div>
          <button onClick={onClose} style={{position:"absolute",right:0,background:"none",border:"none",color:"rgba(255,255,255,.35)",fontSize:22,cursor:"pointer"}} aria-label="Close deploy modal">x</button>
        </div>

        <div className="deploy-body">
          <div className="deploy-grid-2">
          {[
            ["name","Token Name","text","e.g. Ember Nexus"],
            ["symbol","Symbol","text","e.g. EMBER"],
          ].map(([k,l,t,p])=>(
            <div key={k}>
              <label style={labelStyle}>{l.toUpperCase()}</label>
              <input type={t} className="input-f" value={form[k]} onChange={e=>setField(k,e.target.value)} placeholder={p}/>
            </div>
          ))}
        </div>

          <div style={{marginTop:12}}>
          <label style={labelStyle}>DESCRIPTION</label>
          <textarea className="input-f" value={form.description} onChange={e=>setField("description",e.target.value)} placeholder={t("deploy.describeToken")} rows={3} style={{resize:"vertical"}}/>
        </div>

          <div style={{marginTop:12}}>
          <label style={labelStyle}>TOKEN MEDIA</label>
          <div style={{display:"grid",gridTemplateColumns:"minmax(0,1fr) 148px",gap:10,alignItems:"start"}}>
            <div>
              <input
                type="file"
                accept="image/jpeg,image/png,image/gif,video/mp4"
                className="input-f"
                onChange={e=>{
                  const nextFile = e.target.files?.[0] || null;
                  if (!nextFile) {
                    setImageFile(null);
                    return;
                  }
                  const nextError = validateDeployMediaFile(nextFile);
                  if (nextError) {
                    setError(nextError);
                    setImageFile(null);
                    e.target.value = "";
                    return;
                  }
                  setError("");
                  setImageFile(nextFile);
                }}
              />
              <div style={{fontSize:11,color:"rgba(255,255,255,.38)",marginTop:6,lineHeight:1.5}}>
                Image: max 15MB. JPG, GIF, or PNG recommended.
                <br/>
                Video: max 30MB. MP4 recommended.
                <br/>
                Image: min 1000x1000, 1:1 square recommended.
                <br/>
                Video: 16:9 or 9:16, 1080p+ recommended.
              </div>
            </div>
            <div style={{height:148,borderRadius:14,border:"1px solid rgba(255,255,255,.1)",background:"rgba(255,255,255,.03)",display:"flex",alignItems:"center",justifyContent:"center",overflow:"hidden"}}>
              {imagePreviewUrl ? (
                getDeployMediaKind(imageFile) === "video" ? (
                  <video src={imagePreviewUrl} style={{width:"100%",height:"100%",objectFit:"cover"}} muted controls playsInline />
                ) : (
                  <img src={imagePreviewUrl} alt="Token preview" style={{width:"100%",height:"100%",objectFit:"cover"}} />
                )
              ) : (
                <span style={{fontSize:11,color:"rgba(255,255,255,.35)",fontWeight:700,letterSpacing:.5}}>MEDIA PREVIEW</span>
              )}
            </div>
          </div>
        </div>

          <div style={{marginTop:12}}>
          <label style={labelStyle}>BANNER (OPTIONAL)</label>
          <div style={{display:"grid",gridTemplateColumns:"minmax(0,1fr)",gap:10}}>
            <div>
              <input
                type="file"
                accept="image/jpeg,image/png,image/gif"
                className="input-f"
                onChange={e=>{
                  const nextFile = e.target.files?.[0] || null;
                  if (!nextFile) {
                    setBannerFile(null);
                    return;
                  }
                  const nextError = validateDeployBannerFile(nextFile);
                  if (nextError) {
                    setError(nextError);
                    setBannerFile(null);
                    e.target.value = "";
                    return;
                  }
                  setError("");
                  setBannerFile(nextFile);
                }}
              />
              <div style={{fontSize:11,color:"rgba(255,255,255,.38)",marginTop:6,lineHeight:1.5}}>
                Image only: max 4.3MB. JPG, GIF, or PNG recommended.
                <br/>
                3:1 aspect ratio, 1500x500 recommended.
                <br/>
                Banner is creation-time only and cannot be changed later.
              </div>
            </div>
            <div style={{height:110,borderRadius:14,border:"1px solid rgba(255,255,255,.1)",background:"rgba(255,255,255,.03)",display:"flex",alignItems:"center",justifyContent:"center",overflow:"hidden"}}>
              {bannerPreviewUrl ? (
                <img src={bannerPreviewUrl} alt="Token banner preview" style={{width:"100%",height:"100%",objectFit:"cover"}} />
              ) : (
                <span style={{fontSize:11,color:"rgba(255,255,255,.35)",fontWeight:700,letterSpacing:.5}}>BANNER PREVIEW</span>
              )}
            </div>
          </div>
        </div>

          <div className="deploy-grid-3" style={{marginTop:12}}>
          {[
            ["twitter","Twitter URL","https://x.com/..."],
            ["telegram","Telegram URL","https://t.me/..."],
            ["website","Website URL","https://..."],
          ].map(([k,l,p])=>(
            <div key={k}>
              <label style={labelStyle}>{l.toUpperCase()}</label>
              <input className="input-f" value={form[k]} onChange={e=>setField(k,e.target.value)} placeholder={p}/>
            </div>
          ))}
        </div>

          <div style={{marginTop:12}}>
          <label style={labelStyle}>INITIAL BUY</label>
          <div className="deploy-grid-2">
            <div>
              <label style={{...labelStyle,marginBottom:4}}>SOL AMOUNT</label>
              <input
                type="number"
                className="input-f"
                min={0}
                step="0.0001"
                value={form.initialBuySol}
                onChange={e=>onInitialBuySolChange(e.target.value)}
              />
            </div>
            <div>
              <label style={{...labelStyle,marginBottom:4}}>TOKEN AMOUNT</label>
              <input
                type="number"
                className="input-f"
                min={0}
                step="0.000001"
                value={form.initialBuyTokens}
                onChange={e=>onInitialBuyTokensChange(e.target.value)}
              />
            </div>
          </div>
          <div style={{marginTop:5,fontSize:11,color:"rgba(255,255,255,.38)",lineHeight:1.6}}>
            {t("deploy.solSubmitHint")}
          </div>
          {quoteLoading && (
            <div style={{marginTop:5,fontSize:11,color:"rgba(255,255,255,.34)",lineHeight:1.5}}>
              {t("deploy.loadingCurve")}
            </div>
          )}
          {quoteError && (
            <div style={{marginTop:5,fontSize:11,color:"#ff9f9f",lineHeight:1.5}}>
              {quoteError}
            </div>
          )}
        </div>

          <div style={{marginTop:12}}>
          <label style={labelStyle}>MAYHEM MODE (OPTIONAL)</label>
          <label style={{display:"flex",alignItems:"center",gap:10,cursor:"pointer",userSelect:"none",padding:"10px 12px",borderRadius:10,border:"1px solid rgba(255,255,255,.14)",background:"rgba(255,255,255,.03)"}}>
            <input
              type="checkbox"
              checked={Boolean(form.mayhemMode)}
              onChange={e=>setField("mayhemMode", e.target.checked)}
              style={{accentColor:"#ff8a3d",width:16,height:16,cursor:"pointer"}}
            />
            <span style={{fontSize:13,fontWeight:700,color:"#fff"}}>Enable Mayhem Mode</span>
          </label>
          <div style={{marginTop:6,fontSize:11,color:"rgba(255,255,255,.38)",lineHeight:1.55}}>
            Increases launch price volume behavior for the first 24h. Set only at creation time.
          </div>
        </div>

          <div style={{marginTop:12}}>
          <label style={labelStyle}>AUTO-ATTACH BOT (OPTIONAL)</label>
          <select
            className="input-f"
            value={selectedBot}
            onChange={e=>setSelectedBot(e.target.value)}
            style={{backgroundColor:"rgba(255,255,255,.05)",color:"#fff"}}
          >
            {BOT_ATTACH_OPTIONS.map(o => (
              <option
                key={o.value || "none"}
                value={o.value}
                disabled={Boolean(o.disabled)}
                style={{background:"#12080f",color:o.disabled?"rgba(248,230,216,.45)":"#f8e6d8"}}
              >
                {o.label}
              </option>
            ))}
          </select>
          <div style={{fontSize:11,color:"rgba(255,255,255,.42)",marginTop:6,lineHeight:1.5}}>
            {autoAttach
              ? t("deploy.autoAttachOn")
              : t("deploy.autoAttachOff")}
          </div>
          <div style={{fontSize:11,color:"rgba(255,255,255,.36)",marginTop:5,lineHeight:1.5}}>
            {t("deploy.afterDeploy")}
          </div>
        </div>

          <div style={{marginTop:12,background:"rgba(255,106,0,.08)",border:"1px solid rgba(255,106,0,.22)",borderRadius:10,padding:"10px 12px",fontSize:12,color:"rgba(255,232,211,.85)",lineHeight:1.55}}>
          {t("deploy.executesThroughPumpfun")}
        </div>
        {noBuyIn && (
          <div style={{marginTop:12,background:"rgba(255,200,0,.08)",border:"1px solid rgba(255,200,0,.24)",borderRadius:10,padding:"10px 12px",fontSize:12,color:"rgba(255,232,180,.92)",lineHeight:1.55}}>
            {t("deploy.noInitialBuy")}
          </div>
        )}

        {requiresLoginForSubmit && (
          <div style={{marginTop:12,background:"rgba(255,200,0,.08)",border:"1px solid rgba(255,200,0,.22)",borderRadius:10,padding:"10px 12px",fontSize:12,color:"rgba(255,230,185,.88)",lineHeight:1.55,display:"flex",justifyContent:"space-between",alignItems:"center",gap:12}}>
            <span>{t("deploy.signInWithAutoAttach")}</span>
            <button className="btn-ghost" onClick={onRequireLogin} style={{padding:"8px 12px",fontSize:12}}>{t("login.signIn")}</button>
          </div>
        )}

        {error && <div style={{marginTop:12,background:"rgba(255,64,96,.1)",border:"1px solid rgba(255,64,96,.2)",borderRadius:8,padding:"10px 12px",fontSize:13,color:"#ff8080"}}>{error}</div>}

        {result && (
          <div style={{marginTop:12,background:"rgba(56,189,248,.08)",border:"1px solid rgba(56,189,248,.26)",borderRadius:10,padding:"12px 14px"}}>
            <div style={{fontSize:12,fontWeight:800,color:"#8be5ff",marginBottom:8,letterSpacing:.5}}>DEPLOY SUCCESS</div>
            <div style={{fontSize:12,color:"rgba(255,255,255,.72)",lineHeight:1.7}}>
              <div>{t("deploy.mint")}: <a className="tx-link" href={result.solscanMint} target="_blank" rel="noopener noreferrer">{result.mint} [open]</a></div>
              <div>{t("deploy.transaction")}: {result.solscanTx ? <a className="tx-link" href={result.solscanTx} target="_blank" rel="noopener noreferrer">{result.signature} [open]</a> : t("deploy.pending")}</div>
              <div>Pump.fun: {result.pumpfunUrl ? <a className="tx-link" href={result.pumpfunUrl} target="_blank" rel="noopener noreferrer">{t("deploy.openPumpfun")}</a> : t("burns.na")}</div>
              {result.autoAttached && (
                <div style={{marginTop:8,color:"#95f0c6",fontWeight:700}}>{t("deploy.autoAttachedSuccess")}</div>
              )}
              {result.postDeployWarning && (
                <div style={{marginTop:8,color:"#ffd182",fontWeight:700}}>{result.postDeployWarning}</div>
              )}
            </div>
            {user && (
              <div style={{marginTop:10}}>
                <button className="btn-ghost" onClick={onGoDashboard} style={{padding:"8px 12px",fontSize:12}}>{t("deploy.openDashboard")}</button>
              </div>
            )}
          </div>
        )}
        </div>

        <div className="deploy-actions">
          <button className="btn-ghost" onClick={onClose} style={{padding:"10px 16px",fontSize:13}}>{t("common.close")}</button>
          <button className="btn-fire" onClick={submit} disabled={loading || requiresLoginForSubmit || walletConnecting} style={{padding:"10px 18px",fontSize:13}}>
            {loading ? t("deploy.deploying") : walletConnecting ? t("deploy.connectingWallet") : t("deploy.deploy")}
          </button>
        </div>
      </div>
    </div>
  );
}
