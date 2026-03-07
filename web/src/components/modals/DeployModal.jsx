import { useEffect, useState } from "react";
import {
  apiGetVanityDeployWalletStatus,
  apiRecordDeploy,
  apiReserveVanityDeployWallet,
  apiSubmitVanityDeploy,
} from "../../api/client";
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
  DEPLOY_VANITY_BUFFER_SOL,
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

function getInjectedWallets() {
  if (typeof window === "undefined") return [];
  const out = [];
  const pushWallet = (key, provider, label) => {
    if (!provider) return;
    if (typeof provider.connect !== "function") return;
    if (out.some((w) => w.key === key)) return;
    out.push({ key, provider, label });
  };

  const phantom = window?.phantom?.solana;
  if (phantom?.isPhantom) pushWallet("phantom", phantom, "Phantom");

  const solflare = window?.solflare;
  if (solflare?.isSolflare) pushWallet("solflare", solflare, "Solflare");

  const generic = window?.solana;
  if (generic?.isPhantom) pushWallet("phantom", generic, "Phantom");
  if (generic?.isSolflare) pushWallet("solflare", generic, "Solflare");

  return out;
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

function fileToDataUri(file) {
  return new Promise((resolve, reject) => {
    if (!file) {
      reject(new Error("File is required."));
      return;
    }
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(new Error("Failed to read file."));
    reader.readAsDataURL(file);
  });
}

function base64ToBytes(base64Text) {
  const raw = atob(String(base64Text || ""));
  const out = new Uint8Array(raw.length);
  for (let i = 0; i < raw.length; i += 1) {
    out[i] = raw.charCodeAt(i);
  }
  return out;
}

function bytesToBase64(bytes) {
  const chunkSize = 0x8000;
  let binary = "";
  for (let i = 0; i < bytes.length; i += chunkSize) {
    const chunk = bytes.subarray(i, i + chunkSize);
    binary += String.fromCharCode(...chunk);
  }
  return btoa(binary);
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 35000, label = "Request") {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } catch (error) {
    if (error?.name === "AbortError") {
      throw new Error(`${label} timed out. Please try again.`);
    }
    throw error;
  } finally {
    clearTimeout(id);
  }
}

async function readErrorFromResponse(res, fallbackMessage) {
  const statusText = String(res?.statusText || "").trim();
  const text = await res.text().catch(() => "");
  let message = "";
  if (text) {
    try {
      const json = JSON.parse(text);
      message = String(json?.error || json?.message || "").trim();
    } catch {
      message = text.trim();
    }
  }
  if ((!message || /^bad request$/i.test(message)) && statusText) {
    message = statusText;
  }
  return message || fallbackMessage;
}

async function withTimeout(promise, timeoutMs, label) {
  let timer = null;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timed out. Please try again.`)), timeoutMs);
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

export default function DeployModal({ onClose, user, onRequireLogin, onGoDashboard, onAutoAttach }) {
  const { t } = useI18n();
  const [form, setForm] = useState({
    name: "",
    symbol: "",
    description: "Deployed on EMBER.nexus",
    twitter: "",
    telegram: "",
    website: "",
    initialBuySol: "0.1",
    initialBuyTokens: "0",
    mayhemMode: false,
  });
  const [loading, setLoading] = useState(false);
  const [deployStep, setDeployStep] = useState("");
  const [error, setError] = useState("");
  const [result, setResult] = useState(null);
  const [selectedBot, setSelectedBot] = useState("");
  const [imageFile, setImageFile] = useState(null);
  const [imagePreviewUrl, setImagePreviewUrl] = useState("");
  const [bannerFile, setBannerFile] = useState(null);
  const [bannerPreviewUrl, setBannerPreviewUrl] = useState("");
  const [walletProvider, setWalletProvider] = useState(null);
  const [walletAddress, setWalletAddress] = useState("");
  const [walletChoice, setWalletChoice] = useState("");
  const [walletConnecting, setWalletConnecting] = useState(false);
  const [showWalletSelect, setShowWalletSelect] = useState(false);
  const [globalReserves, setGlobalReserves] = useState(null);
  const [quoteLoading, setQuoteLoading] = useState(true);
  const [quoteError, setQuoteError] = useState("");
  const [deployMode, setDeployMode] = useState("wallet");
  const [vanityWallet, setVanityWallet] = useState(null);
  const [vanityLoading, setVanityLoading] = useState(false);
  const [vanityMessage, setVanityMessage] = useState("");
  const [showVanitySecret, setShowVanitySecret] = useState(false);
  const [deployWalletStyle, setDeployWalletStyle] = useState("vanity");

  const setField = (key, value) => setForm(prev => ({ ...prev, [key]: value }));
  const isDecimalInput = (raw) => /^(\d+|\d*\.\d*)$/.test(raw) || raw === "";

  const onInitialBuySolChange = (raw) => {
    if (!isDecimalInput(raw)) return;
    if (vanityWallet && raw !== form.initialBuySol) {
      invalidateVanityWallet("EMBER/EMBR deploy wallet cleared because the initial buy changed. Generate a new one.");
    }
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
    if (vanityWallet && raw !== form.initialBuyTokens) {
      invalidateVanityWallet("EMBER/EMBR deploy wallet cleared because the initial buy changed. Generate a new one.");
    }
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
  const vanityReady = Boolean(vanityWallet?.funded);

  const invalidateVanityWallet = (message = "") => {
    if (!vanityWallet) return;
    setVanityWallet(null);
    setShowVanitySecret(false);
    if (message) {
      setVanityMessage(message);
    }
  };

  useEffect(() => {
    invalidateVanityWallet("");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [deployWalletStyle]);

  const copyText = async (value, successMessage) => {
    const text = String(value || "").trim();
    if (!text) return;
    try {
      await navigator.clipboard.writeText(text);
      setVanityMessage(successMessage);
    } catch {
      setVanityMessage("Copy failed. Please copy it manually.");
    }
  };

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

  useEffect(() => {
    if (!vanityWallet?.reservationId || vanityWallet?.status === "deployed") return undefined;
    let cancelled = false;
    const refresh = async () => {
      try {
        const next = await apiGetVanityDeployWalletStatus(vanityWallet.reservationId);
        if (!cancelled) {
          setVanityWallet((prev) => {
            if (!prev || prev.reservationId !== vanityWallet.reservationId) return prev;
            return { ...prev, ...next };
          });
        }
      } catch {}
    };
    const id = setInterval(() => {
      void refresh();
    }, 5000);
    void refresh();
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [vanityWallet?.reservationId, vanityWallet?.status]);

  const connectWallet = async (preferredKey = "") => {
    setError("");
    const wallets = getInjectedWallets();
    if (!wallets.length) {
      setError("No supported wallet found. Install Phantom or Solflare.");
      return null;
    }
    let found = null;
    if (preferredKey) {
      found = wallets.find((w) => w.key === preferredKey) || null;
      if (!found) {
        const label = preferredKey === "solflare" ? "Solflare" : preferredKey === "phantom" ? "Phantom" : "Selected wallet";
        setError(`${label} was not detected. Make sure the extension is installed and unlocked.`);
        return null;
      }
    }
    if (!found && wallets.length === 1) {
      found = wallets[0];
    }
    if (!found) {
      setError("Select Phantom or Solflare first.");
      return null;
    }
    setWalletChoice(found.key);
    setWalletConnecting(true);
    try {
      if (found.key === "phantom" && !found.provider?.isPhantom) {
        throw new Error("Phantom is not available in this browser session.");
      }
      if (found.key === "solflare" && !found.provider?.isSolflare) {
        throw new Error("Solflare is not available in this browser session.");
      }
      setDeployStep(`Connecting ${found.label}...`);
      const res =
        found.provider?.isConnected && found.provider?.publicKey
          ? { publicKey: found.provider.publicKey }
          : await withTimeout(
              found.key === "phantom"
                ? found.provider.connect({ onlyIfTrusted: false })
                : found.provider.connect(),
              45000,
              `${found.label} connection`
            );
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
      const msg = e?.message || "Wallet connection failed.";
      setError(msg);
      throw new Error(msg);
    } finally {
      setWalletConnecting(false);
    }
  };

  const submit = async (preferredWalletKey = "") => {
    let activeProvider = walletProvider || null;
    let activeWalletAddress = walletAddress || "";
    let resolvedWalletKey = preferredWalletKey || walletChoice;
    setError("");
    setResult(null);
    setDeployStep("");
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
    if (initialBuySolNum <= 0) {
      setError("Pump deploy requires an initial buy greater than 0 SOL.");
      return;
    }
    if (Number(form.initialBuyTokens || 0) < 0) {
      setError(t("deploy.errors.tokensNegative"));
      return;
    }

    if (!activeProvider || !activeWalletAddress) {
      const wallets = getInjectedWallets();
      if (resolvedWalletKey && !wallets.some((w) => w.key === resolvedWalletKey)) {
        resolvedWalletKey = "";
        setWalletChoice("");
      }
      if (!resolvedWalletKey) {
        if (wallets.length > 0) {
          setShowWalletSelect(true);
          return;
        }
        setError("No supported wallet found. Install Phantom or Solflare.");
        return;
      }
      setWalletChoice(resolvedWalletKey);
    }

    setLoading(true);
    try {
      if (!activeProvider || !activeWalletAddress) {
        const connected = await connectWallet(resolvedWalletKey);
        if (!connected) {
          throw new Error("Wallet connection failed. Select Phantom or Solflare and try again.");
        }
        activeProvider = connected.provider;
        activeWalletAddress = connected.address;
      }
      const { Connection, Keypair, VersionedTransaction } = await import("@solana/web3.js");
      if (!activeProvider) {
        throw new Error("Wallet provider not found.");
      }
      if (typeof activeProvider.signTransaction !== "function") {
        throw new Error("Connected wallet does not support transaction signing.");
      }

      const symbol = form.symbol.trim().toUpperCase().slice(0, 12);
      const name = form.name.trim();
      const description = form.description.trim();

      setDeployStep("Uploading metadata...");
      const imageDataUri = await fileToDataUri(imageFile);
      const bannerDataUri = bannerFile ? await fileToDataUri(bannerFile) : "";
      let metadataData = {};
      let metadataProxyError = "";
      try {
        const metadataRes = await fetchWithTimeout("/api/deploy/pump-ipfs", {
          method: "POST",
          credentials: "include",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            imageDataUri,
            imageFileName: imageFile?.name || "token-media",
            bannerDataUri,
            bannerFileName: bannerFile?.name || "token-banner",
            name,
            symbol,
            description,
            twitter: form.twitter.trim(),
            telegram: form.telegram.trim(),
            website: form.website.trim(),
          }),
        }, 45000, "Metadata upload");
        if (!metadataRes.ok) {
          const message = await readErrorFromResponse(metadataRes, "Metadata upload failed.");
          throw new Error(message);
        }
        metadataData = await metadataRes.json().catch(() => ({}));
      } catch (proxyError) {
        metadataProxyError = proxyError?.message || "proxy upload failed";
        setDeployStep("Uploading metadata (fallback)...");
        const metadataForm = new FormData();
        metadataForm.append("file", imageFile, imageFile?.name || "token-media");
        if (bannerFile) metadataForm.append("banner", bannerFile, bannerFile?.name || "token-banner");
        metadataForm.append("name", name);
        metadataForm.append("symbol", symbol);
        metadataForm.append("description", description);
        metadataForm.append("twitter", form.twitter.trim());
        metadataForm.append("telegram", form.telegram.trim());
        metadataForm.append("website", form.website.trim());
        metadataForm.append("showName", "true");
        const metadataResDirect = await fetchWithTimeout("https://pump.fun/api/ipfs", {
          method: "POST",
          body: metadataForm,
        }, 45000, "Metadata upload fallback");
        if (!metadataResDirect.ok) {
          const directMessage = await readErrorFromResponse(metadataResDirect, "direct upload failed");
          throw new Error(`Metadata upload failed (${metadataProxyError} | ${directMessage})`);
        }
        metadataData = await metadataResDirect.json().catch(() => ({}));
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

      setDeployStep("Building transaction...");
      const basePayload = {
        publicKey: activeWalletAddress,
        action: "create",
        tokenMetadata: { name, symbol, uri: metadataUri },
        denominatedInSol: "true",
        amount,
        slippage: DEPLOY_SLIPPAGE,
        priorityFee: DEPLOY_PRIORITY_FEE,
        pool: "pump",
        isMayhemMode: form.mayhemMode ? "true" : "false",
      };
      const payloadCandidates = [
        { ...basePayload, mint: mintKeypair.publicKey.toBase58() },
      ];

      let txBytes = null;
      const buildErrors = [];
      for (const candidate of payloadCandidates) {
        if (txBytes) break;
        try {
          const tradeRes = await fetchWithTimeout("/api/deploy/pump-trade-local", {
            method: "POST",
            credentials: "include",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(candidate),
          }, 30000, "Transaction build");
          if (tradeRes.ok) {
            const tradeData = await tradeRes.json().catch(() => ({}));
            if (tradeData?.txBase64) {
              txBytes = base64ToBytes(tradeData.txBase64);
              break;
            }
            buildErrors.push("proxy: missing tx payload");
          } else {
            const message = await readErrorFromResponse(tradeRes, "proxy bad request");
            buildErrors.push(`proxy: ${message}`);
          }
        } catch (err) {
          buildErrors.push(`proxy: ${err?.message || "request failed"}`);
        }

        if (txBytes) break;
        setDeployStep("Building transaction (fallback)...");
        try {
          const tradeResDirect = await fetchWithTimeout("https://pumpportal.fun/api/trade-local", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(candidate),
          }, 30000, "Transaction build fallback");
          if (tradeResDirect.ok) {
            txBytes = new Uint8Array(await tradeResDirect.arrayBuffer());
            break;
          }
          const message = await readErrorFromResponse(tradeResDirect, "direct bad request");
          buildErrors.push(`direct: ${message}`);
        } catch (err) {
          buildErrors.push(`direct: ${err?.message || "request failed"}`);
        }
      }
      if (!txBytes) {
        throw new Error(`Transaction build failed: ${buildErrors.slice(0, 4).join(" | ") || "Bad Request"}`);
      }
      if (!txBytes || !txBytes.length) {
        throw new Error("Transaction build failed: empty transaction payload.");
      }
      const tx = VersionedTransaction.deserialize(txBytes);
      tx.sign([mintKeypair]);
      setDeployStep("Waiting for wallet signature...");
      const signedTx = await activeProvider.signTransaction(tx);

      setDeployStep("Sending transaction...");
      const signedTxBytes = signedTx.serialize();
      let signature = "";
      let relayError = "";
      try {
        const relayRes = await fetchWithTimeout("/api/deploy/submit-signed", {
          method: "POST",
          credentials: "include",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ txBase64: bytesToBase64(signedTxBytes) }),
        }, 45000, "Transaction relay");
        if (!relayRes.ok) {
          const message = await readErrorFromResponse(relayRes, "relay submit failed");
          throw new Error(message);
        }
        const relayData = await relayRes.json().catch(() => ({}));
        signature = String(relayData?.signature || "").trim();
        if (!signature) {
          throw new Error("Relay submit returned no signature.");
        }
      } catch (err) {
        relayError = err?.message || "relay submit failed";
      }

      if (!signature) {
        const connection = new Connection(DEPLOY_RPC_URL, "confirmed");
        try {
          signature = await withTimeout(connection.sendRawTransaction(signedTxBytes, {
            maxRetries: 5,
            skipPreflight: false,
          }), 30000, "Transaction submit");
        } catch (directErr) {
          const directMessage = directErr?.message || "direct submit failed";
          throw new Error(`Transaction submit failed: relay: ${relayError || "failed"} | direct: ${directMessage}`);
        }
      }

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
      setDeployStep("");
      setLoading(false);
    }
  };

  const handleDeployClick = () => {
    if (loading || walletConnecting || requiresLoginForSubmit) return;
    setError("");
    const wallets = getInjectedWallets();
    if (!walletProvider || !walletAddress) {
      if (!wallets.length) {
        setError("No supported wallet found. Install Phantom or Solflare.");
        return;
      }
      setShowWalletSelect(true);
      return;
    }
    void submit(walletChoice || "");
  };

  const refreshVanityWallet = async () => {
    if (!vanityWallet?.reservationId) return;
    setVanityLoading(true);
    setVanityMessage("");
    setError("");
    try {
      const next = await apiGetVanityDeployWalletStatus(vanityWallet.reservationId);
      setVanityWallet((prev) => ({ ...(prev || {}), ...next }));
      setVanityMessage(
        next?.funded
          ? "Deploy wallet funded. You can deploy now."
          : "Funding status refreshed."
      );
    } catch (e) {
      setError(e?.message || "Failed to refresh EMBER/EMBR deploy wallet status.");
    } finally {
      setVanityLoading(false);
    }
  };

  const reserveVanityWallet = async () => {
    setError("");
    setResult(null);
    setVanityMessage("");
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
    if (!Number.isFinite(initialBuySolNum) || initialBuySolNum <= 0) {
      setError("Pump deploy requires an initial buy greater than 0 SOL.");
      return;
    }

    setVanityLoading(true);
    try {
      const next = await apiReserveVanityDeployWallet({
        initialBuySol: initialBuySolNum,
        useVanity: deployWalletStyle !== "regular",
      });
      setVanityWallet(next);
      setShowVanitySecret(false);
      setVanityMessage(`${deployWalletStyle === "regular" ? "Regular" : "Branded"} deploy wallet generated. Fund it, then click Deploy.`);
    } catch (e) {
      setError(e?.message || "Failed to generate deploy wallet.");
    } finally {
      setVanityLoading(false);
    }
  };

  const submitVanity = async () => {
    if (!vanityWallet?.reservationId) {
      setError("Generate a deploy wallet first.");
      return;
    }
    if (requiresLoginForSubmit) {
      setError(t("deploy.errors.signInRequired"));
      return;
    }
    if (!vanityWallet?.funded) {
      setError("Fund the deploy wallet before deploying.");
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

    setLoading(true);
    setError("");
    setResult(null);
    setDeployStep("Preparing deploy wallet...");
    try {
      const imageDataUri = await fileToDataUri(imageFile);
      const bannerDataUri = bannerFile ? await fileToDataUri(bannerFile) : "";
      setDeployStep(`Submitting deploy from ${deployWalletStyle === "regular" ? "regular" : "branded"} wallet...`);
      const deployRes = await apiSubmitVanityDeploy({
        reservationId: vanityWallet.reservationId,
        name: form.name.trim(),
        symbol: form.symbol.trim().toUpperCase().slice(0, 12),
        description: form.description.trim(),
        twitter: form.twitter.trim(),
        telegram: form.telegram.trim(),
        website: form.website.trim(),
        initialBuySol: Number(form.initialBuySol || 0),
        mayhemMode: Boolean(form.mayhemMode),
        imageDataUri,
        imageFileName: imageFile?.name || "token-media",
        bannerDataUri,
        bannerFileName: bannerFile?.name || "token-banner",
        autoAttach,
        selectedBot: autoAttach ? selectedBot : "",
      });

      if (deployRes?.attachedToken && onAutoAttach) {
        await onAutoAttach(deployRes.attachedToken);
      } else if (deployRes?.autoAttached && onAutoAttach) {
        await onAutoAttach();
      }

      setVanityWallet((prev) =>
        prev
          ? {
              ...prev,
              status: "deployed",
              deployedMint: deployRes?.mint || "",
              deploySignature: deployRes?.signature || "",
            }
          : prev
      );
      setResult({
        mint: deployRes?.mint || "",
        signature: deployRes?.signature || "",
        solscanTx: deployRes?.solscanTx || "",
        solscanMint: deployRes?.solscanMint || "",
        pumpfunUrl: deployRes?.pumpfunUrl || "",
        autoAttached: Boolean(deployRes?.autoAttached),
        postDeployWarning: deployRes?.remainingSol > 0
          ? `Remaining SOL stays in the ${vanityWallet?.vanity ? "branded" : "regular"} deploy wallet (${Number(deployRes.remainingSol || 0).toFixed(6)} SOL).`
          : "",
      });
    } catch (e) {
      setError(e?.message || "Deploy wallet launch failed.");
    } finally {
      setDeployStep("");
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
    <div style={{position:"fixed",inset:0,zIndex:999,display:"flex",alignItems:"center",justifyContent:"center",animation:"fadeIn .2s ease"}}>
      <div style={{position:"absolute",inset:0,background:"rgba(0,0,0,.82)",backdropFilter:"blur(10px)"}} onClick={onClose}/>
      <div className="glass deploy-panel" onClick={e=>e.stopPropagation()} style={{position:"relative",zIndex:1,animation:"slideUp .25s ease",boxShadow:"0 32px 80px rgba(0,0,0,.6),0 0 60px rgba(255,106,0,.07)"}}>
        <div className="deploy-header" style={{position:"relative",display:"flex",justifyContent:"center",alignItems:"center"}}>
          <div style={{textAlign:"center"}}>
            <div style={{display:"inline-flex",alignItems:"center",gap:8,padding:"6px 10px",borderRadius:999,background:"rgba(255,106,0,.12)",border:"1px solid rgba(255,106,0,.28)",fontSize:11,fontWeight:800,color:"#ff9f5a",letterSpacing:.8,marginBottom:10}}>
              <span style={{fontSize:13}}>{"\u{1F525}"}</span>
              <span>DEPLOY</span>
            </div>
            <div style={{fontWeight:800,fontSize:24,color:"#fff",marginBottom:4}}>{t("deploy.launchToken")}</div>
            <div style={{fontSize:12,color:"rgba(255,255,255,.45)"}}>{t("deploy.subtitle")}</div>
            <div style={{display:"inline-flex",gap:8,marginTop:14,padding:4,borderRadius:999,background:"rgba(255,255,255,.04)",border:"1px solid rgba(255,255,255,.08)"}}>
              {[
                { key: "wallet", label: "Wallet Sign" },
                { key: "embr", label: "EMBER/EMBR Address" },
              ].map((tab) => {
                const active = deployMode === tab.key;
                return (
                  <button
                    key={tab.key}
                    className={active ? "btn-fire" : "btn-ghost"}
                    onClick={() => {
                      setDeployMode(tab.key);
                      setShowWalletSelect(false);
                      setError("");
                      setVanityMessage("");
                    }}
                    style={{
                      padding: "8px 14px",
                      fontSize: 12,
                      borderRadius: 999,
                      minWidth: 116,
                      opacity: active ? 1 : 0.9,
                    }}
                  >
                    {tab.label}
                  </button>
                );
              })}
            </div>
          </div>
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

        {deployMode === "embr" && (
          <div style={{marginTop:12,border:"1px solid rgba(255,106,0,.24)",borderRadius:14,background:"linear-gradient(180deg, rgba(255,106,0,.08), rgba(255,255,255,.02))",padding:"14px 14px"}}>
            <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",gap:12,marginBottom:10}}>
              <div>
                <div style={{fontSize:14,fontWeight:800,color:"#fff"}}>Deploy With EMBER/EMBR Address</div>
                <div style={{fontSize:12,color:"rgba(255,255,255,.58)",marginTop:4,lineHeight:1.55}}>
                  Reserve either a branded EMBR / EMBER wallet or a regular random wallet, fund the minimum required SOL, then deploy without connecting a wallet. This becomes the real creator wallet for the token.
                </div>
              </div>
              <div
                style={{
                  minWidth: 12,
                  width: 12,
                  height: 12,
                  borderRadius: 999,
                  background: vanityReady ? "#52ffb3" : "rgba(255,255,255,.18)",
                  boxShadow: vanityReady ? "0 0 18px rgba(82,255,179,.75)" : "none",
                }}
              />
            </div>

            <div style={{background:"rgba(255,64,96,.08)",border:"1px solid rgba(255,64,96,.2)",borderRadius:12,padding:"10px 12px",fontSize:12,color:"rgba(255,230,235,.9)",lineHeight:1.65}}>
              This wallet is generated for this deploy flow and becomes the actual deployer/creator wallet on-chain. Developer buys and sells from this wallet can show on the chart as creator activity. External deposit and creator-reward bots do not make the bot wallet the token creator. Fund only the amount you are comfortable using for launch. Save the private key immediately. Anyone with that key controls the wallet and any leftover SOL.
            </div>

            {!vanityWallet && (
              <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:10,marginTop:12}}>
                {[
                  { key: "vanity", label: "Branded EMBR / EMBER", hint: "Uses a vanity wallet from the address pool." },
                  { key: "regular", label: "Regular Random", hint: "Skips vanity and generates a normal Solana wallet." },
                ].map((option) => {
                  const active = deployWalletStyle === option.key;
                  return (
                    <button
                      key={option.key}
                      type="button"
                      onClick={() => setDeployWalletStyle(option.key)}
                      style={{
                        textAlign:"left",
                        border: active ? "1px solid rgba(255,106,0,.45)" : "1px solid rgba(255,255,255,.08)",
                        background: active ? "rgba(255,106,0,.10)" : "rgba(255,255,255,.03)",
                        borderRadius: 12,
                        padding: "12px 14px",
                        color: "#fff",
                        cursor: "pointer",
                      }}
                    >
                      <div style={{fontSize:13,fontWeight:800}}>{option.label}</div>
                      <div style={{fontSize:11,color:"rgba(255,255,255,.46)",marginTop:5,lineHeight:1.5}}>{option.hint}</div>
                    </button>
                  );
                })}
              </div>
            )}

            {!vanityWallet && (
              <div style={{marginTop:12,display:"flex",justifyContent:"space-between",alignItems:"center",gap:12,flexWrap:"wrap"}}>
                <div style={{fontSize:12,color:"rgba(255,255,255,.62)",lineHeight:1.6}}>
                  Minimum funding required: <span style={{color:"#fff",fontWeight:800}}>{(Number(form.initialBuySol || 0) + DEPLOY_VANITY_BUFFER_SOL).toFixed(6)} SOL</span>
                  <br />
                  This includes your initial buy plus launch buffer for network and account costs. When this wallet is funded, deploy unlocks automatically.
                </div>
                <button className="btn-fire" onClick={reserveVanityWallet} disabled={vanityLoading || loading} style={{padding:"10px 14px",fontSize:12}}>
                  {vanityLoading ? "Generating..." : deployWalletStyle === "regular" ? "Generate Regular Wallet" : "Generate Branded Wallet"}
                </button>
              </div>
            )}

            {vanityWallet && (
              <div style={{marginTop:12,display:"grid",gap:12}}>
                <div style={{display:"grid",gridTemplateColumns:"1fr auto",gap:10,alignItems:"start"}}>
                  <div style={{padding:"12px 12px",borderRadius:12,background:"rgba(255,255,255,.03)",border:"1px solid rgba(255,255,255,.08)"}}>
                    <div style={{fontSize:11,color:"rgba(255,255,255,.42)",fontWeight:700,letterSpacing:1,marginBottom:6}}>
                      {vanityWallet?.vanity ? "BRANDED DEPLOY ADDRESS" : "REGULAR DEPLOY ADDRESS"}
                    </div>
                    <div style={{fontSize:13,color:"#fff",wordBreak:"break-all",lineHeight:1.6}}>{vanityWallet.deposit}</div>
                  </div>
                  <button className="btn-ghost" onClick={() => copyText(vanityWallet.deposit, "Deploy address copied.")} style={{padding:"10px 12px",fontSize:12}}>
                    Copy
                  </button>
                </div>

                <div style={{display:"grid",gridTemplateColumns:"repeat(3, minmax(0,1fr))",gap:10}}>
                  <div style={{padding:"10px 12px",borderRadius:12,background:"rgba(255,255,255,.03)",border:"1px solid rgba(255,255,255,.08)"}}>
                    <div style={{fontSize:11,color:"rgba(255,255,255,.42)",fontWeight:700,letterSpacing:1,marginBottom:6}}>REQUIRED</div>
                    <div style={{fontSize:16,fontWeight:800,color:"#fff"}}>{Number(vanityWallet.requiredSol || 0).toFixed(6)} SOL</div>
                  </div>
                  <div style={{padding:"10px 12px",borderRadius:12,background:"rgba(255,255,255,.03)",border:"1px solid rgba(255,255,255,.08)"}}>
                    <div style={{fontSize:11,color:"rgba(255,255,255,.42)",fontWeight:700,letterSpacing:1,marginBottom:6}}>CURRENT BALANCE</div>
                    <div style={{fontSize:16,fontWeight:800,color:vanityReady ? "#7fffd0" : "#fff"}}>{Number(vanityWallet.balanceSol || 0).toFixed(6)} SOL</div>
                  </div>
                  <div style={{padding:"10px 12px",borderRadius:12,background:"rgba(255,255,255,.03)",border:"1px solid rgba(255,255,255,.08)"}}>
                    <div style={{fontSize:11,color:"rgba(255,255,255,.42)",fontWeight:700,letterSpacing:1,marginBottom:6}}>STATUS</div>
                    <div style={{fontSize:14,fontWeight:800,color:vanityReady ? "#7fffd0" : "#ffcf7d"}}>
                      {vanityReady ? "FUNDED" : "AWAITING FUNDING"}
                    </div>
                  </div>
                </div>

                {!vanityReady && (
                  <div style={{fontSize:12,color:"rgba(255,255,255,.62)",lineHeight:1.6}}>
                    Shortfall: <span style={{color:"#fff",fontWeight:800}}>{Number(vanityWallet.shortfallSol || 0).toFixed(6)} SOL</span>
                  </div>
                )}

                <div style={{display:"flex",gap:8,flexWrap:"wrap"}}>
                  <button className="btn-ghost" onClick={refreshVanityWallet} disabled={vanityLoading || loading} style={{padding:"9px 12px",fontSize:12}}>
                    {vanityLoading ? "Refreshing..." : "Refresh Funding"}
                  </button>
                  <button className="btn-ghost" onClick={() => setShowVanitySecret((prev) => !prev)} style={{padding:"9px 12px",fontSize:12}}>
                    {showVanitySecret ? "Hide Private Key" : "Reveal Private Key"}
                  </button>
                </div>

                {showVanitySecret && (
                  <div style={{display:"grid",gap:10}}>
                    <div style={{padding:"12px 12px",borderRadius:12,background:"rgba(0,0,0,.28)",border:"1px solid rgba(255,64,96,.22)"}}>
                      <div style={{fontSize:11,color:"rgba(255,255,255,.42)",fontWeight:700,letterSpacing:1,marginBottom:6}}>PRIVATE KEY (BASE58)</div>
                      <div style={{fontSize:12,color:"#fff",wordBreak:"break-all",lineHeight:1.6}}>{vanityWallet.privateKeyBase58}</div>
                    </div>
                    <div style={{display:"flex",gap:8,flexWrap:"wrap"}}>
                      <button className="btn-ghost" onClick={() => copyText(vanityWallet.privateKeyBase58, "Private key copied.")} style={{padding:"9px 12px",fontSize:12}}>
                        Copy Base58
                      </button>
                      <button className="btn-ghost" onClick={() => copyText(JSON.stringify(vanityWallet.privateKeyArray || []), "Private key array copied.")} style={{padding:"9px 12px",fontSize:12}}>
                        Copy JSON Array
                      </button>
                    </div>
                  </div>
                )}

                {vanityMessage && (
                  <div style={{fontSize:12,color:"rgba(158,245,197,.92)",lineHeight:1.6}}>{vanityMessage}</div>
                )}
              </div>
            )}
          </div>
        )}

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
        {loading && deployStep && (
          <div style={{marginTop:10,fontSize:12,color:"rgba(255,255,255,.68)"}}>{deployStep}</div>
        )}

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
          <button
            className="btn-fire"
            onClick={deployMode === "embr" ? submitVanity : handleDeployClick}
            disabled={
              loading ||
              requiresLoginForSubmit ||
              walletConnecting ||
              (deployMode === "embr" && (!vanityWallet?.reservationId || !vanityReady))
            }
            style={{padding:"10px 18px",fontSize:13}}
          >
            {loading
              ? t("deploy.deploying")
                : walletConnecting
                  ? t("deploy.connectingWallet")
                : deployMode === "embr"
                  ? deployWalletStyle === "regular"
                    ? "Deploy From Regular Wallet"
                    : "Deploy From Branded Wallet"
                  : t("deploy.deploy")}
          </button>
        </div>
      </div>
      {deployMode === "wallet" && showWalletSelect && (
        <div
          style={{position:"fixed",inset:0,zIndex:1200,display:"flex",alignItems:"center",justifyContent:"center"}}
          onClick={()=>setShowWalletSelect(false)}
        >
          <div style={{position:"absolute",inset:0,background:"rgba(0,0,0,.72)"}} />
          <div
            className="glass"
            onClick={(e)=>e.stopPropagation()}
            style={{position:"relative",zIndex:1,width:"min(360px,92vw)",padding:"16px 16px",border:"1px solid rgba(255,106,0,.24)"}}
          >
            <div style={{fontSize:16,fontWeight:800,color:"#fff",marginBottom:6}}>Select Wallet</div>
            <div style={{fontSize:12,color:"rgba(255,255,255,.5)",marginBottom:12}}>Choose which wallet signs this deploy.</div>
            <div style={{display:"flex",flexDirection:"column",gap:8}}>
              {getInjectedWallets().map((w) => (
                <button
                  key={w.key}
                  className="btn-fire"
                  style={{padding:"10px 12px",fontSize:13}}
                  onClick={() => {
                    setShowWalletSelect(false);
                    setWalletChoice(w.key);
                    void submit(w.key);
                  }}
                >
                  {w.label}
                </button>
              ))}
              {!getInjectedWallets().length && (
                <div style={{fontSize:12,color:"#ff8f8f"}}>No supported wallet found. Install Phantom or Solflare.</div>
              )}
            </div>
            <div style={{display:"flex",justifyContent:"flex-end",marginTop:12}}>
              <button className="btn-ghost" style={{padding:"8px 12px",fontSize:12}} onClick={()=>setShowWalletSelect(false)}>Cancel</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
