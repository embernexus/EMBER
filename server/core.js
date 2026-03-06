import crypto from "node:crypto";
import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import { createRequire } from "node:module";
import os from "node:os";
import path from "node:path";
import bcrypt from "bcryptjs";
import BN from "bn.js";
import bs58 from "bs58";
import {
  Connection,
  Keypair,
  LAMPORTS_PER_SOL,
  PublicKey,
  SystemProgram,
  Transaction,
  TransactionMessage,
  VersionedTransaction,
} from "@solana/web3.js";
import {
  TOKEN_2022_PROGRAM_ID,
  TOKEN_PROGRAM_ID,
  createAssociatedTokenAccountInstruction,
  createTransferInstruction,
  getAssociatedTokenAddressSync,
} from "@solana/spl-token";
import { config } from "./config.js";
import { makeId, makeSessionToken, pool, withTx } from "./db.js";
import { fmtInt, resolveMint } from "./utils.js";

const require = createRequire(import.meta.url);
const pumpSdkPkg = require("@pump-fun/pump-sdk");
const {
  OnlinePumpSdk,
  PumpSdk,
  feeSharingConfigPda,
  getBuyTokenAmountFromSolAmount,
} = pumpSdkPkg || {};
const pumpOfflineSdk = PumpSdk ? new PumpSdk() : null;

function toToken(row) {
  const moduleConfig =
    row.module_config && typeof row.module_config === "object"
      ? row.module_config
      : {};
  const moduleState =
    row.module_state && typeof row.module_state === "object"
      ? row.module_state
      : {};
  return {
    id: row.id,
    symbol: row.symbol,
    name: row.name,
    mint: row.mint,
    pictureUrl: row.picture_url || "",
    deposit: row.deposit,
    claimSec: Number(row.claim_sec),
    burnSec: Number(row.burn_sec),
    splits: Number(row.splits),
    selectedBot: String(row.selected_bot || "burn"),
    active: row.active,
    disconnected: Boolean(row.disconnected),
    burned: Number(
      Number.isFinite(Number(row.display_burned))
        ? Number(row.display_burned)
        : row.burned
    ),
    pending: Number(row.pending),
    txCount: Number(row.tx_count),
    marketCap: Number(row.market_cap) || 0,
    moduleType: String(row.module_type || row.selected_bot || "burn"),
    moduleEnabled:
      typeof row.module_enabled === "boolean"
        ? row.module_enabled
        : Boolean(row.active) && !Boolean(row.disconnected),
    moduleConfig,
    moduleState,
    moduleLastError: row.module_last_error ? String(row.module_last_error) : "",
  };
}

function toEvent(row) {
  const createdAt = new Date(row.created_at).getTime();
  const age = Math.max(0, Math.floor((Date.now() - createdAt) / 1000));
  return {
    id: row.id,
    tokenId: row.token_id,
    token: row.token_symbol,
    moduleType: row.module_type ? String(row.module_type) : "",
    type: row.event_type,
    amount: Number(row.amount || 0),
    msg: row.message,
    tx: row.tx,
    age,
    createdAt: row.created_at,
  };
}

function parseBurnAmount(row) {
  const direct = Number(row?.amount || 0);
  if (Number.isFinite(direct) && direct > 0) return direct;
  const match = String(row?.message || "").match(/([0-9][0-9,._]*)/);
  if (!match) return 0;
  const normalized = match[1].replace(/,/g, "");
  const amount = Number(normalized);
  return Number.isFinite(amount) && amount > 0 ? amount : 0;
}

function buildChartData(rows) {
  const dayLetters = ["S", "M", "T", "W", "T", "F", "S"];
  const days = [];
  const byDate = new Map();
  const now = new Date();

  for (let i = 6; i >= 0; i -= 1) {
    const d = new Date(now);
    d.setHours(0, 0, 0, 0);
    d.setDate(d.getDate() - i);
    const key = d.toISOString().slice(0, 10);
    days.push({
      key,
      d: dayLetters[d.getDay()],
      v: 0,
    });
    byDate.set(key, days[days.length - 1]);
  }

  for (const row of rows) {
    const created = new Date(row.created_at);
    if (Number.isNaN(created.getTime())) continue;
    const key = created.toISOString().slice(0, 10);
    const bucket = byDate.get(key);
    if (!bucket) continue;
    const amount = parseBurnAmount(row);
    bucket.v += amount > 0 ? amount : 1;
  }

  return days;
}

function normalizeUsername(username) {
  const value = String(username || "").trim();
  if (!/^[A-Za-z0-9_]{3,24}$/.test(value)) {
    throw new Error("Username must be 3-24 chars (letters, numbers, underscore).");
  }
  return value;
}

function normalizePassword(password) {
  const value = String(password || "");
  if (value.length < 6) {
    throw new Error("Password must be at least 6 characters.");
  }
  return value;
}

function sanitizeInterval(value, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(60, Math.floor(n));
}

function sanitizeSplits(value, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(1, Math.floor(n));
}

function sanitizeRange(value, fallback, min, max) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, n));
}

function sanitizeSol(value, fallback, min = 0, max = 10_000) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, n));
}

const MODULE_TYPES = {
  burn: "burn",
  volume: "volume",
  personalBurn: "personal_burn",
};

function moduleTypeLabel(moduleType) {
  if (moduleType === MODULE_TYPES.volume) return "Volume Bot";
  if (moduleType === "market_maker") return "Market Maker Bot";
  if (moduleType === MODULE_TYPES.personalBurn) return "Personal Burn Bot";
  return "Burn Bot";
}

function isSoftClaimError(error) {
  const msg = String(error?.message || "").toLowerCase();
  if (!msg) return false;
  return (
    msg.includes("nothing to collect") ||
    msg.includes("no creator fee") ||
    msg.includes("no claimable") ||
    msg.includes("no creator rewards")
  );
}

function extractClaimLamportDeficit(error) {
  const logs = Array.isArray(error?.logs) ? error.logs.join("\n") : "";
  const raw = [String(error?.message || ""), logs].filter(Boolean).join("\n");
  if (!raw) return 0;
  const match = raw.match(/insufficient lamports\s+(\d+),\s+need\s+(\d+)/i);
  if (!match) return 0;
  const have = Number(match[1] || 0);
  const need = Number(match[2] || 0);
  if (!Number.isFinite(have) || !Number.isFinite(need)) return 0;
  return Math.max(0, Math.floor(need - have));
}

const MODULE_LOCK_SEED = "ember:module:lock";

function normalizeModuleType(value, fallback = MODULE_TYPES.burn) {
  const raw = String(value || "").trim().toLowerCase();
  if (raw === MODULE_TYPES.volume) return MODULE_TYPES.volume;
  if (raw === MODULE_TYPES.personalBurn) return MODULE_TYPES.personalBurn;
  return fallback;
}

function toLamports(sol) {
  const n = Number(sol || 0);
  if (!Number.isFinite(n) || n <= 0) return 0;
  return Math.floor(n * LAMPORTS_PER_SOL);
}

function fromLamports(lamports) {
  const n = Number(lamports || 0);
  if (!Number.isFinite(n) || n <= 0) return 0;
  return n / LAMPORTS_PER_SOL;
}

function normalizeMint(value) {
  const mint = String(value || "").trim();
  if (mint.length < 32) throw new Error("Mint is required.");
  try {
    return new PublicKey(mint).toBase58();
  } catch {
    throw new Error("Mint address is invalid.");
  }
}

function knownMintFromResolve(mint) {
  const value = String(mint || "").trim();
  if (!value) return null;
  const resolved = resolveMint(value);
  if (!resolved) return null;
  const fallbackSymbol = value.slice(0, 4).toUpperCase();
  const fallbackName = `Token (${value.slice(0, 6)}...)`;
  const symbol = String(resolved.symbol || "").trim();
  const name = String(resolved.name || "").trim();
  const pictureUrl = normalizeMediaUrl(resolved.pictureUrl || "");
  const isFallback = symbol === fallbackSymbol && name === fallbackName && !pictureUrl;
  if (isFallback) return null;
  return {
    symbol: symbol.slice(0, 12),
    name: name.slice(0, 64),
    pictureUrl: pictureUrl.slice(0, 255),
  };
}

async function fetchJsonWithTimeout(url, options = {}, timeoutMs = 4500) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      ...options,
      signal: ctrl.signal,
    });
    if (!res.ok) return null;
    const data = await res.json().catch(() => null);
    return data && typeof data === "object" ? data : null;
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

function normalizeMediaUrl(input) {
  const value = String(input || "").trim();
  if (!value) return "";
  if (value.startsWith("ipfs://")) {
    return `https://ipfs.io/ipfs/${value.slice("ipfs://".length).replace(/^ipfs\//i, "")}`;
  }
  if (value.startsWith("ar://")) {
    return `https://arweave.net/${value.slice("ar://".length)}`;
  }
  return value;
}

async function resolveMintViaDas(mint) {
  const rpcUrl = String(config.rpcUrl || "").trim();
  if (!rpcUrl) return null;

  const body = {
    jsonrpc: "2.0",
    id: "mint-meta",
    method: "getAsset",
    params: {
      id: mint,
      displayOptions: {
        showFungible: true,
      },
    },
  };

  const data = await fetchJsonWithTimeout(
    rpcUrl,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    },
    5000
  );
  if (!data?.result) return null;

  const result = data.result;
  const metadata = result.content?.metadata || {};
  const tokenInfo = result.token_info || {};
  const files = Array.isArray(result.content?.files) ? result.content.files : [];
  const file = files[0] || {};
  const symbol = String(metadata.symbol || tokenInfo.symbol || "").trim();
  const name = String(metadata.name || "").trim();
  const pictureUrl = normalizeMediaUrl(
    result.content?.links?.image || file.cdn_uri || file.uri || ""
  );

  if (!symbol && !name && !pictureUrl) return null;
  return {
    symbol: symbol.slice(0, 12),
    name: name.slice(0, 64),
    pictureUrl: pictureUrl.slice(0, 255),
  };
}

async function resolveMintViaPump(mint) {
  const data = await fetchJsonWithTimeout(`https://frontend-api.pump.fun/coins/${mint}`, {}, 4500);
  if (!data) return null;
  const symbol = String(data.symbol || "").trim();
  const name = String(data.name || "").trim();
  const pictureUrl = normalizeMediaUrl(data.image_uri || data.image || "");
  if (!symbol && !name && !pictureUrl) return null;
  return {
    symbol: symbol.slice(0, 12),
    name: name.slice(0, 64),
    pictureUrl: pictureUrl.slice(0, 255),
  };
}

async function resolveMintViaDexscreener(mint) {
  const data = await fetchJsonWithTimeout(
    `https://api.dexscreener.com/latest/dex/tokens/${mint}`,
    {},
    4500
  );
  const pairs = Array.isArray(data?.pairs) ? data.pairs : [];
  if (!pairs.length) return null;

  const match = pairs.find((p) => {
    const baseAddr = String(p?.baseToken?.address || "");
    const quoteAddr = String(p?.quoteToken?.address || "");
    return baseAddr === mint || quoteAddr === mint;
  }) || pairs[0];

  const baseAddr = String(match?.baseToken?.address || "");
  const picked =
    baseAddr === mint
      ? match?.baseToken
      : String(match?.quoteToken?.address || "") === mint
        ? match?.quoteToken
        : match?.baseToken;

  const symbol = String(picked?.symbol || "").trim();
  const name = String(picked?.name || "").trim();
  const pictureUrl = normalizeMediaUrl(match?.info?.imageUrl || "");
  if (!symbol && !name && !pictureUrl) return null;
  return {
    symbol: symbol.slice(0, 12),
    name: name.slice(0, 64),
    pictureUrl: pictureUrl.slice(0, 255),
  };
}

export async function resolveMintMetadata(mintInput) {
  const mint = normalizeMint(mintInput);
  let marketCap = 0;
  try {
    marketCap = await fetchMarketCapUsd(mint);
  } catch {
    marketCap = 0;
  }

  const known = knownMintFromResolve(mint);
  if (known?.symbol && known?.name) {
    return { mint, ...known, marketCap };
  }

  const sources = [
    () => resolveMintViaDas(mint),
    () => resolveMintViaPump(mint),
    () => resolveMintViaDexscreener(mint),
  ];

  for (const source of sources) {
    const data = await source();
    if (!data) continue;
    const symbol = String(data.symbol || "").trim().toUpperCase().slice(0, 12);
    const name = String(data.name || "").trim().slice(0, 64);
    if (!symbol || !name) continue;
    return {
      mint,
      symbol,
      name,
      pictureUrl: normalizeMediaUrl(data.pictureUrl || "").slice(0, 255),
      marketCap,
    };
  }

  throw new Error("Unable to resolve token metadata for this mint.");
}

function moduleConfigIntervalSec(moduleType, configJson = {}) {
  if (moduleType === MODULE_TYPES.volume) {
    const speed = Math.max(0, Math.min(100, Number(configJson.speed ?? 35)));
    const sec = 25 - Math.round(speed * 0.2);
    return Math.max(3, sec);
  }
  if (moduleType === MODULE_TYPES.personalBurn) return 120;
  const burnSec = Math.max(60, Math.floor(Number(configJson.burnIntervalSec ?? configJson.intervalSec ?? 300)));
  const claimEnabled = configJson.claimEnabled !== false;
  const claimSec = Math.max(60, Math.floor(Number(configJson.claimIntervalSec ?? 120)));
  const loopSec = claimEnabled ? Math.min(burnSec, claimSec) : burnSec;
  return Math.max(5, loopSec);
}

const MARKET_CAP_REFRESH_MS = 10_000;
const MARKET_CAP_RETRY_MS = 7_500;
const MARKET_CAP_CACHE_MAX = 2_000;
const marketCapCache = new Map();

function parseNumberish(value) {
  if (typeof value === "number") return Number.isFinite(value) ? value : 0;
  if (typeof value === "string") {
    const cleaned = value.replace(/[$,\s]/g, "");
    const n = Number(cleaned);
    return Number.isFinite(n) ? n : 0;
  }
  return 0;
}

function pickDexPairForMint(pairs, mint) {
  const list = Array.isArray(pairs) ? pairs : [];
  const relevant = list.filter((pair) => {
    const base = String(pair?.baseToken?.address || "");
    const quote = String(pair?.quoteToken?.address || "");
    return base === mint || quote === mint;
  });
  const candidates = relevant.length ? relevant : list;
  if (!candidates.length) return null;
  return candidates
    .slice()
    .sort((a, b) => parseNumberish(b?.liquidity?.usd) - parseNumberish(a?.liquidity?.usd))[0];
}

async function fetchMarketCapViaDexscreener(mint) {
  const data = await fetchJsonWithTimeout(
    `https://api.dexscreener.com/latest/dex/tokens/${mint}`,
    {},
    3500
  );
  const pair = pickDexPairForMint(data?.pairs || [], mint);
  if (!pair) return 0;
  const marketCap = parseNumberish(pair?.marketCap);
  if (marketCap > 0) return marketCap;
  const fdv = parseNumberish(pair?.fdv);
  return fdv > 0 ? fdv : 0;
}

async function fetchMarketCapViaPump(mint) {
  const data = await fetchJsonWithTimeout(`https://frontend-api.pump.fun/coins/${mint}`, {}, 3500);
  if (!data || typeof data !== "object") return 0;
  const fields = [
    data.usd_market_cap,
    data.market_cap,
    data.marketCap,
    data.usdMarketCap,
    data.fdv,
  ];
  for (const field of fields) {
    const value = parseNumberish(field);
    if (value > 0) return value;
  }
  return 0;
}

async function fetchMarketCapUsd(mint) {
  const dex = await fetchMarketCapViaDexscreener(mint);
  if (dex > 0) return dex;
  const pump = await fetchMarketCapViaPump(mint);
  return pump > 0 ? pump : 0;
}

function getMarketCapEntry(mint) {
  const key = String(mint || "").trim();
  if (!key) return null;
  let entry = marketCapCache.get(key);
  if (!entry) {
    entry = { value: 0, at: 0, nextRefreshAt: 0, inflight: null };
    marketCapCache.set(key, entry);
    if (marketCapCache.size > MARKET_CAP_CACHE_MAX) {
      const oldest = marketCapCache.keys().next().value;
      if (oldest) marketCapCache.delete(oldest);
    }
  }
  return entry;
}

function scheduleMarketCapRefresh(mint, entry) {
  if (!entry || entry.inflight) return;
  entry.inflight = (async () => {
    try {
      const value = await fetchMarketCapUsd(mint);
      entry.value = value > 0 ? value : 0;
      entry.at = Date.now();
      entry.nextRefreshAt = entry.at + MARKET_CAP_REFRESH_MS;
    } catch {
      entry.nextRefreshAt = Date.now() + MARKET_CAP_RETRY_MS;
    } finally {
      entry.inflight = null;
    }
  })();
}

function readCachedMarketCapUsd(mint) {
  const key = String(mint || "").trim();
  if (!key) return 0;
  const entry = getMarketCapEntry(key);
  if (!entry) return 0;
  if (Date.now() >= Number(entry.nextRefreshAt || 0)) {
    scheduleMarketCapRefresh(key, entry);
  }
  return Number(entry.value) || 0;
}

function attachMarketCaps(rows) {
  if (!Array.isArray(rows) || !rows.length) return [];
  const byMint = new Map();
  for (const row of rows) {
    const mint = String(row?.mint || "").trim();
    if (!mint || byMint.has(mint)) continue;
    byMint.set(mint, readCachedMarketCapUsd(mint));
  }
  return rows.map((row) => {
    const mint = String(row?.mint || "").trim();
    return {
      ...row,
      market_cap: Number(byMint.get(mint)) || 0,
    };
  });
}

function defaultBurnModuleConfig(tokenRow) {
  return {
    claimEnabled: true,
    claimIntervalSec: Math.max(60, Number(tokenRow?.claim_sec || 120)),
    burnIntervalSec: Math.max(60, Number(tokenRow?.burn_sec || 300)),
    splitBuys: Math.max(1, Number(tokenRow?.splits || 1)),
    minProcessSol: 0.01,
    slippageBps: 1000,
    pool: "auto",
    reserveSol: Math.max(0.005, Number(config.botSolReserve || 0.01)),
  };
}

function defaultVolumeModuleConfig() {
  return {
    claimEnabled: false,
    claimIntervalSec: 120,
    tradeWalletCount: 1,
    speed: 35,
    aggression: 35,
    minTradeSol: Math.max(0.001, Number(config.volumeDefaultMinTradeSol || 0.01)),
    maxTradeSol: Math.max(0.005, Number(config.volumeDefaultMaxTradeSol || 0.05)),
    reserveSol: Math.max(0.005, Number(config.botSolReserve || 0.01)),
    pool: "auto",
  };
}

function mergeModuleConfig(moduleType, currentConfig, nextConfig = {}) {
  const base =
    moduleType === MODULE_TYPES.volume
      ? defaultVolumeModuleConfig()
      : moduleType === MODULE_TYPES.personalBurn
        ? defaultBurnModuleConfig({})
        : defaultBurnModuleConfig({});
  return {
    ...base,
    ...(currentConfig || {}),
    ...(nextConfig || {}),
  };
}

function normalizeHttpUrl(value, label) {
  const str = String(value || "").trim();
  let parsed;
  try {
    parsed = new URL(str);
  } catch {
    throw new Error(`${label} must be a valid URL.`);
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    throw new Error(`${label} must start with http:// or https://`);
  }
  return parsed.toString();
}

function sanitizeDeployString(value, maxLen, label) {
  const v = String(value || "").trim();
  if (!v) throw new Error(`${label} is required.`);
  return v.slice(0, maxLen);
}

function sanitizeDeployNumber(value, fallback, min, max) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, n));
}

function parseDataUriMedia(dataUri) {
  const raw = String(dataUri || "").trim();
  const match = raw.match(/^data:([a-zA-Z0-9.+/-]+);base64,([A-Za-z0-9+/=]+)$/);
  if (!match) {
    throw new Error("Token media upload is invalid. Please choose a valid JPG, PNG, GIF, or MP4 file.");
  }
  const imageType = String(match[1] || "").toLowerCase();
  const allowedImageTypes = new Set(["image/jpeg", "image/png", "image/gif"]);
  const allowedVideoTypes = new Set(["video/mp4"]);
  const isImage = allowedImageTypes.has(imageType);
  const isVideo = allowedVideoTypes.has(imageType);
  if (!isImage && !isVideo) {
    throw new Error("Unsupported media type. Allowed: JPG, PNG, GIF, MP4.");
  }
  const base64 = match[2];
  const bytes = Buffer.from(base64, "base64");
  if (!bytes.length) {
    throw new Error("Token media upload is empty.");
  }
  if (isImage && bytes.length > 15 * 1024 * 1024) {
    throw new Error("Image must be 15MB or smaller.");
  }
  if (isVideo && bytes.length > 30 * 1024 * 1024) {
    throw new Error("Video must be 30MB or smaller.");
  }
  return {
    imageType,
    imageBlob: new Blob([bytes], { type: imageType }),
  };
}

function parseDataUriBanner(dataUri) {
  const raw = String(dataUri || "").trim();
  const match = raw.match(/^data:([a-zA-Z0-9.+/-]+);base64,([A-Za-z0-9+/=]+)$/);
  if (!match) {
    throw new Error("Banner upload is invalid. Please choose a valid JPG, PNG, or GIF file.");
  }
  const bannerType = String(match[1] || "").toLowerCase();
  const allowedBannerTypes = new Set(["image/jpeg", "image/png", "image/gif"]);
  if (!allowedBannerTypes.has(bannerType)) {
    throw new Error("Unsupported banner type. Allowed: JPG, PNG, GIF.");
  }
  const base64 = match[2];
  const bytes = Buffer.from(base64, "base64");
  if (!bytes.length) {
    throw new Error("Banner upload is empty.");
  }
  if (bytes.length > Math.floor(4.3 * 1024 * 1024)) {
    throw new Error("Banner must be 4.3MB or smaller.");
  }
  return {
    bannerType,
    bannerBlob: new Blob([bytes], { type: bannerType }),
  };
}

const DEPLOY_BOT_PRESETS = {
  burn: { claimSec: 120, burnSec: 300, splits: 1 },
  volume: { claimSec: 90, burnSec: 240, splits: 3 },
  market_maker: { claimSec: 60, burnSec: 180, splits: 4 },
  ai_trading: { claimSec: 60, burnSec: 120, splits: 5 },
};

function getDeployBotPreset(value) {
  const key = String(value || "").trim();
  return DEPLOY_BOT_PRESETS[key] || null;
}

function getConnection() {
  if (!config.rpcUrl) {
    throw new Error("SOLANA_RPC_URL is required for bot execution.");
  }
  return new Connection(config.rpcUrl, "confirmed");
}

function keypairFromBase58(secretKeyBase58) {
  const raw = String(secretKeyBase58 || "").trim();
  if (!raw) throw new Error("Missing secret key.");
  const bytes = bs58.decode(raw);
  return Keypair.fromSecretKey(bytes);
}

function clampPct(value, fallback = 0) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(0, Math.min(1, n));
}

async function sendSolTransfer(connection, signer, toAddress, lamports) {
  const amount = Math.floor(Number(lamports || 0));
  if (amount <= 0) return null;
  const toPubkey = new PublicKey(String(toAddress || "").trim());
  const latest = await connection.getLatestBlockhash("confirmed");
  const tx = new Transaction({
    feePayer: signer.publicKey,
    blockhash: latest.blockhash,
    lastValidBlockHeight: latest.lastValidBlockHeight,
  }).add(
    SystemProgram.transfer({
      fromPubkey: signer.publicKey,
      toPubkey,
      lamports: amount,
    })
  );
  tx.sign(signer);
  const sig = await connection.sendRawTransaction(tx.serialize(), { skipPreflight: false, maxRetries: 5 });
  await connection.confirmTransaction(
    { signature: sig, blockhash: latest.blockhash, lastValidBlockHeight: latest.lastValidBlockHeight },
    "confirmed"
  );
  return sig;
}

async function getOwnerTokenBalanceUi(connection, owner, mint) {
  const ownerPk = owner instanceof PublicKey ? owner : new PublicKey(owner);
  const mintPk = mint instanceof PublicKey ? mint : new PublicKey(mint);
  const balances = await getOwnerTokenAccountBalancesForMint(connection, ownerPk, mintPk, null);
  const rawPathTotal = balances.reduce((sum, item) => sum + item.uiAmount, 0);

  // Parsed fallback path for RPCs that expose parsed balances but not raw account bytes consistently.
  let parsedTotal = 0;
  for (const programId of [TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID]) {
    try {
      const parsed = await connection.getParsedTokenAccountsByOwner(ownerPk, { programId }, "confirmed");
      for (const item of parsed?.value || []) {
        const tokenMint = String(item?.account?.data?.parsed?.info?.mint || "");
        if (tokenMint !== mintPk.toBase58()) continue;
        const ui = Number(item?.account?.data?.parsed?.info?.tokenAmount?.uiAmount || 0);
        if (Number.isFinite(ui) && ui > 0) parsedTotal += ui;
      }
    } catch {
      // best effort
    }
  }

  return Math.max(rawPathTotal, parsedTotal);
}

function getTokenAccountMintAddress(rawData) {
  if (!rawData) return "";
  let buf = null;
  if (Buffer.isBuffer(rawData)) {
    buf = rawData;
  } else if (rawData instanceof Uint8Array) {
    buf = Buffer.from(rawData);
  } else if (Array.isArray(rawData) && typeof rawData[0] === "string") {
    try {
      buf = Buffer.from(rawData[0], "base64");
    } catch {
      buf = null;
    }
  }
  if (!buf || buf.length < 32) return "";
  try {
    return new PublicKey(buf.subarray(0, 32)).toBase58();
  } catch {
    return "";
  }
}

async function getOwnerTokenAccountBalancesForMint(connection, owner, mint, tokenProgramId = null) {
  const ownerPk = owner instanceof PublicKey ? owner : new PublicKey(owner);
  const mintPk = mint instanceof PublicKey ? mint : new PublicKey(mint);
  const mintText = mintPk.toBase58();
  const balances = [];
  const programs = tokenProgramId
    ? [tokenProgramId]
    : [TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID];

  for (const programId of programs) {
    let accounts = [];
    try {
      const res = await connection.getTokenAccountsByOwner(ownerPk, { programId }, "confirmed");
      accounts = Array.isArray(res?.value) ? res.value : [];
    } catch {
      accounts = [];
    }

    for (const item of accounts) {
      const tokenMint = getTokenAccountMintAddress(item?.account?.data);
      if (tokenMint !== mintText) continue;
      const pubkey = item?.pubkey;
      if (!pubkey) continue;
      try {
        const bal = await connection.getTokenAccountBalance(pubkey, "confirmed");
        const amountRaw = BigInt(String(bal?.value?.amount || "0"));
        const decimals = Number(bal?.value?.decimals || 0);
        const uiAmount = Number(bal?.value?.uiAmountString || bal?.value?.uiAmount || 0);
        if (amountRaw <= 0n) continue;
        balances.push({
          pubkey,
          amountRaw,
          decimals,
          uiAmount: Number.isFinite(uiAmount) ? uiAmount : 0,
          programId,
        });
      } catch {
        // best effort
      }
    }
  }

  return balances;
}

function trimNumber(value, digits = 6) {
  const n = Number(value || 0);
  if (!Number.isFinite(n)) return "0";
  return n.toFixed(digits).replace(/\.?0+$/, "");
}

async function sendTelegramAnnouncement({ title, lines = [], imageUrl = "" }) {
  if (!config.telegramBotToken || !config.telegramChatId) return;
  const message = [title, ...lines].filter(Boolean).join("\n");
  const apiBase = `https://api.telegram.org/bot${config.telegramBotToken}`;

  const textPayload = {
    chat_id: config.telegramChatId,
    text: message,
    disable_web_page_preview: false,
  };
  if (config.telegramTopicId) {
    textPayload.message_thread_id = Number(config.telegramTopicId) || undefined;
  }

  try {
    if (imageUrl) {
      const photoPayload = {
        chat_id: config.telegramChatId,
        photo: imageUrl,
        caption: message.slice(0, 1024),
      };
      if (config.telegramTopicId) {
        photoPayload.message_thread_id = Number(config.telegramTopicId) || undefined;
      }
      const photoRes = await fetch(`${apiBase}/sendPhoto`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(photoPayload),
      });
      if (photoRes.ok) return;
    }

    await fetch(`${apiBase}/sendMessage`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(textPayload),
    });
  } catch {
    // Best effort. Announcement failures should never break attach/deploy flows.
  }
}

async function sendTokenToIncinerator(connection, signer, mintAddress, uiAmount) {
  const amountUi = Number(uiAmount || 0);
  if (!Number.isFinite(amountUi) || amountUi <= 0) return null;
  const mint = new PublicKey(mintAddress);
  const owner = signer.publicKey;
  const incineratorOwner = new PublicKey("1nc1nerator11111111111111111111111111111111");

  const sources = await getOwnerTokenAccountBalancesForMint(connection, owner, mint, null);
  if (!sources.length) return null;
  const totalsByProgram = new Map();
  for (const item of sources) {
    const key = item.programId?.toBase58?.() || "";
    const prev = totalsByProgram.get(key) || 0n;
    totalsByProgram.set(key, prev + BigInt(item.amountRaw || 0));
  }
  let tokenProgramId = TOKEN_PROGRAM_ID;
  let bestTotal = 0n;
  for (const [key, total] of totalsByProgram.entries()) {
    if (total > bestTotal) {
      bestTotal = total;
      tokenProgramId = new PublicKey(key);
    }
  }
  const filteredSources = sources.filter((s) => s.programId?.equals?.(tokenProgramId));
  if (!filteredSources.length) return null;
  const decimals = Number(filteredSources[0]?.decimals || 0);
  const totalRawBalance = filteredSources.reduce((sum, item) => sum + item.amountRaw, 0n);
  if (totalRawBalance <= 0n) return null;

  const unit = 10 ** Math.max(0, decimals);
  const requestedRaw = BigInt(Math.floor(amountUi * unit));
  const rawAmount = requestedRaw > totalRawBalance ? totalRawBalance : requestedRaw;
  if (rawAmount <= 0n) return null;

  const destAta = getAssociatedTokenAddressSync(mint, incineratorOwner, true, tokenProgramId);

  const instructions = [];
  const destInfo = await connection.getAccountInfo(destAta, "confirmed");
  if (!destInfo) {
    instructions.push(
      createAssociatedTokenAccountInstruction(
        signer.publicKey,
        destAta,
        incineratorOwner,
        mint,
        tokenProgramId
      )
    );
  }

  let remaining = rawAmount;
  filteredSources.sort((a, b) => (a.amountRaw > b.amountRaw ? -1 : a.amountRaw < b.amountRaw ? 1 : 0));
  for (const source of filteredSources) {
    if (remaining <= 0n) break;
    const take = source.amountRaw > remaining ? remaining : source.amountRaw;
    if (take <= 0n) continue;
    instructions.push(
      createTransferInstruction(
        source.pubkey,
        destAta,
        signer.publicKey,
        take,
        [],
        tokenProgramId
      )
    );
    remaining -= take;
  }
  if (remaining > 0n) return null;

  const latest = await connection.getLatestBlockhash("confirmed");
  const tx = new Transaction({
    feePayer: signer.publicKey,
    blockhash: latest.blockhash,
    lastValidBlockHeight: latest.lastValidBlockHeight,
  });
  instructions.forEach((ix) => tx.add(ix));
  tx.sign(signer);
  const sig = await connection.sendRawTransaction(tx.serialize(), { skipPreflight: false, maxRetries: 5 });
  await connection.confirmTransaction(
    { signature: sig, blockhash: latest.blockhash, lastValidBlockHeight: latest.lastValidBlockHeight },
    "confirmed"
  );
  return sig;
}

async function fetchPumpPortalLocalTx(requestBody) {
  const res = await fetch("https://pumpportal.fun/api/trade-local", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(requestBody),
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(txt || "PumpPortal local transaction build failed.");
  }
  return new Uint8Array(await res.arrayBuffer());
}

async function signAndSendVersioned(connection, signer, txBytes) {
  const tx = VersionedTransaction.deserialize(txBytes);
  tx.sign([signer]);
  const sig = await connection.sendRawTransaction(tx.serialize(), {
    skipPreflight: false,
    maxRetries: 5,
  });
  await connection.confirmTransaction(sig, "confirmed");
  return sig;
}

async function sendAllSolTransfer(connection, signer, toAddress) {
  const toPubkey = new PublicKey(String(toAddress || "").trim());
  const balance = await connection.getBalance(signer.publicKey, "confirmed");
  if (balance <= 0) return { signature: null, sentLamports: 0, feeLamports: 0 };

  const latest = await connection.getLatestBlockhash("confirmed");
  const probeTx = new Transaction({
    feePayer: signer.publicKey,
    blockhash: latest.blockhash,
    lastValidBlockHeight: latest.lastValidBlockHeight,
  }).add(
    SystemProgram.transfer({
      fromPubkey: signer.publicKey,
      toPubkey,
      lamports: 1,
    })
  );
  const feeRes = await connection.getFeeForMessage(probeTx.compileMessage(), "confirmed");
  const feeLamports = Math.max(0, Number(feeRes?.value || 0));
  const lamportsToSend = Math.max(0, balance - feeLamports);
  if (lamportsToSend <= 0) {
    return { signature: null, sentLamports: 0, feeLamports };
  }

  const tx = new Transaction({
    feePayer: signer.publicKey,
    blockhash: latest.blockhash,
    lastValidBlockHeight: latest.lastValidBlockHeight,
  }).add(
    SystemProgram.transfer({
      fromPubkey: signer.publicKey,
      toPubkey,
      lamports: lamportsToSend,
    })
  );
  tx.sign(signer);
  const sig = await connection.sendRawTransaction(tx.serialize(), { skipPreflight: false, maxRetries: 5 });
  await connection.confirmTransaction(
    { signature: sig, blockhash: latest.blockhash, lastValidBlockHeight: latest.lastValidBlockHeight },
    "confirmed"
  );
  return { signature: sig, sentLamports: lamportsToSend, feeLamports };
}

async function signAndSendLegacyInstructions(connection, signer, instructions = []) {
  const ixs = Array.isArray(instructions) ? instructions.filter(Boolean) : [];
  if (!ixs.length) return null;
  const latest = await connection.getLatestBlockhash("confirmed");
  const tx = new Transaction({
    feePayer: signer.publicKey,
    blockhash: latest.blockhash,
    lastValidBlockHeight: latest.lastValidBlockHeight,
  });
  for (const ix of ixs) tx.add(ix);
  tx.sign(signer);
  const sig = await connection.sendRawTransaction(tx.serialize(), {
    skipPreflight: false,
    maxRetries: 5,
  });
  await connection.confirmTransaction(
    { signature: sig, blockhash: latest.blockhash, lastValidBlockHeight: latest.lastValidBlockHeight },
    "confirmed"
  );
  return sig;
}

function isSoftSharingClaimError(error) {
  const msg = String(error?.message || "").toLowerCase();
  if (!msg) return false;
  return (
    msg.includes("minimum distributable") ||
    msg.includes("distributable") ||
    msg.includes("sharing config") ||
    msg.includes("no account") ||
    msg.includes("account does not exist")
  );
}

async function tryDistributeSharingCreatorFees({ connection, signer, mint }) {
  if (!pumpOfflineSdk || !OnlinePumpSdk || !feeSharingConfigPda) {
    return { attempted: false, reason: "sdk_unavailable", signature: null, claimedLamports: 0 };
  }

  const mintPk = new PublicKey(String(mint || "").trim());
  const sharingConfigAddress = feeSharingConfigPda(mintPk);
  const sharingInfo = await connection.getAccountInfo(sharingConfigAddress, "confirmed");
  if (!sharingInfo) {
    return { attempted: false, reason: "no_sharing_config", signature: null, claimedLamports: 0 };
  }

  const sharingConfig = pumpOfflineSdk.decodeSharingConfig(sharingInfo);
  const signerPk = signer.publicKey;
  const isShareholder = Array.isArray(sharingConfig?.shareholders)
    && sharingConfig.shareholders.some((shareholder) => shareholder?.address?.equals?.(signerPk));
  if (!isShareholder) {
    return { attempted: false, reason: "not_shareholder", signature: null, claimedLamports: 0 };
  }

  const onlineSdk = new OnlinePumpSdk(connection);
  const distribution = await onlineSdk.buildDistributeCreatorFeesInstructions(mintPk);
  const instructions = Array.isArray(distribution?.instructions) ? distribution.instructions : [];
  if (!instructions.length) {
    return { attempted: true, reason: "no_instructions", signature: null, claimedLamports: 0 };
  }

  const sig = await signAndSendLegacyInstructions(connection, signer, instructions);
  const summary = await getClaimExecutionSummary(connection, sig, signerPk.toBase58());
  const claimedLamports = Math.max(0, Number(summary?.grossClaimLamports || 0));
  return {
    attempted: true,
    reason: "distributed",
    signature: sig,
    claimedLamports,
    isGraduated: Boolean(distribution?.isGraduated),
  };
}

function buildPumpCreatorRewardsProfileUrl(wallet) {
  const addr = String(wallet || "").trim();
  if (!addr) return "";
  return `https://pump.fun/profile/${encodeURIComponent(addr)}?tab=creator-rewards`;
}

function bnToLamportsNumber(value) {
  if (!value) return 0;
  try {
    const raw = String(value.toString?.() ?? value ?? "0");
    const bi = BigInt(raw);
    if (bi <= 0n) return 0;
    const maxSafe = BigInt(Number.MAX_SAFE_INTEGER);
    return Number(bi > maxSafe ? maxSafe : bi);
  } catch {
    return 0;
  }
}

const CREATOR_REWARDS_CACHE_TTL_MS = 20_000;
const CREATOR_REWARDS_CACHE_MAX = 1500;
const creatorRewardsCache = new Map();

function readCreatorRewardsCache(key) {
  const hit = creatorRewardsCache.get(key);
  if (!hit) return null;
  if (Date.now() - Number(hit.at || 0) > CREATOR_REWARDS_CACHE_TTL_MS) return null;
  return hit.value || null;
}

function writeCreatorRewardsCache(key, value) {
  creatorRewardsCache.set(key, { at: Date.now(), value });
  if (creatorRewardsCache.size > CREATOR_REWARDS_CACHE_MAX) {
    const oldest = creatorRewardsCache.keys().next().value;
    if (oldest) creatorRewardsCache.delete(oldest);
  }
}

async function getCreatorRewardsPreview({ connection, mint, wallet }) {
  const mintText = String(mint || "").trim();
  const walletText = String(wallet || "").trim();
  const profileUrl = buildPumpCreatorRewardsProfileUrl(walletText);
  const result = {
    profileUrl,
    directLamports: 0,
    shareableLamports: 0,
    distributableLamports: 0,
    totalLamports: 0,
    directSol: 0,
    shareableSol: 0,
    distributableSol: 0,
    totalSol: 0,
    shareableEnabled: false,
    isShareholder: false,
    shareBps: 0,
    canDistribute: false,
    isGraduated: false,
  };

  if (!mintText || !walletText) return result;
  const cacheKey = `${mintText}:${walletText}`;
  const cached = readCreatorRewardsCache(cacheKey);
  if (cached) return cached;

  if (!OnlinePumpSdk || !pumpOfflineSdk || !feeSharingConfigPda) {
    writeCreatorRewardsCache(cacheKey, result);
    return result;
  }

  let mintPk;
  let walletPk;
  try {
    mintPk = new PublicKey(mintText);
    walletPk = new PublicKey(walletText);
  } catch {
    writeCreatorRewardsCache(cacheKey, result);
    return result;
  }

  const onlineSdk = new OnlinePumpSdk(connection);

  try {
    const directBn = await onlineSdk.getCreatorVaultBalanceBothPrograms(walletPk);
    result.directLamports = Math.max(0, bnToLamportsNumber(directBn));
  } catch {
    result.directLamports = 0;
  }

  try {
    const sharingConfigAddress = feeSharingConfigPda(mintPk);
    const sharingInfo = await connection.getAccountInfo(sharingConfigAddress, "confirmed");
    if (sharingInfo) {
      result.shareableEnabled = true;
      const sharingConfig = pumpOfflineSdk.decodeSharingConfig(sharingInfo);
      const shareholders = Array.isArray(sharingConfig?.shareholders)
        ? sharingConfig.shareholders
        : [];
      const shareholder = shareholders.find((item) =>
        item?.address?.equals?.(walletPk)
      );
      if (shareholder) {
        result.isShareholder = true;
        result.shareBps = Math.max(0, Number(shareholder.shareBps || 0));
        try {
          const minimum = await onlineSdk.getMinimumDistributableFee(mintPk);
          result.canDistribute = Boolean(minimum?.canDistribute);
          result.isGraduated = Boolean(minimum?.isGraduated);
          result.distributableLamports = Math.max(
            0,
            bnToLamportsNumber(minimum?.distributableFees)
          );
          result.shareableLamports =
            result.canDistribute && result.shareBps > 0
              ? Math.max(
                  0,
                  Math.floor((result.distributableLamports * result.shareBps) / 10_000)
                )
              : 0;
        } catch {
          // best effort: keep share flags without distributable estimate
        }
      }
    }
  } catch {
    // best effort
  }

  result.totalLamports = Math.max(
    0,
    Number(result.directLamports || 0) + Number(result.shareableLamports || 0)
  );
  result.directSol = fromLamports(result.directLamports);
  result.shareableSol = fromLamports(result.shareableLamports);
  result.distributableSol = fromLamports(result.distributableLamports);
  result.totalSol = fromLamports(result.totalLamports);

  writeCreatorRewardsCache(cacheKey, result);
  return result;
}

async function pumpPortalTrade({
  connection,
  signer,
  mint,
  action,
  amount,
  denominatedInSol = true,
  slippage = 10,
  pool = "auto",
}) {
  const txBytes = await fetchPumpPortalLocalTx({
    publicKey: signer.publicKey.toBase58(),
    action,
    mint,
    denominatedInSol: denominatedInSol ? "true" : "false",
    amount,
    slippage,
    priorityFee: Number(config.basePriorityFeeSol || 0.0005),
    pool,
  });
  const sig = await signAndSendVersioned(connection, signer, txBytes);
  return sig;
}

async function pumpPortalCollectCreatorFeeRequest({ connection, signer, requestBody }) {
  const txBytes = await fetchPumpPortalLocalTx(requestBody);
  return signAndSendVersioned(connection, signer, txBytes);
}

async function readTransactionWithRetry(connection, signature, maxAttempts = 5) {
  for (let i = 0; i < maxAttempts; i += 1) {
    const tx = await connection.getTransaction(signature, {
      commitment: "confirmed",
      maxSupportedTransactionVersion: 0,
    });
    if (tx?.meta && tx?.transaction?.message) return tx;
    if (i < maxAttempts - 1) {
      await new Promise((resolve) => setTimeout(resolve, 350 * (i + 1)));
    }
  }
  return null;
}

async function getClaimExecutionSummary(connection, signature, signerPubkey) {
  const tx = await readTransactionWithRetry(connection, signature, 5);
  if (!tx?.meta || !tx?.transaction?.message) {
    return {
      grossClaimLamports: 0,
      feeLamports: 0,
      noRewardsHint: false,
      txUnavailable: true,
    };
  }

  const feeLamports = Number(tx.meta.fee || 0);
  const logs = Array.isArray(tx.meta.logMessages) ? tx.meta.logMessages : [];
  const logText = logs.join("\n").toLowerCase();
  const noRewardsHint =
    logText.includes("no creator fee to collect") ||
    logText.includes("no coin creator fee to collect");

  let signerIndex = -1;
  const accountKeys = tx.transaction.message.staticAccountKeys || [];
  for (let i = 0; i < accountKeys.length; i += 1) {
    if (accountKeys[i]?.toBase58?.() === signerPubkey) {
      signerIndex = i;
      break;
    }
  }

  if (signerIndex < 0) {
    return {
      grossClaimLamports: 0,
      feeLamports,
      noRewardsHint,
      txUnavailable: false,
    };
  }

  const pre = Number(tx.meta.preBalances?.[signerIndex] || 0);
  const post = Number(tx.meta.postBalances?.[signerIndex] || 0);
  const netDelta = post - pre;
  const grossClaimLamports = Math.max(0, netDelta + feeLamports);

  return {
    grossClaimLamports,
    feeLamports,
    noRewardsHint,
    txUnavailable: false,
  };
}

async function pumpPortalCollectCreatorFee({ connection, signer, mint, pool = "auto" }) {
  const requestedPool = String(pool || "auto").trim() || "auto";
  const requestedMint = String(mint || "").trim();
  const attempts = [];
  const pushAttempt = ({ poolValue = "", includeMint = true } = {}) => {
    const req = {
      publicKey: signer.publicKey.toBase58(),
      action: "collectCreatorFee",
      priorityFee: Number(config.basePriorityFeeSol || 0.0005),
    };
    if (poolValue) req.pool = poolValue;
    if (includeMint && requestedMint) req.mint = requestedMint;
    attempts.push(req);
  };

  if (requestedPool === "meteora-dbc") {
    // Meteora claims are mint-specific.
    pushAttempt({ poolValue: "meteora-dbc", includeMint: true });
  } else {
    // For pump-designated rewards, wallet-level claim should run first (claims all eligible rewards).
    pushAttempt({ poolValue: "pump", includeMint: false });
    pushAttempt({ poolValue: "", includeMint: false });
    pushAttempt({ poolValue: "auto", includeMint: false });

    // Fallbacks for legacy or mint-scoped claim routes.
    pushAttempt({ poolValue: requestedPool, includeMint: true });
    pushAttempt({ poolValue: "pump", includeMint: true });
    pushAttempt({ poolValue: "", includeMint: true });
    pushAttempt({ poolValue: "auto", includeMint: true });
  }

  const deduped = [];
  const seen = new Set();
  for (const req of attempts) {
    const key = JSON.stringify(req);
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(req);
  }

  let lastBuildError = null;
  let lastSendError = null;
  let insufficientLamportsError = null;
  for (const requestBody of deduped) {
    try {
      const txBytes = await fetchPumpPortalLocalTx(requestBody);
      try {
        const sig = await signAndSendVersioned(connection, signer, txBytes);
        return sig;
      } catch (sendError) {
        const requestTag = JSON.stringify(requestBody);
        if (sendError && typeof sendError.message === "string" && !sendError.message.includes(requestTag)) {
          sendError.message = `${sendError.message} (collectCreatorFee request: ${requestTag})`;
        }
        if (sendError && typeof sendError === "object") {
          sendError.claimRequestBody = requestBody;
        }
        lastSendError = sendError;
        if (extractClaimLamportDeficit(sendError) > 0) {
          insufficientLamportsError = sendError;
        }
      }
    } catch (error) {
      const requestTag = JSON.stringify(requestBody);
      if (error && typeof error.message === "string" && !error.message.includes(requestTag)) {
        error.message = `${error.message} (collectCreatorFee request: ${requestTag})`;
      }
      lastBuildError = error;
    }
  }
  throw insufficientLamportsError || lastSendError || lastBuildError || new Error("Creator fee claim failed.");
}

const DEPOSIT_POOL_MAX = 20;
const DEPOSIT_POOL_LOCK_KEY = "884420002001";
let depositPoolRefilling = false;

function validateDepositVanityPrefix(input) {
  const prefix = String(input || "").trim().toUpperCase();
  if (!prefix) throw new Error("Deposit vanity prefix is empty.");
  if (!/^[1-9A-HJ-NP-Z]{1,8}$/.test(prefix)) {
    throw new Error("Deposit vanity prefix must be Base58-safe and <= 8 chars.");
  }
  return prefix;
}

let cachedDepositEncryptionKey = undefined;

function getDepositEncryptionKey() {
  if (cachedDepositEncryptionKey !== undefined) return cachedDepositEncryptionKey;
  const raw = String(config.depositKeyEncryptionKey || "").trim();
  if (!raw) {
    cachedDepositEncryptionKey = null;
    return cachedDepositEncryptionKey;
  }
  let key;
  if (/^[0-9a-fA-F]{64}$/.test(raw)) {
    key = Buffer.from(raw, "hex");
  } else {
    key = Buffer.from(raw, "base64");
  }
  if (!Buffer.isBuffer(key) || key.length !== 32) {
    throw new Error("DEPOSIT_KEY_ENCRYPTION_KEY must be 32 bytes (hex-64 or base64).");
  }
  cachedDepositEncryptionKey = key;
  return cachedDepositEncryptionKey;
}

function encryptDepositSecret(secretKeyBase58) {
  const key = getDepositEncryptionKey();
  const clear = String(secretKeyBase58 || "").trim();
  if (!key) return clear;
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv("aes-256-gcm", key, iv);
  const ciphertext = Buffer.concat([cipher.update(clear, "utf8"), cipher.final()]);
  const tag = cipher.getAuthTag();
  return `enc:v1:${iv.toString("base64")}:${tag.toString("base64")}:${ciphertext.toString("base64")}`;
}

function decryptDepositSecret(storedValue) {
  const raw = String(storedValue || "").trim();
  if (!raw.startsWith("enc:v1:")) return raw;
  const key = getDepositEncryptionKey();
  if (!key) {
    throw new Error("Encrypted deposit key found but DEPOSIT_KEY_ENCRYPTION_KEY is not configured.");
  }
  const parts = raw.split(":");
  if (parts.length !== 5) {
    throw new Error("Encrypted deposit key format is invalid.");
  }
  const iv = Buffer.from(parts[2], "base64");
  const tag = Buffer.from(parts[3], "base64");
  const ciphertext = Buffer.from(parts[4], "base64");
  const decipher = crypto.createDecipheriv("aes-256-gcm", key, iv);
  decipher.setAuthTag(tag);
  return Buffer.concat([decipher.update(ciphertext), decipher.final()]).toString("utf8");
}

async function runSolanaKeygenGrind(prefix) {
  const safePrefix = validateDepositVanityPrefix(prefix);
  const threads = Math.max(1, Math.floor(Number(config.depositVanityThreads) || 4));
  const timeoutMs = Math.max(1000, Math.floor(Number(config.depositVanityTimeoutMs) || 300000));
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "ember-vanity-"));

  try {
    await new Promise((resolve, reject) => {
      const args = [
        "grind",
        "--starts-with",
        `${safePrefix}:1`,
        "--num-threads",
        String(threads),
        "--no-bip39-passphrase",
      ];

      const child = spawn(config.solanaKeygenBin, args, {
        cwd: tempDir,
        windowsHide: true,
        stdio: ["ignore", "pipe", "pipe"],
      });

      let stderr = "";
      const timer = setTimeout(() => {
        child.kill("SIGKILL");
        reject(new Error(`Vanity deposit generation timed out after ${timeoutMs}ms.`));
      }, timeoutMs);

      child.stdout.on("data", () => {
        // Drain stdout to avoid backpressure in long grind runs.
      });
      child.stderr.on("data", (chunk) => {
        stderr += String(chunk || "");
      });

      child.on("error", (error) => {
        clearTimeout(timer);
        reject(new Error(`Failed to start solana-keygen: ${error.message}`));
      });

      child.on("close", (code) => {
        clearTimeout(timer);
        if (code !== 0) {
          const detail = stderr.trim() || `exit code ${code}`;
          reject(new Error(`solana-keygen grind failed: ${detail}`));
          return;
        }
        resolve();
      });
    });

    const files = await fs.readdir(tempDir);
    const keyFile = files.find((name) => String(name || "").toLowerCase().endsWith(".json"));
    if (!keyFile) {
      throw new Error("solana-keygen did not produce a keypair file.");
    }

    const keyFilePath = path.join(tempDir, keyFile);
    const raw = await fs.readFile(keyFilePath, "utf8");
    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch {
      throw new Error("Generated vanity keypair file is invalid JSON.");
    }
    if (!Array.isArray(parsed) || parsed.length !== 64) {
      throw new Error("Generated vanity keypair must contain 64 bytes.");
    }

    const invalid = parsed.some((n) => !Number.isInteger(n) || n < 0 || n > 255);
    if (invalid) {
      throw new Error("Generated vanity keypair contains invalid byte values.");
    }

    const secretKey = Uint8Array.from(parsed);
    const keypair = Keypair.fromSecretKey(secretKey);
    const pubkey = keypair.publicKey.toBase58();
    if (!pubkey.startsWith(safePrefix)) {
      throw new Error(`Generated pubkey prefix mismatch. Expected ${safePrefix}, got ${pubkey.slice(0, safePrefix.length)}.`);
    }

    return {
      pubkey,
      secretKeyBase58: bs58.encode(secretKey),
    };
  } finally {
    await fs.rm(tempDir, { recursive: true, force: true }).catch(() => {});
  }
}

async function runJsVanityGrind(prefix) {
  const safePrefix = validateDepositVanityPrefix(prefix);
  const timeoutMs = Math.max(1000, Math.floor(Number(config.depositVanityTimeoutMs) || 300000));
  const yieldEvery = Math.max(200, Math.floor(Number(process.env.DEPOSIT_VANITY_JS_YIELD_EVERY) || 1000));
  const deadline = Date.now() + timeoutMs;
  let attempts = 0;

  while (Date.now() < deadline) {
    const keypair = Keypair.generate();
    const pubkey = keypair.publicKey.toBase58();
    attempts += 1;
    if (pubkey.startsWith(safePrefix)) {
      return {
        pubkey,
        secretKeyBase58: bs58.encode(keypair.secretKey),
      };
    }
    if (attempts % yieldEvery === 0) {
      await new Promise((resolve) => setImmediate(resolve));
    }
  }

  throw new Error(`Vanity deposit generation timed out after ${timeoutMs}ms.`);
}

function generateNonVanityDeposit() {
  const keypair = Keypair.generate();
  return {
    pubkey: keypair.publicKey.toBase58(),
    secretKeyBase58: bs58.encode(keypair.secretKey),
  };
}

async function generateVanityDeposit(prefix) {
  const isRenderRuntime =
    String(process.env.RENDER || "").toLowerCase() === "true" ||
    Boolean(process.env.RENDER_SERVICE_ID);
  const allowRenderJsFallback =
    String(process.env.RENDER_ALLOW_JS_VANITY_FALLBACK || "false").toLowerCase() === "true";
  try {
    return await runSolanaKeygenGrind(prefix);
  } catch (error) {
    if (isRenderRuntime && !allowRenderJsFallback) {
      console.warn(
        `[deposit] solana-keygen unavailable on Render; using non-vanity keypair: ${error?.message || error}`
      );
      return generateNonVanityDeposit();
    }
    if (!config.depositVanityAllowJsFallback) {
      throw error;
    }
    console.warn(`[deposit] solana-keygen unavailable or failed, using JS fallback: ${error?.message || error}`);
    try {
      return await runJsVanityGrind(prefix);
    } catch (jsError) {
      // Last-resort fallback for constrained hosts (e.g., Render free/shared CPU):
      // always return a valid keypair so bot setup is never blocked by vanity grind timing.
      console.warn(`[deposit] JS vanity grind failed, using non-vanity keypair: ${jsError?.message || jsError}`);
      return generateNonVanityDeposit();
    }
  }
}

function clampDepositPoolTarget(raw) {
  const n = Number(raw);
  if (!Number.isFinite(n)) return DEPOSIT_POOL_MAX;
  return Math.max(0, Math.min(DEPOSIT_POOL_MAX, Math.floor(n)));
}

function makeHttpError(status, message) {
  const error = new Error(message);
  error.status = status;
  return error;
}

function buildPoolWarmupMessage(required, available) {
  const missing = Math.max(1, Number(required || 0) - Number(available || 0));
  const etaPerAddress = Math.max(10, Math.floor(Number(config.depositPoolEtaPerAddressSec) || 45));
  const minSec = Math.max(10, Math.floor(missing * etaPerAddress * 0.6));
  const maxSec = Math.max(minSec + 10, Math.floor(missing * etaPerAddress * 1.8));
  return `Address pool is warming up. Creating addresses 1 out of ${missing} needed. Approx ${minSec}-${maxSec}s remaining.`;
}

async function withDepositPoolLock(fn) {
  const client = await pool.connect();
  try {
    const lockRes = await client.query("SELECT pg_try_advisory_lock($1::bigint) AS ok", [DEPOSIT_POOL_LOCK_KEY]);
    const ok = Boolean(lockRes.rows[0]?.ok);
    if (!ok) return { locked: false };
    try {
      const result = await fn(client);
      return { locked: true, result };
    } finally {
      await client.query("SELECT pg_advisory_unlock($1::bigint)", [DEPOSIT_POOL_LOCK_KEY]).catch(() => {});
    }
  } finally {
    client.release();
  }
}

export async function ensureDepositPool(targetInput = config.depositPoolTarget) {
  const target = clampDepositPoolTarget(targetInput);
  if (target <= 0) return { target, created: 0, total: 0 };
  if (depositPoolRefilling) return { target, created: 0, total: null, skipped: true };
  depositPoolRefilling = true;
  try {
    const lockResult = await withDepositPoolLock(async (client) => {
      let created = 0;
      while (true) {
        const countRes = await client.query("SELECT COUNT(*)::int AS c FROM token_deposit_pool");
        const total = Number(countRes.rows[0]?.c || 0);
        if (total >= target) {
          return { target, created, total };
        }
        const generated = await generateVanityDeposit(config.depositVanityPrefix || "EMBR");
        const storedSecret = encryptDepositSecret(generated.secretKeyBase58);
        try {
          await client.query(
            `
              INSERT INTO token_deposit_pool (deposit_pubkey, secret_key_base58, status)
              VALUES ($1, $2, 'available')
            `,
            [generated.pubkey, storedSecret]
          );
          created += 1;
        } catch {
          // Duplicate key collisions are very unlikely; just retry loop.
        }
      }
    });

    if (!lockResult.locked) {
      const countRes = await pool.query("SELECT COUNT(*)::int AS c FROM token_deposit_pool");
      return { target, created: 0, total: Number(countRes.rows[0]?.c || 0), skipped: true };
    }
    return lockResult.result;
  } finally {
    depositPoolRefilling = false;
  }
}

export async function reserveDepositAddresses(userId, countInput = 1) {
  const count = Math.max(1, Math.min(DEPOSIT_POOL_MAX, Math.floor(Number(countInput) || 1)));
  const availableRowsRes = await withTx(async (client) => {
    const rowsRes = await client.query(
      `
        SELECT id, deposit_pubkey
        FROM token_deposit_pool
        WHERE status = 'available'
        ORDER BY created_at ASC
        LIMIT $1
        FOR UPDATE SKIP LOCKED
      `,
      [count]
    );
    const rows = rowsRes.rows || [];
    if (rows.length < count) {
      return { ok: false, available: rows.length };
    }

    const reserved = [];
    for (const row of rows) {
      const reservationId = makeId("dep");
      await client.query(
        `
          UPDATE token_deposit_pool
          SET status = 'reserved', reservation_id = $1, reserved_user_id = $2, reserved_at = NOW()
          WHERE id = $3
        `,
        [reservationId, userId, row.id]
      );
      reserved.push({
        pendingDepositId: reservationId,
        deposit: row.deposit_pubkey,
      });
    }
    return { ok: true, reserved };
  });

  if (!availableRowsRes.ok) {
    void ensureDepositPool();
    throw makeHttpError(503, buildPoolWarmupMessage(count, availableRowsRes.available));
  }

  return availableRowsRes.reserved;
}

export async function generatePendingDepositAddress(userId, countInput = 1) {
  const count = Math.max(1, Math.min(DEPOSIT_POOL_MAX, Math.floor(Number(countInput) || 1)));
  const reserved = await reserveDepositAddresses(userId, count);
  if (count === 1) {
    return reserved[0];
  }
  return { deposits: reserved };
}

async function consumeReservedDepositAddress(client, userId, pendingDepositId) {
  const key = String(pendingDepositId || "").trim();
  if (!key) return null;
  const res = await client.query(
    `
      SELECT id, deposit_pubkey, secret_key_base58
      FROM token_deposit_pool
      WHERE status = 'reserved' AND reservation_id = $1 AND reserved_user_id = $2
      LIMIT 1
      FOR UPDATE
    `,
    [key, userId]
  );
  if (!res.rowCount) {
    throw new Error("Pending deposit address not found or expired. Generate a new one.");
  }
  const row = res.rows[0];
  await client.query("DELETE FROM token_deposit_pool WHERE id = $1", [row.id]);
  return {
    pubkey: row.deposit_pubkey,
    secretKeyBase58: decryptDepositSecret(row.secret_key_base58),
  };
}

function sessionExpiryDate() {
  return new Date(Date.now() + config.sessionTtlDays * 24 * 60 * 60 * 1000);
}

export function cookieMaxAgeMs() {
  return config.sessionTtlDays * 24 * 60 * 60 * 1000;
}

export async function getTokenDepositSigningKey(userId, tokenId) {
  const res = await pool.query(
    `
      SELECT secret_key_base58
      FROM token_deposit_keys
      WHERE user_id = $1 AND token_id = $2
      LIMIT 1
    `,
    [userId, tokenId]
  );
  if (!res.rowCount) {
    throw new Error("Deposit signing key not found.");
  }
  return decryptDepositSecret(res.rows[0].secret_key_base58);
}

export async function registerUser(usernameInput, passwordInput) {
  const username = normalizeUsername(usernameInput);
  const password = normalizePassword(passwordInput);
  const passwordHash = await bcrypt.hash(password, 10);

  const user = await withTx(async (client) => {
    const existing = await client.query("SELECT id FROM users WHERE username = $1", [username]);
    if (existing.rowCount > 0) {
      throw new Error("Username already exists.");
    }

    const inserted = await client.query(
      "INSERT INTO users (username, password_hash) VALUES ($1, $2) RETURNING id, username",
      [username, passwordHash]
    );

    return inserted.rows[0];
  });

  const sessionToken = await createSession(user.id);
  return {
    user: { id: user.id, username: user.username },
    sessionToken,
  };
}

export async function loginUser(usernameInput, passwordInput) {
  const username = normalizeUsername(usernameInput);
  const password = normalizePassword(passwordInput);

  const result = await pool.query(
    "SELECT id, username, password_hash FROM users WHERE username = $1",
    [username]
  );

  if (!result.rowCount) {
    throw new Error("Invalid username or password.");
  }

  const user = result.rows[0];
  const ok = await bcrypt.compare(password, user.password_hash);
  if (!ok) {
    throw new Error("Invalid username or password.");
  }

  const sessionToken = await createSession(user.id);
  return {
    user: { id: user.id, username: user.username },
    sessionToken,
  };
}

export async function createSession(userId) {
  const token = makeSessionToken();
  await pool.query("INSERT INTO sessions (token, user_id, expires_at) VALUES ($1, $2, $3)", [
    token,
    userId,
    sessionExpiryDate(),
  ]);
  return token;
}

export async function clearSession(token) {
  if (!token) return;
  await pool.query("DELETE FROM sessions WHERE token = $1", [token]);
}

export async function getUserBySession(token) {
  if (!token) return null;

  const result = await pool.query(
    `
      SELECT u.id, u.username
      FROM sessions s
      JOIN users u ON u.id = s.user_id
      WHERE s.token = $1 AND s.expires_at > NOW()
      LIMIT 1
    `,
    [token]
  );

  if (!result.rowCount) return null;
  return result.rows[0];
}

export async function getDashboard(userId) {
  const [tokensRes, feedRes, logsRes, chartRes] = await Promise.all([
    pool.query(
      `
        SELECT
          t.*,
          GREATEST(
            COALESCE(t.burned::numeric, 0),
            COALESCE(be.burned_from_events, 0)
          ) AS display_burned,
          m.module_type,
          m.enabled AS module_enabled,
          m.config_json AS module_config,
          m.state_json AS module_state,
          m.last_error AS module_last_error
        FROM tokens t
        LEFT JOIN (
          SELECT
            token_id,
            COALESCE(SUM(amount), 0)::numeric AS burned_from_events
          FROM token_events
          WHERE user_id = $1 AND event_type = 'burn'
          GROUP BY token_id
        ) be ON be.token_id = t.id
        LEFT JOIN bot_modules m
          ON m.token_id = t.id
         AND m.module_type = t.selected_bot
        WHERE t.user_id = $1
        ORDER BY t.created_at DESC
      `,
      [userId]
    ),
    pool.query(
      "SELECT id, token_id, token_symbol, module_type, event_type, amount, message, tx, created_at FROM token_events WHERE user_id = $1 ORDER BY created_at DESC LIMIT 30",
      [userId]
    ),
    pool.query(
      "SELECT id, token_id, token_symbol, module_type, event_type, amount, message, tx, created_at FROM token_events WHERE user_id = $1 ORDER BY created_at DESC LIMIT 200",
      [userId]
    ),
    pool.query(
      `
        SELECT event_type, amount, message, created_at
        FROM token_events
        WHERE user_id = $1
          AND event_type = 'burn'
          AND created_at >= NOW() - INTERVAL '7 days'
        ORDER BY created_at DESC
      `,
      [userId]
    ),
  ]);

  const tokenRows = attachMarketCaps(tokensRes.rows);

  return {
    tokens: tokenRows.map(toToken),
    feed: feedRes.rows.map(toEvent),
    logs: logsRes.rows.map(toEvent),
    chartData: buildChartData(chartRes.rows),
  };
}

async function readTokenWalletAddresses(client, userId, tokenRow) {
  const tokenId = String(tokenRow.id);
  const addresses = [
    {
      type: "deposit",
      label: "Deposit Wallet",
      pubkey: String(tokenRow.deposit),
    },
  ];

  const isVolume =
    String(tokenRow.selected_bot || "").toLowerCase() === MODULE_TYPES.volume;
  if (!isVolume) return addresses;

  const walletsRes = await client.query(
    `
      SELECT label, wallet_pubkey
      FROM volume_trade_wallets
      WHERE user_id = $1 AND token_id = $2
      ORDER BY created_at ASC
    `,
    [userId, tokenId]
  );

  for (const row of walletsRes.rows) {
    addresses.push({
      type: "trade",
      label: String(row.label || "trade"),
      pubkey: String(row.wallet_pubkey || ""),
    });
  }
  return addresses;
}

async function collectWalletBalances(tokenRow, addresses) {
  const connection = getConnection();
  const mint = String(tokenRow.mint || "");
  const enriched = [];
  for (const item of addresses) {
    const pubkey = String(item.pubkey || "").trim();
    if (!pubkey) continue;
    try {
      const [lamports, tokenBal] = await Promise.all([
        connection.getBalance(new PublicKey(pubkey), "confirmed"),
        getOwnerTokenBalanceUi(connection, pubkey, mint),
      ]);
      enriched.push({
        ...item,
        solBalance: fromLamports(lamports),
        tokenBalance: Number(tokenBal || 0),
      });
    } catch {
      enriched.push({
        ...item,
        solBalance: 0,
        tokenBalance: 0,
      });
    }
  }
  return enriched;
}

async function getVolumeTokenModule(client, userId, tokenId, { forUpdate = false } = {}) {
  const lockClause = forUpdate ? "FOR UPDATE" : "";
  const tokenRes = await client.query(
    `
      SELECT id, user_id, symbol, name, mint, picture_url, deposit, selected_bot, active
      FROM tokens
      WHERE user_id = $1 AND id = $2
      LIMIT 1
      ${lockClause}
    `,
    [userId, tokenId]
  );
  if (!tokenRes.rowCount) throw new Error("Token not found.");
  const tokenRow = tokenRes.rows[0];
  const selectedBot = normalizeModuleType(tokenRow.selected_bot, MODULE_TYPES.burn);
  if (selectedBot !== MODULE_TYPES.volume) {
    throw new Error("Sweep/withdraw is available only for Volume Bot tokens.");
  }

  const moduleRes = await client.query(
    `
      SELECT id AS module_id, user_id, token_id, module_type, enabled, config_json, state_json
      FROM bot_modules
      WHERE token_id = $1 AND module_type = $2
      LIMIT 1
      ${lockClause}
    `,
    [tokenId, MODULE_TYPES.volume]
  );
  if (!moduleRes.rowCount) throw new Error("Volume module is not configured for this token.");
  return { tokenRow, moduleRow: moduleRes.rows[0] };
}

async function getBurnTokenModule(client, userId, tokenId, { forUpdate = false } = {}) {
  const lockClause = forUpdate ? "FOR UPDATE" : "";
  const tokenRes = await client.query(
    `
      SELECT id, user_id, symbol, name, mint, picture_url, deposit, selected_bot, active
      FROM tokens
      WHERE user_id = $1 AND id = $2
      LIMIT 1
      ${lockClause}
    `,
    [userId, tokenId]
  );
  if (!tokenRes.rowCount) throw new Error("Token not found.");
  const tokenRow = tokenRes.rows[0];
  const selectedBot = normalizeModuleType(tokenRow.selected_bot, MODULE_TYPES.burn);
  if (selectedBot !== MODULE_TYPES.burn) {
    throw new Error("Burn withdraw is available only for Burn Bot tokens.");
  }

  const moduleRes = await client.query(
    `
      SELECT id AS module_id, user_id, token_id, module_type, enabled, config_json, state_json
      FROM bot_modules
      WHERE token_id = $1 AND module_type = $2
      LIMIT 1
      ${lockClause}
    `,
    [tokenId, MODULE_TYPES.burn]
  );
  if (!moduleRes.rowCount) throw new Error("Burn module is not configured for this token.");
  return { tokenRow, moduleRow: moduleRes.rows[0] };
}

function normalizePubkeyString(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  return new PublicKey(raw).toBase58();
}

function accountKeyToBase58(key) {
  if (!key) return "";
  if (typeof key === "string") return key;
  if (typeof key.pubkey === "string") return key.pubkey;
  if (key.pubkey?.toBase58) return key.pubkey.toBase58();
  return "";
}

async function readDepositFundingSources(connection, depositPubkey, internalWallets = []) {
  const internalSet = new Set(
    internalWallets.map((w) => String(w || "").trim()).filter(Boolean)
  );
  const funding = new Map();
  const signatures = await connection.getSignaturesForAddress(new PublicKey(depositPubkey), {
    limit: 80,
  });

  for (const sigInfo of signatures) {
    if (!sigInfo?.signature || sigInfo.err) continue;
    let tx;
    try {
      tx = await connection.getParsedTransaction(sigInfo.signature, {
        commitment: "confirmed",
        maxSupportedTransactionVersion: 0,
      });
    } catch {
      tx = null;
    }
    if (!tx?.meta || !tx?.transaction?.message) continue;

    const keys = Array.isArray(tx.transaction.message.accountKeys)
      ? tx.transaction.message.accountKeys.map(accountKeyToBase58)
      : [];
    const depIdx = keys.findIndex((k) => k === depositPubkey);
    if (depIdx < 0) continue;

    const pre = Number(tx.meta.preBalances?.[depIdx] || 0);
    const post = Number(tx.meta.postBalances?.[depIdx] || 0);
    const deltaLamports = Math.max(0, post - pre);
    if (deltaLamports <= 0) continue;

    let matchedInstruction = false;
    const instructionBuckets = [
      tx.transaction.message.instructions || [],
      ...(Array.isArray(tx.meta.innerInstructions)
        ? tx.meta.innerInstructions.map((i) => i.instructions || [])
        : []),
    ];

    for (const instructions of instructionBuckets) {
      for (const instruction of instructions) {
        if (!instruction?.parsed || instruction.program !== "system") continue;
        if (instruction.parsed.type !== "transfer") continue;
        const info = instruction.parsed.info || {};
        const source = String(info.source || "").trim();
        const destination = String(info.destination || "").trim();
        const lamports = Number(info.lamports || 0);
        if (!source || !destination || lamports <= 0) continue;
        if (destination !== depositPubkey) continue;
        if (internalSet.has(source)) continue;
        const next = (funding.get(source) || 0) + lamports;
        funding.set(source, next);
        matchedInstruction = true;
      }
    }

    if (!matchedInstruction) {
      let sourceIdx = -1;
      let bestDecrease = 0;
      for (let i = 0; i < keys.length; i += 1) {
        if (i === depIdx) continue;
        const key = keys[i];
        if (!key || internalSet.has(key)) continue;
        const preBal = Number(tx.meta.preBalances?.[i] || 0);
        const postBal = Number(tx.meta.postBalances?.[i] || 0);
        const decrease = Math.max(0, preBal - postBal);
        if (decrease > bestDecrease) {
          bestDecrease = decrease;
          sourceIdx = i;
        }
      }
      if (sourceIdx >= 0) {
        const source = keys[sourceIdx];
        if (source && !internalSet.has(source)) {
          funding.set(source, (funding.get(source) || 0) + deltaLamports);
        }
      }
    }
  }

  return Array.from(funding.entries())
    .map(([wallet, lamports]) => ({
      wallet,
      lamports: Math.max(0, Math.floor(Number(lamports || 0))),
      sol: fromLamports(lamports),
    }))
    .filter((row) => row.wallet && row.lamports > 0)
    .sort((a, b) => b.lamports - a.lamports);
}

async function persistFundingSources(client, userId, tokenId, sources = []) {
  for (const source of sources) {
    await client.query(
      `
        INSERT INTO token_funding_sources (id, user_id, token_id, source_wallet, total_lamports)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (token_id, source_wallet)
        DO UPDATE
        SET total_lamports = GREATEST(token_funding_sources.total_lamports, EXCLUDED.total_lamports)
      `,
      [makeId("src"), userId, tokenId, source.wallet, source.lamports]
    );
  }
}

export async function getVolumeWithdrawOptions(userId, tokenId) {
  const tokenIdText = String(tokenId || "").trim();
  if (!tokenIdText) throw new Error("Token id is required.");

  return withTx(async (client) => {
    const { tokenRow, moduleRow } = await getVolumeTokenModule(client, userId, tokenIdText);
    const configJson = mergeModuleConfig(MODULE_TYPES.volume, moduleRow.config_json, {});
    const reserveLamports = toLamports(configJson.reserveSol || config.botSolReserve || 0.01);
    const feeSafetyLamports = getTxFeeSafetyLamports();
    const addresses = await readTokenWalletAddresses(client, userId, tokenRow);
    const internalWallets = addresses.map((a) => String(a.pubkey || "").trim()).filter(Boolean);
    const connection = getConnection();
    const depositPubkey = String(tokenRow.deposit);
    const [depositLamports, scannedSources, storedRes] = await Promise.all([
      connection.getBalance(new PublicKey(depositPubkey), "confirmed"),
      readDepositFundingSources(connection, depositPubkey, internalWallets),
      client.query(
        `
          SELECT source_wallet, total_lamports
          FROM token_funding_sources
          WHERE user_id = $1 AND token_id = $2
          ORDER BY total_lamports DESC
        `,
        [userId, tokenIdText]
      ),
    ]);

    await persistFundingSources(client, userId, tokenIdText, scannedSources);

    const sourceMap = new Map();
    for (const row of storedRes.rows || []) {
      const wallet = String(row.source_wallet || "").trim();
      const lamports = Math.max(0, Number(row.total_lamports || 0));
      if (!wallet || lamports <= 0) continue;
      sourceMap.set(wallet, lamports);
    }
    for (const row of scannedSources) {
      sourceMap.set(row.wallet, Math.max(Number(sourceMap.get(row.wallet) || 0), row.lamports));
    }

    const sources = Array.from(sourceMap.entries())
      .map(([wallet, lamports]) => ({
        wallet,
        totalSol: fromLamports(lamports),
      }))
      .filter((row) => row.wallet && row.totalSol > 0)
      .sort((a, b) => b.totalSol - a.totalSol);

    const withdrawableLamports = Math.max(0, depositLamports - reserveLamports - feeSafetyLamports);

    return {
      deposit: depositPubkey,
      active: Boolean(tokenRow.active),
      reserveSol: fromLamports(reserveLamports),
      withdrawableSol: fromLamports(withdrawableLamports),
      sources,
    };
  });
}

export async function getTokenLiveDetails(userId, tokenId) {
  const tokenRes = await pool.query(
    `
      SELECT id, symbol, name, mint, picture_url, deposit, selected_bot, active
      FROM tokens
      WHERE user_id = $1 AND id = $2
      LIMIT 1
    `,
    [userId, tokenId]
  );
  if (!tokenRes.rowCount) throw new Error("Token not found.");
  const tokenRow = tokenRes.rows[0];

  const addresses = await withTx(async (client) =>
    readTokenWalletAddresses(client, userId, tokenRow)
  );
  const connection = getConnection();

  let enriched = [];
  try {
    enriched = await collectWalletBalances(tokenRow, addresses);
  } catch {
    enriched = addresses.map((a) => ({ ...a, solBalance: 0, tokenBalance: 0 }));
  }

  const totals = enriched.reduce(
    (acc, item) => {
      acc.sol += Number(item.solBalance || 0);
      acc.token += Number(item.tokenBalance || 0);
      return acc;
    },
    { sol: 0, token: 0 }
  );

  let creatorRewards = {
    profileUrl: buildPumpCreatorRewardsProfileUrl(tokenRow.deposit),
    directLamports: 0,
    shareableLamports: 0,
    distributableLamports: 0,
    totalLamports: 0,
    directSol: 0,
    shareableSol: 0,
    distributableSol: 0,
    totalSol: 0,
    shareableEnabled: false,
    isShareholder: false,
    shareBps: 0,
    canDistribute: false,
    isGraduated: false,
  };
  try {
    creatorRewards = await getCreatorRewardsPreview({
      connection,
      mint: tokenRow.mint,
      wallet: tokenRow.deposit,
    });
  } catch {
    // keep zero preview on failure
  }

  return {
    token: {
      id: String(tokenRow.id),
      symbol: String(tokenRow.symbol),
      name: String(tokenRow.name),
      mint: String(tokenRow.mint),
      pictureUrl: String(tokenRow.picture_url || ""),
      selectedBot: String(tokenRow.selected_bot || "burn"),
      active: Boolean(tokenRow.active),
      disconnected: Boolean(tokenRow.disconnected),
    },
    addresses: enriched,
    totals,
    creatorRewards,
  };
}

export async function deleteToken(userId, tokenId) {
  const tokenRes = await pool.query(
    `
      SELECT id, symbol, name, mint, picture_url, deposit, selected_bot, active, disconnected
      FROM tokens
      WHERE user_id = $1 AND id = $2
      LIMIT 1
    `,
    [userId, tokenId]
  );
  if (!tokenRes.rowCount) throw new Error("Token not found.");
  const tokenRow = tokenRes.rows[0];
  const moduleType = normalizeModuleType(tokenRow.selected_bot, MODULE_TYPES.burn);
  const connection = getConnection();

  if (moduleType === MODULE_TYPES.burn) {
    try {
      const depositSecret = await getTokenDepositSigningKey(userId, String(tokenRow.id));
      const depositSigner = keypairFromBase58(depositSecret);
      const tokenBal = await getOwnerTokenBalanceUi(connection, depositSigner.publicKey, tokenRow.mint);
      const burnAmount = Number(tokenBal.toFixed(6));
      if (burnAmount > 0.000001) {
        const burnSig = await sendTokenToIncinerator(connection, depositSigner, tokenRow.mint, burnAmount);
        if (burnSig) {
          await insertEvent(
            pool,
            userId,
            String(tokenRow.id),
            String(tokenRow.symbol || ""),
            "burn",
            `Delete cleanup incinerated ${fmtInt(burnAmount.toFixed(6))} ${tokenRow.symbol}`,
            burnSig,
            { moduleType: MODULE_TYPES.burn }
          );
        }
      }
    } catch (error) {
      await insertEvent(
        pool,
        userId,
        String(tokenRow.id),
        String(tokenRow.symbol || ""),
        "error",
        `Delete cleanup burn failed: ${error.message}`,
        null,
        { moduleType: MODULE_TYPES.burn }
      );
    }
  } else if (moduleType === MODULE_TYPES.volume) {
    const moduleRes = await pool.query(
      `
        SELECT id, config_json
        FROM bot_modules
        WHERE token_id = $1 AND module_type = $2
        LIMIT 1
      `,
      [String(tokenRow.id), MODULE_TYPES.volume]
    );
    const configJson = mergeModuleConfig(
      MODULE_TYPES.volume,
      moduleRes.rows[0]?.config_json || {},
      {}
    );
    const signers = [];
    try {
      const depositSecret = await getTokenDepositSigningKey(userId, String(tokenRow.id));
      signers.push({ signer: keypairFromBase58(depositSecret), label: "deposit" });
    } catch {}
    const walletRes = await pool.query(
      `
        SELECT label, secret_key_base58
        FROM volume_trade_wallets
        WHERE user_id = $1 AND token_id = $2
      `,
      [userId, String(tokenRow.id)]
    );
    for (const row of walletRes.rows || []) {
      try {
        signers.push({
          signer: keypairFromBase58(decryptDepositSecret(row.secret_key_base58)),
          label: String(row.label || "trade"),
        });
      } catch {}
    }

    for (const item of signers) {
      const tokenBal = await getOwnerTokenBalanceUi(connection, item.signer.publicKey, tokenRow.mint);
      const sellAmount = Number(tokenBal.toFixed(6));
      if (sellAmount <= 0.000001) continue;
      try {
        const sellSig = await pumpPortalTrade({
          connection,
          signer: item.signer,
          mint: tokenRow.mint,
          action: "sell",
          amount: sellAmount,
          denominatedInSol: false,
          slippage: Math.max(1, Math.floor(Number(configJson.slippageBps || 1000) / 100)),
          pool: configJson.pool || "auto",
        });
        await insertEvent(
          pool,
          userId,
          String(tokenRow.id),
          String(tokenRow.symbol || ""),
          "sell",
          `Delete cleanup sold ${sellAmount.toFixed(6)} ${tokenRow.symbol} from ${item.label}`,
          sellSig,
          { moduleType: MODULE_TYPES.volume }
        );
      } catch (error) {
        await insertEvent(
          pool,
          userId,
          String(tokenRow.id),
          String(tokenRow.symbol || ""),
          "error",
          `Delete cleanup sell failed (${item.label}): ${error.message}`,
          null,
          { moduleType: MODULE_TYPES.volume }
        );
      }
    }
  }

  const addresses = await withTx(async (client) =>
    readTokenWalletAddresses(client, userId, tokenRow)
  );
  const balances = await collectWalletBalances(tokenRow, addresses);

  const hasFunds = balances.some(
    (a) => Number(a.solBalance || 0) > 0.00005 || Number(a.tokenBalance || 0) > 0.000001
  );
  if (hasFunds) {
    throw new Error(
      "Bot cannot be deleted while funds remain. Sweep/withdraw all wallet balances first."
    );
  }

  await withTx(async (client) => {
    await client.query(
      `
        INSERT INTO token_events (user_id, token_id, token_symbol, event_type, message, tx)
        VALUES ($1, $2, $3, 'delete', $4, NULL)
      `,
      [userId, tokenId, String(tokenRow.symbol), `Token disconnected from Nexus: ${tokenRow.symbol} (${tokenRow.mint})`]
    );
    await client.query(
      `
        UPDATE tokens
        SET active = FALSE,
            disconnected = TRUE
        WHERE user_id = $1 AND id = $2
      `,
      [userId, tokenId]
    );
    await client.query(
      `
        UPDATE bot_modules
        SET enabled = FALSE
        WHERE token_id = $1
      `,
      [tokenId]
    );
  });

  return { ok: true };
}

export async function sweepVolumeWallets(userId, tokenId) {
  const tokenIdText = String(tokenId || "").trim();
  if (!tokenIdText) throw new Error("Token id is required.");

  return withTx(async (client) => {
    const { tokenRow, moduleRow } = await getVolumeTokenModule(client, userId, tokenIdText, {
      forUpdate: true,
    });
    if (tokenRow.active) {
      throw new Error("Pause the volume bot before sweeping wallets.");
    }

    const lockKey = `${tokenIdText}:${MODULE_TYPES.volume}`;
    const lock = await withTokenModuleLock(client, lockKey, async () => {
      const connection = getConnection();
      const configJson = mergeModuleConfig(MODULE_TYPES.volume, moduleRow.config_json, {});
      const depositSecret = await getTokenDepositSigningKey(userId, tokenIdText);
      const depositSigner = keypairFromBase58(depositSecret);
      const reserveLamports = toLamports(configJson.reserveSol || config.botSolReserve || 0.01);
      const tradeReserveLamports = toLamports(getTradeWalletReserveSol());
      const feeSafetyLamports = getTxFeeSafetyLamports();
      const state = { ...(moduleRow.state_json || {}) };
      const wallets = await ensureVolumeTradeWallets(
        client,
        moduleRow,
        configJson.tradeWalletCount || 1
      );
      const eventPrefix = `manual:sweep:${moduleRow.module_id}:${Date.now()}`;

      let txCreated = 0;
      let walletsSwept = 0;
      let sweptLamports = 0;
      let soldTokens = 0;

      for (const wallet of wallets) {
        const label = String(wallet.label || wallet.wallet_pubkey || "trade");
        const tradeSigner = keypairFromBase58(decryptDepositSecret(wallet.secret_key_base58));
        const tokenBal = await getOwnerTokenBalanceUi(connection, tradeSigner.publicKey, tokenRow.mint);

        if (tokenBal > 0.000001) {
          try {
            const sellAmount = Number(tokenBal.toFixed(6));
            const sig = await pumpPortalTrade({
              connection,
              signer: tradeSigner,
              mint: tokenRow.mint,
              action: "sell",
              amount: sellAmount,
              denominatedInSol: false,
              slippage: Math.max(1, Math.floor(Number(configJson.slippageBps || 1000) / 100)),
              pool: configJson.pool || "auto",
            });
            txCreated += sig ? 1 : 0;
            soldTokens += sellAmount;
            await insertEvent(
              client,
              userId,
              tokenIdText,
              tokenRow.symbol,
              "sell",
              `Sweep sold ${sellAmount.toFixed(4)} ${tokenRow.symbol} from ${label}`,
              sig,
              {
                moduleType: MODULE_TYPES.volume,
                amount: sellAmount,
                idempotencyKey: `${eventPrefix}:sell:${wallet.wallet_pubkey}`,
              }
            );
          } catch (error) {
            await insertEvent(
              client,
              userId,
              tokenIdText,
              tokenRow.symbol,
              "error",
              `Sweep sell failed (${label}): ${error.message}`,
              null,
              {
                moduleType: MODULE_TYPES.volume,
                idempotencyKey: `${eventPrefix}:sell:error:${wallet.wallet_pubkey}`,
              }
            );
          }
        }

        const walletLamports = await connection.getBalance(tradeSigner.publicKey, "confirmed");
        const movable = Math.max(0, walletLamports - tradeReserveLamports - feeSafetyLamports);
        if (movable <= 0) continue;

        try {
          const sig = await sendSolTransfer(
            connection,
            tradeSigner,
            depositSigner.publicKey.toBase58(),
            movable
          );
          txCreated += sig ? 1 : 0;
          walletsSwept += 1;
          sweptLamports += movable;
          await insertEvent(
            client,
            userId,
            tokenIdText,
            tokenRow.symbol,
            "transfer",
            `Sweep moved ${fromLamports(movable).toFixed(6)} SOL from ${label} to deposit`,
            sig,
            {
              moduleType: MODULE_TYPES.volume,
              amount: fromLamports(movable),
              idempotencyKey: `${eventPrefix}:sweep:${wallet.wallet_pubkey}`,
            }
          );
        } catch (error) {
          await insertEvent(
            client,
            userId,
            tokenIdText,
            tokenRow.symbol,
            "error",
            `Sweep transfer failed (${label}): ${error.message}`,
            null,
            {
              moduleType: MODULE_TYPES.volume,
              idempotencyKey: `${eventPrefix}:sweep:error:${wallet.wallet_pubkey}`,
            }
          );
        }
      }

      const depositAfter = await connection.getBalance(depositSigner.publicKey, "confirmed");
      state.fannedOut = false;
      state.ignoreInflowLamports =
        Math.max(0, Number(state.ignoreInflowLamports || 0)) + sweptLamports;
      state.lastDepositBalanceLamports = Math.max(0, depositAfter - reserveLamports);
      await upsertModuleState(client, moduleRow.module_id, state, null);

      if (txCreated > 0) {
        await client.query(
          `
            UPDATE tokens
            SET tx_count = tx_count + $1
            WHERE id = $2
          `,
          [txCreated, tokenIdText]
        );
      }

      return {
        ok: true,
        walletsTotal: wallets.length,
        walletsSwept,
        soldTokens,
        sweptSol: fromLamports(sweptLamports),
        txCreated,
      };
    });

    if (!lock.locked) {
      throw new Error("Token/module is currently locked by another executor.");
    }
    return lock.result;
  });
}

export async function withdrawVolumeFunds(userId, tokenId, payload = {}) {
  const tokenIdText = String(tokenId || "").trim();
  if (!tokenIdText) throw new Error("Token id is required.");
  const destination = normalizePubkeyString(payload.destinationWallet || payload.destination || "");
  if (!destination) {
    throw new Error("Destination wallet is required.");
  }
  const requestedSol = payload.amountSol === undefined ? null : sanitizeSol(payload.amountSol, 0, 0, 10_000);

  return withTx(async (client) => {
    const { tokenRow, moduleRow } = await getVolumeTokenModule(client, userId, tokenIdText, {
      forUpdate: true,
    });
    if (tokenRow.active) {
      throw new Error("Pause the volume bot before withdrawing.");
    }

    const lockKey = `${tokenIdText}:${MODULE_TYPES.volume}`;
    const lock = await withTokenModuleLock(client, lockKey, async () => {
      const connection = getConnection();
      const configJson = mergeModuleConfig(MODULE_TYPES.volume, moduleRow.config_json, {});
      const reserveLamports = toLamports(configJson.reserveSol || config.botSolReserve || 0.01);
      const feeSafetyLamports = getTxFeeSafetyLamports();
      const depositSecret = await getTokenDepositSigningKey(userId, tokenIdText);
      const depositSigner = keypairFromBase58(depositSecret);
      const balanceBefore = await connection.getBalance(depositSigner.publicKey, "confirmed");
      const withdrawableLamports = Math.max(0, balanceBefore - reserveLamports - feeSafetyLamports);
      if (withdrawableLamports <= 0) {
        throw new Error("No withdrawable SOL available. Sweep first or fund the deposit wallet.");
      }

      let lamportsToSend = withdrawableLamports;
      if (requestedSol !== null) {
        const requestedLamports = toLamports(requestedSol);
        if (requestedLamports <= 0) throw new Error("Withdrawal amount must be greater than 0.");
        if (requestedLamports > withdrawableLamports) {
          throw new Error(
            `Requested amount exceeds withdrawable balance (${fromLamports(withdrawableLamports).toFixed(6)} SOL).`
          );
        }
        lamportsToSend = requestedLamports;
      }

      const sig = await sendSolTransfer(
        connection,
        depositSigner,
        destination,
        lamportsToSend
      );
      const balanceAfter = await connection.getBalance(depositSigner.publicKey, "confirmed");
      const eventPrefix = `manual:withdraw:${moduleRow.module_id}:${Date.now()}`;
      await insertEvent(
        client,
        userId,
        tokenIdText,
        tokenRow.symbol,
        "withdraw",
        `Withdrawn ${fromLamports(lamportsToSend).toFixed(6)} SOL to ${destination}`,
        sig,
        {
          moduleType: MODULE_TYPES.volume,
          amount: fromLamports(lamportsToSend),
          idempotencyKey: `${eventPrefix}:${destination}`,
        }
      );

      const state = { ...(moduleRow.state_json || {}) };
      state.lastDepositBalanceLamports = Math.max(0, balanceAfter - reserveLamports);
      if (balanceAfter <= reserveLamports + feeSafetyLamports) {
        state.fannedOut = false;
      }
      await upsertModuleState(client, moduleRow.module_id, state, null);

      await client.query(
        `
          UPDATE tokens
          SET tx_count = tx_count + 1
          WHERE id = $1
        `,
        [tokenIdText]
      );

      return {
        ok: true,
        signature: sig,
        sentSol: fromLamports(lamportsToSend),
        remainingSol: fromLamports(Math.max(0, balanceAfter - reserveLamports)),
      };
    });

    if (!lock.locked) {
      throw new Error("Token/module is currently locked by another executor.");
    }
    return lock.result;
  });
}

let holderCountCache = { mint: "", value: 0, at: 0 };

async function getEmberHolderCount() {
  const mint = String(config.emberTokenMint || "").trim();
  if (!mint) return 0;
  const now = Date.now();
  if (holderCountCache.mint === mint && now - holderCountCache.at < 60_000) {
    return holderCountCache.value;
  }

  if (!config.rpcUrl) return holderCountCache.mint === mint ? holderCountCache.value : 0;

  try {
    const connection = getConnection();
    const mintPk = new PublicKey(mint);
    const mintInfo = await connection.getAccountInfo(mintPk, "confirmed");
    const programId =
      mintInfo?.owner?.equals?.(TOKEN_2022_PROGRAM_ID)
        ? TOKEN_2022_PROGRAM_ID
        : TOKEN_PROGRAM_ID;
    const filters = [{ memcmp: { offset: 0, bytes: mintPk.toBase58() } }];
    if (programId.equals(TOKEN_PROGRAM_ID)) {
      filters.unshift({ dataSize: 165 });
    }
    const accounts = await connection.getParsedProgramAccounts(programId, {
      commitment: "confirmed",
      filters,
    });

    let holders = 0;
    for (const account of accounts) {
      const rawAmount = String(
        account.account?.data?.parsed?.info?.tokenAmount?.amount || "0"
      );
      try {
        if (BigInt(rawAmount) > 0n) holders += 1;
      } catch {
        // ignore malformed account payloads
      }
    }
    holderCountCache = { mint, value: holders, at: now };
    return holders;
  } catch {
    return holderCountCache.mint === mint ? holderCountCache.value : 0;
  }
}

export async function getPublicMetrics() {
  const emberMint = String(config.emberTokenMint || "").trim();
  const [tokenAggRes, eventAggRes, totalHolders, emberMarketCap, protocolAggRes] = await Promise.all([
    pool.query(`
      SELECT
        COUNT(*)::bigint AS active_tokens
      FROM tokens
    `),
    pool.query(`
      SELECT
        COALESCE(SUM(CASE WHEN event_type = 'burn' THEN amount ELSE 0 END), 0)::numeric AS lifetime_incinerated,
        COUNT(*) FILTER (WHERE tx IS NOT NULL)::bigint AS total_bot_transactions,
        COUNT(*) FILTER (WHERE tx IS NOT NULL)::bigint AS burn_buyback_transactions,
        COALESCE(SUM(CASE WHEN event_type = 'burn' AND UPPER(COALESCE(token_symbol, '')) = 'EMBER' THEN amount ELSE 0 END), 0)::numeric AS ember_incinerated,
        COALESCE(SUM(CASE WHEN event_type = 'claim' THEN amount ELSE 0 END), 0)::numeric AS rewards_processed_sol,
        COALESCE(SUM(CASE WHEN event_type = 'fee' THEN amount ELSE 0 END), 0)::numeric AS fees_taken_sol
      FROM token_events
    `),
    getEmberHolderCount(),
    emberMint ? fetchMarketCapUsd(emberMint).catch(() => 0) : Promise.resolve(0),
    pool.query(`
      SELECT
        total_bot_transactions,
        lifetime_incinerated,
        ember_incinerated,
        rewards_processed_sol,
        fees_taken_sol
      FROM protocol_metrics
      WHERE id = 1
      LIMIT 1
    `).catch(() => ({ rows: [] })),
  ]);

  const tokenAgg = tokenAggRes.rows[0] || {};
  const eventAgg = eventAggRes.rows[0] || {};
  const protocolAgg = protocolAggRes.rows[0] || {};

  return {
    lifetimeIncinerated:
      (Number(eventAgg.lifetime_incinerated) || 0) + (Number(protocolAgg.lifetime_incinerated) || 0),
    totalBotTransactions:
      (Number(eventAgg.total_bot_transactions) || 0) + (Number(protocolAgg.total_bot_transactions) || 0),
    transactions:
      (Number(eventAgg.total_bot_transactions) || 0) + (Number(protocolAgg.total_bot_transactions) || 0),
    burnBuybackTransactions:
      (Number(eventAgg.burn_buyback_transactions) || 0) + (Number(protocolAgg.total_bot_transactions) || 0),
    activeTokens: Number(tokenAgg.active_tokens) || 0,
    totalHolders: Number(totalHolders) || 0,
    emberMarketCap: Number(emberMarketCap) || 0,
    emberIncinerated:
      (Number(eventAgg.ember_incinerated) || 0) + (Number(protocolAgg.ember_incinerated) || 0),
    totalRewardsProcessedSol:
      (Number(eventAgg.rewards_processed_sol) || 0) + (Number(protocolAgg.rewards_processed_sol) || 0),
    totalFeesTakenSol:
      (Number(eventAgg.fees_taken_sol) || 0) + (Number(protocolAgg.fees_taken_sol) || 0),
  };
}

export async function buildDeployLocalTx(payload) {
  if (!pumpOfflineSdk || !OnlinePumpSdk || !getBuyTokenAmountFromSolAmount) {
    throw new Error("Pump SDK is unavailable for local deploy build.");
  }

  const body = payload && typeof payload === "object" ? payload : {};
  const action = String(body.action || "").trim().toLowerCase();
  if (action !== "create") {
    throw new Error("Only create action is supported.");
  }

  const publicKeyRaw = String(body.publicKey || "").trim();
  const mintRaw = String(body.mint || "").trim();
  if (!publicKeyRaw) throw new Error("publicKey is required.");
  if (!mintRaw) throw new Error("mint is required.");
  let userPk;
  let mintPk;
  try {
    userPk = new PublicKey(publicKeyRaw);
    mintPk = new PublicKey(mintRaw);
  } catch {
    throw new Error("publicKey or mint is invalid.");
  }

  const tokenMetadata =
    body.tokenMetadata && typeof body.tokenMetadata === "object" ? body.tokenMetadata : {};
  const name = sanitizeDeployString(tokenMetadata.name, 40, "Token name");
  const symbol = sanitizeDeployString(tokenMetadata.symbol, 12, "Token symbol").toUpperCase();
  const uri = sanitizeDeployString(tokenMetadata.uri, 300, "Token metadata URI");

  const denominatedInSol = String(body.denominatedInSol || "true").trim().toLowerCase() !== "false";
  if (!denominatedInSol) {
    throw new Error("Create requires denominatedInSol='true'.");
  }

  const amountSol = Number(body.amount);
  if (!Number.isFinite(amountSol) || amountSol <= 0) {
    throw new Error("Create amount must be greater than 0 SOL.");
  }

  const mayhemMode = String(body.isMayhemMode || "false").trim().toLowerCase() === "true";
  const solLamports = Math.floor(amountSol * LAMPORTS_PER_SOL);
  if (solLamports <= 0) {
    throw new Error("Create amount is too small.");
  }

  const connection = getConnection();
  const onlineSdk = new OnlinePumpSdk(connection);
  const global = await onlineSdk.fetchGlobal();
  const solAmount = new BN(solLamports);
  const tokenAmount = getBuyTokenAmountFromSolAmount({
    global,
    feeConfig: null,
    mintSupply: null,
    bondingCurve: null,
    amount: solAmount,
  });

  const instructions = await pumpOfflineSdk.createV2AndBuyInstructions({
    global,
    mint: mintPk,
    name,
    symbol,
    uri,
    creator: userPk,
    user: userPk,
    amount: tokenAmount,
    solAmount,
    mayhemMode,
  });

  if (!Array.isArray(instructions) || !instructions.length) {
    throw new Error("Local deploy builder returned no instructions.");
  }

  const latest = await connection.getLatestBlockhash("confirmed");
  const message = new TransactionMessage({
    payerKey: userPk,
    recentBlockhash: latest.blockhash,
    instructions,
  }).compileToV0Message();
  const tx = new VersionedTransaction(message);
  return tx.serialize();
}

export async function submitSignedDeployTx(payload) {
  const body = payload && typeof payload === "object" ? payload : {};
  const txBase64 = String(body.txBase64 || "").trim();
  if (!txBase64) {
    throw new Error("Signed transaction payload is required.");
  }

  let raw;
  try {
    raw = Buffer.from(txBase64, "base64");
  } catch {
    throw new Error("Signed transaction payload is invalid.");
  }
  if (!raw?.length) {
    throw new Error("Signed transaction payload is empty.");
  }

  const connection = getConnection();
  const signature = await connection.sendRawTransaction(raw, {
    skipPreflight: false,
    maxRetries: 5,
  });
  return { signature };
}

export async function deployToken(userId, payload) {
  if (!config.pumpPortalApiKey) {
    throw new Error("Deploy is not configured yet. Missing PUMPPORTAL_API_KEY.");
  }

  const deployUserId = userId || null;
  const requestedAutoAttach = Boolean(payload.autoAttach);
  const botPreset = getDeployBotPreset(payload.selectedBot);
  const autoAttach = requestedAutoAttach && Boolean(botPreset);
  if (autoAttach && !deployUserId) {
    throw new Error("Sign in is required when Auto-Attach is enabled.");
  }

  const name = sanitizeDeployString(payload.name, 40, "Name");
  const symbol = sanitizeDeployString(payload.symbol, 12, "Symbol").toUpperCase();
  const description = sanitizeDeployString(payload.description, 300, "Description");
  const initialBuyMode = String(payload.initialBuyMode || "sol").trim().toLowerCase() === "tokens" ? "tokens" : "sol";
  const initialBuySol = sanitizeDeployNumber(payload.initialBuySol, 0.1, 0, 100);
  const initialBuyTokens = sanitizeDeployNumber(payload.initialBuyTokens, 0, 0, 1_000_000_000_000);
  const twitter = payload.twitter ? normalizeHttpUrl(payload.twitter, "Twitter URL") : "";
  const telegram = payload.telegram ? normalizeHttpUrl(payload.telegram, "Telegram URL") : "";
  const website = payload.website ? normalizeHttpUrl(payload.website, "Website URL") : "";
  const mayhemMode = Boolean(payload.mayhemMode);
  const slippage = 10;
  const priorityFee = 0.0005;
  if (initialBuyMode === "tokens" && initialBuyTokens < 0) {
    throw new Error("Initial buy token amount cannot be negative.");
  }
  if (initialBuyMode === "sol" && initialBuySol < 0) {
    throw new Error("Initial buy SOL amount cannot be negative.");
  }

  const uploadImageDataUri = String(payload.imageDataUri || "").trim();
  const uploadImageFileName = String(payload.imageFileName || "token").trim().slice(0, 80) || "token";
  const uploadBannerDataUri = String(payload.bannerDataUri || "").trim();
  const uploadBannerFileName = String(payload.bannerFileName || "banner").trim().slice(0, 80) || "banner";
  const fallbackBannerUrl = String(payload.bannerUrl || "").trim();
  const fallbackImageUrl = String(payload.imageUrl || "").trim();
  let imageType = "image/png";
  let imageBlob = null;
  if (uploadImageDataUri) {
    const parsed = parseDataUriMedia(uploadImageDataUri);
    imageType = parsed.imageType;
    imageBlob = parsed.imageBlob;
  } else if (fallbackImageUrl) {
    const safeImageUrl = normalizeHttpUrl(fallbackImageUrl, "Image URL");
    const imageRes = await fetch(safeImageUrl);
    if (!imageRes.ok) {
      throw new Error("Failed to fetch token image from provided URL.");
    }
    imageType = imageRes.headers.get("content-type") || "image/png";
    imageBlob = await imageRes.blob();
  } else {
    throw new Error("Token media upload is required.");
  }

  let bannerType = "";
  let bannerBlob = null;
  if (uploadBannerDataUri) {
    const parsedBanner = parseDataUriBanner(uploadBannerDataUri);
    bannerType = parsedBanner.bannerType;
    bannerBlob = parsedBanner.bannerBlob;
  } else if (fallbackBannerUrl) {
    const safeBannerUrl = normalizeHttpUrl(fallbackBannerUrl, "Banner URL");
    const bannerRes = await fetch(safeBannerUrl);
    if (!bannerRes.ok) {
      throw new Error("Failed to fetch token banner from provided URL.");
    }
    bannerType = (bannerRes.headers.get("content-type") || "image/png").toLowerCase();
    if (!new Set(["image/jpeg", "image/png", "image/gif"]).has(bannerType)) {
      throw new Error("Unsupported banner type. Allowed: JPG, PNG, GIF.");
    }
    bannerBlob = await bannerRes.blob();
    if (Number(bannerBlob.size || 0) > Math.floor(4.3 * 1024 * 1024)) {
      throw new Error("Banner must be 4.3MB or smaller.");
    }
  }

  const ext = imageType.includes("jpeg")
    ? "jpg"
    : imageType.includes("gif")
      ? "gif"
      : imageType.includes("mp4")
        ? "mp4"
        : imageType.includes("webp")
          ? "webp"
          : "png";

  const formData = new FormData();
  formData.append("file", imageBlob, `${uploadImageFileName}.${ext}`);
  if (bannerBlob) {
    const bannerExt = bannerType.includes("jpeg")
      ? "jpg"
      : bannerType.includes("gif")
        ? "gif"
        : "png";
    formData.append("banner", bannerBlob, `${uploadBannerFileName}.${bannerExt}`);
  }
  formData.append("name", name);
  formData.append("symbol", symbol);
  formData.append("description", description);
  formData.append("twitter", twitter);
  formData.append("telegram", telegram);
  formData.append("website", website);
  formData.append("showName", "true");

  const metadataRes = await fetch("https://pump.fun/api/ipfs", {
    method: "POST",
    body: formData,
  });

  if (!metadataRes.ok) {
    throw new Error("Metadata upload failed. Verify deploy inputs and uploaded image.");
  }

  const metadataJson = await metadataRes.json();
  const metadataUri = String(metadataJson.metadataUri || "").trim();
  if (!metadataUri) {
    throw new Error("Metadata upload did not return a metadata URI.");
  }

  const mintKeypair = Keypair.generate();
  const deployPayload = {
    action: "create",
    tokenMetadata: {
      name,
      symbol,
      uri: metadataUri,
    },
    mint: bs58.encode(mintKeypair.secretKey),
    denominatedInSol: initialBuyMode === "sol" ? "true" : "false",
    amount: initialBuyMode === "sol" ? initialBuySol : initialBuyTokens,
    slippage,
    priorityFee,
    pool: "pump",
    isMayhemMode: mayhemMode ? "true" : "false",
  };

  const tradeRes = await fetch(
    `https://pumpportal.fun/api/trade?api-key=${encodeURIComponent(config.pumpPortalApiKey)}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(deployPayload),
    }
  );

  const tradeText = await tradeRes.text();
  let tradeJson = null;
  try {
    tradeJson = JSON.parse(tradeText);
  } catch {
    tradeJson = { raw: tradeText };
  }

  if (!tradeRes.ok) {
    const reason =
      (tradeJson && (tradeJson.error || tradeJson.message || tradeJson.raw)) ||
      "PumpPortal deploy request failed.";
    throw new Error(String(reason));
  }

  const signature =
    (tradeJson && (tradeJson.signature || tradeJson.txSignature || tradeJson.result?.signature)) ||
    null;
  const mint = mintKeypair.publicKey.toBase58();
  const metadataImage =
    String(
      metadataJson?.metadata?.image ||
        metadataJson?.image ||
        metadataJson?.imageUri ||
        ""
    ).trim() || null;

  let attachedToken = null;
  if (autoAttach && deployUserId && botPreset) {
    attachedToken = await attachToken(deployUserId, {
      mint,
      symbol,
      name,
      pictureUrl: metadataImage || "",
      claimSec: botPreset.claimSec,
      burnSec: botPreset.burnSec,
      splits: botPreset.splits,
      selectedBot: payload.selectedBot,
      attachSource: "deploy-auto-attach",
    });
  }

  if (deployUserId) {
    await pool.query(
      `
        INSERT INTO token_events (user_id, token_id, token_symbol, event_type, message, tx)
        VALUES ($1, $2, $3, 'deploy', $4, $5)
      `,
      [
        deployUserId,
        attachedToken?.id || null,
        symbol,
        `Token deployed via EMBER Deploy: ${symbol} (${mint})`,
        signature,
      ]
    );
  }

  return {
    ok: true,
    mint,
    signature,
    metadataUri,
    metadataImage,
    pumpfunUrl: `https://pump.fun/coin/${mint}`,
    solscanTx: signature ? `https://solscan.io/tx/${signature}` : null,
    solscanMint: `https://solscan.io/token/${mint}`,
    autoAttached: Boolean(attachedToken),
    attachedToken,
  };
}

export async function recordDeployFromChain(userId, payload) {
  const deployUserId = userId || null;
  const mint = String(payload.mint || "").trim();
  if (mint.length < 32) {
    throw new Error("Mint is required.");
  }

  const symbolRaw = String(payload.symbol || "").trim();
  const symbol = (symbolRaw || "TOKEN").toUpperCase().slice(0, 12);
  const nameRaw = String(payload.name || "").trim();
  const name = (nameRaw || symbol).slice(0, 64);
  const signature = String(payload.signature || "").trim() || null;
  const pictureUrl = String(payload.pictureUrl || payload.metadataImage || "").trim().slice(0, 255);

  const requestedAutoAttach = Boolean(payload.autoAttach);
  const botPreset = getDeployBotPreset(payload.selectedBot);
  const autoAttach = requestedAutoAttach && Boolean(botPreset);
  if (autoAttach && !deployUserId) {
    throw new Error("Sign in is required when Auto-Attach is enabled.");
  }

  let attachedToken = null;
  if (autoAttach && deployUserId && botPreset) {
    const existingRes = await pool.query(
      "SELECT * FROM tokens WHERE user_id = $1 AND mint = $2 LIMIT 1",
      [deployUserId, mint]
    );
    if (existingRes.rowCount) {
      attachedToken = toToken(existingRes.rows[0]);
    } else {
      attachedToken = await attachToken(deployUserId, {
        mint,
        symbol,
        name,
        pictureUrl,
        claimSec: botPreset.claimSec,
        burnSec: botPreset.burnSec,
        splits: botPreset.splits,
        selectedBot: payload.selectedBot,
        attachSource: "deploy-auto-attach",
      });
    }
  }

  if (deployUserId) {
    await pool.query(
      `
        INSERT INTO token_events (user_id, token_id, token_symbol, event_type, message, tx)
        VALUES ($1, $2, $3, 'deploy', $4, $5)
      `,
      [
        deployUserId,
        attachedToken?.id || null,
        symbol,
        `Token deployed via wallet: ${symbol} (${mint})`,
        signature,
      ]
    );
  }

  return {
    ok: true,
    mint,
    signature,
    pumpfunUrl: `https://pump.fun/coin/${mint}`,
    autoAttached: Boolean(attachedToken),
    attachedToken,
  };
}
export async function attachToken(userId, payload) {
  const mint = String(payload.mint || "").trim();
  if (mint.length < 32) {
    throw new Error("A valid mint address is required.");
  }

  const tokenCountPrecheck = await pool.query(
    "SELECT COUNT(*)::int AS c FROM tokens WHERE user_id = $1 AND disconnected = FALSE",
    [userId]
  );
  if (tokenCountPrecheck.rows[0].c >= config.maxTokensPerAccount) {
    throw new Error(`Max ${config.maxTokensPerAccount} burners per account reached.`);
  }

  const existsPrecheck = await pool.query(
    "SELECT id, disconnected FROM tokens WHERE user_id = $1 AND mint = $2 LIMIT 1",
    [userId, mint]
  );
  if (existsPrecheck.rowCount && !Boolean(existsPrecheck.rows[0].disconnected)) {
    throw new Error("That mint is already attached on your account.");
  }

  const claimSec = sanitizeInterval(payload.claimSec, 120);
  const burnSec = sanitizeInterval(payload.burnSec, 300);
  const splits = sanitizeSplits(payload.splits, 1);
  const selectedBot = normalizeModuleType(payload.selectedBot, MODULE_TYPES.burn);
  const resolved = resolveMint(mint) || {};
  const symbol = String(payload.symbol || resolved.symbol || "TOK").toUpperCase().slice(0, 12);
  const name = String(payload.name || resolved.name || "Token").slice(0, 64);
  const pictureUrl = normalizeMediaUrl(payload.pictureUrl || resolved.pictureUrl || "").slice(0, 255);
  const pendingDepositId = String(payload.pendingDepositId || "").trim();
  const moduleType = selectedBot === MODULE_TYPES.volume ? MODULE_TYPES.volume : MODULE_TYPES.burn;
  const volumeOverrides = {
    claimIntervalSec: sanitizeInterval(payload.claimSec, claimSec),
    tradeWalletCount: Math.floor(sanitizeRange(payload.tradeWalletCount, 1, 1, 5)),
    speed: sanitizeRange(payload.speed, 35, 0, 100),
    aggression: sanitizeRange(payload.aggression, 35, 0, 100),
    minTradeSol: sanitizeSol(payload.minTradeSol, 0.01, 0.001, 10),
    maxTradeSol: sanitizeSol(payload.maxTradeSol, 0.05, 0.001, 100),
  };
  if (payload.claimEnabled !== undefined) {
    volumeOverrides.claimEnabled = Boolean(payload.claimEnabled);
  }
  const initialConfig =
    moduleType === MODULE_TYPES.volume
      ? mergeModuleConfig(MODULE_TYPES.volume, defaultVolumeModuleConfig(), volumeOverrides)
      : mergeModuleConfig(
          MODULE_TYPES.burn,
          defaultBurnModuleConfig({ claim_sec: claimSec, burn_sec: burnSec, splits }),
          {
            claimIntervalSec: sanitizeInterval(payload.claimSec, claimSec),
            burnIntervalSec: sanitizeInterval(payload.burnSec, burnSec),
            splitBuys: sanitizeSplits(payload.splits, splits),
          }
        );
  const token = await withTx(async (client) => {
    const tokenCountRes = await client.query(
      "SELECT COUNT(*)::int AS c FROM tokens WHERE user_id = $1 AND disconnected = FALSE",
      [userId]
    );

    if (tokenCountRes.rows[0].c >= config.maxTokensPerAccount) {
      throw new Error(`Max ${config.maxTokensPerAccount} burners per account reached.`);
    }

    const existsRes = await client.query(
      "SELECT * FROM tokens WHERE user_id = $1 AND mint = $2 LIMIT 1 FOR UPDATE",
      [userId, mint]
    );
    if (existsRes.rowCount && !Boolean(existsRes.rows[0].disconnected)) {
      throw new Error("That mint is already attached on your account.");
    }

    const now = Date.now();
    let tokenId;
    let tokenRow;

    if (existsRes.rowCount && Boolean(existsRes.rows[0].disconnected)) {
      const existing = existsRes.rows[0];
      tokenId = String(existing.id);
      let deposit = String(existing.deposit || "").trim();

      if (pendingDepositId) {
        const generatedDeposit = await consumeReservedDepositAddress(client, userId, pendingDepositId);
        const nextDeposit = String(generatedDeposit?.pubkey || "").trim();
        if (!nextDeposit) {
          throw new Error("Failed to generate deposit address.");
        }
        const depositSecretKeyBase58 = String(generatedDeposit?.secretKeyBase58 || "").trim();
        if (!depositSecretKeyBase58) {
          throw new Error("Failed to generate deposit private key.");
        }
        const storedDepositSecret = encryptDepositSecret(depositSecretKeyBase58);
        deposit = nextDeposit;
        await client.query(
          `
            INSERT INTO token_deposit_keys (token_id, user_id, deposit_pubkey, secret_key_base58)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (token_id)
            DO UPDATE SET
              deposit_pubkey = EXCLUDED.deposit_pubkey,
              secret_key_base58 = EXCLUDED.secret_key_base58
          `,
          [tokenId, userId, deposit, storedDepositSecret]
        );
      }

      const updated = await client.query(
        `
          UPDATE tokens
          SET symbol = $1,
              name = $2,
              picture_url = $3,
              deposit = $4,
              claim_sec = $5,
              burn_sec = $6,
              splits = $7,
              selected_bot = $8,
              active = FALSE,
              disconnected = FALSE,
              next_claim_at = to_timestamp($9 / 1000.0),
              next_burn_at = to_timestamp($10 / 1000.0)
          WHERE id = $11
          RETURNING *
        `,
        [
          symbol,
          name,
          pictureUrl,
          deposit,
          claimSec,
          burnSec,
          splits,
          selectedBot,
          now + claimSec * 1000,
          now + burnSec * 1000,
          tokenId,
        ]
      );
      tokenRow = updated.rows[0];

      await client.query(
        `
          INSERT INTO token_events (user_id, token_id, token_symbol, event_type, message, tx)
          VALUES ($1, $2, $3, 'claim', $4, NULL)
        `,
        [userId, tokenId, symbol, `Token re-attached: ${symbol} reconnected in paused mode. Configure settings, then start.`]
      );
    } else {
      const generatedDeposit = pendingDepositId
        ? await consumeReservedDepositAddress(client, userId, pendingDepositId)
        : await generateVanityDeposit(config.depositVanityPrefix || "EMBR");
      const deposit = String(generatedDeposit?.pubkey || "").trim();
      if (!deposit) {
        throw new Error("Failed to generate deposit address.");
      }
      const depositSecretKeyBase58 = String(generatedDeposit?.secretKeyBase58 || "").trim();
      if (!depositSecretKeyBase58) {
        throw new Error("Failed to generate deposit private key.");
      }
      const storedDepositSecret = encryptDepositSecret(depositSecretKeyBase58);
      tokenId = makeId("tok");

      const inserted = await client.query(
        `
          INSERT INTO tokens (
            id, user_id, symbol, name, mint, picture_url, deposit,
            claim_sec, burn_sec, splits, selected_bot, active, disconnected, burned, pending, tx_count,
            next_claim_at, next_burn_at
          )
          VALUES (
            $1, $2, $3, $4, $5, $6, $7,
            $8, $9, $10, $11, FALSE, FALSE, 0, 0, 0,
            to_timestamp($12 / 1000.0), to_timestamp($13 / 1000.0)
          )
          RETURNING *
        `,
        [
          tokenId,
          userId,
          symbol,
          name,
          mint,
          pictureUrl,
          deposit,
          claimSec,
          burnSec,
          splits,
          selectedBot,
          now + claimSec * 1000,
          now + burnSec * 1000,
        ]
      );
      tokenRow = inserted.rows[0];

      await client.query(
        `
          INSERT INTO token_deposit_keys (token_id, user_id, deposit_pubkey, secret_key_base58)
          VALUES ($1, $2, $3, $4)
        `,
        [tokenId, userId, deposit, storedDepositSecret]
      );

      await client.query(
        `
          INSERT INTO token_events (user_id, token_id, token_symbol, event_type, message, tx)
          VALUES ($1, $2, $3, 'claim', $4, NULL)
        `,
        [userId, tokenId, symbol, `Token attached: ${symbol} created in paused mode. Configure settings, then start.`]
      );
    }

    await client.query(
      `
        INSERT INTO bot_modules (id, user_id, token_id, module_type, enabled, config_json, state_json, next_run_at)
        VALUES ($1, $2, $3, $4, FALSE, $5::jsonb, '{}'::jsonb, NOW())
        ON CONFLICT (token_id, module_type)
        DO UPDATE SET
          enabled = FALSE,
          config_json = EXCLUDED.config_json,
          state_json = '{}'::jsonb,
          next_run_at = NOW(),
          last_error = NULL
      `,
      [makeId("mod"), userId, tokenId, moduleType, JSON.stringify(initialConfig)]
    );

    await client.query(
      `
        UPDATE bot_modules
        SET enabled = FALSE
        WHERE token_id = $1
          AND module_type <> $2
      `,
      [tokenId, moduleType]
    );

    return toToken(tokenRow);
  });

  if (pendingDepositId) {
    void ensureDepositPool();
  }

  const source = String(payload.attachSource || "attach").trim() || "attach";
  void sendTelegramAnnouncement({
    title:
      source === "deploy-auto-attach"
        ? "[EMBER] New coin launched + attached on EMBER.nexus"
        : "[EMBER] New coin attached on EMBER.nexus",
    lines: [
      `Name: ${token.name}`,
      `Symbol: $${token.symbol}`,
      `CA: ${token.mint}`,
      `Bot: ${String(token.selectedBot || "burn").toUpperCase()}`,
      `Pump: https://pump.fun/coin/${token.mint}`,
    ],
    imageUrl: token.pictureUrl || "",
  });

  return token;
}

export async function updateToken(userId, tokenId, payload) {
  return withTx(async (client) => {
    const currentRes = await client.query("SELECT * FROM tokens WHERE user_id = $1 AND id = $2 LIMIT 1", [
      userId,
      tokenId,
    ]);

    if (!currentRes.rowCount) {
      throw new Error("Token not found.");
    }

    const current = currentRes.rows[0];
    if (current.disconnected) {
      throw new Error("Token is disconnected. Re-attach this mint to resume automation.");
    }
    const previousBot = normalizeModuleType(current.selected_bot, current.selected_bot || MODULE_TYPES.burn);
    const previousActive = Boolean(current.active);
    const claimSec = payload.claimSec === undefined ? Number(current.claim_sec) : sanitizeInterval(payload.claimSec, Number(current.claim_sec));
    const burnSec = payload.burnSec === undefined ? Number(current.burn_sec) : sanitizeInterval(payload.burnSec, Number(current.burn_sec));
    const splits = payload.splits === undefined ? Number(current.splits) : sanitizeSplits(payload.splits, Number(current.splits));
    const active = payload.active === undefined ? current.active : Boolean(payload.active);
    const selectedBot = normalizeModuleType(payload.selectedBot || current.selected_bot, current.selected_bot || MODULE_TYPES.burn);
    const shouldStartNow = payload.active !== undefined && Boolean(active) && !previousActive;

    const now = Date.now();

    const updatedRes = await client.query(
      `
        UPDATE tokens
        SET
          claim_sec = $1,
          burn_sec = $2,
          splits = $3,
          active = $4,
          selected_bot = $10,
          next_claim_at = CASE
            WHEN $5 OR $6 THEN to_timestamp($7 / 1000.0)
            ELSE next_claim_at
          END,
          next_burn_at = CASE
            WHEN $5 OR $6 THEN to_timestamp($8 / 1000.0)
            ELSE next_burn_at
          END
        WHERE id = $9
        RETURNING *
      `,
      [
        claimSec,
        burnSec,
        splits,
        active,
        payload.active !== undefined,
        payload.claimSec !== undefined || payload.burnSec !== undefined,
        now + claimSec * 1000,
        now + burnSec * 1000,
        tokenId,
        selectedBot,
      ]
    );

    const moduleType = selectedBot === MODULE_TYPES.volume ? MODULE_TYPES.volume : MODULE_TYPES.burn;
    const baseConfig =
      moduleType === MODULE_TYPES.volume
        ? defaultVolumeModuleConfig()
        : defaultBurnModuleConfig({ claim_sec: claimSec, burn_sec: burnSec, splits });

    const nextConfig = {};
    if (payload.claimEnabled !== undefined) {
      nextConfig.claimEnabled = Boolean(payload.claimEnabled);
    }
    if (payload.claimSec !== undefined) {
      nextConfig.claimIntervalSec = sanitizeInterval(payload.claimSec, claimSec);
    }
    if (payload.burnSec !== undefined && moduleType === MODULE_TYPES.burn) {
      nextConfig.burnIntervalSec = sanitizeInterval(payload.burnSec, burnSec);
    }
    if (payload.splits !== undefined && moduleType === MODULE_TYPES.burn) {
      nextConfig.splitBuys = sanitizeSplits(payload.splits, splits);
    }
    if (payload.minProcessSol !== undefined && moduleType === MODULE_TYPES.burn) {
      nextConfig.minProcessSol = sanitizeSol(payload.minProcessSol, 0.01, 0.001, 100);
    }
    if (payload.tradeWalletCount !== undefined && moduleType === MODULE_TYPES.volume) {
      nextConfig.tradeWalletCount = Math.floor(
        sanitizeRange(payload.tradeWalletCount, 1, 1, 5)
      );
    }
    if (payload.speed !== undefined && moduleType === MODULE_TYPES.volume) {
      nextConfig.speed = sanitizeRange(payload.speed, 35, 0, 100);
    }
    if (payload.aggression !== undefined && moduleType === MODULE_TYPES.volume) {
      nextConfig.aggression = sanitizeRange(payload.aggression, 35, 0, 100);
    }
    if (payload.minTradeSol !== undefined && moduleType === MODULE_TYPES.volume) {
      nextConfig.minTradeSol = sanitizeSol(payload.minTradeSol, 0.01, 0.001, 10);
    }
    if (payload.maxTradeSol !== undefined && moduleType === MODULE_TYPES.volume) {
      nextConfig.maxTradeSol = sanitizeSol(payload.maxTradeSol, 0.05, 0.001, 100);
    }
    if (payload.reserveSol !== undefined) {
      nextConfig.reserveSol = sanitizeSol(payload.reserveSol, Number(config.botSolReserve || 0.01), 0.001, 10);
    }
    if (payload.slippageBps !== undefined) {
      nextConfig.slippageBps = Math.floor(sanitizeRange(payload.slippageBps, 1000, 100, 5000));
    }
    if (payload.pool !== undefined) {
      nextConfig.pool = String(payload.pool || "auto").trim().toLowerCase() || "auto";
    }

    const existingModuleRes = await client.query(
      "SELECT id, config_json FROM bot_modules WHERE token_id = $1 AND module_type = $2 LIMIT 1",
      [tokenId, moduleType]
    );
    let moduleId = "";
    if (!existingModuleRes.rowCount) {
      moduleId = makeId("mod");
      await client.query(
        `
          INSERT INTO bot_modules (id, user_id, token_id, module_type, enabled, config_json, state_json, next_run_at)
          VALUES ($1, $2, $3, $4, $5, $6::jsonb, '{}'::jsonb, NOW())
        `,
        [
          moduleId,
          userId,
          tokenId,
          moduleType,
          active,
          JSON.stringify({ ...baseConfig, ...nextConfig }),
        ]
      );
    } else {
      moduleId = String(existingModuleRes.rows[0].id || "");
      const shouldResetVolumeState =
        moduleType === MODULE_TYPES.volume &&
        shouldStartNow;
      const merged = mergeModuleConfig(moduleType, existingModuleRes.rows[0].config_json, {
        ...baseConfig,
        ...nextConfig,
      });
      await client.query(
        `
          UPDATE bot_modules
          SET enabled = $1,
              config_json = $2::jsonb,
              state_json = CASE WHEN $4 THEN '{}'::jsonb ELSE state_json END
          WHERE id = $3
        `,
        [active, JSON.stringify(merged), existingModuleRes.rows[0].id, shouldResetVolumeState]
      );
    }

    await client.query(
      `
        UPDATE bot_modules
        SET enabled = FALSE
        WHERE token_id = $1 AND module_type <> $2
      `,
      [tokenId, moduleType]
    );

    if (shouldStartNow && moduleId) {
      if (moduleType === MODULE_TYPES.burn) {
        await client.query(
          `
            UPDATE bot_modules
            SET next_run_at = NOW(),
                state_json = COALESCE(state_json, '{}'::jsonb) || jsonb_build_object('nextClaimAt', 0, 'nextBurnAt', 0)
            WHERE id = $1
          `,
          [moduleId]
        );
      } else {
        await client.query(
          `
            UPDATE bot_modules
            SET next_run_at = NOW(),
                state_json = COALESCE(state_json, '{}'::jsonb) || jsonb_build_object('nextClaimAt', 0)
            WHERE id = $1
          `,
          [moduleId]
        );
      }
    }

    if (previousBot !== moduleType) {
      await insertEvent(
        client,
        userId,
        tokenId,
        String(current.symbol || ""),
        "status",
        `Bot mode changed: ${moduleTypeLabel(previousBot)} -> ${moduleTypeLabel(moduleType)}.`,
        null,
        { moduleType, idempotencyKey: `status:mode:${tokenId}:${Date.now()}` }
      );
    }

    if (previousActive !== Boolean(active)) {
      await insertEvent(
        client,
        userId,
        tokenId,
        String(current.symbol || ""),
        "status",
        Boolean(active)
          ? `${moduleTypeLabel(moduleType)} started.`
          : `${moduleTypeLabel(moduleType)} paused.`,
        null,
        { moduleType, idempotencyKey: `status:active:${tokenId}:${Date.now()}` }
      );
    }

    return toToken(updatedRes.rows[0]);
  });
}

export async function withdrawBurnFunds(userId, tokenId, payload = {}) {
  const tokenIdText = String(tokenId || "").trim();
  if (!tokenIdText) throw new Error("Token id is required.");
  const destination = normalizePubkeyString(payload.destinationWallet || payload.destination || "");
  if (!destination) {
    throw new Error("Destination wallet is required.");
  }

  return withTx(async (client) => {
    const { tokenRow, moduleRow } = await getBurnTokenModule(client, userId, tokenIdText, {
      forUpdate: true,
    });
    if (tokenRow.active) {
      throw new Error("Pause the burn bot before withdrawing.");
    }

    const lockKey = `${tokenIdText}:${MODULE_TYPES.burn}`;
    const lock = await withTokenModuleLock(client, lockKey, async () => {
      const connection = getConnection();
      const depositSecret = await getTokenDepositSigningKey(userId, tokenIdText);
      const depositSigner = keypairFromBase58(depositSecret);
      const eventPrefix = `manual:withdraw:burn:${moduleRow.module_id}:${Date.now()}`;
      let burnedToken = 0;
      const tokenBal = await getOwnerTokenBalanceUi(connection, depositSigner.publicKey, tokenRow.mint);
      const burnAmount = Number(tokenBal.toFixed(6));
      if (burnAmount > 0.000001) {
        const burnSig = await sendTokenToIncinerator(connection, depositSigner, tokenRow.mint, burnAmount);
        const tokenAfter = await getOwnerTokenBalanceUi(connection, depositSigner.publicKey, tokenRow.mint);
        burnedToken = Math.max(0, burnAmount - tokenAfter);
        await insertEvent(
          client,
          userId,
          tokenIdText,
          tokenRow.symbol,
          "burn",
          `Withdraw cleanup incinerated ${fmtInt(burnedToken.toFixed(6))} ${tokenRow.symbol}`,
          burnSig,
          {
            moduleType: MODULE_TYPES.burn,
            amount: burnedToken,
            idempotencyKey: `${eventPrefix}:burn`,
          }
        );
      }

      const sweep = await sendAllSolTransfer(connection, depositSigner, destination);
      if (!sweep.signature || sweep.sentLamports <= 0) {
        if (burnedToken > 0) {
          const balanceAfterToken = await connection.getBalance(depositSigner.publicKey, "confirmed");
          const configJson = mergeModuleConfig(MODULE_TYPES.burn, moduleRow.config_json, {
            burnIntervalSec: Number(tokenRow.burn_sec || 300),
            splitBuys: Number(tokenRow.splits || 1),
          });
          const reserveLamports = toLamports(configJson.reserveSol || config.botSolReserve || 0.005);
          const state = { ...(moduleRow.state_json || {}) };
          state.lastBalanceLamports = Math.max(0, balanceAfterToken - reserveLamports);
          await upsertModuleState(client, moduleRow.module_id, state, null);
          await client.query(
            `
              UPDATE tokens
              SET tx_count = tx_count + 1
              WHERE id = $1
            `,
            [tokenIdText]
          );
          return {
            ok: true,
            signature: null,
            sentSol: 0,
            remainingSol: fromLamports(balanceAfterToken),
            sentTokenBurned: burnedToken,
          };
        }
        throw new Error("No withdrawable SOL or token balance available in the burn deposit wallet.");
      }
      const balanceAfter = await connection.getBalance(depositSigner.publicKey, "confirmed");
      await insertEvent(
        client,
        userId,
        tokenIdText,
        tokenRow.symbol,
        "withdraw",
        `Burn wallet withdrawn ${fromLamports(sweep.sentLamports).toFixed(6)} SOL to ${destination}`,
        sweep.signature,
        {
          moduleType: MODULE_TYPES.burn,
          amount: fromLamports(sweep.sentLamports),
          idempotencyKey: `${eventPrefix}:${destination}`,
        }
      );

      const configJson = mergeModuleConfig(MODULE_TYPES.burn, moduleRow.config_json, {
        burnIntervalSec: Number(tokenRow.burn_sec || 300),
        splitBuys: Number(tokenRow.splits || 1),
      });
      const reserveLamports = toLamports(configJson.reserveSol || config.botSolReserve || 0.005);
      const state = { ...(moduleRow.state_json || {}) };
      state.lastBalanceLamports = Math.max(0, balanceAfter - reserveLamports);
      await upsertModuleState(client, moduleRow.module_id, state, null);

      await client.query(
        `
          UPDATE tokens
          SET tx_count = tx_count + 1
          WHERE id = $1
        `,
        [tokenIdText]
      );

      return {
        ok: true,
        signature: sweep.signature,
        sentSol: fromLamports(sweep.sentLamports),
        remainingSol: fromLamports(balanceAfter),
        sentTokenBurned: burnedToken,
      };
    });

    if (!lock.locked) {
      throw new Error("Token/module is currently locked by another executor.");
    }
    return lock.result;
  });
}

async function insertEvent(
  client,
  userId,
  tokenId,
  symbol,
  type,
  message,
  tx = null,
  options = {}
) {
  const amount = Number(options.amount || 0);
  const moduleType = options.moduleType ? String(options.moduleType) : null;
  const idempotencyKey = options.idempotencyKey ? String(options.idempotencyKey) : null;
  const metadata = options.metadata && typeof options.metadata === "object" ? options.metadata : null;
  await client.query(
    `
      INSERT INTO token_events (user_id, token_id, token_symbol, module_type, event_type, amount, message, tx, idempotency_key, metadata)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      ON CONFLICT DO NOTHING
    `,
    [userId, tokenId, symbol, moduleType, type, amount, message, tx, idempotencyKey, metadata ? JSON.stringify(metadata) : null]
  );
}

async function incrementProtocolMetrics(client, delta = {}) {
  const totalBotTransactions = Math.max(0, Math.floor(Number(delta.totalBotTransactions || 0)));
  const lifetimeIncinerated = Math.max(0, Number(delta.lifetimeIncinerated || 0));
  const emberIncinerated = Math.max(0, Number(delta.emberIncinerated || 0));
  const rewardsProcessedSol = Math.max(0, Number(delta.rewardsProcessedSol || 0));
  const feesTakenSol = Math.max(0, Number(delta.feesTakenSol || 0));

  if (
    totalBotTransactions <= 0
    && lifetimeIncinerated <= 0
    && emberIncinerated <= 0
    && rewardsProcessedSol <= 0
    && feesTakenSol <= 0
  ) {
    return;
  }

  const upsertMetrics = async () =>
    client.query(
      `
        INSERT INTO protocol_metrics (
          id,
          total_bot_transactions,
          lifetime_incinerated,
          ember_incinerated,
          rewards_processed_sol,
          fees_taken_sol,
          updated_at
        )
        VALUES (1, $1, $2, $3, $4, $5, NOW())
        ON CONFLICT (id) DO UPDATE
        SET
          total_bot_transactions = protocol_metrics.total_bot_transactions + EXCLUDED.total_bot_transactions,
          lifetime_incinerated = protocol_metrics.lifetime_incinerated + EXCLUDED.lifetime_incinerated,
          ember_incinerated = protocol_metrics.ember_incinerated + EXCLUDED.ember_incinerated,
          rewards_processed_sol = protocol_metrics.rewards_processed_sol + EXCLUDED.rewards_processed_sol,
          fees_taken_sol = protocol_metrics.fees_taken_sol + EXCLUDED.fees_taken_sol,
          updated_at = NOW()
      `,
      [totalBotTransactions, lifetimeIncinerated, emberIncinerated, rewardsProcessedSol, feesTakenSol]
    );

  try {
    await upsertMetrics();
  } catch (error) {
    const code = String(error?.code || "").trim();
    if (code === "42P01" || String(error?.message || "").toLowerCase().includes("protocol_metrics")) {
      try {
        await client.query(`
          CREATE TABLE IF NOT EXISTS protocol_metrics (
            id SMALLINT PRIMARY KEY,
            total_bot_transactions BIGINT NOT NULL DEFAULT 0,
            lifetime_incinerated NUMERIC NOT NULL DEFAULT 0,
            ember_incinerated NUMERIC NOT NULL DEFAULT 0,
            rewards_processed_sol NUMERIC NOT NULL DEFAULT 0,
            fees_taken_sol NUMERIC NOT NULL DEFAULT 0,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
          );
        `);
        await client.query(`
          INSERT INTO protocol_metrics (
            id, total_bot_transactions, lifetime_incinerated, ember_incinerated, rewards_processed_sol, fees_taken_sol
          )
          VALUES (1, 0, 0, 0, 0, 0)
          ON CONFLICT (id) DO NOTHING;
        `);
        await upsertMetrics();
        return;
      } catch (recoveryError) {
        console.warn(`[metrics] protocol metrics recovery failed: ${recoveryError?.message || recoveryError}`);
        return;
      }
    }
    console.warn(`[metrics] protocol metrics update skipped: ${error?.message || error}`);
  }
}

function lockPairFromKey(key) {
  const digest = crypto.createHash("sha256").update(`${MODULE_LOCK_SEED}:${key}`).digest();
  return [digest.readInt32BE(0), digest.readInt32BE(4)];
}

async function withTokenModuleLock(client, key, fn) {
  const [a, b] = lockPairFromKey(key);
  const gotRes = await client.query("SELECT pg_try_advisory_lock($1, $2) AS ok", [a, b]);
  if (!gotRes.rows[0]?.ok) return { locked: false };
  try {
    const result = await fn();
    return { locked: true, result };
  } finally {
    await client.query("SELECT pg_advisory_unlock($1, $2)", [a, b]).catch(() => {});
  }
}

async function ensureTokenModules(client) {
  const rows = await client.query("SELECT id, user_id, selected_bot, claim_sec, burn_sec, splits FROM tokens");
  for (const row of rows.rows) {
    const moduleType = normalizeModuleType(row.selected_bot, MODULE_TYPES.burn);
    const configJson =
      moduleType === MODULE_TYPES.volume
        ? defaultVolumeModuleConfig()
        : defaultBurnModuleConfig(row);
    await client.query(
      `
        INSERT INTO bot_modules (id, user_id, token_id, module_type, enabled, config_json, state_json, next_run_at)
        VALUES ($1, $2, $3, $4, FALSE, $5::jsonb, '{}'::jsonb, NOW())
        ON CONFLICT (token_id, module_type) DO NOTHING
      `,
      [makeId("mod"), row.user_id, row.id, moduleType, JSON.stringify(configJson)]
    );
  }
}

async function enqueueDueJobs(client) {
  const limit = Math.max(1, Number(config.schedulerBatchLimit || 500));
  const dueRes = await client.query(
    `
      SELECT m.id AS module_id, m.user_id, m.token_id, m.module_type, m.config_json, m.next_run_at
      FROM bot_modules m
      JOIN tokens t ON t.id = m.token_id
      WHERE m.enabled = TRUE
        AND t.active = TRUE
        AND (m.next_run_at IS NULL OR m.next_run_at <= NOW())
      ORDER BY m.next_run_at NULLS FIRST, m.id
      LIMIT $1
      FOR UPDATE SKIP LOCKED
    `,
    [limit]
  );

  let enqueued = 0;
  for (const row of dueRes.rows) {
    const baseTs = row.next_run_at ? new Date(row.next_run_at).getTime() : Date.now();
    const idempotencyKey = `job:${row.module_id}:${Math.floor(baseTs / 1000)}`;
    const ins = await client.query(
      `
        INSERT INTO bot_jobs (
          id, module_id, user_id, token_id, module_type, status, run_after,
          attempts, max_attempts, priority, idempotency_key, payload
        )
        VALUES ($1, $2, $3, $4, $5, 'queued', NOW(), 0, 5, 100, $6, $7::jsonb)
        ON CONFLICT (idempotency_key) DO NOTHING
      `,
      [makeId("job"), row.module_id, row.user_id, row.token_id, row.module_type, idempotencyKey, JSON.stringify({})]
    );
    if (ins.rowCount > 0) enqueued += 1;

    const intervalSec = moduleConfigIntervalSec(row.module_type, row.config_json || {});
    await client.query(
      "UPDATE bot_modules SET next_run_at = NOW() + ($1::text || ' seconds')::interval WHERE id = $2",
      [String(intervalSec), row.module_id]
    );
  }
  return { dueCount: dueRes.rowCount, enqueued };
}

async function leaseQueuedJobs(client) {
  const limit = Math.max(1, Number(config.executorBatchLimit || 32));
  const rowsRes = await client.query(
    `
      SELECT id
      FROM bot_jobs
      WHERE status = 'queued' AND run_after <= NOW()
      ORDER BY priority ASC, created_at ASC
      LIMIT $1
      FOR UPDATE SKIP LOCKED
    `,
    [limit]
  );
  const ids = rowsRes.rows.map((r) => r.id);
  if (!ids.length) return [];
  await client.query(
    `
      UPDATE bot_jobs
      SET status = 'running',
          attempts = attempts + 1,
          lease_until = NOW() + INTERVAL '60 seconds'
      WHERE id = ANY($1::text[])
    `,
    [ids]
  );
  const jobsRes = await client.query(
    `
      SELECT j.*, m.config_json, m.state_json, t.symbol, t.mint, t.deposit, t.splits
      FROM bot_jobs j
      JOIN bot_modules m ON m.id = j.module_id
      JOIN tokens t ON t.id = j.token_id
      WHERE j.id = ANY($1::text[])
    `,
    [ids]
  );
  return jobsRes.rows;
}

async function upsertModuleState(client, moduleId, nextState, lastError = null) {
  await client.query(
    `
      UPDATE bot_modules
      SET state_json = $1::jsonb,
          last_run_at = NOW(),
          last_error = $2
      WHERE id = $3
    `,
    [JSON.stringify(nextState || {}), lastError, moduleId]
  );
}

async function markJobSuccess(client, jobId, resultJson = null) {
  await client.query(
    `
      UPDATE bot_jobs
      SET status = 'completed',
          lease_until = NULL,
          result_json = $2::jsonb,
          error = NULL
      WHERE id = $1
    `,
    [jobId, JSON.stringify(resultJson || {})]
  );
}

async function markJobFailure(client, row, error) {
  const message = String(error?.message || error || "Unknown executor error");
  const attempts = Number(row.attempts || 1);
  const maxAttempts = Number(row.max_attempts || 5);
  const shouldRetry = attempts < maxAttempts;
  if (shouldRetry) {
    const backoffSec = Math.min(120, Math.max(2, attempts * 4));
    await client.query(
      `
        UPDATE bot_jobs
        SET status = 'queued',
            lease_until = NULL,
            error = $2,
            run_after = NOW() + ($3::text || ' seconds')::interval
        WHERE id = $1
      `,
      [row.id, message, String(backoffSec)]
    );
  } else {
    await client.query(
      `
        UPDATE bot_jobs
        SET status = 'failed',
            lease_until = NULL,
            error = $2
        WHERE id = $1
      `,
      [row.id, message]
    );
  }
  await client.query(
    "UPDATE bot_modules SET last_error = $2 WHERE id = $1",
    [row.module_id, message]
  );
}

const TRADE_WALLET_SOL_RESERVE = 0.003;
const TX_FEE_SAFETY_SOL = 0.001;

function getTradeWalletReserveSol() {
  return TRADE_WALLET_SOL_RESERVE;
}

function getTxFeeSafetyLamports() {
  return toLamports(TX_FEE_SAFETY_SOL);
}

async function ensureClaimGasFromTreasury({
  client,
  row,
  connection,
  depositSigner,
  eventPrefix,
  moduleType,
  targetLamportsOverride = 0,
}) {
  if (!config.treasuryWalletPrivateKey) return { toppedUp: 0, signature: null, skipped: "no_treasury_signer" };
  const configuredTarget = toLamports(Math.max(0.0005, Number(config.claimGasTopupSol || 0.003)));
  const overrideTarget = Math.floor(Number(targetLamportsOverride || 0));
  const capLamports = toLamports(0.05);
  const targetLamports = Math.min(capLamports, Math.max(configuredTarget, overrideTarget));
  if (targetLamports <= 0) return { toppedUp: 0, signature: null, skipped: "target_zero" };

  const depositBalance = await connection.getBalance(depositSigner.publicKey, "confirmed");
  if (depositBalance >= targetLamports) return { toppedUp: 0, signature: null, skipped: "already_funded" };

  const neededLamports = targetLamports - depositBalance;
  const treasurySigner = Keypair.fromSecretKey(config.treasuryWalletPrivateKey);
  if (treasurySigner.publicKey.equals(depositSigner.publicKey)) {
    return { toppedUp: 0, signature: null, skipped: "same_wallet" };
  }

  const treasuryBalance = await connection.getBalance(treasurySigner.publicKey, "confirmed");
  const maxSpendable = Math.max(0, treasuryBalance - getTxFeeSafetyLamports());
  if (maxSpendable <= 0) {
    return { toppedUp: 0, signature: null, skipped: "treasury_empty" };
  }

  const topupLamports = Math.min(neededLamports, maxSpendable);
  if (topupLamports <= 0) return { toppedUp: 0, signature: null, skipped: "no_topup_needed" };

  const sig = await sendSolTransfer(
    connection,
    treasurySigner,
    depositSigner.publicKey.toBase58(),
    topupLamports
  );
  if (sig) {
    await insertEvent(
      client,
      row.user_id,
      row.token_id,
      row.symbol,
      "transfer",
      `Treasury claim-gas top-up (${fromLamports(topupLamports).toFixed(6)} SOL)`,
      sig,
      {
        moduleType,
        amount: fromLamports(topupLamports),
        idempotencyKey: `${eventPrefix}:claim:gas`,
      }
    );
  }
  return { toppedUp: topupLamports, signature: sig || null, skipped: null };
}

function pickTradeSol(configJson, walletSol, reserveSol = getTradeWalletReserveSol()) {
  const minSol = Math.max(0.001, Number(configJson.minTradeSol || 0.01));
  const maxSolInput = Math.max(minSol, Number(configJson.maxTradeSol || minSol));
  const aggression = Math.max(0, Math.min(100, Number(configJson.aggression || 35)));
  const curve = (aggression / 100) ** 1.5;
  const maxSol = minSol + (maxSolInput - minSol) * curve;
  const spendable = Math.max(0, walletSol - Math.max(0.0005, Number(reserveSol || 0.003)));
  const upper = Math.max(minSol, Math.min(maxSol, spendable));
  if (upper <= 0) return 0;
  const rand = Math.random();
  return Math.max(minSol, minSol + rand * (upper - minSol));
}

async function ensureTradeWalletGasBuffer({
  client,
  connection,
  depositSigner,
  row,
  walletPubkey,
  reserveLamports,
  depositReserveLamports,
  eventPrefix,
}) {
  const tradeBal = await connection.getBalance(new PublicKey(walletPubkey), "confirmed");
  if (tradeBal >= reserveLamports) return { toppedUp: 0, signature: null };

  const needed = reserveLamports - tradeBal;
  if (needed <= 0) return { toppedUp: 0, signature: null };

  const feeSafetyLamports = getTxFeeSafetyLamports();
  const depositBal = await connection.getBalance(depositSigner.publicKey, "confirmed");
  const safeAvailable = Math.max(0, depositBal - depositReserveLamports - feeSafetyLamports);
  const topup = Math.min(needed, safeAvailable);
  if (topup <= 0) return { toppedUp: 0, signature: null };

  const sig = await sendSolTransfer(connection, depositSigner, walletPubkey, topup);
  if (sig) {
    await insertEvent(
      client,
      row.user_id,
      row.token_id,
      row.symbol,
      "transfer",
      `Gas buffer top-up (${fromLamports(topup).toFixed(6)} SOL) to trade wallet`,
      sig,
      {
        moduleType: MODULE_TYPES.volume,
        amount: fromLamports(topup),
        idempotencyKey: `${eventPrefix}:gas:${walletPubkey}`,
      }
    );
  }
  return { toppedUp: topup, signature: sig };
}

async function ensureVolumeTradeWallets(client, moduleRow, count) {
  const desired = Math.max(1, Math.min(5, Math.floor(Number(count) || 1)));
  const existingRes = await client.query(
    `
      SELECT id, label, wallet_pubkey, secret_key_base58, funded_from_deposit_lamports
      FROM volume_trade_wallets
      WHERE module_id = $1
      ORDER BY created_at ASC
    `,
    [moduleRow.module_id]
  );
  const existing = existingRes.rows || [];
  if (existing.length >= desired) return existing.slice(0, desired);

  const need = desired - existing.length;
  const created = [];
  for (let i = 0; i < need; i += 1) {
    const kp = await generateVanityDeposit(config.depositVanityPrefix || "EMBR");
    const id = makeId("vw");
    const storedSecret = encryptDepositSecret(kp.secretKeyBase58);
    await client.query(
      `
        INSERT INTO volume_trade_wallets (
          id, module_id, user_id, token_id, label, wallet_pubkey, secret_key_base58
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
      `,
      [
        id,
        moduleRow.module_id,
        moduleRow.user_id,
        moduleRow.token_id,
        `trade-${existing.length + i + 1}`,
        kp.pubkey,
        storedSecret,
      ]
    );
    created.push({
      id,
      label: `trade-${existing.length + i + 1}`,
      wallet_pubkey: kp.pubkey,
      secret_key_base58: storedSecret,
      funded_from_deposit_lamports: 0,
    });
  }

  return existing.concat(created);
}

async function runBurnExecutor(client, row) {
  const connection = getConnection();
  const configJson = mergeModuleConfig(MODULE_TYPES.burn, row.config_json, {
    burnIntervalSec: Number(row.burn_sec || 300),
    splitBuys: Number(row.splits || 1),
  });
  const state = { ...(row.state_json || {}) };
  const signerSecret = await getTokenDepositSigningKey(row.user_id, row.token_id);
  const signer = keypairFromBase58(signerSecret);
  const reserveLamports = toLamports(configJson.reserveSol || 0.01);
  const minProcessLamports = toLamports(configJson.minProcessSol || 0.01);
  const now = Date.now();
  const eventPrefix = `${row.id}:${now}`;
  let txCreated = 0;
  let claimGasTopupLamports = 0;
  let claimedLamportsFromClaims = 0;

  if (configJson.claimEnabled && (!state.nextClaimAt || Number(state.nextClaimAt) <= now)) {
    const claimIntervalSec = Math.max(60, Number(configJson.claimIntervalSec || 120));
    let claimFailedSoft = false;
    let claimedLamportsAny = 0;
    try {
      const claimGasTopup = await ensureClaimGasFromTreasury({
        client,
        row,
        connection,
        depositSigner: signer,
        eventPrefix,
        moduleType: MODULE_TYPES.burn,
      });
      claimGasTopupLamports += Math.max(0, Number(claimGasTopup.toppedUp || 0));
      if (claimGasTopup.signature) txCreated += 1;

      try {
        const sharingClaim = await tryDistributeSharingCreatorFees({
          connection,
          signer,
          mint: row.mint,
        });
        if (sharingClaim.signature) txCreated += 1;
        const sharingLamports = Math.max(0, Number(sharingClaim.claimedLamports || 0));
        if (sharingClaim.signature && sharingLamports > 0) {
          claimedLamportsAny += sharingLamports;
          claimedLamportsFromClaims += sharingLamports;
          await insertEvent(
            client,
            row.user_id,
            row.token_id,
            row.symbol,
            "claim",
            `Creator rewards claimed (${fromLamports(sharingLamports).toFixed(6)} SOL)`,
            sharingClaim.signature,
            {
              moduleType: MODULE_TYPES.burn,
              amount: fromLamports(sharingLamports),
              idempotencyKey: `${eventPrefix}:claim:sharing`,
            }
          );
        }
      } catch (sharingError) {
        if (!isSoftSharingClaimError(sharingError)) {
          await insertEvent(
            client,
            row.user_id,
            row.token_id,
            row.symbol,
            "error",
            `Sharing claim path failed: ${sharingError.message}`,
            null,
            { moduleType: MODULE_TYPES.burn, idempotencyKey: `${eventPrefix}:claim:sharing:error` }
          );
        }
      }

      let claimSig = null;
      try {
        claimSig = await pumpPortalCollectCreatorFee({
          connection,
          signer,
          mint: row.mint,
          pool: configJson.pool || "auto",
        });
      } catch (firstError) {
        const lamportDeficit = extractClaimLamportDeficit(firstError);
        if (lamportDeficit > 0) {
          const balanceNow = await connection.getBalance(signer.publicKey, "confirmed");
          const retryTarget = balanceNow + lamportDeficit + getTxFeeSafetyLamports();
          const retryTopup = await ensureClaimGasFromTreasury({
            client,
            row,
            connection,
            depositSigner: signer,
            eventPrefix: `${eventPrefix}:retry`,
            moduleType: MODULE_TYPES.burn,
            targetLamportsOverride: retryTarget,
          });
          claimGasTopupLamports += Math.max(0, Number(retryTopup.toppedUp || 0));
          if (retryTopup.signature) txCreated += 1;
          if (firstError?.claimRequestBody) {
            claimSig = await pumpPortalCollectCreatorFeeRequest({
              connection,
              signer,
              requestBody: firstError.claimRequestBody,
            });
          } else {
            claimSig = await pumpPortalCollectCreatorFee({
              connection,
              signer,
              mint: row.mint,
              pool: configJson.pool || "auto",
            });
          }
        } else {
          throw firstError;
        }
      }
      const claimSummary = await getClaimExecutionSummary(
        connection,
        claimSig,
        signer.publicKey.toBase58()
      );
      const claimNetLamports = Math.max(0, Number(claimSummary.grossClaimLamports || 0));
      const claimNetSol = fromLamports(claimNetLamports);
      txCreated += 1;
      if (claimNetLamports <= 0 || claimSummary.noRewardsHint) {
        if (claimedLamportsAny <= 0) {
          claimFailedSoft = true;
          await insertEvent(
            client,
            row.user_id,
            row.token_id,
            row.symbol,
            "status",
            "Claim skipped: no creator rewards available yet.",
            claimSig,
            {
              moduleType: MODULE_TYPES.burn,
              idempotencyKey: `${eventPrefix}:claim:skip:zero`,
              metadata: {
                noRewardsHint: !!claimSummary.noRewardsHint,
                txUnavailable: !!claimSummary.txUnavailable,
              },
            }
          );
        }
      } else {
        claimedLamportsAny += claimNetLamports;
        claimedLamportsFromClaims += claimNetLamports;
        await insertEvent(
          client,
          row.user_id,
          row.token_id,
          row.symbol,
          "claim",
          `Creator rewards claimed (${claimNetSol.toFixed(6)} SOL)`,
          claimSig,
          {
            moduleType: MODULE_TYPES.burn,
            amount: claimNetSol,
            idempotencyKey: `${eventPrefix}:claim`,
          }
        );
      }
    } catch (error) {
      claimFailedSoft = isSoftClaimError(error);
      if (claimFailedSoft && claimedLamportsAny <= 0) {
        await insertEvent(
          client,
          row.user_id,
          row.token_id,
          row.symbol,
          "status",
          "Claim skipped: no creator rewards available yet.",
          null,
          { moduleType: MODULE_TYPES.burn, idempotencyKey: `${eventPrefix}:claim:skip` }
        );
      } else {
        await insertEvent(
          client,
          row.user_id,
          row.token_id,
          row.symbol,
          "error",
          `Claim attempt failed: ${error.message}`,
          null,
          { moduleType: MODULE_TYPES.burn, idempotencyKey: `${eventPrefix}:claim:error` }
        );
      }
    }
    if (claimedLamportsAny > 0) claimFailedSoft = false;
    const nextClaimSec = claimFailedSoft ? Math.max(300, claimIntervalSec) : claimIntervalSec;
    state.nextClaimAt = now + nextClaimSec * 1000;
  }

  const burnIntervalSec = Math.max(60, Number(configJson.burnIntervalSec || row.burn_sec || 300));
  const burnDue = !state.nextBurnAt || Number(state.nextBurnAt) <= now;
  if (!burnDue) {
    await upsertModuleState(client, row.module_id, state, null);
    return { txCreated, burnedAmount: 0 };
  }
  state.nextBurnAt = now + burnIntervalSec * 1000;

  const balanceBefore = await connection.getBalance(signer.publicKey, "confirmed");
  const availableBefore = Math.max(0, balanceBefore - reserveLamports);
  const last = Math.max(0, Number(state.lastBalanceLamports || 0));
  const rawDelta = Math.max(0, availableBefore - last);
  const externalDepositLamports = Math.max(
    0,
    rawDelta - Math.max(0, claimGasTopupLamports) - Math.max(0, claimedLamportsFromClaims)
  );
  if (externalDepositLamports > toLamports(0.00005)) {
    await insertEvent(
      client,
      row.user_id,
      row.token_id,
      row.symbol,
      "deposit",
      `Deposit received (${fromLamports(externalDepositLamports).toFixed(6)} SOL)`,
      null,
      {
        moduleType: MODULE_TYPES.burn,
        amount: fromLamports(externalDepositLamports),
        idempotencyKey: `${eventPrefix}:deposit`,
      }
    );
  }
  const delta = Math.max(0, rawDelta - Math.max(0, claimGasTopupLamports));
  if (delta <= minProcessLamports) {
    state.lastBalanceLamports = availableBefore;
    await upsertModuleState(client, row.module_id, state, null);
    return { txCreated, burnedAmount: 0 };
  }

  const feeLamports = Math.floor(delta * 0.05);
  const treasuryLamports = Math.floor(feeLamports / 2);
  const devLamports = feeLamports - treasuryLamports;
  const netLamports = Math.max(0, delta - feeLamports);

  if (treasuryLamports > 0) {
    const sig = await sendSolTransfer(connection, signer, config.treasuryWallet, treasuryLamports);
    txCreated += sig ? 1 : 0;
    await insertEvent(
      client,
      row.user_id,
      row.token_id,
      row.symbol,
      "fee",
      `Protocol fee sent to treasury (${fromLamports(treasuryLamports).toFixed(6)} SOL)`,
      sig,
      { moduleType: MODULE_TYPES.burn, amount: fromLamports(treasuryLamports), idempotencyKey: `${eventPrefix}:fee:treasury` }
    );
  }

  if (devLamports > 0 && config.devWalletPublicKey) {
    const sig = await sendSolTransfer(connection, signer, config.devWalletPublicKey, devLamports);
    txCreated += sig ? 1 : 0;
    await insertEvent(
      client,
      row.user_id,
      row.token_id,
      row.symbol,
      "fee",
      `Protocol fee sent to dev burn wallet (${fromLamports(devLamports).toFixed(6)} SOL)`,
      sig,
      { moduleType: MODULE_TYPES.burn, amount: fromLamports(devLamports), idempotencyKey: `${eventPrefix}:fee:dev` }
    );
  }

  if (netLamports <= minProcessLamports) {
    const balanceAfterFee = await connection.getBalance(signer.publicKey, "confirmed");
    state.lastBalanceLamports = Math.max(0, balanceAfterFee - reserveLamports);
    await upsertModuleState(client, row.module_id, state, null);
    return { txCreated, burnedAmount: 0 };
  }

  const beforeToken = await getOwnerTokenBalanceUi(connection, signer.publicKey, row.mint);
  const desiredSplitBuys = Math.max(
    1,
    Math.floor(Number(configJson.splitBuys || row.splits || 1))
  );
  const minSplitLamports = toLamports(0.0001);
  const maxAllowedSplits = Math.max(1, Math.floor(netLamports / Math.max(1, minSplitLamports)));
  const splitBuys = Math.max(1, Math.min(desiredSplitBuys, maxAllowedSplits));
  const baseChunk = Math.floor(netLamports / splitBuys);
  const remainder = netLamports - baseChunk * splitBuys;

  for (let i = 0; i < splitBuys; i += 1) {
    const chunkLamports = baseChunk + (i < remainder ? 1 : 0);
    if (chunkLamports <= 0) continue;
    const buySig = await pumpPortalTrade({
      connection,
      signer,
      mint: row.mint,
      action: "buy",
      amount: Number(fromLamports(chunkLamports).toFixed(9)),
      denominatedInSol: true,
      slippage: Math.max(1, Math.floor(Number(configJson.slippageBps || 1000) / 100)),
      pool: configJson.pool || "auto",
    });
    txCreated += 1;
    const splitLabel =
      splitBuys > 1
        ? `Buyback split ${i + 1}/${splitBuys} (${fromLamports(chunkLamports).toFixed(6)} SOL)`
        : `Buyback executed (${fromLamports(chunkLamports).toFixed(6)} SOL)`;
    await insertEvent(
      client,
      row.user_id,
      row.token_id,
      row.symbol,
      "buyback",
      splitLabel,
      buySig,
      {
        moduleType: MODULE_TYPES.burn,
        amount: fromLamports(chunkLamports),
        idempotencyKey: `${eventPrefix}:buyback:${i + 1}`,
      }
    );
  }

  const afterToken = await getOwnerTokenBalanceUi(connection, signer.publicKey, row.mint);
  const bought = Math.max(0, afterToken - beforeToken);
  let burnSig = null;
  if (bought > 0) {
    burnSig = await sendTokenToIncinerator(connection, signer, row.mint, bought);
    txCreated += burnSig ? 1 : 0;
    await insertEvent(
      client,
      row.user_id,
      row.token_id,
      row.symbol,
      "burn",
      `Incinerated ${fmtInt(bought.toFixed(2))} ${row.symbol}`,
      burnSig,
      { moduleType: MODULE_TYPES.burn, amount: bought, idempotencyKey: `${eventPrefix}:burn` }
    );
  }

  await client.query(
    `
      UPDATE tokens
      SET burned = burned + $1,
          tx_count = tx_count + $2
      WHERE id = $3
    `,
    [Math.max(0, Math.floor(bought)), txCreated, row.token_id]
  );

  const balanceAfter = await connection.getBalance(signer.publicKey, "confirmed");
  state.lastBalanceLamports = Math.max(0, balanceAfter - reserveLamports);
  await upsertModuleState(client, row.module_id, state, null);
  return { txCreated, burnedAmount: bought };
}

async function runVolumeExecutor(client, row) {
  const connection = getConnection();
  const configJson = mergeModuleConfig(MODULE_TYPES.volume, row.config_json, {});
  const state = { ...(row.state_json || {}) };
  const depositSecret = await getTokenDepositSigningKey(row.user_id, row.token_id);
  const depositSigner = keypairFromBase58(depositSecret);
  const reserveLamports = toLamports(configJson.reserveSol || config.botSolReserve || 0.01);
  const tradeReserveLamports = toLamports(getTradeWalletReserveSol());
  const txFeeSafetyLamports = getTxFeeSafetyLamports();
  const now = Date.now();
  let txCreated = 0;
  const eventPrefix = `${row.id}:${now}`;
  let claimGasTopupLamports = 0;
  let claimedLamportsFromClaims = 0;

  const wallets = await ensureVolumeTradeWallets(client, row, configJson.tradeWalletCount || 1);

  if (configJson.claimEnabled && (!state.nextClaimAt || Number(state.nextClaimAt) <= now)) {
    const claimIntervalSec = Math.max(60, Number(configJson.claimIntervalSec || 120));
    let claimFailedSoft = false;
    let claimedLamportsAny = 0;
    try {
      const claimGasTopup = await ensureClaimGasFromTreasury({
        client,
        row,
        connection,
        depositSigner,
        eventPrefix,
        moduleType: MODULE_TYPES.volume,
      });
      claimGasTopupLamports += Math.max(0, Number(claimGasTopup.toppedUp || 0));
      if (claimGasTopup.signature) txCreated += 1;

      try {
        const sharingClaim = await tryDistributeSharingCreatorFees({
          connection,
          signer: depositSigner,
          mint: row.mint,
        });
        if (sharingClaim.signature) txCreated += 1;
        const sharingLamports = Math.max(0, Number(sharingClaim.claimedLamports || 0));
        if (sharingClaim.signature && sharingLamports > 0) {
          claimedLamportsAny += sharingLamports;
          claimedLamportsFromClaims += sharingLamports;
          await insertEvent(
            client,
            row.user_id,
            row.token_id,
            row.symbol,
            "claim",
            `Volume module claimed creator rewards (${fromLamports(sharingLamports).toFixed(6)} SOL)`,
            sharingClaim.signature,
            {
              moduleType: MODULE_TYPES.volume,
              amount: fromLamports(sharingLamports),
              idempotencyKey: `${eventPrefix}:claim:sharing`,
            }
          );
        }
      } catch (sharingError) {
        if (!isSoftSharingClaimError(sharingError)) {
          await insertEvent(
            client,
            row.user_id,
            row.token_id,
            row.symbol,
            "error",
            `Volume sharing-claim path failed: ${sharingError.message}`,
            null,
            { moduleType: MODULE_TYPES.volume, idempotencyKey: `${eventPrefix}:claim:sharing:error` }
          );
        }
      }

      let claimSig = null;
      try {
        claimSig = await pumpPortalCollectCreatorFee({
          connection,
          signer: depositSigner,
          mint: row.mint,
          pool: configJson.pool || "auto",
        });
      } catch (firstError) {
        const lamportDeficit = extractClaimLamportDeficit(firstError);
        if (lamportDeficit > 0) {
          const balanceNow = await connection.getBalance(depositSigner.publicKey, "confirmed");
          const retryTarget = balanceNow + lamportDeficit + getTxFeeSafetyLamports();
          const retryTopup = await ensureClaimGasFromTreasury({
            client,
            row,
            connection,
            depositSigner,
            eventPrefix: `${eventPrefix}:retry`,
            moduleType: MODULE_TYPES.volume,
            targetLamportsOverride: retryTarget,
          });
          claimGasTopupLamports += Math.max(0, Number(retryTopup.toppedUp || 0));
          if (retryTopup.signature) txCreated += 1;
          if (firstError?.claimRequestBody) {
            claimSig = await pumpPortalCollectCreatorFeeRequest({
              connection,
              signer: depositSigner,
              requestBody: firstError.claimRequestBody,
            });
          } else {
            claimSig = await pumpPortalCollectCreatorFee({
              connection,
              signer: depositSigner,
              mint: row.mint,
              pool: configJson.pool || "auto",
            });
          }
        } else {
          throw firstError;
        }
      }
      const claimSummary = await getClaimExecutionSummary(
        connection,
        claimSig,
        depositSigner.publicKey.toBase58()
      );
      const claimNetLamports = Math.max(0, Number(claimSummary.grossClaimLamports || 0));
      const claimNetSol = fromLamports(claimNetLamports);
      txCreated += 1;
      if (claimNetLamports <= 0 || claimSummary.noRewardsHint) {
        if (claimedLamportsAny <= 0) {
          claimFailedSoft = true;
          await insertEvent(
            client,
            row.user_id,
            row.token_id,
            row.symbol,
            "status",
            "Volume claim skipped: no creator rewards available yet.",
            claimSig,
            {
              moduleType: MODULE_TYPES.volume,
              idempotencyKey: `${eventPrefix}:claim:skip:zero`,
              metadata: {
                noRewardsHint: !!claimSummary.noRewardsHint,
                txUnavailable: !!claimSummary.txUnavailable,
              },
            }
          );
        }
      } else {
        claimedLamportsAny += claimNetLamports;
        claimedLamportsFromClaims += claimNetLamports;
        await insertEvent(
          client,
          row.user_id,
          row.token_id,
          row.symbol,
          "claim",
          `Volume module claimed creator rewards (${claimNetSol.toFixed(6)} SOL)`,
          claimSig,
          {
            moduleType: MODULE_TYPES.volume,
            amount: claimNetSol,
            idempotencyKey: `${eventPrefix}:claim`,
          }
        );
      }
    } catch (error) {
      claimFailedSoft = isSoftClaimError(error);
      if (claimFailedSoft && claimedLamportsAny <= 0) {
        await insertEvent(
          client,
          row.user_id,
          row.token_id,
          row.symbol,
          "status",
          "Volume claim skipped: no creator rewards available yet.",
          null,
          { moduleType: MODULE_TYPES.volume, idempotencyKey: `${eventPrefix}:claim:skip` }
        );
      } else {
        await insertEvent(
          client,
          row.user_id,
          row.token_id,
          row.symbol,
          "error",
          `Volume claim failed: ${error.message}`,
          null,
          { moduleType: MODULE_TYPES.volume, idempotencyKey: `${eventPrefix}:claim:error` }
        );
      }
    }
    if (claimedLamportsAny > 0) claimFailedSoft = false;
    const nextClaimSec = claimFailedSoft ? Math.max(300, claimIntervalSec) : claimIntervalSec;
    state.nextClaimAt = now + nextClaimSec * 1000;
  }

  const depositBalance = await connection.getBalance(depositSigner.publicKey, "confirmed");
  const availableDeposit = Math.max(0, depositBalance - reserveLamports);
  const lastDeposit = Math.max(0, Number(state.lastDepositBalanceLamports || 0));
  const rawDelta = Math.max(0, availableDeposit - lastDeposit);
  const ignore = Math.max(0, Number(state.ignoreInflowLamports || 0));
  const externalDepositLamports = Math.max(
    0,
    rawDelta - ignore - Math.max(0, claimGasTopupLamports) - Math.max(0, claimedLamportsFromClaims)
  );
  if (externalDepositLamports > toLamports(0.00005)) {
    await insertEvent(
      client,
      row.user_id,
      row.token_id,
      row.symbol,
      "deposit",
      `Deposit received (${fromLamports(externalDepositLamports).toFixed(6)} SOL)`,
      null,
      {
        moduleType: MODULE_TYPES.volume,
        amount: fromLamports(externalDepositLamports),
        idempotencyKey: `${eventPrefix}:deposit`,
      }
    );
  }
  const effectiveDelta = Math.max(0, rawDelta - ignore);
  state.ignoreInflowLamports = Math.max(0, ignore - rawDelta);

  if (effectiveDelta > toLamports(0.0001)) {
    const feeLamports = Math.floor(effectiveDelta * 0.05);
    const treasuryLamports = Math.floor(feeLamports / 2);
    const devLamports = feeLamports - treasuryLamports;
    if (treasuryLamports > 0) {
      const sig = await sendSolTransfer(connection, depositSigner, config.treasuryWallet, treasuryLamports);
      txCreated += sig ? 1 : 0;
      await insertEvent(
        client,
        row.user_id,
        row.token_id,
        row.symbol,
        "fee",
        `Volume fee to treasury (${fromLamports(treasuryLamports).toFixed(6)} SOL)`,
        sig,
        { moduleType: MODULE_TYPES.volume, amount: fromLamports(treasuryLamports), idempotencyKey: `${eventPrefix}:fee:treasury` }
      );
    }
    if (devLamports > 0 && config.devWalletPublicKey) {
      const sig = await sendSolTransfer(connection, depositSigner, config.devWalletPublicKey, devLamports);
      txCreated += sig ? 1 : 0;
      await insertEvent(
        client,
        row.user_id,
        row.token_id,
        row.symbol,
        "fee",
        `Volume fee to dev burn wallet (${fromLamports(devLamports).toFixed(6)} SOL)`,
        sig,
        { moduleType: MODULE_TYPES.volume, amount: fromLamports(devLamports), idempotencyKey: `${eventPrefix}:fee:dev` }
      );
    }
  }

  if (!state.fannedOut && wallets.length) {
    const freshBal = await connection.getBalance(depositSigner.publicKey, "confirmed");
    const spendable = Math.max(0, freshBal - reserveLamports - txFeeSafetyLamports);
    if (spendable > 0) {
      const each = Math.floor(spendable / wallets.length);
      if (each > 0) {
        for (const wallet of wallets) {
          const sig = await sendSolTransfer(connection, depositSigner, wallet.wallet_pubkey, each);
          txCreated += sig ? 1 : 0;
        }
        state.fannedOut = true;
        state.ignoreInflowLamports = Math.max(0, Number(state.ignoreInflowLamports || 0)) + 0;
      }
    }
  }

  for (const wallet of wallets) {
    try {
      const topup = await ensureTradeWalletGasBuffer({
        client,
        connection,
        depositSigner,
        row,
        walletPubkey: wallet.wallet_pubkey,
        reserveLamports: tradeReserveLamports,
        depositReserveLamports: reserveLamports,
        eventPrefix,
      });
      if (topup.signature) txCreated += 1;
    } catch (error) {
      await insertEvent(
        client,
        row.user_id,
        row.token_id,
        row.symbol,
        "error",
        `Trade wallet gas top-up failed (${wallet.label}): ${error.message}`,
        null,
        {
          moduleType: MODULE_TYPES.volume,
          idempotencyKey: `${eventPrefix}:gas:error:${wallet.wallet_pubkey}`,
        }
      );
    }
  }

  const wallet = wallets[Math.floor(Math.random() * wallets.length)];
  if (wallet) {
    const tradeSigner = keypairFromBase58(decryptDepositSecret(wallet.secret_key_base58));
    const walletSolLamports = await connection.getBalance(tradeSigner.publicKey, "confirmed");
    const walletSol = fromLamports(walletSolLamports);
    const tokenBal = await getOwnerTokenBalanceUi(connection, tradeSigner.publicKey, row.mint);
    let action = Math.random() < 0.5 ? "buy" : "sell";
    if (tokenBal <= 0.000001) action = "buy";
    if (walletSol < Number(configJson.minTradeSol || 0.01) + fromLamports(tradeReserveLamports)) action = "sell";

    if (action === "buy") {
      const tradeSol = pickTradeSol(configJson, walletSol, fromLamports(tradeReserveLamports));
      if (tradeSol > 0.0005) {
        const sig = await pumpPortalTrade({
          connection,
          signer: tradeSigner,
          mint: row.mint,
          action: "buy",
          amount: Number(tradeSol.toFixed(6)),
          denominatedInSol: true,
          slippage: Math.max(1, Math.floor(Number(configJson.slippageBps || 1000) / 100)),
          pool: configJson.pool || "auto",
        });
        txCreated += 1;
        await insertEvent(
          client,
          row.user_id,
          row.token_id,
          row.symbol,
          "buyback",
          `Volume buy (${tradeSol.toFixed(4)} SOL) via ${wallet.label}`,
          sig,
          { moduleType: MODULE_TYPES.volume, amount: tradeSol, idempotencyKey: `${eventPrefix}:buy` }
        );
      }
    } else if (tokenBal > 0.000001 && walletSolLamports > txFeeSafetyLamports) {
      const sellAmount = Number((tokenBal * (0.2 + Math.random() * 0.5)).toFixed(6));
      if (sellAmount > 0) {
        const sig = await pumpPortalTrade({
          connection,
          signer: tradeSigner,
          mint: row.mint,
          action: "sell",
          amount: sellAmount,
          denominatedInSol: false,
          slippage: Math.max(1, Math.floor(Number(configJson.slippageBps || 1000) / 100)),
          pool: configJson.pool || "auto",
        });
        txCreated += 1;
        await insertEvent(
          client,
          row.user_id,
          row.token_id,
          row.symbol,
          "sell",
          `Volume sell (${sellAmount.toFixed(4)} ${row.symbol}) via ${wallet.label}`,
          sig,
          { moduleType: MODULE_TYPES.volume, amount: sellAmount, idempotencyKey: `${eventPrefix}:sell` }
        );
      }
    }
  }

  const depositAfter = await connection.getBalance(depositSigner.publicKey, "confirmed");
  state.lastDepositBalanceLamports = Math.max(0, depositAfter - reserveLamports);
  await upsertModuleState(client, row.module_id, state, null);

  await client.query(
    `
      UPDATE tokens
      SET tx_count = tx_count + $1
      WHERE id = $2
    `,
    [txCreated, row.token_id]
  );

  return { txCreated };
}

async function runPersonalBurnExecutor(client) {
  if (!config.devWalletPrivateKey || !config.devWalletPublicKey) {
    return { ran: false, txCreated: 0, reason: "missing-dev-wallet" };
  }
  if (!config.emberTokenMint) {
    return { ran: false, txCreated: 0, reason: "missing-ember-mint" };
  }
  const claimMints = Array.isArray(config.personalCreatorMints) && config.personalCreatorMints.length
    ? config.personalCreatorMints
    : [config.emberTokenMint];
  if (!claimMints.length) return { ran: false, txCreated: 0, reason: "no-claim-mints" };

  const connection = getConnection();
  const signer = Keypair.fromSecretKey(config.devWalletPrivateKey);
  const reserveLamports = toLamports(Math.max(0.001, Number(config.devWalletSolReserve || 0.01)));
  const beforeClaimBalance = await connection.getBalance(signer.publicKey, "confirmed");
  let txCreated = 0;
  let claimSuccess = 0;
  let claimFailures = 0;
  let burnedAmount = 0;

  for (const mint of claimMints) {
    try {
      await pumpPortalCollectCreatorFee({
        connection,
        signer,
        mint,
        pool: "auto",
      });
      txCreated += 1;
      claimSuccess += 1;
    } catch (error) {
      claimFailures += 1;
      console.warn(`[personal-burn] claim failed for ${mint}: ${error?.message || error}`);
    }
  }

  const afterClaim = await connection.getBalance(signer.publicKey, "confirmed");
  const claimedLamports = Math.max(0, afterClaim - beforeClaimBalance);
  const spendable = Math.max(0, afterClaim - reserveLamports);
  const burnableBefore = await getOwnerTokenBalanceUi(connection, signer.publicKey, config.emberTokenMint);
  const hasGasForBurn = afterClaim > getTxFeeSafetyLamports();
  console.log(
    `[personal-burn] mint=${config.emberTokenMint} balance=${burnableBefore.toFixed(6)} spendable=${fromLamports(
      spendable
    ).toFixed(6)}`
  );
  if (spendable <= toLamports(0.0005)) {
    if (burnableBefore > 0 && hasGasForBurn) {
      try {
        await sendTokenToIncinerator(connection, signer, config.emberTokenMint, burnableBefore);
        txCreated += 1;
        burnedAmount += burnableBefore;
        console.log(`[personal-burn] burned ${burnableBefore.toFixed(6)} EMBER (carry balance)`);
      } catch (error) {
        console.warn(`[personal-burn] carry-balance burn failed: ${error?.message || error}`);
      }
    }
    await incrementProtocolMetrics(client, {
      totalBotTransactions: txCreated,
      lifetimeIncinerated: burnedAmount,
      emberIncinerated: burnedAmount,
      rewardsProcessedSol: fromLamports(claimedLamports),
      feesTakenSol: 0,
    });
    console.log(
      `[personal-burn] no spendable SOL after reserve (${fromLamports(afterClaim).toFixed(6)} total, reserve ${fromLamports(
        reserveLamports
      ).toFixed(6)})`
    );
    return { ran: true, txCreated, claimSuccess, claimFailures, spendableLamports: spendable };
  }

  const treasuryShare = Math.floor(spendable * 0.5);
  const burnShare = spendable - treasuryShare;
  if (treasuryShare > 0) {
    await sendSolTransfer(connection, signer, config.treasuryWallet, treasuryShare);
    txCreated += 1;
  }
  if (burnShare > 0) {
    await pumpPortalTrade({
      connection,
      signer,
      mint: config.emberTokenMint,
      action: "buy",
      amount: Number(fromLamports(burnShare).toFixed(6)),
      denominatedInSol: true,
      slippage: 10,
      pool: "auto",
    });
    txCreated += 1;
  }

  const burnable = await getOwnerTokenBalanceUi(connection, signer.publicKey, config.emberTokenMint);
  if (burnable > 0 && hasGasForBurn) {
    try {
      await sendTokenToIncinerator(connection, signer, config.emberTokenMint, burnable);
      txCreated += 1;
      burnedAmount += burnable;
      console.log(`[personal-burn] burned ${burnable.toFixed(6)} EMBER`);
    } catch (error) {
      console.warn(`[personal-burn] burn failed: ${error?.message || error}`);
    }
  }
  await incrementProtocolMetrics(client, {
    totalBotTransactions: txCreated,
    lifetimeIncinerated: burnedAmount,
    emberIncinerated: burnedAmount,
    rewardsProcessedSol: fromLamports(claimedLamports),
    feesTakenSol: 0,
  });
  console.log(
    `[personal-burn] executed tx=${txCreated} claims_ok=${claimSuccess} claims_fail=${claimFailures} spendable=${fromLamports(
      spendable
    ).toFixed(6)}`
  );
  return { ran: true, txCreated, claimSuccess, claimFailures, spendableLamports: spendable };
}

async function executeLeasedJob(client, row) {
  const lockKey = `${row.token_id}:${row.module_type}`;
  const lock = await withTokenModuleLock(client, lockKey, async () => {
    if (row.module_type === MODULE_TYPES.volume) {
      return runVolumeExecutor(client, row);
    }
    if (row.module_type === MODULE_TYPES.burn) {
      return runBurnExecutor(client, row);
    }
    return { txCreated: 0 };
  });
  if (!lock.locked) {
    throw new Error("Token/module is currently locked by another executor.");
  }
  return lock.result || { txCreated: 0 };
}

let lastPersonalBurnRunAt = 0;
const personalBurnIntervalMs = Math.max(
  30_000,
  Math.floor(Number(process.env.PERSONAL_BURN_INTERVAL_SEC || 120) * 1000)
);

export async function runWorkerTick() {
  const scheduleSummary = await withTx(async (client) => {
    await ensureTokenModules(client);
    return enqueueDueJobs(client);
  });

  const leasedJobs = await withTx(async (client) => leaseQueuedJobs(client));
  let eventsCreated = 0;
  let executedJobs = 0;

  for (const row of leasedJobs) {
    const client = await pool.connect();
    try {
      const result = await executeLeasedJob(client, row);
      executedJobs += 1;
      eventsCreated += Number(result?.txCreated || 0);
      await markJobSuccess(client, row.id, result || {});
    } catch (error) {
      await markJobFailure(client, row, error);
      await insertEvent(
        client,
        row.user_id,
        row.token_id,
        row.symbol,
        "error",
        `Job ${row.module_type} failed: ${error.message}`,
        null,
        { moduleType: row.module_type, idempotencyKey: `joberr:${row.id}:${row.attempts}` }
      );
    } finally {
      client.release();
    }
  }

  const now = Date.now();
  if (now - lastPersonalBurnRunAt >= personalBurnIntervalMs) {
    const client = await pool.connect();
    try {
      const result = await runPersonalBurnExecutor(client);
      eventsCreated += Number(result?.txCreated || 0);
    } catch (error) {
      console.warn(`[personal-burn] executor failed: ${error?.message || error}`);
    } finally {
      client.release();
    }
    lastPersonalBurnRunAt = now;
  }

  return {
    dueTokens: scheduleSummary.dueCount,
    eventsCreated,
    enqueuedJobs: scheduleSummary.enqueued,
    executedJobs,
  };
}


