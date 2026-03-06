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
  createBurnCheckedInstruction,
} from "@solana/spl-token";
import { config } from "./config.js";
import { makeId, makeSessionToken, pool, withTx } from "./db.js";
import { fmtInt, resolveMint } from "./utils.js";

const noisyTokenAccountLogWindowMs = Math.max(
  60_000,
  Number(process.env.NOISY_TOKEN_LOG_WINDOW_MS || 300_000)
);
const noisyTokenAccountLogMaxPerWindow = Math.max(
  1,
  Number(process.env.NOISY_TOKEN_LOG_MAX_PER_WINDOW || 1)
);

if (!globalThis.__emberNoisyTokenWarnFilterInstalled) {
  globalThis.__emberNoisyTokenWarnFilterInstalled = true;
  const originalWarn = console.warn.bind(console);
  const state = {
    windowStart: 0,
    shown: 0,
    suppressed: 0,
  };

  console.warn = (...args) => {
    const text = args
      .map((arg) => {
        if (typeof arg === "string") return arg;
        if (arg instanceof Error) return `${arg.name}: ${arg.message}`;
        return "";
      })
      .join(" ");

    const isNoisyTokenWarning =
      /error fetching token account/i.test(text) ||
      /tokenaccountnotfounderror/i.test(text);

    if (!isNoisyTokenWarning) {
      originalWarn(...args);
      return;
    }

    const now = Date.now();
    if (now - state.windowStart >= noisyTokenAccountLogWindowMs) {
      if (state.suppressed > 0) {
        originalWarn(
          `[log-filter] suppressed ${state.suppressed} token-account warnings in last ${Math.round(
            noisyTokenAccountLogWindowMs / 1000
          )}s`
        );
      }
      state.windowStart = now;
      state.shown = 0;
      state.suppressed = 0;
    }

    if (state.shown < noisyTokenAccountLogMaxPerWindow) {
      state.shown += 1;
      originalWarn(...args);
      return;
    }

    state.suppressed += 1;
  };
}

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
    deployedViaEmber: Boolean(row.deployed_via_ember),
    deployWalletPubkey: String(row.deploy_wallet_pubkey || ""),
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
  marketMaker: "market_maker",
  dca: "dca",
  rekindle: "rekindle",
  personalBurn: "personal_burn",
};

function moduleTypeLabel(moduleType) {
  if (moduleType === MODULE_TYPES.volume) return "Volume Bot";
  if (moduleType === MODULE_TYPES.marketMaker) return "Market Maker Bot";
  if (moduleType === MODULE_TYPES.dca) return "DCA Bot";
  if (moduleType === MODULE_TYPES.rekindle) return "Rekindle Bot";
  if (moduleType === MODULE_TYPES.personalBurn) return "Personal Burn Bot";
  return "Burn Bot";
}

function isTradeBotModuleType(moduleType) {
  return [
    MODULE_TYPES.volume,
    MODULE_TYPES.marketMaker,
    MODULE_TYPES.dca,
    MODULE_TYPES.rekindle,
  ].includes(moduleType);
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
  if (raw === MODULE_TYPES.marketMaker) return MODULE_TYPES.marketMaker;
  if (raw === MODULE_TYPES.dca) return MODULE_TYPES.dca;
  if (raw === MODULE_TYPES.rekindle) return MODULE_TYPES.rekindle;
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

function getDeployVanityBufferLamports() {
  return toLamports(Math.max(0.005, Number(config.deployVanityBufferSol || 0.03)));
}

function estimateDeployVanityRequiredLamports(initialBuySol) {
  return Math.max(
    0,
    toLamports(Math.max(0, Number(initialBuySol || 0))) + getDeployVanityBufferLamports()
  );
}

function deployVanityReservationExpiryDate() {
  const minutes = Math.max(5, Number(config.deployVanityReservationMinutes || 30));
  return new Date(Date.now() + minutes * 60 * 1000);
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

  // Fallback: if metadata providers fail but mint exists on-chain, allow attach with deterministic defaults.
  try {
    const connection = getConnection();
    const mintPk = new PublicKey(mint);
    const accountInfo = await connection.getAccountInfo(mintPk, "confirmed");
    if (accountInfo) {
      const fallbackSymbol = mint.slice(0, 6).toUpperCase();
      return {
        mint,
        symbol: fallbackSymbol,
        name: `Token (${mint.slice(0, 8)}...)`,
        pictureUrl: "",
        marketCap,
      };
    }
  } catch {
    // best effort fallback only
  }

  throw new Error("Unable to resolve token metadata for this mint.");
}

function moduleConfigIntervalSec(moduleType, configJson = {}) {
  if (moduleType === MODULE_TYPES.volume) {
    const speed = Math.max(0, Math.min(100, Number(configJson.speed ?? 35)));
    const sec = 25 - Math.round(speed * 0.2);
    return Math.max(3, sec);
  }
  if (moduleType === MODULE_TYPES.marketMaker) {
    return Math.max(4, Math.floor(Number(configJson.cycleIntervalSec || 12)));
  }
  if (moduleType === MODULE_TYPES.dca) {
    return Math.max(20, Math.floor(Number(configJson.cycleIntervalSec || 135)));
  }
  if (moduleType === MODULE_TYPES.rekindle) {
    return Math.max(20, Math.floor(Number(configJson.cycleIntervalSec || 75)));
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

const MARKET_STATE_REFRESH_MS = 8_000;
const marketStateCache = new Map();

async function fetchMarketStateForMint(mint) {
  const mintText = String(mint || "").trim();
  if (!mintText) {
    return {
      mint: "",
      marketCapUsd: 0,
      liquidityUsd: 0,
      priceUsd: 0,
      priceSol: 0,
      pairAddress: "",
      dexId: "",
      pairCreatedAt: 0,
      buysM5: 0,
      sellsM5: 0,
      buysH1: 0,
      sellsH1: 0,
      poolHint: "auto",
    };
  }

  const now = Date.now();
  const cached = marketStateCache.get(mintText);
  if (cached && now - Number(cached.at || 0) < MARKET_STATE_REFRESH_MS) {
    return cached.value;
  }

  const result = {
    mint: mintText,
    marketCapUsd: 0,
    liquidityUsd: 0,
    priceUsd: 0,
    priceSol: 0,
    pairAddress: "",
    dexId: "",
    pairCreatedAt: 0,
    buysM5: 0,
    sellsM5: 0,
    buysH1: 0,
    sellsH1: 0,
    poolHint: "auto",
  };

  const dexData = await fetchJsonWithTimeout(
    `https://api.dexscreener.com/latest/dex/tokens/${mintText}`,
    {},
    3500
  );
  const pair = pickDexPairForMint(dexData?.pairs || [], mintText);
  if (pair) {
    result.marketCapUsd = parseNumberish(pair?.marketCap) || parseNumberish(pair?.fdv) || 0;
    result.liquidityUsd = parseNumberish(pair?.liquidity?.usd);
    result.priceUsd = parseNumberish(pair?.priceUsd);
    result.priceSol = parseNumberish(pair?.priceNative);
    result.pairAddress = String(pair?.pairAddress || "");
    result.dexId = String(pair?.dexId || "").toLowerCase();
    result.pairCreatedAt = Number(pair?.pairCreatedAt || 0);
    result.buysM5 = Math.max(0, Math.floor(parseNumberish(pair?.txns?.m5?.buys)));
    result.sellsM5 = Math.max(0, Math.floor(parseNumberish(pair?.txns?.m5?.sells)));
    result.buysH1 = Math.max(0, Math.floor(parseNumberish(pair?.txns?.h1?.buys)));
    result.sellsH1 = Math.max(0, Math.floor(parseNumberish(pair?.txns?.h1?.sells)));

    const dexId = result.dexId;
    if (dexId.includes("raydium")) {
      result.poolHint = "raydium";
    } else if (dexId.includes("pump")) {
      result.poolHint = "pump";
    }
  }

  if (result.marketCapUsd <= 0) {
    result.marketCapUsd = await fetchMarketCapUsd(mintText);
  }

  marketStateCache.set(mintText, { at: now, value: result });
  return result;
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
      const now = Date.now();
      if (value > 0) {
        entry.value = value;
        entry.at = now;
        entry.nextRefreshAt = now + MARKET_CAP_REFRESH_MS;
      } else {
        // Keep last known good market cap on transient zero/miss responses.
        entry.nextRefreshAt = now + MARKET_CAP_RETRY_MS;
      }
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

function defaultMarketMakerModuleConfig() {
  return {
    claimEnabled: true,
    claimIntervalSec: 120,
    tradeWalletCount: 2,
    aggression: 45,
    minTradeSol: Math.max(0.001, Number(config.volumeDefaultMinTradeSol || 0.01)),
    maxTradeSol: Math.max(0.005, Number(config.volumeDefaultMaxTradeSol || 0.05)),
    reserveSol: Math.max(0.005, Number(config.botSolReserve || 0.01)),
    slippageBps: 1200,
    pool: "auto",
    targetInventoryPct: 50,
    inventoryBandPct: 10,
    childTrades: 2,
    cycleIntervalSec: 12,
    cooldownSec: 16,
    buyPressureBiasPct: 8,
    sellPressureBiasPct: 8,
  };
}

function defaultDcaModuleConfig() {
  return {
    claimEnabled: true,
    claimIntervalSec: 120,
    tradeWalletCount: 1,
    aggression: 35,
    minTradeSol: Math.max(0.001, Number(config.volumeDefaultMinTradeSol || 0.01)),
    maxTradeSol: Math.max(0.005, Number(config.volumeDefaultMaxTradeSol || 0.05)),
    reserveSol: Math.max(0.005, Number(config.botSolReserve || 0.01)),
    slippageBps: 1000,
    pool: "auto",
    cycleIntervalSec: 135,
  };
}

function deriveDcaConfig(configJson = {}) {
  const merged = {
    ...defaultDcaModuleConfig(),
    ...(configJson || {}),
  };
  const aggression = Math.max(0, Math.min(100, Number(merged.aggression || 35)));
  const minTradeSol = Math.max(0.001, Number(merged.minTradeSol || 0.01));
  const cap = Math.max(0.08, minTradeSol * 6);
  const autoMaxTradeSol = Number((minTradeSol + (cap - minTradeSol) * (aggression / 100)).toFixed(3));
  return {
    ...merged,
    aggression,
    minTradeSol,
    maxTradeSol: Math.max(minTradeSol, Number(merged.maxTradeSol || autoMaxTradeSol)),
    tradeWalletCount: Math.max(1, Math.min(5, Math.floor(Number(merged.tradeWalletCount || 1)))),
    claimIntervalSec: Math.max(60, Math.floor(Number(merged.claimIntervalSec || 120))),
    cycleIntervalSec: Math.max(20, 180 - Math.round(aggression * 1.2)),
    slippageBps: Math.max(100, Math.min(5000, Math.floor(Number(merged.slippageBps || 1000)))),
    reserveSol: Math.max(0.001, Math.min(10, Number(merged.reserveSol || config.botSolReserve || 0.01))),
  };
}

function defaultRekindleModuleConfig() {
  return {
    claimEnabled: true,
    claimIntervalSec: 120,
    tradeWalletCount: 1,
    aggression: 42,
    minTradeSol: Math.max(0.001, Number(config.volumeDefaultMinTradeSol || 0.01)),
    maxTradeSol: Math.max(0.005, Number(config.volumeDefaultMaxTradeSol || 0.05)),
    reserveSol: Math.max(0.005, Number(config.botSolReserve || 0.01)),
    slippageBps: 1200,
    pool: "auto",
    cycleIntervalSec: 75,
    dipTriggerPct: 9,
    cooldownSec: 135,
  };
}

function deriveRekindleConfig(configJson = {}) {
  const merged = {
    ...defaultRekindleModuleConfig(),
    ...(configJson || {}),
  };
  const aggression = Math.max(0, Math.min(100, Number(merged.aggression || 42)));
  const minTradeSol = Math.max(0.001, Number(merged.minTradeSol || 0.01));
  const cap = Math.max(0.12, minTradeSol * 8);
  const autoMaxTradeSol = Number((minTradeSol + (cap - minTradeSol) * (aggression / 100)).toFixed(3));
  return {
    ...merged,
    aggression,
    minTradeSol,
    maxTradeSol: Math.max(minTradeSol, Number(merged.maxTradeSol || autoMaxTradeSol)),
    tradeWalletCount: Math.max(1, Math.min(5, Math.floor(Number(merged.tradeWalletCount || 1)))),
    claimIntervalSec: Math.max(60, Math.floor(Number(merged.claimIntervalSec || 120))),
    cycleIntervalSec: Math.max(20, 110 - Math.round(aggression * 0.5)),
    dipTriggerPct: Math.max(3, Math.min(18, Number(merged.dipTriggerPct || (14 - aggression * 0.09)))),
    cooldownSec: Math.max(30, 180 - Math.round(aggression * 1.1)),
    slippageBps: Math.max(100, Math.min(5000, Math.floor(Number(merged.slippageBps || 1200)))),
    reserveSol: Math.max(0.001, Math.min(10, Number(merged.reserveSol || config.botSolReserve || 0.01))),
  };
}

function deriveMarketMakerConfig(configJson = {}) {
  const merged = {
    ...defaultMarketMakerModuleConfig(),
    ...(configJson || {}),
  };
  const aggression = Math.max(0, Math.min(100, Number(merged.aggression || 45)));
  const minTradeSol = Math.max(0.001, Number(merged.minTradeSol || 0.01));
  const autoMaxTradeSol = Number((minTradeSol + (Math.max(0.2, minTradeSol * 10) - minTradeSol) * (aggression / 100)).toFixed(3));
  const configuredMax = Math.max(minTradeSol, Number(merged.maxTradeSol || autoMaxTradeSol));
  const cycleIntervalSec = Math.max(4, 22 - Math.round(aggression * 0.16));
  const childTrades = Math.max(1, Math.min(4, 1 + Math.floor(aggression / 28)));
  const inventoryBandPct = Math.max(6, 18 - Math.round(aggression * 0.1));
  const cooldownSec = Math.max(8, 26 - Math.round(aggression * 0.14));

  return {
    ...merged,
    aggression,
    minTradeSol,
    maxTradeSol: configuredMax,
    targetInventoryPct: Math.max(20, Math.min(80, Number(merged.targetInventoryPct || 50))),
    inventoryBandPct,
    childTrades,
    cycleIntervalSec,
    cooldownSec,
    tradeWalletCount: Math.max(1, Math.min(5, Math.floor(Number(merged.tradeWalletCount || 2)))),
    buyPressureBiasPct: Math.max(0, Math.min(25, Number(merged.buyPressureBiasPct || 8))),
    sellPressureBiasPct: Math.max(0, Math.min(25, Number(merged.sellPressureBiasPct || 8))),
    slippageBps: Math.max(100, Math.min(5000, Math.floor(Number(merged.slippageBps || 1200)))),
    claimIntervalSec: Math.max(60, Math.floor(Number(merged.claimIntervalSec || 120))),
    reserveSol: Math.max(0.001, Math.min(10, Number(merged.reserveSol || config.botSolReserve || 0.01))),
  };
}

function mergeModuleConfig(moduleType, currentConfig, nextConfig = {}) {
  const base =
    moduleType === MODULE_TYPES.volume
      ? defaultVolumeModuleConfig()
      : moduleType === MODULE_TYPES.marketMaker
        ? defaultMarketMakerModuleConfig()
      : moduleType === MODULE_TYPES.dca
        ? defaultDcaModuleConfig()
      : moduleType === MODULE_TYPES.rekindle
        ? defaultRekindleModuleConfig()
      : moduleType === MODULE_TYPES.personalBurn
        ? defaultBurnModuleConfig({})
        : defaultBurnModuleConfig({});
  const merged = {
    ...base,
    ...(currentConfig || {}),
    ...(nextConfig || {}),
  };
  if (moduleType === MODULE_TYPES.marketMaker) {
    return deriveMarketMakerConfig(merged);
  }
  if (moduleType === MODULE_TYPES.dca) {
    return deriveDcaConfig(merged);
  }
  if (moduleType === MODULE_TYPES.rekindle) {
    return deriveRekindleConfig(merged);
  }
  return merged;
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

async function getOwnerTokenBalanceSnapshot(connection, owner, mint) {
  const ownerPk = owner instanceof PublicKey ? owner : new PublicKey(owner);
  const mintPk = mint instanceof PublicKey ? mint : new PublicKey(mint);
  const balances = await getOwnerTokenAccountBalancesForMint(connection, ownerPk, mintPk, null);
  const totalUi = balances.reduce((sum, item) => sum + Number(item.uiAmount || 0), 0);
  const totalRaw = balances.reduce((sum, item) => sum + BigInt(item.amountRaw || 0), 0n);
  return {
    totalUi,
    totalRaw,
    accountCount: balances.length,
  };
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

const TELEGRAM_ALERT_DELIVERY = {
  smart: "smart",
  instant: "instant",
  digest: "digest",
};

let telegramBotProfileCache = {
  username: "",
  fetchedAt: 0,
};
let telegramUpdateOffset = 0;
let telegramUpdatesInFlight = false;
let telegramAlertDeliveryInFlight = false;

function normalizeTelegramDeliveryMode(value, fallback = TELEGRAM_ALERT_DELIVERY.smart) {
  const raw = String(value || "").trim().toLowerCase();
  if (raw === TELEGRAM_ALERT_DELIVERY.instant) return TELEGRAM_ALERT_DELIVERY.instant;
  if (raw === TELEGRAM_ALERT_DELIVERY.digest) return TELEGRAM_ALERT_DELIVERY.digest;
  return fallback;
}

function defaultTelegramAlertPrefs() {
  return {
    enabled: false,
    deliveryMode: TELEGRAM_ALERT_DELIVERY.smart,
    digestIntervalMin: 15,
    alertDeposit: true,
    alertClaim: true,
    alertBurn: true,
    alertTrade: false,
    alertError: true,
    alertStatus: true,
  };
}

function sanitizeTelegramAlertPrefs(input = {}, current = {}) {
  const base = {
    ...defaultTelegramAlertPrefs(),
    ...(current || {}),
    ...(input || {}),
  };
  return {
    enabled: Boolean(base.enabled),
    deliveryMode: normalizeTelegramDeliveryMode(base.deliveryMode, TELEGRAM_ALERT_DELIVERY.smart),
    digestIntervalMin: Math.max(5, Math.min(120, Math.floor(Number(base.digestIntervalMin || 15)))),
    alertDeposit: Boolean(base.alertDeposit),
    alertClaim: Boolean(base.alertClaim),
    alertBurn: Boolean(base.alertBurn),
    alertTrade: Boolean(base.alertTrade),
    alertError: Boolean(base.alertError),
    alertStatus: Boolean(base.alertStatus),
  };
}

function maskTelegramChatId(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  if (raw.length <= 4) return raw;
  return `...${raw.slice(-4)}`;
}

async function fetchTelegramBotProfile() {
  if (!config.telegramBotToken) return { username: "" };
  const now = Date.now();
  if (telegramBotProfileCache.username && now - telegramBotProfileCache.fetchedAt < 60 * 60 * 1000) {
    return { username: telegramBotProfileCache.username };
  }
  try {
    const res = await fetch(`https://api.telegram.org/bot${config.telegramBotToken}/getMe`);
    const data = await res.json().catch(() => ({}));
    const username = String(data?.result?.username || "").trim();
    if (username) {
      telegramBotProfileCache = { username, fetchedAt: now };
    }
    return { username };
  } catch {
    return { username: telegramBotProfileCache.username || "" };
  }
}

function buildTelegramConnectUrl(botUsername, token) {
  const username = String(botUsername || "").trim().replace(/^@+/, "");
  const connectToken = String(token || "").trim();
  if (!username || !connectToken) return "";
  return `https://t.me/${encodeURIComponent(username)}?start=${encodeURIComponent(connectToken)}`;
}

async function getOrCreateTelegramConnectToken(client, userId) {
  const existing = await client.query(
    `
      SELECT token, expires_at
      FROM user_telegram_connect_tokens
      WHERE user_id = $1
        AND consumed_at IS NULL
        AND expires_at > NOW()
      ORDER BY created_at DESC
      LIMIT 1
    `,
    [userId]
  );
  if (existing.rowCount) {
    return {
      token: String(existing.rows[0].token || ""),
      expiresAt: existing.rows[0].expires_at,
    };
  }

  const token = crypto.randomBytes(18).toString("hex");
  const expiresAt = new Date(Date.now() + 24 * 60 * 60 * 1000);
  await client.query(
    `
      INSERT INTO user_telegram_connect_tokens (token, user_id, expires_at)
      VALUES ($1, $2, $3)
    `,
    [token, userId, expiresAt]
  );
  return { token, expiresAt };
}

function isUserAlertWorthyStatus(message) {
  const text = String(message || "").toLowerCase();
  if (!text) return false;
  if (text.includes("claim skipped")) return false;
  if (text.includes("waiting")) return false;
  return (
    text.includes("started") ||
    text.includes("paused") ||
    text.includes("disconnected") ||
    text.includes("reconnected") ||
    text.includes("withdrawn") ||
    text.includes("wallets updated") ||
    text.includes("bot mode changed")
  );
}

function classifyTelegramAlert(eventType, message, prefs) {
  const type = String(eventType || "").trim().toLowerCase();
  const normalizedPrefs = sanitizeTelegramAlertPrefs(prefs);
  if (!normalizedPrefs.enabled) return null;

  let category = "";
  if (type === "deposit") category = "deposit";
  else if (type === "claim") category = "claim";
  else if (type === "burn") category = "burn";
  else if (type === "error") category = "error";
  else if (["buy", "sell", "buyback", "transfer", "fee"].includes(type)) category = "trade";
  else if (type === "status" && isUserAlertWorthyStatus(message)) category = "status";
  else if (["withdraw", "delete"].includes(type)) category = "status";
  else return null;

  const enabledByCategory =
    (category === "deposit" && normalizedPrefs.alertDeposit) ||
    (category === "claim" && normalizedPrefs.alertClaim) ||
    (category === "burn" && normalizedPrefs.alertBurn) ||
    (category === "trade" && normalizedPrefs.alertTrade) ||
    (category === "error" && normalizedPrefs.alertError) ||
    (category === "status" && normalizedPrefs.alertStatus);
  if (!enabledByCategory) return null;

  let deliveryKind = "immediate";
  if (category === "error") {
    deliveryKind = "immediate";
  } else if (normalizedPrefs.deliveryMode === TELEGRAM_ALERT_DELIVERY.instant) {
    deliveryKind = "immediate";
  } else if (normalizedPrefs.deliveryMode === TELEGRAM_ALERT_DELIVERY.digest) {
    deliveryKind = "digest";
  } else {
    deliveryKind = category === "trade" ? "digest" : "immediate";
  }

  return {
    category,
    deliveryKind,
    digestIntervalMin: normalizedPrefs.digestIntervalMin,
  };
}

function buildTelegramDigestBucket(intervalMin, at = Date.now()) {
  const sizeMs = Math.max(5, Math.min(120, Number(intervalMin || 15))) * 60 * 1000;
  const windowStart = Math.floor(Number(at || Date.now()) / sizeMs) * sizeMs;
  return {
    digestKey: String(windowStart),
    scheduledAt: new Date(windowStart + sizeMs),
  };
}

function escapeTelegramText(value) {
  return String(value || "").replace(/[<>]/g, "");
}

async function sendTelegramDirectMessage(chatId, text) {
  if (!config.telegramBotToken || !chatId || !text) return false;
  try {
    const res = await fetch(`https://api.telegram.org/bot${config.telegramBotToken}/sendMessage`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        chat_id: chatId,
        text: String(text || "").slice(0, 4096),
        disable_web_page_preview: true,
      }),
    });
    return res.ok;
  } catch {
    return false;
  }
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

async function queueTelegramAlertsForEvent(
  client,
  ownerUserId,
  tokenId,
  symbol,
  moduleType,
  eventType,
  message,
  tx,
  amount
) {
  if (!config.telegramBotToken) return;
  const recipientsRes = await client.query(
    `
      WITH recipients AS (
        SELECT $1::bigint AS user_id
        UNION
        SELECT grantee_user_id
        FROM user_access_grants
        WHERE owner_user_id = $1
      )
      SELECT
        r.user_id,
        l.chat_id,
        p.enabled,
        p.delivery_mode,
        p.digest_interval_min,
        p.alert_deposit,
        p.alert_claim,
        p.alert_burn,
        p.alert_trade,
        p.alert_error,
        p.alert_status
      FROM recipients r
      JOIN user_telegram_links l
        ON l.user_id = r.user_id
       AND l.is_connected = TRUE
      JOIN user_telegram_alert_prefs p
        ON p.user_id = r.user_id
    `,
    [ownerUserId]
  );

  for (const row of recipientsRes.rows) {
    const strategy = classifyTelegramAlert(eventType, message, {
      enabled: row.enabled,
      deliveryMode: row.delivery_mode,
      digestIntervalMin: row.digest_interval_min,
      alertDeposit: row.alert_deposit,
      alertClaim: row.alert_claim,
      alertBurn: row.alert_burn,
      alertTrade: row.alert_trade,
      alertError: row.alert_error,
      alertStatus: row.alert_status,
    });
    if (!strategy) continue;

    const digestMeta =
      strategy.deliveryKind === "digest"
        ? buildTelegramDigestBucket(strategy.digestIntervalMin, Date.now())
        : { digestKey: null, scheduledAt: new Date() };

    await client.query(
      `
        INSERT INTO user_telegram_alert_queue (
          user_id, owner_user_id, token_id, token_symbol, module_type, event_type,
          amount, message, tx, delivery_kind, digest_key, scheduled_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
      `,
      [
        Number(row.user_id),
        ownerUserId,
        tokenId || null,
        symbol || null,
        moduleType || null,
        eventType,
        Number(amount || 0),
        String(message || ""),
        tx || null,
        strategy.deliveryKind,
        digestMeta.digestKey,
        digestMeta.scheduledAt,
      ]
    );
  }
}

async function sendTokenToIncinerator(connection, signer, mintAddress, uiAmount) {
  const amountUi = Number(uiAmount || 0);
  if (!Number.isFinite(amountUi) || amountUi <= 0) return null;
  const mint = new PublicKey(mintAddress);
  const owner = signer.publicKey;

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

  const instructions = [];

  let remaining = rawAmount;
  filteredSources.sort((a, b) => (a.amountRaw > b.amountRaw ? -1 : a.amountRaw < b.amountRaw ? 1 : 0));
  for (const source of filteredSources) {
    if (remaining <= 0n) break;
    const take = source.amountRaw > remaining ? remaining : source.amountRaw;
    if (take <= 0n) continue;
    instructions.push(
      createBurnCheckedInstruction(
        source.pubkey,
        mint,
        signer.publicKey,
        take,
        decimals,
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
  const timeoutMs = Math.max(3000, Number(process.env.PUMPPORTAL_TIMEOUT_MS || 15000));
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  let res;
  try {
    res = await fetch("https://pumpportal.fun/api/trade-local", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(requestBody),
      signal: ctrl.signal,
    });
  } catch (error) {
    if (error?.name === "AbortError") {
      throw new Error(`PumpPortal request timed out after ${timeoutMs}ms.`);
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
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
let depositFallbackModeLogged = false;
let depositFallbackFailureLoggedAt = 0;
let depositPoolProgress = {
  startedAt: 0,
  lastLoggedAt: 0,
  checked: 0,
  found: 0,
  currentPrefix: "",
};

function validateDepositVanityPrefix(input) {
  const prefix = String(input || "").trim().toUpperCase();
  if (!prefix) throw new Error("Deposit vanity prefix is empty.");
  if (!/^[1-9A-HJ-NP-Z]{1,8}$/.test(prefix)) {
    throw new Error("Deposit vanity prefix must be Base58-safe and <= 8 chars.");
  }
  return prefix;
}

function getPreferredDepositVanityPrefixes() {
  const raw = [
    config.depositVanityPrimaryPrefix,
    config.depositVanityFallbackPrefix,
    config.depositVanityPrefix,
    "EMBR",
  ];
  const out = [];
  for (const item of raw) {
    const prefix = String(item || "").trim().toUpperCase();
    if (!prefix) continue;
    try {
      const valid = validateDepositVanityPrefix(prefix);
      if (!out.includes(valid)) out.push(valid);
    } catch {
      // ignore invalid env values and continue with valid fallbacks
    }
  }
  return out.length ? out : ["EMBR"];
}

function getDepositPoolTargets(targetInput = config.depositPoolTarget) {
  const prefixes = getPreferredDepositVanityPrefixes();
  const primary = prefixes[0] || "EMBER";
  const fallback = prefixes[1] || "";
  const legacyTarget = clampDepositPoolTarget(targetInput);
  const primaryTarget = clampDepositPoolTarget(config.depositPoolTargetEmber || legacyTarget);
  const fallbackTarget = fallback
    ? clampDepositPoolTarget(
        config.depositPoolTargetEmbr !== undefined && config.depositPoolTargetEmbr !== null
          ? config.depositPoolTargetEmbr
          : 0
      )
    : 0;
  const entries = [{ prefix: primary, target: primaryTarget }];
  if (fallback && fallback !== primary && fallbackTarget > 0) {
    entries.push({ prefix: fallback, target: fallbackTarget });
  }
  return entries.filter((entry) => entry.target > 0);
}

function preferredPrefixSortCase(column = "prefix") {
  const prefixes = getPreferredDepositVanityPrefixes();
  const primary = prefixes[0] || "EMBER";
  const fallback = prefixes[1] || "";
  const clauses = [`CASE WHEN ${column} = '${primary}' THEN 0`];
  if (fallback && fallback !== primary) {
    clauses.push(`WHEN ${column} = '${fallback}' THEN 1`);
  }
  clauses.push("ELSE 99 END");
  return clauses.join(" ");
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
        checkedCount: attempts,
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
    checkedCount: 1,
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
      if (!depositFallbackModeLogged) {
        depositFallbackModeLogged = true;
        console.warn("[deposit] solana-keygen unavailable on Render; using non-vanity fallback for wallet generation.");
      }
      return generateNonVanityDeposit();
    }
    if (!config.depositVanityAllowJsFallback) {
      throw error;
    }
    if (!depositFallbackModeLogged) {
      depositFallbackModeLogged = true;
      console.warn("[deposit] solana-keygen unavailable or failed; using JS vanity fallback for wallet generation.");
    }
    try {
      return await runJsVanityGrind(prefix);
    } catch (jsError) {
      // Last-resort fallback for constrained hosts (e.g., Render free/shared CPU):
      // always return a valid keypair so bot setup is never blocked by vanity grind timing.
      const now = Date.now();
      if (now - depositFallbackFailureLoggedAt >= 60_000) {
        depositFallbackFailureLoggedAt = now;
        console.warn("[deposit] JS vanity grind timed out; using non-vanity fallback for wallet generation.");
      }
      return generateNonVanityDeposit();
    }
  }
}

async function generatePreferredVanityDeposit() {
  const prefixes = getPreferredDepositVanityPrefixes();
  let lastError = null;
  for (const prefix of prefixes) {
    try {
      const generated = await generateVanityDeposit(prefix);
      if (String(generated?.pubkey || "").startsWith(prefix)) {
        return { ...generated, prefix };
      }
      if (!lastError) {
        lastError = new Error(`Generated keypair did not match requested ${prefix} prefix.`);
      }
    } catch (error) {
      lastError = error;
    }
  }
  if (lastError) throw lastError;
  const generated = generateNonVanityDeposit();
  return { ...generated, prefix: "" };
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

function formatDepositPoolReady(byPrefix = []) {
  const entries = Array.isArray(byPrefix) ? byPrefix : [];
  const read = (prefix) => {
    const row = entries.find((item) => String(item?.prefix || "") === prefix);
    return {
      total: Number(row?.total || 0),
      target: Number(row?.target || 0),
    };
  };
  const ember = read("EMBER");
  const embr = read("EMBR");
  return `${embr.total}/${embr.target} EMBR and ${ember.total}/${ember.target} EMBER wallets ready`;
}

async function loadDepositPoolCounts(client, targets = getDepositPoolTargets()) {
  const rows = [];
  for (const entry of targets) {
    const countRes = await client.query(
      "SELECT COUNT(*)::int AS c FROM token_deposit_pool WHERE prefix = $1",
      [entry.prefix]
    );
    rows.push({
      prefix: entry.prefix,
      target: entry.target,
      total: Number(countRes.rows[0]?.c || 0),
    });
  }
  return rows;
}

function maybeLogDepositPoolProgress(byPrefix = []) {
  const now = Date.now();
  if (!depositPoolProgress.startedAt) return;
  if (now - depositPoolProgress.lastLoggedAt < 60_000) return;
  depositPoolProgress.lastLoggedAt = now;
  console.log(
    `[deposit] filling pool, checked ${Math.max(0, depositPoolProgress.checked)} wallets, found ${Math.max(0, depositPoolProgress.found)}; ${formatDepositPoolReady(byPrefix)}`
  );
}

export async function getDepositPoolStatus() {
  const targets = getDepositPoolTargets();
  if (!targets.length) {
    return { byPrefix: [], summary: "0/0 EMBR and 0/0 EMBER wallets ready", total: 0, target: 0 };
  }
  const client = await pool.connect();
  try {
    const byPrefix = await loadDepositPoolCounts(client, targets);
    return {
      byPrefix,
      summary: formatDepositPoolReady(byPrefix),
      total: byPrefix.reduce((sum, row) => sum + Number(row.total || 0), 0),
      target: byPrefix.reduce((sum, row) => sum + Number(row.target || 0), 0),
    };
  } finally {
    client.release();
  }
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
  const targets = getDepositPoolTargets(targetInput);
  if (!targets.length) return { target: 0, created: 0, total: 0, byPrefix: [] };
  const totalTarget = targets.reduce((sum, item) => sum + item.target, 0);
  if (depositPoolRefilling) return { target: totalTarget, created: 0, total: null, skipped: true };
  depositPoolRefilling = true;
  depositPoolProgress = {
    startedAt: Date.now(),
    lastLoggedAt: 0,
    checked: 0,
    found: 0,
    currentPrefix: "",
  };
  try {
    const lockResult = await withDepositPoolLock(async (client) => {
      let created = 0;
      while (true) {
        let missingEntry = null;
        const byPrefix = await loadDepositPoolCounts(client, targets);
        for (const entry of byPrefix) {
          const totalForPrefix = Number(entry.total || 0);
          if (!missingEntry && totalForPrefix < entry.target) {
            missingEntry = entry;
          }
        }
        if (!missingEntry) {
          const total = byPrefix.reduce((sum, item) => sum + item.total, 0);
          const target = byPrefix.reduce((sum, item) => sum + item.target, 0);
          return { target, created, total, byPrefix };
        }
        depositPoolProgress.currentPrefix = String(missingEntry.prefix || "");
        const generated = await generateVanityDeposit(missingEntry.prefix);
        depositPoolProgress.checked += Math.max(1, Number(generated?.checkedCount || 1));
        const storedSecret = encryptDepositSecret(generated.secretKeyBase58);
        try {
          await client.query(
            `
              INSERT INTO token_deposit_pool (prefix, deposit_pubkey, secret_key_base58, status)
              VALUES ($1, $2, $3, 'available')
            `,
            [missingEntry.prefix, generated.pubkey, storedSecret]
          );
          created += 1;
          depositPoolProgress.found += 1;
        } catch {
          // Duplicate key collisions are very unlikely; just retry loop.
        }
        const nextByPrefix = await loadDepositPoolCounts(client, targets);
        maybeLogDepositPoolProgress(nextByPrefix);
      }
    });

    if (!lockResult.locked) {
      const totalRes = await pool.query("SELECT COUNT(*)::int AS c FROM token_deposit_pool");
      const byPrefix = [];
      for (const entry of targets) {
        const countRes = await pool.query(
          "SELECT COUNT(*)::int AS c FROM token_deposit_pool WHERE prefix = $1",
          [entry.prefix]
        );
        byPrefix.push({ prefix: entry.prefix, target: entry.target, total: Number(countRes.rows[0]?.c || 0) });
      }
      return {
        target: totalTarget,
        created: 0,
        total: Number(totalRes.rows[0]?.c || 0),
        byPrefix,
        skipped: true,
      };
    }
    return lockResult.result;
  } finally {
    depositPoolRefilling = false;
    depositPoolProgress = {
      startedAt: 0,
      lastLoggedAt: 0,
      checked: 0,
      found: 0,
      currentPrefix: "",
    };
  }
}

export async function reserveDepositAddresses(userId, countInput = 1) {
  const count = Math.max(1, Math.min(DEPOSIT_POOL_MAX, Math.floor(Number(countInput) || 1)));
  const prefixCase = preferredPrefixSortCase("prefix");
  const availableRowsRes = await withTx(async (client) => {
    const rowsRes = await client.query(
      `
        SELECT id, deposit_pubkey
        FROM token_deposit_pool
        WHERE status = 'available'
        ORDER BY ${prefixCase}, created_at ASC
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
  const scope = await resolveUserAccessScopeFromPool(userId);
  const count = Math.max(1, Math.min(DEPOSIT_POOL_MAX, Math.floor(Number(countInput) || 1)));
  const reserved = await reserveDepositAddresses(scope.ownerUserId, count);
  if (count === 1) {
    return reserved[0];
  }
  return { deposits: reserved };
}

function toDeployWalletReservation(row, balanceLamportsInput = null) {
  const balanceLamports = Math.max(
    0,
    Number(balanceLamportsInput ?? row?.balance_lamports ?? 0) || 0
  );
  const requiredLamports = Math.max(0, Number(row?.required_lamports || 0) || 0);
  const funded = balanceLamports >= requiredLamports && requiredLamports > 0;
  return {
    reservationId: String(row?.id || ""),
    deposit: String(row?.deposit_pubkey || ""),
    requiredLamports,
    requiredSol: fromLamports(requiredLamports),
    bufferSol: fromLamports(getDeployVanityBufferLamports()),
    balanceLamports,
    balanceSol: fromLamports(balanceLamports),
    funded,
    shortfallLamports: Math.max(0, requiredLamports - balanceLamports),
    shortfallSol: fromLamports(Math.max(0, requiredLamports - balanceLamports)),
    status: String(row?.status || "reserved"),
    expiresAt: row?.expires_at ? new Date(row.expires_at).toISOString() : null,
    deployedMint: String(row?.deployed_mint || ""),
    deploySignature: String(row?.deploy_signature || ""),
    lastError: String(row?.last_error || ""),
  };
}

async function getDeployWalletReservationRow(client, reservationId, { forUpdate = false } = {}) {
  const id = String(reservationId || "").trim();
  if (!id) throw new Error("Deploy wallet reservation is required.");
  const lockClause = forUpdate ? "FOR UPDATE" : "";
  const res = await client.query(
    `
      SELECT *
      FROM deploy_wallet_reservations
      WHERE id = $1
      LIMIT 1
      ${lockClause}
    `,
    [id]
  );
  if (!res.rowCount) {
    throw new Error("Deploy wallet reservation not found.");
  }
  return res.rows[0];
}

export async function reserveVanityDeployWallet(userId, payload) {
  const deployUserId = Number(userId) > 0 ? Number(userId) : null;
  const initialBuySol = sanitizeDeployNumber(payload?.initialBuySol, 0.1, 0, 100);
  if (!Number.isFinite(initialBuySol) || initialBuySol <= 0) {
    throw new Error("Pump deploy requires an initial buy greater than 0 SOL.");
  }

  const requiredLamports = estimateDeployVanityRequiredLamports(initialBuySol);
  const expiresAt = deployVanityReservationExpiryDate();
  const prefixCase = preferredPrefixSortCase("prefix");

  const reservedRes = await withTx(async (client) => {
    const poolRes = await client.query(
      `
        SELECT id, prefix, deposit_pubkey, secret_key_base58
        FROM token_deposit_pool
        WHERE status = 'available'
        ORDER BY ${prefixCase}, created_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      `
    );
    if (!poolRes.rowCount) {
      return { ok: false, available: 0 };
    }

    const poolRow = poolRes.rows[0];
    const reservationId = makeId("dvw");
    await client.query("DELETE FROM token_deposit_pool WHERE id = $1", [poolRow.id]);
    await client.query(
      `
        INSERT INTO deploy_wallet_reservations (
          id, user_id, deposit_pubkey, secret_key_base58, required_lamports, expires_at
        )
        VALUES ($1, $2, $3, $4, $5, $6)
      `,
      [
        reservationId,
        deployUserId,
        poolRow.deposit_pubkey,
        poolRow.secret_key_base58,
        requiredLamports,
        expiresAt,
      ]
    );

    return {
      ok: true,
      row: {
        id: reservationId,
        user_id: deployUserId,
        deposit_pubkey: poolRow.deposit_pubkey,
        secret_key_base58: poolRow.secret_key_base58,
        required_lamports: requiredLamports,
        balance_lamports: 0,
        status: "reserved",
        expires_at: expiresAt,
        deployed_mint: "",
        deploy_signature: "",
        last_error: "",
      },
      secretKeyBase58: decryptDepositSecret(poolRow.secret_key_base58),
    };
  });

  if (!reservedRes.ok) {
    void ensureDepositPool();
    throw makeHttpError(503, buildPoolWarmupMessage(1, 0));
  }

  void ensureDepositPool();
  return {
    ...toDeployWalletReservation(reservedRes.row, 0),
    privateKeyBase58: reservedRes.secretKeyBase58,
    privateKeyArray: Array.from(keypairFromBase58(reservedRes.secretKeyBase58).secretKey),
  };
}

export async function getVanityDeployWalletStatus(reservationId) {
  const row = await getDeployWalletReservationRow(pool, reservationId);
  const balanceLamports = await getConnection().getBalance(new PublicKey(row.deposit_pubkey), "confirmed");
  await pool.query(
    `
      UPDATE deploy_wallet_reservations
      SET balance_lamports = $2
      WHERE id = $1
    `,
    [row.id, balanceLamports]
  ).catch(() => {});
  return toDeployWalletReservation(row, balanceLamports);
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

export async function getTokenDeployWallet(userId, tokenId) {
  return withTx(async (client) => {
    const scope = await resolveUserAccessScope(client, userId);
    assertOwnerPermission(scope, "Managers cannot view EMBR deploy wallet keys.");
    const res = await client.query(
      `
        SELECT deploy_wallet_pubkey, deploy_wallet_secret_key_base58, deployed_via_ember
        FROM tokens
        WHERE user_id = $1 AND id = $2
        LIMIT 1
      `,
      [scope.ownerUserId, tokenId]
    );
    if (!res.rowCount) {
      throw new Error("Token not found.");
    }
    const row = res.rows[0];
    if (!Boolean(row.deployed_via_ember) || !String(row.deploy_wallet_secret_key_base58 || "").trim()) {
      throw new Error("No EMBR deploy wallet is stored for this token.");
    }
    const privateKeyBase58 = decryptDepositSecret(row.deploy_wallet_secret_key_base58);
    return {
      publicKey: String(row.deploy_wallet_pubkey || ""),
      privateKeyBase58,
      privateKeyArray: Array.from(keypairFromBase58(privateKeyBase58).secretKey),
    };
  });
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
    user: await getUserAuthProfile(user.id),
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
    user: await getUserAuthProfile(user.id),
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
  return getUserAuthProfile(Number(result.rows[0].id));
}

async function resolveUserAccessScope(client, actorUserId) {
  const actorId = Number(actorUserId || 0);
  if (!Number.isFinite(actorId) || actorId <= 0) {
    throw new Error("User is required.");
  }

  const actorRes = await client.query(
    "SELECT id, username FROM users WHERE id = $1 LIMIT 1",
    [actorId]
  );
  if (!actorRes.rowCount) {
    throw new Error("User not found.");
  }
  const actor = actorRes.rows[0];

  const grantRes = await client.query(
    `
      SELECT g.owner_user_id, owner.username AS owner_username
      FROM user_access_grants g
      JOIN users owner ON owner.id = g.owner_user_id
      WHERE g.grantee_user_id = $1
      LIMIT 1
    `,
    [actorId]
  );

  const isOperator = grantRes.rowCount > 0;
  const ownerUserId = isOperator ? Number(grantRes.rows[0].owner_user_id) : actorId;
  const ownerUsername = isOperator ? String(grantRes.rows[0].owner_username || "") : String(actor.username || "");

  return {
    actorUserId: actorId,
    actorUsername: String(actor.username || ""),
    ownerUserId,
    ownerUsername,
    role: isOperator ? "manager" : "owner",
    isOperator,
    isOwner: !isOperator,
    canManageFunds: !isOperator,
    canDelete: !isOperator,
    canManageAccess: !isOperator,
  };
}

async function resolveUserAccessScopeFromPool(actorUserId) {
  return withTx(async (client) => resolveUserAccessScope(client, actorUserId));
}

function assertOwnerPermission(scope, message = "Only the primary account can perform this action.") {
  if (!scope?.canManageFunds) {
    throw new Error(message);
  }
}

export async function getUserAuthProfile(userId) {
  const scope = await resolveUserAccessScopeFromPool(userId);
  return {
    id: scope.actorUserId,
    username: scope.actorUsername,
    ownerUserId: scope.ownerUserId,
    ownerUsername: scope.ownerUsername,
    role: scope.role,
    isOperator: scope.isOperator,
    canManageFunds: scope.canManageFunds,
    canDelete: scope.canDelete,
    canManageAccess: scope.canManageAccess,
  };
}

export async function getOperatorAccess(userId) {
  return withTx(async (client) => {
    const scope = await resolveUserAccessScope(client, userId);
    assertOwnerPermission(scope, "Only the primary account can manage manager access.");
    const grantRes = await client.query(
      `
        SELECT g.grantee_user_id, u.username
        FROM user_access_grants g
        JOIN users u ON u.id = g.grantee_user_id
        WHERE g.owner_user_id = $1
        LIMIT 1
      `,
      [scope.ownerUserId]
    );
    if (!grantRes.rowCount) {
      return { enabled: false, username: "" };
    }
    return {
      enabled: true,
      username: String(grantRes.rows[0].username || ""),
      userId: Number(grantRes.rows[0].grantee_user_id || 0),
    };
  });
}

export async function upsertOperatorAccess(userId, usernameInput, passwordInput) {
  const username = normalizeUsername(usernameInput);
  const password = normalizePassword(passwordInput);
  const passwordHash = await bcrypt.hash(password, 10);

  return withTx(async (client) => {
    const scope = await resolveUserAccessScope(client, userId);
    assertOwnerPermission(scope, "Only the primary account can manage manager access.");

    const existingGrantRes = await client.query(
      `
        SELECT g.grantee_user_id, u.username
        FROM user_access_grants g
        JOIN users u ON u.id = g.grantee_user_id
        WHERE g.owner_user_id = $1
        LIMIT 1
        FOR UPDATE
      `,
      [scope.ownerUserId]
    );

    const usernameRes = await client.query(
      "SELECT id FROM users WHERE username = $1 LIMIT 1",
      [username]
    );
    const usernameOwner = usernameRes.rowCount ? Number(usernameRes.rows[0].id || 0) : 0;

    if (existingGrantRes.rowCount) {
      const granteeUserId = Number(existingGrantRes.rows[0].grantee_user_id || 0);
      if (usernameOwner && usernameOwner !== granteeUserId) {
        throw new Error("Username already exists.");
      }
      await client.query(
        `
          UPDATE users
          SET username = $1,
              password_hash = $2
          WHERE id = $3
        `,
        [username, passwordHash, granteeUserId]
      );
      return {
        enabled: true,
        username,
        userId: granteeUserId,
      };
    }

    if (usernameOwner) {
      throw new Error("Username already exists.");
    }

    const insertedUser = await client.query(
      `
        INSERT INTO users (username, password_hash)
        VALUES ($1, $2)
        RETURNING id
      `,
      [username, passwordHash]
    );
    const granteeUserId = Number(insertedUser.rows[0].id || 0);
    await client.query(
      `
        INSERT INTO user_access_grants (owner_user_id, grantee_user_id, role)
        VALUES ($1, $2, 'manager')
      `,
      [scope.ownerUserId, granteeUserId]
    );
    return {
      enabled: true,
      username,
      userId: granteeUserId,
    };
  });
}

export async function deleteOperatorAccess(userId) {
  return withTx(async (client) => {
    const scope = await resolveUserAccessScope(client, userId);
    assertOwnerPermission(scope, "Only the primary account can manage manager access.");

    const existingGrantRes = await client.query(
      `
        SELECT grantee_user_id
        FROM user_access_grants
        WHERE owner_user_id = $1
        LIMIT 1
        FOR UPDATE
      `,
      [scope.ownerUserId]
    );
    if (!existingGrantRes.rowCount) {
      return { ok: true, removed: false };
    }
    const granteeUserId = Number(existingGrantRes.rows[0].grantee_user_id || 0);
    await client.query("DELETE FROM sessions WHERE user_id = $1", [granteeUserId]);
    await client.query("DELETE FROM user_access_grants WHERE owner_user_id = $1", [scope.ownerUserId]);
    await client.query("DELETE FROM users WHERE id = $1", [granteeUserId]);
    return { ok: true, removed: true };
  });
}

export async function getTelegramAlertSettings(userId) {
  return withTx(async (client) => {
    const scope = await resolveUserAccessScope(client, userId);
    const linkRes = await client.query(
      `
        SELECT chat_id, telegram_username, first_name, last_name, is_connected, connected_at, updated_at
        FROM user_telegram_links
        WHERE user_id = $1
        LIMIT 1
      `,
      [scope.actorUserId]
    );
    const prefRes = await client.query(
      `
        INSERT INTO user_telegram_alert_prefs (user_id)
        VALUES ($1)
        ON CONFLICT (user_id) DO NOTHING
      `,
      [scope.actorUserId]
    );
    void prefRes;
    const prefsReadRes = await client.query(
      `
        SELECT enabled, delivery_mode, digest_interval_min, alert_deposit, alert_claim, alert_burn,
               alert_trade, alert_error, alert_status
        FROM user_telegram_alert_prefs
        WHERE user_id = $1
        LIMIT 1
      `,
      [scope.actorUserId]
    );
    const pending = await getOrCreateTelegramConnectToken(client, scope.actorUserId);
    const botProfile = await fetchTelegramBotProfile();
    const connectUrl = buildTelegramConnectUrl(botProfile.username, pending.token);
    const prefs = sanitizeTelegramAlertPrefs({
      enabled: prefsReadRes.rows[0]?.enabled,
      deliveryMode: prefsReadRes.rows[0]?.delivery_mode,
      digestIntervalMin: prefsReadRes.rows[0]?.digest_interval_min,
      alertDeposit: prefsReadRes.rows[0]?.alert_deposit,
      alertClaim: prefsReadRes.rows[0]?.alert_claim,
      alertBurn: prefsReadRes.rows[0]?.alert_burn,
      alertTrade: prefsReadRes.rows[0]?.alert_trade,
      alertError: prefsReadRes.rows[0]?.alert_error,
      alertStatus: prefsReadRes.rows[0]?.alert_status,
    });
    const link = linkRes.rows[0] || null;
    return {
      connected: Boolean(link?.is_connected),
      chatIdMasked: maskTelegramChatId(link?.chat_id),
      telegramUsername: String(link?.telegram_username || ""),
      firstName: String(link?.first_name || ""),
      lastName: String(link?.last_name || ""),
      connectedAt: link?.connected_at || null,
      updatedAt: link?.updated_at || null,
      botUsername: String(botProfile.username || ""),
      connectToken: pending.token,
      connectUrl,
      prefs,
    };
  });
}

export async function updateTelegramAlertSettings(userId, payload = {}) {
  return withTx(async (client) => {
    const scope = await resolveUserAccessScope(client, userId);
    const currentRes = await client.query(
      `
        SELECT enabled, delivery_mode, digest_interval_min, alert_deposit, alert_claim, alert_burn,
               alert_trade, alert_error, alert_status
        FROM user_telegram_alert_prefs
        WHERE user_id = $1
        LIMIT 1
      `,
      [scope.actorUserId]
    );
    const prefs = sanitizeTelegramAlertPrefs({
      enabled: currentRes.rows[0]?.enabled,
      deliveryMode: currentRes.rows[0]?.delivery_mode,
      digestIntervalMin: currentRes.rows[0]?.digest_interval_min,
      alertDeposit: currentRes.rows[0]?.alert_deposit,
      alertClaim: currentRes.rows[0]?.alert_claim,
      alertBurn: currentRes.rows[0]?.alert_burn,
      alertTrade: currentRes.rows[0]?.alert_trade,
      alertError: currentRes.rows[0]?.alert_error,
      alertStatus: currentRes.rows[0]?.alert_status,
      ...payload,
    });

    await client.query(
      `
        INSERT INTO user_telegram_alert_prefs (
          user_id, enabled, delivery_mode, digest_interval_min,
          alert_deposit, alert_claim, alert_burn, alert_trade, alert_error, alert_status, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
        ON CONFLICT (user_id) DO UPDATE
        SET
          enabled = EXCLUDED.enabled,
          delivery_mode = EXCLUDED.delivery_mode,
          digest_interval_min = EXCLUDED.digest_interval_min,
          alert_deposit = EXCLUDED.alert_deposit,
          alert_claim = EXCLUDED.alert_claim,
          alert_burn = EXCLUDED.alert_burn,
          alert_trade = EXCLUDED.alert_trade,
          alert_error = EXCLUDED.alert_error,
          alert_status = EXCLUDED.alert_status,
          updated_at = NOW()
      `,
      [
        scope.actorUserId,
        prefs.enabled,
        prefs.deliveryMode,
        prefs.digestIntervalMin,
        prefs.alertDeposit,
        prefs.alertClaim,
        prefs.alertBurn,
        prefs.alertTrade,
        prefs.alertError,
        prefs.alertStatus,
      ]
    );

    return { prefs };
  });
}

export async function disconnectTelegramAlerts(userId) {
  return withTx(async (client) => {
    const scope = await resolveUserAccessScope(client, userId);
    await client.query("DELETE FROM user_telegram_links WHERE user_id = $1", [scope.actorUserId]);
    await client.query(
      `
        UPDATE user_telegram_alert_prefs
        SET enabled = FALSE, updated_at = NOW()
        WHERE user_id = $1
      `,
      [scope.actorUserId]
    );
    await client.query(
      `
        UPDATE user_telegram_alert_queue
        SET status = 'skipped', error = 'telegram disconnected'
        WHERE user_id = $1 AND status = 'pending'
      `,
      [scope.actorUserId]
    );
    return { ok: true };
  });
}

export async function sendTelegramTestAlert(userId) {
  const settings = await getTelegramAlertSettings(userId);
  if (!settings.connected) {
    throw new Error("Connect Telegram first.");
  }
  const scope = await resolveUserAccessScopeFromPool(userId);
  const linkRes = await pool.query(
    "SELECT chat_id FROM user_telegram_links WHERE user_id = $1 LIMIT 1",
    [scope.actorUserId]
  );
  if (!linkRes.rowCount) {
    throw new Error("Telegram link not found.");
  }
  const ok = await sendTelegramDirectMessage(
    String(linkRes.rows[0].chat_id || ""),
    [
      "EMBER Alert",
      `Account: ${scope.actorUsername}`,
      "This is a test alert from your dashboard connection.",
      `Time: ${new Date().toISOString()}`,
    ].join("\n")
  );
  if (!ok) {
    throw new Error("Unable to send Telegram test alert.");
  }
  return { ok: true };
}

const PROTOCOL_SYSTEM_USERNAME = "__ember_protocol__";
let protocolSystemUserIdCache = null;

async function getOrCreateProtocolSystemUserId(client) {
  if (protocolSystemUserIdCache) return protocolSystemUserIdCache;

  const existing = await client.query(
    "SELECT id FROM users WHERE username = $1 LIMIT 1",
    [PROTOCOL_SYSTEM_USERNAME]
  );
  if (existing.rowCount) {
    protocolSystemUserIdCache = Number(existing.rows[0].id);
    return protocolSystemUserIdCache;
  }

  const seed = config.treasuryWalletPublicKey || config.devWalletPublicKey || "protocol";
  const passwordHash = await bcrypt.hash(`${PROTOCOL_SYSTEM_USERNAME}:${seed}`, 10);
  const inserted = await client.query(
    `
      INSERT INTO users (username, password_hash)
      VALUES ($1, $2)
      ON CONFLICT (username) DO NOTHING
      RETURNING id
    `,
    [PROTOCOL_SYSTEM_USERNAME, passwordHash]
  );
  if (inserted.rowCount) {
    protocolSystemUserIdCache = Number(inserted.rows[0].id);
    return protocolSystemUserIdCache;
  }

  const fetched = await client.query(
    "SELECT id FROM users WHERE username = $1 LIMIT 1",
    [PROTOCOL_SYSTEM_USERNAME]
  );
  if (!fetched.rowCount) {
    throw new Error("Unable to create protocol system user.");
  }
  protocolSystemUserIdCache = Number(fetched.rows[0].id);
  return protocolSystemUserIdCache;
}

export async function getDashboard(userId) {
  const scope = await resolveUserAccessScopeFromPool(userId);
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
      [scope.ownerUserId]
    ),
    pool.query(
      "SELECT id, token_id, token_symbol, module_type, event_type, amount, message, tx, created_at FROM token_events WHERE user_id = $1 ORDER BY created_at DESC LIMIT 30",
      [scope.ownerUserId]
    ),
    pool.query(
      "SELECT id, token_id, token_symbol, module_type, event_type, amount, message, tx, created_at FROM token_events WHERE user_id = $1 ORDER BY created_at DESC LIMIT 200",
      [scope.ownerUserId]
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
      [scope.ownerUserId]
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

export async function getPublicDashboard() {
  const [tokensRes, logsRes, chartRes, burnBreakdownRes, protocolAggRes] = await Promise.all([
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
          WHERE event_type = 'burn'
          GROUP BY token_id
        ) be ON be.token_id = t.id
        LEFT JOIN bot_modules m
          ON m.token_id = t.id
         AND m.module_type = t.selected_bot
        WHERE m.id IS NOT NULL
          AND COALESCE(t.disconnected, false) = false
        ORDER BY t.active DESC, t.updated_at DESC, t.created_at DESC
        LIMIT 200
      `
    ),
    pool.query(
      `
        SELECT id, token_id, token_symbol, module_type, event_type, amount, message, tx, created_at
        FROM token_events
        WHERE event_type IN ('burn', 'buyback')
           OR (
             module_type IN ('burn', 'personal_burn')
             AND event_type IN ('claim', 'status', 'transfer', 'error', 'withdraw', 'sell', 'buy')
           )
        ORDER BY created_at DESC
        LIMIT 500
      `
    ),
    pool.query(
      `
        SELECT event_type, amount, message, created_at
        FROM token_events
        WHERE event_type = 'burn'
          AND created_at >= NOW() - INTERVAL '7 days'
        ORDER BY created_at DESC
      `
    ),
    pool.query(
      `
        SELECT
          UPPER(COALESCE(NULLIF(token_symbol, ''), 'UNKNOWN')) AS symbol,
          COALESCE(SUM(amount), 0)::numeric AS amount
        FROM token_events
        WHERE event_type = 'burn'
        GROUP BY 1
        ORDER BY amount DESC
      `
    ),
    pool.query(
      `
        SELECT
          lifetime_incinerated,
          ember_incinerated,
          updated_at
        FROM protocol_metrics
        WHERE id = 1
        LIMIT 1
      `
    ).catch(() => ({ rows: [] })),
  ]);

  const tokenRows = attachMarketCaps(tokensRes.rows);
  let burnBreakdown = burnBreakdownRes.rows.map((row) => ({
    symbol: String(row.symbol || "UNKNOWN"),
    amount: Number(row.amount || 0),
  }));
  let logRows = logsRes.rows || [];
  let chartRows = chartRes.rows || [];
  const protocolAgg = protocolAggRes.rows[0] || {};
  const protocolLifetime = Math.max(0, Number(protocolAgg.lifetime_incinerated || 0));
  const protocolEmber = Math.max(0, Number(protocolAgg.ember_incinerated || 0));
  const protocolAtRaw = protocolAgg.updated_at;
  const protocolAt = protocolAtRaw ? new Date(protocolAtRaw) : new Date();
  const protocolCreatedAt = Number.isNaN(protocolAt.getTime())
    ? new Date().toISOString()
    : protocolAt.toISOString();

  // Keep burn breakdown aligned with public metrics, which include protocol_metrics aggregates.
  if (protocolEmber > 0) {
    const emberIdx = burnBreakdown.findIndex(
      (row) => String(row?.symbol || "").trim().toUpperCase() === "EMBER"
    );
    if (emberIdx >= 0) {
      burnBreakdown[emberIdx].amount = Math.max(0, Number(burnBreakdown[emberIdx].amount || 0)) + protocolEmber;
    } else {
      burnBreakdown.push({ symbol: "EMBER", amount: protocolEmber });
    }
    burnBreakdown.sort((a, b) => Number(b.amount || 0) - Number(a.amount || 0));
  }

  // Fallback path: if historical rows are empty but protocol aggregates are non-zero,
  // surface a synthetic protocol burn entry so Burns page is never blank.
  if (!logRows.length && protocolLifetime > 0) {
    const fallbackAmount = protocolEmber > 0 ? protocolEmber : protocolLifetime;
    logRows = [
      {
        id: "protocol_aggregate_burn",
        token_id: null,
        token_symbol: protocolEmber > 0 ? "EMBER" : "PROTOCOL",
        module_type: MODULE_TYPES.personalBurn,
        event_type: "burn",
        amount: fallbackAmount,
        message: "Historical protocol burn aggregate (metrics fallback).",
        tx: null,
        created_at: protocolCreatedAt,
      },
    ];
  }

  if (!chartRows.length && protocolLifetime > 0) {
    chartRows = [
      {
        event_type: "burn",
        amount: protocolEmber > 0 ? protocolEmber : protocolLifetime,
        message: "Historical protocol burn aggregate (metrics fallback).",
        created_at: protocolCreatedAt,
      },
    ];
  }

  if (!burnBreakdown.length && protocolLifetime > 0) {
    burnBreakdown = [
      {
        symbol: protocolEmber > 0 ? "EMBER" : "PROTOCOL",
        amount: protocolEmber > 0 ? protocolEmber : protocolLifetime,
      },
    ];
  }

  return {
    tokens: tokenRows.map(toToken),
    logs: logRows.map(toEvent),
    chartData: buildChartData(chartRows),
    burnBreakdown,
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

  const selectedBot = normalizeModuleType(tokenRow.selected_bot, MODULE_TYPES.burn);
  if (!isTradeBotModuleType(selectedBot)) return addresses;

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

async function getTradeBotTokenModule(client, userId, tokenId, { forUpdate = false } = {}) {
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
  if (!isTradeBotModuleType(selectedBot)) {
    throw new Error("Sweep/withdraw is available only for multi-wallet trade bots.");
  }

  const moduleRes = await client.query(
    `
      SELECT id AS module_id, user_id, token_id, module_type, enabled, config_json, state_json
      FROM bot_modules
      WHERE token_id = $1 AND module_type = $2
      LIMIT 1
      ${lockClause}
    `,
    [tokenId, selectedBot]
  );
  if (!moduleRes.rowCount) throw new Error(`${moduleTypeLabel(selectedBot)} is not configured for this token.`);
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
    const scope = await resolveUserAccessScope(client, userId);
    assertOwnerPermission(scope, "Only the primary account can view withdraw options.");
    const { tokenRow, moduleRow } = await getTradeBotTokenModule(client, scope.ownerUserId, tokenIdText);
    const moduleType = normalizeModuleType(moduleRow.module_type, MODULE_TYPES.volume);
    const configJson = mergeModuleConfig(moduleType, moduleRow.config_json, {});
    const reserveLamports = toLamports(configJson.reserveSol || config.botSolReserve || 0.01);
    const feeSafetyLamports = getTxFeeSafetyLamports();
    const addresses = await readTokenWalletAddresses(client, scope.ownerUserId, tokenRow);
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
        [scope.ownerUserId, tokenIdText]
      ),
    ]);

    await persistFundingSources(client, scope.ownerUserId, tokenIdText, scannedSources);

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
  const scope = await resolveUserAccessScopeFromPool(userId);
  const tokenRes = await pool.query(
    `
      SELECT id, symbol, name, mint, picture_url, deposit, selected_bot, active
      FROM tokens
      WHERE user_id = $1 AND id = $2
      LIMIT 1
    `,
    [scope.ownerUserId, tokenId]
  );
  if (!tokenRes.rowCount) throw new Error("Token not found.");
  const tokenRow = tokenRes.rows[0];

  const addresses = await withTx(async (client) =>
    readTokenWalletAddresses(client, scope.ownerUserId, tokenRow)
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
  const scope = await resolveUserAccessScopeFromPool(userId);
  assertOwnerPermission(scope, "Managers cannot delete tokens.");
  const tokenRes = await pool.query(
    `
      SELECT id, symbol, name, mint, picture_url, deposit, selected_bot, active, disconnected
      FROM tokens
      WHERE user_id = $1 AND id = $2
      LIMIT 1
    `,
    [scope.ownerUserId, tokenId]
  );
  if (!tokenRes.rowCount) throw new Error("Token not found.");
  const tokenRow = tokenRes.rows[0];
  const moduleType = normalizeModuleType(tokenRow.selected_bot, MODULE_TYPES.burn);
  const connection = getConnection();

  if (moduleType === MODULE_TYPES.burn) {
    try {
      const depositSecret = await getTokenDepositSigningKey(scope.ownerUserId, String(tokenRow.id));
      const depositSigner = keypairFromBase58(depositSecret);
      const tokenBal = await getOwnerTokenBalanceUi(connection, depositSigner.publicKey, tokenRow.mint);
      const burnAmount = Number(tokenBal.toFixed(6));
      if (burnAmount > 0.000001) {
        const burnSig = await sendTokenToIncinerator(connection, depositSigner, tokenRow.mint, burnAmount);
        if (burnSig) {
          await insertEvent(
            pool,
            scope.ownerUserId,
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
        scope.ownerUserId,
        String(tokenRow.id),
        String(tokenRow.symbol || ""),
        "error",
        `Delete cleanup burn failed: ${error.message}`,
        null,
        { moduleType: MODULE_TYPES.burn }
      );
    }
  } else if (isTradeBotModuleType(moduleType)) {
    const moduleRes = await pool.query(
      `
        SELECT id, config_json
        FROM bot_modules
        WHERE token_id = $1 AND module_type = $2
        LIMIT 1
      `,
      [String(tokenRow.id), moduleType]
    );
    const configJson = mergeModuleConfig(
      moduleType,
      moduleRes.rows[0]?.config_json || {},
      {}
    );
    const signers = [];
    try {
      const depositSecret = await getTokenDepositSigningKey(scope.ownerUserId, String(tokenRow.id));
      signers.push({ signer: keypairFromBase58(depositSecret), label: "deposit" });
    } catch {}
    const walletRes = await pool.query(
      `
        SELECT label, secret_key_base58
        FROM volume_trade_wallets
        WHERE user_id = $1 AND token_id = $2
      `,
      [scope.ownerUserId, String(tokenRow.id)]
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
          scope.ownerUserId,
          String(tokenRow.id),
          String(tokenRow.symbol || ""),
          "sell",
          `Delete cleanup sold ${sellAmount.toFixed(6)} ${tokenRow.symbol} from ${item.label}`,
          sellSig,
          { moduleType }
        );
      } catch (error) {
        await insertEvent(
          pool,
          scope.ownerUserId,
          String(tokenRow.id),
          String(tokenRow.symbol || ""),
          "error",
          `Delete cleanup sell failed (${item.label}): ${error.message}`,
          null,
          { moduleType }
        );
      }
    }
  }

  const addresses = await withTx(async (client) =>
    readTokenWalletAddresses(client, scope.ownerUserId, tokenRow)
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
      [scope.ownerUserId, tokenId, String(tokenRow.symbol), `Token disconnected from Nexus: ${tokenRow.symbol} (${tokenRow.mint})`]
    );
    await client.query(
      `
        UPDATE tokens
        SET active = FALSE,
            disconnected = TRUE
        WHERE user_id = $1 AND id = $2
      `,
      [scope.ownerUserId, tokenId]
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

export async function restoreToken(userId, tokenId) {
  return withTx(async (client) => {
    const scope = await resolveUserAccessScope(client, userId);
    assertOwnerPermission(scope, "Managers cannot restore archived bots.");
    const tokenRes = await client.query(
      `
        SELECT id, symbol, mint, disconnected
        FROM tokens
        WHERE user_id = $1 AND id = $2
        LIMIT 1
        FOR UPDATE
      `,
      [scope.ownerUserId, tokenId]
    );
    if (!tokenRes.rowCount) throw new Error("Token not found.");
    const tokenRow = tokenRes.rows[0];
    if (!Boolean(tokenRow.disconnected)) {
      throw new Error("Token is already active on your dashboard.");
    }

    const updatedRes = await client.query(
      `
        UPDATE tokens
        SET active = FALSE,
            disconnected = FALSE
        WHERE id = $1
        RETURNING *
      `,
      [tokenId]
    );
    await client.query(
      `
        INSERT INTO token_events (user_id, token_id, token_symbol, event_type, message, tx)
        VALUES ($1, $2, $3, 'status', $4, NULL)
      `,
      [
        scope.ownerUserId,
        tokenId,
        String(tokenRow.symbol || ""),
        `Token restored in paused mode: ${tokenRow.symbol} (${tokenRow.mint})`,
      ]
    );
    return toToken(updatedRes.rows[0]);
  });
}

export async function sweepVolumeWallets(userId, tokenId) {
  const tokenIdText = String(tokenId || "").trim();
  if (!tokenIdText) throw new Error("Token id is required.");

  return withTx(async (client) => {
    const scope = await resolveUserAccessScope(client, userId);
    assertOwnerPermission(scope, "Managers cannot sweep wallet balances.");
    const { tokenRow, moduleRow } = await getTradeBotTokenModule(client, scope.ownerUserId, tokenIdText, {
      forUpdate: true,
    });
    const moduleType = normalizeModuleType(moduleRow.module_type, MODULE_TYPES.volume);
    if (tokenRow.active) {
      throw new Error(`Pause the ${moduleTypeLabel(moduleType).toLowerCase()} before sweeping wallets.`);
    }

    const lockKey = `${tokenIdText}:${moduleType}`;
    const lock = await withTokenModuleLock(client, lockKey, async () => {
      const connection = getConnection();
      const configJson = mergeModuleConfig(moduleType, moduleRow.config_json, {});
      const depositSecret = await getTokenDepositSigningKey(scope.ownerUserId, tokenIdText);
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
              scope.ownerUserId,
              tokenIdText,
              tokenRow.symbol,
              "sell",
              `Sweep sold ${sellAmount.toFixed(4)} ${tokenRow.symbol} from ${label}`,
              sig,
              {
                moduleType,
                amount: sellAmount,
                idempotencyKey: `${eventPrefix}:sell:${wallet.wallet_pubkey}`,
              }
            );
          } catch (error) {
            await insertEvent(
              client,
              scope.ownerUserId,
              tokenIdText,
              tokenRow.symbol,
              "error",
              `Sweep sell failed (${label}): ${error.message}`,
              null,
              {
                moduleType,
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
            scope.ownerUserId,
            tokenIdText,
            tokenRow.symbol,
            "transfer",
            `Sweep moved ${fromLamports(movable).toFixed(6)} SOL from ${label} to deposit`,
            sig,
            {
              moduleType,
              amount: fromLamports(movable),
              idempotencyKey: `${eventPrefix}:sweep:${wallet.wallet_pubkey}`,
            }
          );
        } catch (error) {
          await insertEvent(
            client,
            scope.ownerUserId,
            tokenIdText,
            tokenRow.symbol,
            "error",
            `Sweep transfer failed (${label}): ${error.message}`,
            null,
            {
              moduleType,
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
    const scope = await resolveUserAccessScope(client, userId);
    assertOwnerPermission(scope, "Managers cannot withdraw wallet balances.");
    const { tokenRow, moduleRow } = await getTradeBotTokenModule(client, scope.ownerUserId, tokenIdText, {
      forUpdate: true,
    });
    const moduleType = normalizeModuleType(moduleRow.module_type, MODULE_TYPES.volume);
    if (tokenRow.active) {
      throw new Error(`Pause the ${moduleTypeLabel(moduleType).toLowerCase()} before withdrawing.`);
    }

    const lockKey = `${tokenIdText}:${moduleType}`;
    const lock = await withTokenModuleLock(client, lockKey, async () => {
      const connection = getConnection();
      const configJson = mergeModuleConfig(moduleType, moduleRow.config_json, {});
      const reserveLamports = toLamports(configJson.reserveSol || config.botSolReserve || 0.01);
      const feeSafetyLamports = getTxFeeSafetyLamports();
      const depositSecret = await getTokenDepositSigningKey(scope.ownerUserId, tokenIdText);
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
        scope.ownerUserId,
        tokenIdText,
        tokenRow.symbol,
        "withdraw",
        `Withdrawn ${fromLamports(lamportsToSend).toFixed(6)} SOL to ${destination}`,
        sig,
        {
          moduleType,
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
    Promise.resolve(emberMint ? readCachedMarketCapUsd(emberMint) : 0),
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

export async function submitVanityDeploy(userId, payload) {
  const body = payload && typeof payload === "object" ? payload : {};
  const deployUserId = userId || null;
  const requestedAutoAttach = Boolean(body.autoAttach);
  const botPreset = getDeployBotPreset(body.selectedBot);
  const autoAttach = requestedAutoAttach && Boolean(botPreset);
  if (autoAttach && !deployUserId) {
    throw new Error("Sign in is required when Auto-Attach is enabled.");
  }

  const reservationId = String(body.reservationId || "").trim();
  if (!reservationId) {
    throw new Error("Deploy wallet reservation is required.");
  }

  const name = sanitizeDeployString(body.name, 40, "Name");
  const symbol = sanitizeDeployString(body.symbol, 12, "Symbol").toUpperCase();
  const description = sanitizeDeployString(body.description, 300, "Description");
  const initialBuySol = sanitizeDeployNumber(body.initialBuySol, 0.1, 0, 100);
  if (!Number.isFinite(initialBuySol) || initialBuySol <= 0) {
    throw new Error("Pump deploy requires an initial buy greater than 0 SOL.");
  }
  const twitter = body.twitter ? normalizeHttpUrl(body.twitter, "Twitter URL") : "";
  const telegram = body.telegram ? normalizeHttpUrl(body.telegram, "Telegram URL") : "";
  const website = body.website ? normalizeHttpUrl(body.website, "Website URL") : "";
  const mayhemMode = Boolean(body.mayhemMode);

  const uploadImageDataUri = String(body.imageDataUri || "").trim();
  const uploadImageFileName = String(body.imageFileName || "token").trim().slice(0, 80) || "token";
  const uploadBannerDataUri = String(body.bannerDataUri || "").trim();
  const uploadBannerFileName = String(body.bannerFileName || "banner").trim().slice(0, 80) || "banner";
  const fallbackBannerUrl = String(body.bannerUrl || "").trim();
  const fallbackImageUrl = String(body.imageUrl || "").trim();
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

  const reservation = await withTx(async (client) => {
    const row = await getDeployWalletReservationRow(client, reservationId, { forUpdate: true });
    const expectedLamports = estimateDeployVanityRequiredLamports(initialBuySol);
    if (Number(row.required_lamports || 0) !== expectedLamports) {
      throw new Error("Initial buy changed after wallet generation. Generate a new EMBR deploy wallet.");
    }
    if (String(row.status || "").trim().toLowerCase() === "deployed") {
      return row;
    }
    await client.query(
      `
        UPDATE deploy_wallet_reservations
        SET status = 'deploying', last_error = NULL
        WHERE id = $1
      `,
      [row.id]
    );
    return row;
  });

  if (String(reservation.status || "").trim().toLowerCase() === "deployed") {
    const mint = String(reservation.deployed_mint || "").trim();
    const signature = String(reservation.deploy_signature || "").trim();
    return {
      ok: true,
      mint,
      signature,
      deployWallet: String(reservation.deposit_pubkey || ""),
      remainingSol: fromLamports(Math.max(0, Number(reservation.balance_lamports || 0))),
      pumpfunUrl: mint ? `https://pump.fun/coin/${mint}` : null,
      solscanTx: signature ? `https://solscan.io/tx/${signature}` : null,
      solscanMint: mint ? `https://solscan.io/token/${mint}` : null,
      autoAttached: false,
      attachedToken: null,
    };
  }

  try {
    const balanceLamports = await getConnection().getBalance(new PublicKey(reservation.deposit_pubkey), "confirmed");
    if (balanceLamports < Number(reservation.required_lamports || 0)) {
      throw new Error(
        `Funding incomplete. Required ${fromLamports(Number(reservation.required_lamports || 0)).toFixed(6)} SOL, current balance ${fromLamports(balanceLamports).toFixed(6)} SOL.`
      );
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

    const metadataImage =
      String(
        metadataJson?.metadata?.image ||
          metadataJson?.image ||
          metadataJson?.imageUri ||
          ""
      ).trim() || null;

    const deploySigner = keypairFromBase58(decryptDepositSecret(reservation.secret_key_base58));
    const mintKeypair = Keypair.generate();
    const txBytes = await buildDeployLocalTx({
      publicKey: deploySigner.publicKey.toBase58(),
      action: "create",
      tokenMetadata: {
        name,
        symbol,
        uri: metadataUri,
      },
      mint: mintKeypair.publicKey.toBase58(),
      denominatedInSol: "true",
      amount: initialBuySol,
      slippage: 10,
      priorityFee: 0.0005,
      pool: "pump",
      isMayhemMode: mayhemMode ? "true" : "false",
    });

    const tx = VersionedTransaction.deserialize(txBytes);
    tx.sign([mintKeypair, deploySigner]);
    const raw = Buffer.from(tx.serialize());
    const signature = await getConnection().sendRawTransaction(raw, {
      skipPreflight: false,
      maxRetries: 5,
    });

    const mint = mintKeypair.publicKey.toBase58();
    await pool.query(
      `
        UPDATE deploy_wallet_reservations
        SET
          status = 'deployed',
          balance_lamports = $2,
          deployed_mint = $3,
          deploy_signature = $4,
          last_error = NULL
        WHERE id = $1
      `,
      [reservation.id, balanceLamports, mint, signature]
    );

    const recordData = await recordDeployFromChain(deployUserId, {
      mint,
      symbol,
      name,
      pictureUrl: metadataImage || "",
      signature,
      autoAttach,
      selectedBot: autoAttach ? body.selectedBot : "",
      deployWalletPubkey: deploySigner.publicKey.toBase58(),
      deployWalletPrivateKeyBase58: bs58.encode(deploySigner.secretKey),
    });

    return {
      ok: true,
      mint,
      signature,
      metadataUri,
      metadataImage,
      deployWallet: deploySigner.publicKey.toBase58(),
      remainingSol: fromLamports(
        Math.max(0, balanceLamports - Number(reservation.required_lamports || 0))
      ),
      pumpfunUrl: `https://pump.fun/coin/${mint}`,
      solscanTx: `https://solscan.io/tx/${signature}`,
      solscanMint: `https://solscan.io/token/${mint}`,
      autoAttached: Boolean(recordData?.autoAttached),
      attachedToken: recordData?.attachedToken || null,
    };
  } catch (error) {
    await pool.query(
      `
        UPDATE deploy_wallet_reservations
        SET status = 'reserved', last_error = $2
        WHERE id = $1
      `,
      [reservation.id, String(error?.message || error)]
    ).catch(() => {});
    throw error;
  }
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
  const deployWalletPubkey = String(payload.deployWalletPubkey || "").trim();
  const deployWalletPrivateKey = String(payload.deployWalletPrivateKeyBase58 || "").trim();

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

  if (
    deployUserId &&
    attachedToken?.id &&
    deployWalletPubkey &&
    deployWalletPrivateKey
  ) {
    await pool.query(
      `
        UPDATE tokens
        SET
          deployed_via_ember = TRUE,
          deploy_wallet_pubkey = $1,
          deploy_wallet_secret_key_base58 = $2
        WHERE user_id = $3 AND id = $4
      `,
      [
        deployWalletPubkey,
        encryptDepositSecret(deployWalletPrivateKey),
        deployUserId,
        attachedToken.id,
      ]
    );
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
  const scope = await resolveUserAccessScopeFromPool(userId);
  const mint = String(payload.mint || "").trim();
  if (mint.length < 32) {
    throw new Error("A valid mint address is required.");
  }

  const tokenCountPrecheck = await pool.query(
    "SELECT COUNT(*)::int AS c FROM tokens WHERE user_id = $1 AND disconnected = FALSE",
    [scope.ownerUserId]
  );
  if (tokenCountPrecheck.rows[0].c >= config.maxTokensPerAccount) {
    throw new Error(`Max ${config.maxTokensPerAccount} burners per account reached.`);
  }

  const existsPrecheck = await pool.query(
    "SELECT id, disconnected FROM tokens WHERE user_id = $1 AND mint = $2 LIMIT 1",
    [scope.ownerUserId, mint]
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
  const moduleType = isTradeBotModuleType(selectedBot) ? selectedBot : MODULE_TYPES.burn;
  const tradeBotOverrides = {
    claimIntervalSec: sanitizeInterval(payload.claimSec, claimSec),
    tradeWalletCount: Math.floor(sanitizeRange(payload.tradeWalletCount, 1, 1, 5)),
    speed: sanitizeRange(payload.speed, 35, 0, 100),
    aggression: sanitizeRange(payload.aggression, 35, 0, 100),
    minTradeSol: sanitizeSol(payload.minTradeSol, 0.01, 0.001, 10),
    maxTradeSol: sanitizeSol(payload.maxTradeSol, 0.05, 0.001, 100),
    targetInventoryPct: sanitizeRange(payload.targetInventoryPct, 50, 20, 80),
  };
  if (payload.claimEnabled !== undefined) {
    tradeBotOverrides.claimEnabled = Boolean(payload.claimEnabled);
  }
  const initialConfig =
    isTradeBotModuleType(moduleType)
      ? mergeModuleConfig(moduleType, null, tradeBotOverrides)
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
      [scope.ownerUserId]
    );

    if (tokenCountRes.rows[0].c >= config.maxTokensPerAccount) {
      throw new Error(`Max ${config.maxTokensPerAccount} burners per account reached.`);
    }

    const existsRes = await client.query(
      "SELECT * FROM tokens WHERE user_id = $1 AND mint = $2 LIMIT 1 FOR UPDATE",
      [scope.ownerUserId, mint]
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
        const generatedDeposit = await consumeReservedDepositAddress(client, scope.ownerUserId, pendingDepositId);
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
          [tokenId, scope.ownerUserId, deposit, storedDepositSecret]
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
        [scope.ownerUserId, tokenId, symbol, `Token re-attached: ${symbol} reconnected in paused mode. Configure settings, then start.`]
      );
    } else {
      const generatedDeposit = pendingDepositId
        ? await consumeReservedDepositAddress(client, scope.ownerUserId, pendingDepositId)
        : await generatePreferredVanityDeposit();
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
          scope.ownerUserId,
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
        [tokenId, scope.ownerUserId, deposit, storedDepositSecret]
      );

      await client.query(
        `
          INSERT INTO token_events (user_id, token_id, token_symbol, event_type, message, tx)
          VALUES ($1, $2, $3, 'claim', $4, NULL)
        `,
        [scope.ownerUserId, tokenId, symbol, `Token attached: ${symbol} created in paused mode. Configure settings, then start.`]
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
      [makeId("mod"), scope.ownerUserId, tokenId, moduleType, JSON.stringify(initialConfig)]
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
  let postCommitVolumeReconcile = null;

  const token = await withTx(async (client) => {
    const scope = await resolveUserAccessScope(client, userId);
    const currentRes = await client.query("SELECT * FROM tokens WHERE user_id = $1 AND id = $2 LIMIT 1", [
      scope.ownerUserId,
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
    const requestedBot = normalizeModuleType(
      payload.selectedBot === undefined ? current.selected_bot : payload.selectedBot,
      current.selected_bot || MODULE_TYPES.burn
    );
    if (requestedBot !== previousBot && previousActive) {
      throw new Error("Pause the bot before switching bot mode.");
    }
    const claimSec = payload.claimSec === undefined ? Number(current.claim_sec) : sanitizeInterval(payload.claimSec, Number(current.claim_sec));
    const burnSec = payload.burnSec === undefined ? Number(current.burn_sec) : sanitizeInterval(payload.burnSec, Number(current.burn_sec));
    const splits = payload.splits === undefined ? Number(current.splits) : sanitizeSplits(payload.splits, Number(current.splits));
    const active = payload.active === undefined ? current.active : Boolean(payload.active);
    const selectedBot = requestedBot;
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

    const moduleType = isTradeBotModuleType(selectedBot) ? selectedBot : MODULE_TYPES.burn;
    const baseConfig =
      moduleType === MODULE_TYPES.volume
        ? defaultVolumeModuleConfig()
        : moduleType === MODULE_TYPES.marketMaker
          ? defaultMarketMakerModuleConfig()
        : moduleType === MODULE_TYPES.dca
          ? defaultDcaModuleConfig()
        : moduleType === MODULE_TYPES.rekindle
          ? defaultRekindleModuleConfig()
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
    if (payload.tradeWalletCount !== undefined && isTradeBotModuleType(moduleType)) {
      nextConfig.tradeWalletCount = Math.floor(
        sanitizeRange(payload.tradeWalletCount, 1, 1, 5)
      );
    }
    if (payload.speed !== undefined && moduleType === MODULE_TYPES.volume) {
      nextConfig.speed = sanitizeRange(payload.speed, 35, 0, 100);
    }
    if (payload.aggression !== undefined && isTradeBotModuleType(moduleType)) {
      nextConfig.aggression = sanitizeRange(payload.aggression, 35, 0, 100);
    }
    if (payload.minTradeSol !== undefined && isTradeBotModuleType(moduleType)) {
      nextConfig.minTradeSol = sanitizeSol(payload.minTradeSol, 0.01, 0.001, 10);
    }
    if (payload.maxTradeSol !== undefined && isTradeBotModuleType(moduleType)) {
      nextConfig.maxTradeSol = sanitizeSol(payload.maxTradeSol, 0.05, 0.001, 100);
    }
    if (payload.targetInventoryPct !== undefined && moduleType === MODULE_TYPES.marketMaker) {
      nextConfig.targetInventoryPct = sanitizeRange(payload.targetInventoryPct, 50, 20, 80);
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
    let effectiveModuleConfig = { ...baseConfig, ...nextConfig };
    if (!existingModuleRes.rowCount) {
      moduleId = makeId("mod");
      await client.query(
        `
          INSERT INTO bot_modules (id, user_id, token_id, module_type, enabled, config_json, state_json, next_run_at)
          VALUES ($1, $2, $3, $4, $5, $6::jsonb, '{}'::jsonb, NOW())
        `,
        [
          moduleId,
          scope.ownerUserId,
          tokenId,
          moduleType,
          active,
          JSON.stringify(effectiveModuleConfig),
        ]
      );
    } else {
      moduleId = String(existingModuleRes.rows[0].id || "");
      const shouldResetTradeBotState =
        isTradeBotModuleType(moduleType) &&
        shouldStartNow;
      const merged = mergeModuleConfig(moduleType, existingModuleRes.rows[0].config_json, {
        ...baseConfig,
        ...nextConfig,
      });
      effectiveModuleConfig = merged;
      await client.query(
        `
          UPDATE bot_modules
          SET enabled = $1,
              config_json = $2::jsonb,
              state_json = CASE WHEN $4 THEN '{}'::jsonb ELSE state_json END
          WHERE id = $3
        `,
        [active, JSON.stringify(merged), existingModuleRes.rows[0].id, shouldResetTradeBotState]
      );
    }

    if (isTradeBotModuleType(moduleType) && moduleId && payload.tradeWalletCount !== undefined) {
      postCommitVolumeReconcile = {
        userId: scope.ownerUserId,
        tokenId,
        moduleType,
      };
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
        scope.ownerUserId,
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
        scope.ownerUserId,
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

  if (postCommitVolumeReconcile) {
    void (async () => {
      try {
        await withTx(async (client) => {
          const moduleRes = await client.query(
            `
              SELECT m.id AS module_id, m.module_type, m.config_json, t.user_id, t.id AS token_id, t.symbol, t.mint, t.active
              FROM bot_modules m
              JOIN tokens t ON t.id = m.token_id
              WHERE t.user_id = $1 AND t.id = $2 AND m.module_type = $3
              LIMIT 1
            `,
            [postCommitVolumeReconcile.userId, postCommitVolumeReconcile.tokenId, postCommitVolumeReconcile.moduleType]
          );
          if (!moduleRes.rowCount) return;
          const row = moduleRes.rows[0];
          const runtimeModuleType = normalizeModuleType(row.module_type, postCommitVolumeReconcile.moduleType);
          const configJson = mergeModuleConfig(runtimeModuleType, row.config_json, {});
          const reconciled = await reconcileVolumeTradeWalletCount(
            client,
            {
              module_id: row.module_id,
              module_type: runtimeModuleType,
              user_id: row.user_id,
              token_id: row.token_id,
              symbol: row.symbol,
              mint: row.mint,
            },
            configJson,
            Boolean(row.active)
          );

          const walletTxCreated = Math.max(0, Number(reconciled?.txCreated || 0));
          if (walletTxCreated > 0) {
            await client.query(
              `
                UPDATE tokens
                SET tx_count = tx_count + $1
                WHERE id = $2
              `,
              [walletTxCreated, row.token_id]
            );
          }

          const added = Math.max(0, Number(reconciled?.added || 0));
          const removed = Math.max(0, Number(reconciled?.removed || 0));
          if (added > 0 || removed > 0) {
            const parts = [];
            if (added > 0) parts.push(`+${added}`);
            if (removed > 0) parts.push(`-${removed}`);
            await insertEvent(
              client,
              row.user_id,
              row.token_id,
              String(row.symbol || ""),
              "status",
              `${moduleTypeLabel(runtimeModuleType)} wallets updated (${parts.join(" / ")}).`,
              null,
              {
                moduleType: runtimeModuleType,
                idempotencyKey: `status:wallets:${row.token_id}:${Date.now()}`,
              }
            );
          }
        });
      } catch (error) {
        console.warn(`[trade-wallets] reconcile failed for ${postCommitVolumeReconcile.tokenId}: ${error?.message || error}`);
      }
    })();
  }

  return token;
}

export async function withdrawBurnFunds(userId, tokenId, payload = {}) {
  const tokenIdText = String(tokenId || "").trim();
  if (!tokenIdText) throw new Error("Token id is required.");
  const destination = normalizePubkeyString(payload.destinationWallet || payload.destination || "");
  if (!destination) {
    throw new Error("Destination wallet is required.");
  }

  return withTx(async (client) => {
    const scope = await resolveUserAccessScope(client, userId);
    assertOwnerPermission(scope, "Managers cannot withdraw burn wallet balances.");
    const { tokenRow, moduleRow } = await getBurnTokenModule(client, scope.ownerUserId, tokenIdText, {
      forUpdate: true,
    });
    if (tokenRow.active) {
      throw new Error("Pause the burn bot before withdrawing.");
    }

    const lockKey = `${tokenIdText}:${MODULE_TYPES.burn}`;
    const lock = await withTokenModuleLock(client, lockKey, async () => {
      const connection = getConnection();
      const depositSecret = await getTokenDepositSigningKey(scope.ownerUserId, tokenIdText);
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
          scope.ownerUserId,
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
        scope.ownerUserId,
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
  const res = await client.query(
    `
      INSERT INTO token_events (user_id, token_id, token_symbol, module_type, event_type, amount, message, tx, idempotency_key, metadata)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      ON CONFLICT DO NOTHING
    `,
    [userId, tokenId, symbol, moduleType, type, amount, message, tx, idempotencyKey, metadata ? JSON.stringify(metadata) : null]
  );
  if (res.rowCount > 0) {
    await queueTelegramAlertsForEvent(
      client,
      userId,
      tokenId,
      symbol,
      moduleType,
      type,
      message,
      tx,
      amount
    );
  }
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

async function addProtocolFeeCredit(client, credit = {}) {
  const tokenIdRaw = credit?.tokenId;
  const tokenId = tokenIdRaw == null ? null : String(tokenIdRaw).trim();
  const userIdNum = Number(credit?.userId);
  const userId = Number.isFinite(userIdNum) && userIdNum > 0 ? Math.floor(userIdNum) : null;
  const symbol = String(credit?.symbol || "UNKNOWN").trim().toUpperCase() || "UNKNOWN";
  const lamports = Math.max(0, Math.floor(Number(credit?.lamports || 0)));
  if (lamports <= 0) return;

  if (tokenId) {
    await client.query(
      `
        INSERT INTO protocol_fee_credits (
          source_token_id,
          source_user_id,
          source_token_symbol,
          total_lamports,
          pending_lamports,
          spent_lamports
        )
        VALUES ($1, $2, $3, $4, $4, 0)
        ON CONFLICT (source_token_id) WHERE source_token_id IS NOT NULL
        DO UPDATE
        SET
          source_user_id = COALESCE(protocol_fee_credits.source_user_id, EXCLUDED.source_user_id),
          source_token_symbol = EXCLUDED.source_token_symbol,
          total_lamports = protocol_fee_credits.total_lamports + EXCLUDED.total_lamports,
          pending_lamports = protocol_fee_credits.pending_lamports + EXCLUDED.pending_lamports,
          updated_at = NOW()
      `,
      [tokenId, userId, symbol, lamports]
    );
    return;
  }

  await client.query(
    `
      INSERT INTO protocol_fee_credits (
        source_token_id,
        source_user_id,
        source_token_symbol,
        total_lamports,
        pending_lamports,
        spent_lamports
      )
      VALUES (NULL, $1, $2, $3, $3, 0)
    `,
    [userId, symbol, lamports]
  );
}

async function consumeProtocolFeeCredits(client, requestedLamports) {
  const requested = Math.max(0, Math.floor(Number(requestedLamports || 0)));
  if (requested <= 0) {
    return { requestedLamports: 0, consumedLamports: 0, allocations: [] };
  }

  let consumed = 0;
  const allocations = [];

  await client.query("BEGIN");
  try {
    const rowsRes = await client.query(
      `
        SELECT
          id,
          source_token_id,
          source_user_id,
          source_token_symbol,
          pending_lamports
        FROM protocol_fee_credits
        WHERE pending_lamports > 0
        ORDER BY updated_at ASC, id ASC
        FOR UPDATE
      `
    );
    let remaining = requested;
    for (const row of rowsRes.rows) {
      if (remaining <= 0) break;
      const pending = Math.max(0, Math.floor(Number(row.pending_lamports || 0)));
      if (pending <= 0) continue;
      const take = Math.min(remaining, pending);
      if (take <= 0) continue;

      await client.query(
        `
          UPDATE protocol_fee_credits
          SET
            pending_lamports = pending_lamports - $1,
            spent_lamports = spent_lamports + $1,
            updated_at = NOW()
          WHERE id = $2
        `,
        [take, row.id]
      );

      consumed += take;
      remaining -= take;
      allocations.push({
        tokenId: row.source_token_id ? String(row.source_token_id) : null,
        userId: row.source_user_id == null ? null : Number(row.source_user_id),
        symbol: String(row.source_token_symbol || "UNKNOWN").trim().toUpperCase() || "UNKNOWN",
        lamports: take,
      });
    }

    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  }

  return {
    requestedLamports: requested,
    consumedLamports: consumed,
    allocations,
  };
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
        : moduleType === MODULE_TYPES.marketMaker
          ? defaultMarketMakerModuleConfig()
        : moduleType === MODULE_TYPES.dca
          ? defaultDcaModuleConfig()
        : moduleType === MODULE_TYPES.rekindle
          ? defaultRekindleModuleConfig()
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
  moduleType = MODULE_TYPES.volume,
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
        moduleType,
        amount: fromLamports(topup),
        idempotencyKey: `${eventPrefix}:gas:${walletPubkey}`,
      }
    );
  }
  return { toppedUp: topup, signature: sig };
}

async function ensureVolumeTradeWallets(client, moduleRow, count) {
  // `tradeWalletCount` is total volume wallets including deposit wallet.
  // So additional trade wallets in this table are (count - 1).
  const desired = Math.max(0, Math.min(4, Math.floor(Number(count) || 1) - 1));
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
  const prefixCase = preferredPrefixSortCase("prefix");
  for (let i = 0; i < need; i += 1) {
    const id = makeId("vw");
    let walletPubkey = "";
    let storedSecret = "";
    const poolRes = await client.query(
      `
        SELECT id, prefix, deposit_pubkey, secret_key_base58
        FROM token_deposit_pool
        WHERE status = 'available'
        ORDER BY ${prefixCase}, created_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      `
    );
    if (poolRes.rowCount) {
      const picked = poolRes.rows[0];
      walletPubkey = String(picked.deposit_pubkey || "");
      storedSecret = String(picked.secret_key_base58 || "");
      await client.query("DELETE FROM token_deposit_pool WHERE id = $1", [picked.id]);
    } else {
      const kp = await generatePreferredVanityDeposit();
      walletPubkey = kp.pubkey;
      storedSecret = encryptDepositSecret(kp.secretKeyBase58);
    }
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
        walletPubkey,
        storedSecret,
      ]
    );
    created.push({
      id,
      label: `trade-${existing.length + i + 1}`,
      wallet_pubkey: walletPubkey,
      secret_key_base58: storedSecret,
      funded_from_deposit_lamports: 0,
    });
  }

  return existing.concat(created);
}

async function reconcileVolumeTradeWalletCount(client, moduleRow, configJson, tokenActive) {
  const moduleType = normalizeModuleType(moduleRow.module_type, MODULE_TYPES.volume);
  // `tradeWalletCount` includes deposit wallet.
  const desired = Math.max(0, Math.min(4, Math.floor(Number(configJson?.tradeWalletCount) || 1) - 1));
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

  if (existing.length < desired) {
    const wallets = await ensureVolumeTradeWallets(
      client,
      moduleRow,
      Number(configJson?.tradeWalletCount || 1)
    );
    return {
      wallets,
      added: Math.max(0, wallets.length - existing.length),
      removed: 0,
      txCreated: 0,
    };
  }

  if (existing.length <= desired) {
    return { wallets: existing, added: 0, removed: 0, txCreated: 0 };
  }

  if (tokenActive) {
    throw new Error(`Pause the ${moduleTypeLabel(moduleType).toLowerCase()} before reducing trade wallet count.`);
  }

  const keep = existing.slice(0, desired);
  const remove = existing.slice(desired);
  const connection = getConnection();
  const depositSecret = await getTokenDepositSigningKey(moduleRow.user_id, moduleRow.token_id);
  const depositSigner = keypairFromBase58(depositSecret);
  const eventPrefix = `wallet-resize:${moduleRow.module_id}:${Date.now()}`;
  let txCreated = 0;
  const removableIds = [];

  for (const wallet of remove) {
    const tradeSigner = keypairFromBase58(decryptDepositSecret(wallet.secret_key_base58));
    const label = String(wallet.label || wallet.wallet_pubkey || "trade");
    const tokenBal = await getOwnerTokenBalanceUi(connection, tradeSigner.publicKey, moduleRow.mint);

    if (tokenBal > 0.000001) {
      const sellAmount = Number(tokenBal.toFixed(6));
      const sellSig = await pumpPortalTrade({
        connection,
        signer: tradeSigner,
        mint: moduleRow.mint,
        action: "sell",
        amount: sellAmount,
        denominatedInSol: false,
        slippage: Math.max(1, Math.floor(Number(configJson.slippageBps || 1000) / 100)),
        pool: configJson.pool || "auto",
      });
      if (sellSig) txCreated += 1;
      await insertEvent(
        client,
        moduleRow.user_id,
        moduleRow.token_id,
        moduleRow.symbol,
        "sell",
        `Wallet downsize sold ${sellAmount.toFixed(6)} ${moduleRow.symbol} from ${label}`,
        sellSig,
        {
          moduleType,
          amount: sellAmount,
          idempotencyKey: `${eventPrefix}:sell:${wallet.wallet_pubkey}`,
        }
      );
    }

    const sweep = await sendAllSolTransfer(
      connection,
      tradeSigner,
      depositSigner.publicKey.toBase58()
    );
    if (sweep.signature && sweep.sentLamports > 0) {
      txCreated += 1;
      await insertEvent(
        client,
        moduleRow.user_id,
        moduleRow.token_id,
        moduleRow.symbol,
        "transfer",
        `Wallet downsize swept ${fromLamports(sweep.sentLamports).toFixed(6)} SOL from ${label} to deposit`,
        sweep.signature,
        {
          moduleType,
          amount: fromLamports(sweep.sentLamports),
          idempotencyKey: `${eventPrefix}:sweep:${wallet.wallet_pubkey}`,
        }
      );
    }

    const remainingToken = await getOwnerTokenBalanceUi(connection, tradeSigner.publicKey, moduleRow.mint);
    if (remainingToken > 0.000001) {
      throw new Error(`Cannot remove ${label}: token balance remains after sweep.`);
    }
    removableIds.push(String(wallet.id));
  }

  if (removableIds.length > 0) {
    await client.query(
      `
        DELETE FROM volume_trade_wallets
        WHERE module_id = $1
          AND id = ANY($2::text[])
      `,
      [moduleRow.module_id, removableIds]
    );
  }

  const refreshedRes = await client.query(
    `
      SELECT id, label, wallet_pubkey, secret_key_base58, funded_from_deposit_lamports
      FROM volume_trade_wallets
      WHERE module_id = $1
      ORDER BY created_at ASC
    `,
    [moduleRow.module_id]
  );

  return {
    wallets: refreshedRes.rows || keep,
    added: 0,
    removed: removableIds.length,
    txCreated,
  };
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
    await addProtocolFeeCredit(client, {
      tokenId: row.token_id,
      userId: row.user_id,
      symbol: row.symbol,
      lamports: devLamports,
    });
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
  let burnedActual = 0;
  if (bought > 0) {
    burnSig = await sendTokenToIncinerator(connection, signer, row.mint, bought);
    if (burnSig) {
      const tokenAfterBurn = await getOwnerTokenBalanceUi(connection, signer.publicKey, row.mint);
      burnedActual = Math.max(0, afterToken - tokenAfterBurn);
      txCreated += 1;
      await insertEvent(
        client,
        row.user_id,
        row.token_id,
        row.symbol,
        "burn",
        `Incinerated ${fmtInt(burnedActual.toFixed(2))} ${row.symbol}`,
        burnSig,
        { moduleType: MODULE_TYPES.burn, amount: burnedActual, idempotencyKey: `${eventPrefix}:burn` }
      );
    }
  }

  await client.query(
    `
      UPDATE tokens
      SET burned = burned + $1,
          tx_count = tx_count + $2
      WHERE id = $3
    `,
    [Math.max(0, Math.floor(burnedActual)), txCreated, row.token_id]
  );

  const balanceAfter = await connection.getBalance(signer.publicKey, "confirmed");
  state.lastBalanceLamports = Math.max(0, balanceAfter - reserveLamports);
  await upsertModuleState(client, row.module_id, state, null);
  return { txCreated, burnedAmount: burnedActual };
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
  const executionWallets = [
    {
      label: "deposit",
      wallet_pubkey: depositSigner.publicKey.toBase58(),
      signer: depositSigner,
      reserveLamports,
    },
    ...wallets.map((wallet) => ({
      ...wallet,
      signer: keypairFromBase58(decryptDepositSecret(wallet.secret_key_base58)),
      reserveLamports: tradeReserveLamports,
    })),
  ];

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
      await addProtocolFeeCredit(client, {
        tokenId: row.token_id,
        userId: row.user_id,
        symbol: row.symbol,
        lamports: devLamports,
      });
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

  const wallet = executionWallets[Math.floor(Math.random() * executionWallets.length)];
  if (wallet) {
    const tradeSigner = wallet.signer;
    const walletSolLamports = await connection.getBalance(tradeSigner.publicKey, "confirmed");
    const walletSol = fromLamports(walletSolLamports);
    const walletReserveSol = fromLamports(Math.max(0, Number(wallet.reserveLamports || 0)));
    const tokenBal = await getOwnerTokenBalanceUi(connection, tradeSigner.publicKey, row.mint);
    let action = Math.random() < 0.5 ? "buy" : "sell";
    if (tokenBal <= 0.000001) action = "buy";
    if (walletSol < Number(configJson.minTradeSol || 0.01) + walletReserveSol) action = "sell";

    if (action === "buy") {
      const tradeSol = pickTradeSol(configJson, walletSol, walletReserveSol);
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

async function runDcaExecutor(client, row) {
  const moduleType = MODULE_TYPES.dca;
  const moduleLabel = getTradeBotDisplayName(moduleType);
  const connection = getConnection();
  const configJson = mergeModuleConfig(moduleType, row.config_json, {});
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
  const executionWallets = [
    {
      label: "deposit",
      wallet_pubkey: depositSigner.publicKey.toBase58(),
      signer: depositSigner,
      reserveLamports,
      reserveSol: fromLamports(reserveLamports),
    },
    ...wallets.map((wallet) => ({
      ...wallet,
      signer: keypairFromBase58(decryptDepositSecret(wallet.secret_key_base58)),
      reserveLamports: tradeReserveLamports,
      reserveSol: fromLamports(tradeReserveLamports),
    })),
  ];

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
        moduleType,
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
            `${moduleLabel} claimed creator rewards (${fromLamports(sharingLamports).toFixed(6)} SOL)`,
            sharingClaim.signature,
            {
              moduleType,
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
            `${moduleLabel} sharing-claim path failed: ${sharingError.message}`,
            null,
            { moduleType, idempotencyKey: `${eventPrefix}:claim:sharing:error` }
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
            moduleType,
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
            `${moduleLabel} claim skipped: no creator rewards available yet.`,
            claimSig,
            {
              moduleType,
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
          `${moduleLabel} claimed creator rewards (${claimNetSol.toFixed(6)} SOL)`,
          claimSig,
          {
            moduleType,
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
          `${moduleLabel} claim skipped: no creator rewards available yet.`,
          null,
          { moduleType, idempotencyKey: `${eventPrefix}:claim:skip` }
        );
      } else {
        await insertEvent(
          client,
          row.user_id,
          row.token_id,
          row.symbol,
          "error",
          `${moduleLabel} claim failed: ${error.message}`,
          null,
          { moduleType, idempotencyKey: `${eventPrefix}:claim:error` }
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
        moduleType,
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
        `${moduleLabel} fee to treasury (${fromLamports(treasuryLamports).toFixed(6)} SOL)`,
        sig,
        { moduleType, amount: fromLamports(treasuryLamports), idempotencyKey: `${eventPrefix}:fee:treasury` }
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
        `${moduleLabel} fee to dev burn wallet (${fromLamports(devLamports).toFixed(6)} SOL)`,
        sig,
        { moduleType, amount: fromLamports(devLamports), idempotencyKey: `${eventPrefix}:fee:dev` }
      );
      await addProtocolFeeCredit(client, {
        tokenId: row.token_id,
        userId: row.user_id,
        symbol: row.symbol,
        lamports: devLamports,
      });
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
        moduleType,
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
        { moduleType, idempotencyKey: `${eventPrefix}:gas:error:${wallet.wallet_pubkey}` }
      );
    }
  }

  const walletStates = [];
  for (const wallet of executionWallets) {
    const solLamports = await connection.getBalance(wallet.signer.publicKey, "confirmed");
    const sol = fromLamports(solLamports);
    const reserveSol = fromLamports(Math.max(0, Number(wallet.reserveLamports || 0)));
    const tokenBal = await getOwnerTokenBalanceUi(connection, wallet.signer.publicKey, row.mint);
    walletStates.push({
      ...wallet,
      solLamports,
      sol,
      reserveSol,
      spendableSol: Math.max(0, sol - reserveSol - 0.0005),
      tokenBal,
    });
  }

  const plan = buildDcaPlan({ configJson, walletStates });
  if (plan.wallet && plan.amountSol > 0.0005) {
    try {
      const sig = await pumpPortalTrade({
        connection,
        signer: plan.wallet.signer,
        mint: row.mint,
        action: "buy",
        amount: plan.amountSol,
        denominatedInSol: true,
        slippage: Math.max(1, Math.floor(Number(configJson.slippageBps || 1000) / 100)),
        pool: configJson.pool || "auto",
      });
      txCreated += 1;
      state.lastBuyAt = now;
      await insertEvent(
        client,
        row.user_id,
        row.token_id,
        row.symbol,
        "buy",
        `DCA buy (${plan.amountSol.toFixed(4)} SOL) via ${plan.wallet.label}`,
        sig,
        { moduleType, amount: plan.amountSol, idempotencyKey: `${eventPrefix}:buy:${plan.wallet.wallet_pubkey}` }
      );
    } catch (error) {
      await insertEvent(
        client,
        row.user_id,
        row.token_id,
        row.symbol,
        "error",
        `DCA buy failed (${plan.wallet.label}): ${error.message}`,
        null,
        { moduleType, idempotencyKey: `${eventPrefix}:buy:error:${plan.wallet.wallet_pubkey}` }
      );
    }
  } else if (Number(state.lastIdleAt || 0) + 300_000 <= now) {
    state.lastIdleAt = now;
    await insertEvent(
      client,
      row.user_id,
      row.token_id,
      row.symbol,
      "status",
      "DCA waiting for fresh spendable SOL above reserve.",
      null,
      { moduleType, idempotencyKey: `${eventPrefix}:idle` }
    );
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

async function runRekindleExecutor(client, row) {
  const moduleType = MODULE_TYPES.rekindle;
  const moduleLabel = getTradeBotDisplayName(moduleType);
  const connection = getConnection();
  const configJson = mergeModuleConfig(moduleType, row.config_json, {});
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
  const executionWallets = [
    {
      label: "deposit",
      wallet_pubkey: depositSigner.publicKey.toBase58(),
      signer: depositSigner,
      reserveLamports,
      reserveSol: fromLamports(reserveLamports),
    },
    ...wallets.map((wallet) => ({
      ...wallet,
      signer: keypairFromBase58(decryptDepositSecret(wallet.secret_key_base58)),
      reserveLamports: tradeReserveLamports,
      reserveSol: fromLamports(tradeReserveLamports),
    })),
  ];

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
        moduleType,
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
            `${moduleLabel} claimed creator rewards (${fromLamports(sharingLamports).toFixed(6)} SOL)`,
            sharingClaim.signature,
            {
              moduleType,
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
            `${moduleLabel} sharing-claim path failed: ${sharingError.message}`,
            null,
            { moduleType, idempotencyKey: `${eventPrefix}:claim:sharing:error` }
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
            moduleType,
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
            `${moduleLabel} claim skipped: no creator rewards available yet.`,
            claimSig,
            {
              moduleType,
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
          `${moduleLabel} claimed creator rewards (${claimNetSol.toFixed(6)} SOL)`,
          claimSig,
          {
            moduleType,
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
          `${moduleLabel} claim skipped: no creator rewards available yet.`,
          null,
          { moduleType, idempotencyKey: `${eventPrefix}:claim:skip` }
        );
      } else {
        await insertEvent(
          client,
          row.user_id,
          row.token_id,
          row.symbol,
          "error",
          `${moduleLabel} claim failed: ${error.message}`,
          null,
          { moduleType, idempotencyKey: `${eventPrefix}:claim:error` }
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
        moduleType,
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
        `${moduleLabel} fee to treasury (${fromLamports(treasuryLamports).toFixed(6)} SOL)`,
        sig,
        { moduleType, amount: fromLamports(treasuryLamports), idempotencyKey: `${eventPrefix}:fee:treasury` }
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
        `${moduleLabel} fee to dev burn wallet (${fromLamports(devLamports).toFixed(6)} SOL)`,
        sig,
        { moduleType, amount: fromLamports(devLamports), idempotencyKey: `${eventPrefix}:fee:dev` }
      );
      await addProtocolFeeCredit(client, {
        tokenId: row.token_id,
        userId: row.user_id,
        symbol: row.symbol,
        lamports: devLamports,
      });
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
        moduleType,
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
        { moduleType, idempotencyKey: `${eventPrefix}:gas:error:${wallet.wallet_pubkey}` }
      );
    }
  }

  const walletStates = [];
  for (const wallet of executionWallets) {
    const solLamports = await connection.getBalance(wallet.signer.publicKey, "confirmed");
    const sol = fromLamports(solLamports);
    const reserveSol = fromLamports(Math.max(0, Number(wallet.reserveLamports || 0)));
    const tokenBal = await getOwnerTokenBalanceUi(connection, wallet.signer.publicKey, row.mint);
    walletStates.push({
      ...wallet,
      solLamports,
      sol,
      reserveSol,
      spendableSol: Math.max(0, sol - reserveSol - 0.0005),
      tokenBal,
    });
  }

  const marketState = await fetchMarketStateForMint(row.mint);
  state.peakPriceSol = Math.max(
    Number(state.peakPriceSol || 0),
    Number(marketState?.priceSol || 0)
  );
  const plan = buildRekindlePlan({ configJson, marketState, walletStates, state, now });

  if (plan.wallet && plan.amountSol > 0.0005) {
    try {
      const sig = await pumpPortalTrade({
        connection,
        signer: plan.wallet.signer,
        mint: row.mint,
        action: "buy",
        amount: plan.amountSol,
        denominatedInSol: true,
        slippage: Math.max(1, Math.floor(Number(configJson.slippageBps || 1200) / 100)),
        pool: getTradeBotPool(configJson, marketState),
      });
      txCreated += 1;
      state.lastBuyAt = now;
      state.nextDipBuyAt = now + Math.max(30, Number(configJson.cooldownSec || 135)) * 1000;
      state.lastBuyAnchorPriceSol = Number(plan.priceSol || 0);
      state.peakPriceSol = Number(plan.priceSol || state.peakPriceSol || 0);
      await insertEvent(
        client,
        row.user_id,
        row.token_id,
        row.symbol,
        "buy",
        `Rekindle buy (${plan.amountSol.toFixed(4)} SOL) after ${plan.drawdownPct.toFixed(1)}% pullback via ${plan.wallet.label}`,
        sig,
        { moduleType, amount: plan.amountSol, idempotencyKey: `${eventPrefix}:buy:${plan.wallet.wallet_pubkey}` }
      );
    } catch (error) {
      state.nextDipBuyAt = now + Math.max(30, Number(configJson.cooldownSec || 135)) * 1000;
      await insertEvent(
        client,
        row.user_id,
        row.token_id,
        row.symbol,
        "error",
        `Rekindle buy failed (${plan.wallet.label}): ${error.message}`,
        null,
        { moduleType, idempotencyKey: `${eventPrefix}:buy:error:${plan.wallet.wallet_pubkey}` }
      );
    }
  } else if (Number(state.lastIdleAt || 0) + 300_000 <= now) {
    state.lastIdleAt = now;
    const currentDrawdown = Number(plan.drawdownPct || 0).toFixed(1);
    const trigger = Number(plan.triggerPct || configJson.dipTriggerPct || 9).toFixed(1);
    const reason = plan.coolingDown
      ? `cooldown active until ${new Date(Number(plan.coolingDownUntil || now)).toLocaleTimeString()}`
      : !plan.sellPressure
        ? "sell pressure not dominant yet"
        : `current pullback ${currentDrawdown}% vs trigger ${trigger}%`;
    await insertEvent(
      client,
      row.user_id,
      row.token_id,
      row.symbol,
      "status",
      `Rekindle waiting: ${reason}.`,
      null,
      { moduleType, idempotencyKey: `${eventPrefix}:idle` }
    );
  }

  const depositAfter = await connection.getBalance(depositSigner.publicKey, "confirmed");
  state.lastDepositBalanceLamports = Math.max(0, depositAfter - reserveLamports);
  state.lastObservedPriceSol = Number(marketState?.priceSol || 0);
  state.lastObservedMarketCapUsd = Number(marketState?.marketCapUsd || 0);
  await upsertModuleState(client, row.module_id, state, null);

  await client.query(
    `
      UPDATE tokens
      SET tx_count = tx_count + $1
      WHERE id = $2
    `,
    [txCreated, row.token_id]
  );

  return { txCreated, drawdownPct: Number(plan.drawdownPct || 0) };
}

function getTradeBotDisplayName(moduleType) {
  if (moduleType === MODULE_TYPES.marketMaker) return "MM";
  if (moduleType === MODULE_TYPES.dca) return "DCA";
  if (moduleType === MODULE_TYPES.rekindle) return "Rekindle";
  return "Volume";
}

function getTradeBotPool(configJson, marketState) {
  const configured = String(configJson.pool || "auto").trim().toLowerCase() || "auto";
  if (configured !== "auto") return configured;
  const hinted = String(marketState?.poolHint || "").trim().toLowerCase();
  if (hinted === "pump" || hinted === "raydium") return hinted;
  return "auto";
}

function buildMarketMakerPlan({ configJson, marketState, walletStates, lastDirection = "" }) {
  const targetInventoryPct = Math.max(20, Math.min(80, Number(configJson.targetInventoryPct || 50)));
  const inventoryBandPct = Math.max(4, Math.min(30, Number(configJson.inventoryBandPct || 10)));
  const childTrades = Math.max(1, Math.min(4, Math.floor(Number(configJson.childTrades || 2))));
  const priceSol = Math.max(0, Number(marketState?.priceSol || 0));
  const totalSpendableSol = walletStates.reduce((sum, item) => sum + Math.max(0, Number(item.spendableSol || 0)), 0);
  const totalToken = walletStates.reduce((sum, item) => sum + Math.max(0, Number(item.tokenBal || 0)), 0);
  const totalTokenValueSol = priceSol > 0 ? totalToken * priceSol : 0;
  const totalValueSol = totalSpendableSol + totalTokenValueSol;
  const inventoryPct = totalValueSol > 0 ? (totalTokenValueSol / totalValueSol) * 100 : 0;
  const flowBiasPct =
    Number(marketState?.sellsM5 || 0) > Number(marketState?.buysM5 || 0)
      ? Math.max(0, Number(configJson.buyPressureBiasPct || 0))
      : Number(marketState?.buysM5 || 0) > Number(marketState?.sellsM5 || 0)
        ? -Math.max(0, Number(configJson.sellPressureBiasPct || 0))
        : 0;
  const biasPct = (targetInventoryPct - inventoryPct) + flowBiasPct;
  let posture = "neutral";
  if (biasPct > inventoryBandPct) posture = "buy_bias";
  if (biasPct < -inventoryBandPct) posture = "sell_bias";

  const useCounts = new Map();
  const nextUsage = (walletPubkey) => Number(useCounts.get(walletPubkey) || 0);
  const markUsed = (walletPubkey) => useCounts.set(walletPubkey, nextUsage(walletPubkey) + 1);
  const buyCandidates = () =>
    walletStates
      .filter((item) => item.spendableSol >= Math.max(0.0005, Number(configJson.minTradeSol || 0.01)))
      .sort((a, b) => {
        const delta = Number(b.spendableSol || 0) - Number(a.spendableSol || 0);
        return Math.abs(delta) > 0.0000001 ? delta : nextUsage(a.wallet_pubkey) - nextUsage(b.wallet_pubkey);
      });
  const sellCandidates = () =>
    walletStates
      .filter((item) => item.tokenBal > 0.000001 && item.solLamports > getTxFeeSafetyLamports())
      .sort((a, b) => {
        const delta = Number(b.tokenBal || 0) - Number(a.tokenBal || 0);
        return Math.abs(delta) > 0.0000001 ? delta : nextUsage(a.wallet_pubkey) - nextUsage(b.wallet_pubkey);
      });

  const sides = [];
  if (posture === "buy_bias") {
    for (let i = 0; i < childTrades; i += 1) sides.push("buy");
    if (sellCandidates().length && childTrades > 1) sides[childTrades - 1] = "sell";
  } else if (posture === "sell_bias") {
    for (let i = 0; i < childTrades; i += 1) sides.push("sell");
    if (buyCandidates().length && childTrades > 1) sides[childTrades - 1] = "buy";
  } else {
    let side = lastDirection === "buy" ? "sell" : "buy";
    for (let i = 0; i < childTrades; i += 1) {
      sides.push(side);
      side = side === "buy" ? "sell" : "buy";
    }
  }

  const actions = [];
  for (const side of sides) {
    const candidates = side === "buy" ? buyCandidates() : sellCandidates();
    const wallet = candidates[0];
    if (!wallet) continue;
    markUsed(wallet.wallet_pubkey);

    if (side === "buy") {
      const amountSol = Number(
        Math.min(
          wallet.spendableSol,
          pickTradeSol(configJson, wallet.sol, wallet.reserveSol)
        ).toFixed(6)
      );
      if (amountSol >= 0.0005) {
        wallet.spendableSol = Math.max(0, wallet.spendableSol - amountSol);
        actions.push({ side: "buy", wallet, amountSol });
      }
      continue;
    }

    let sellAmount = 0;
    if (priceSol > 0) {
      const desiredNotionalSol = Math.min(
        Number(configJson.maxTradeSol || 0.05),
        Math.max(Number(configJson.minTradeSol || 0.01), wallet.tokenBal * priceSol)
      );
      const desiredToken = desiredNotionalSol / priceSol;
      const fractionCap = posture === "sell_bias" ? 0.7 : 0.35;
      sellAmount = Math.min(wallet.tokenBal, desiredToken, wallet.tokenBal * fractionCap);
    } else {
      const fraction = posture === "sell_bias" ? 0.45 : 0.22;
      sellAmount = wallet.tokenBal * fraction;
    }
    sellAmount = Number(Math.max(0, sellAmount).toFixed(6));
    if (sellAmount > 0.000001) {
      wallet.tokenBal = Math.max(0, wallet.tokenBal - sellAmount);
      actions.push({ side: "sell", wallet, amountToken: sellAmount });
    }
  }

  return {
    posture,
    biasPct,
    inventoryPct,
    targetInventoryPct,
    totalSpendableSol,
    totalToken,
    totalTokenValueSol,
    totalValueSol,
    marketCapUsd: Number(marketState?.marketCapUsd || 0),
    liquidityUsd: Number(marketState?.liquidityUsd || 0),
    priceSol,
    pool: getTradeBotPool(configJson, marketState),
    actions,
  };
}

function buildDcaPlan({ configJson, walletStates }) {
  const candidates = walletStates
    .filter((item) => item.spendableSol >= Math.max(0.0005, Number(configJson.minTradeSol || 0.01)))
    .sort((a, b) => Number(b.spendableSol || 0) - Number(a.spendableSol || 0));
  const wallet = candidates[0] || null;
  if (!wallet) {
    return {
      wallet: null,
      amountSol: 0,
    };
  }
  const amountSol = Number(
    Math.min(
      wallet.spendableSol,
      pickTradeSol(configJson, wallet.sol, wallet.reserveSol)
    ).toFixed(6)
  );
  return { wallet, amountSol };
}

function buildRekindlePlan({ configJson, marketState, walletStates, state = {}, now = Date.now() }) {
  const priceSol = Math.max(0, Number(marketState?.priceSol || 0));
  const peakPriceSol = Math.max(
    priceSol,
    Number(state.peakPriceSol || 0),
    Number(state.lastBuyAnchorPriceSol || 0)
  );
  const drawdownPct =
    peakPriceSol > 0 && priceSol > 0 ? Math.max(0, ((peakPriceSol - priceSol) / peakPriceSol) * 100) : 0;
  const triggerPct = Math.max(3, Math.min(18, Number(configJson.dipTriggerPct || 9)));
  const coolingDownUntil = Number(state.nextDipBuyAt || 0);
  const coolingDown = coolingDownUntil > now;
  const sellPressure = Number(marketState?.sellsM5 || 0) >= Number(marketState?.buysM5 || 0);
  const candidates = walletStates
    .filter((item) => item.spendableSol >= Math.max(0.0005, Number(configJson.minTradeSol || 0.01)))
    .sort((a, b) => Number(b.spendableSol || 0) - Number(a.spendableSol || 0));
  const wallet = candidates[0] || null;
  if (!wallet || priceSol <= 0 || coolingDown || drawdownPct < triggerPct || !sellPressure) {
    return {
      wallet: null,
      amountSol: 0,
      drawdownPct,
      triggerPct,
      peakPriceSol,
      priceSol,
      coolingDown,
      coolingDownUntil,
      sellPressure,
    };
  }
  const baseTradeSol = pickTradeSol(configJson, wallet.sol, wallet.reserveSol);
  const severity = Math.max(1, Math.min(1.8, drawdownPct / Math.max(0.01, triggerPct)));
  const amountSol = Number(Math.min(wallet.spendableSol, baseTradeSol * severity).toFixed(6));
  return {
    wallet,
    amountSol,
    drawdownPct,
    triggerPct,
    peakPriceSol,
    priceSol,
    coolingDown,
    coolingDownUntil,
    sellPressure,
  };
}

async function runMarketMakerExecutor(client, row) {
  const moduleType = MODULE_TYPES.marketMaker;
  const moduleLabel = getTradeBotDisplayName(moduleType);
  const connection = getConnection();
  const configJson = mergeModuleConfig(moduleType, row.config_json, {});
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

  if (Number(state.cooldownUntil || 0) > now) {
    await upsertModuleState(client, row.module_id, state, null);
    return { txCreated: 0, coolingDown: true };
  }

  const wallets = await ensureVolumeTradeWallets(client, row, configJson.tradeWalletCount || 1);
  const executionWallets = [
    {
      label: "deposit",
      wallet_pubkey: depositSigner.publicKey.toBase58(),
      signer: depositSigner,
      reserveLamports,
      reserveSol: fromLamports(reserveLamports),
    },
    ...wallets.map((wallet) => ({
      ...wallet,
      signer: keypairFromBase58(decryptDepositSecret(wallet.secret_key_base58)),
      reserveLamports: tradeReserveLamports,
      reserveSol: fromLamports(tradeReserveLamports),
    })),
  ];

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
        moduleType,
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
            `${moduleLabel} claimed creator rewards (${fromLamports(sharingLamports).toFixed(6)} SOL)`,
            sharingClaim.signature,
            {
              moduleType,
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
            `${moduleLabel} sharing-claim path failed: ${sharingError.message}`,
            null,
            { moduleType, idempotencyKey: `${eventPrefix}:claim:sharing:error` }
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
            moduleType,
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
            `${moduleLabel} claim skipped: no creator rewards available yet.`,
            claimSig,
            {
              moduleType,
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
          `${moduleLabel} claimed creator rewards (${claimNetSol.toFixed(6)} SOL)`,
          claimSig,
          {
            moduleType,
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
          `${moduleLabel} claim skipped: no creator rewards available yet.`,
          null,
          { moduleType, idempotencyKey: `${eventPrefix}:claim:skip` }
        );
      } else {
        await insertEvent(
          client,
          row.user_id,
          row.token_id,
          row.symbol,
          "error",
          `${moduleLabel} claim failed: ${error.message}`,
          null,
          { moduleType, idempotencyKey: `${eventPrefix}:claim:error` }
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
        moduleType,
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
        `${moduleLabel} fee to treasury (${fromLamports(treasuryLamports).toFixed(6)} SOL)`,
        sig,
        { moduleType, amount: fromLamports(treasuryLamports), idempotencyKey: `${eventPrefix}:fee:treasury` }
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
        `${moduleLabel} fee to dev burn wallet (${fromLamports(devLamports).toFixed(6)} SOL)`,
        sig,
        { moduleType, amount: fromLamports(devLamports), idempotencyKey: `${eventPrefix}:fee:dev` }
      );
      await addProtocolFeeCredit(client, {
        tokenId: row.token_id,
        userId: row.user_id,
        symbol: row.symbol,
        lamports: devLamports,
      });
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
        moduleType,
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
          moduleType,
          idempotencyKey: `${eventPrefix}:gas:error:${wallet.wallet_pubkey}`,
        }
      );
    }
  }

  const walletStates = [];
  for (const wallet of executionWallets) {
    const solLamports = await connection.getBalance(wallet.signer.publicKey, "confirmed");
    const sol = fromLamports(solLamports);
    const reserveSol = fromLamports(Math.max(0, Number(wallet.reserveLamports || 0)));
    const tokenBal = await getOwnerTokenBalanceUi(connection, wallet.signer.publicKey, row.mint);
    walletStates.push({
      ...wallet,
      solLamports,
      sol,
      reserveSol,
      spendableSol: Math.max(0, sol - reserveSol - 0.0005),
      tokenBal,
    });
  }

  const marketState = await fetchMarketStateForMint(row.mint);
  const plan = buildMarketMakerPlan({
    configJson,
    marketState,
    walletStates,
    lastDirection: String(state.lastDirection || ""),
  });

  let executedSide = "";
  for (const action of plan.actions) {
    try {
      if (action.side === "buy" && action.amountSol > 0.0005) {
        const sig = await pumpPortalTrade({
          connection,
          signer: action.wallet.signer,
          mint: row.mint,
          action: "buy",
          amount: action.amountSol,
          denominatedInSol: true,
          slippage: Math.max(1, Math.floor(Number(configJson.slippageBps || 1200) / 100)),
          pool: plan.pool,
        });
        txCreated += 1;
        executedSide = "buy";
        await insertEvent(
          client,
          row.user_id,
          row.token_id,
          row.symbol,
          "buy",
          `MM buy (${action.amountSol.toFixed(4)} SOL) via ${action.wallet.label}`,
          sig,
          { moduleType, amount: action.amountSol, idempotencyKey: `${eventPrefix}:buy:${action.wallet.wallet_pubkey}:${txCreated}` }
        );
      } else if (action.side === "sell" && action.amountToken > 0.000001) {
        const sig = await pumpPortalTrade({
          connection,
          signer: action.wallet.signer,
          mint: row.mint,
          action: "sell",
          amount: action.amountToken,
          denominatedInSol: false,
          slippage: Math.max(1, Math.floor(Number(configJson.slippageBps || 1200) / 100)),
          pool: plan.pool,
        });
        txCreated += 1;
        executedSide = "sell";
        await insertEvent(
          client,
          row.user_id,
          row.token_id,
          row.symbol,
          "sell",
          `MM sell (${action.amountToken.toFixed(4)} ${row.symbol}) via ${action.wallet.label}`,
          sig,
          { moduleType, amount: action.amountToken, idempotencyKey: `${eventPrefix}:sell:${action.wallet.wallet_pubkey}:${txCreated}` }
        );
      }
    } catch (error) {
      state.cooldownUntil = now + Math.max(8, Number(configJson.cooldownSec || 16)) * 1000;
      await insertEvent(
        client,
        row.user_id,
        row.token_id,
        row.symbol,
        "error",
        `MM ${action.side} failed (${action.wallet.label}): ${error.message}`,
        null,
        { moduleType, idempotencyKey: `${eventPrefix}:error:${action.side}:${action.wallet.wallet_pubkey}` }
      );
      break;
    }
  }

  if (!plan.actions.length && Number(state.lastIdlePostureAt || 0) + 300_000 <= now) {
    state.lastIdlePostureAt = now;
    await insertEvent(
      client,
      row.user_id,
      row.token_id,
      row.symbol,
      "status",
      `MM holding ${plan.posture.replace("_", " ")} posture (${plan.inventoryPct.toFixed(1)}% token inventory target ${plan.targetInventoryPct.toFixed(1)}%).`,
      null,
      { moduleType, idempotencyKey: `${eventPrefix}:idle` }
    );
  }

  if (executedSide) {
    state.lastDirection = executedSide;
    state.cooldownUntil = 0;
  }
  state.lastInventoryPct = Number(plan.inventoryPct.toFixed(3));
  state.lastPool = plan.pool;
  state.lastMarketCapUsd = plan.marketCapUsd;
  state.lastLiquidityUsd = plan.liquidityUsd;
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

  return {
    txCreated,
    posture: plan.posture,
    inventoryPct: plan.inventoryPct,
    targetInventoryPct: plan.targetInventoryPct,
  };
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

  const protocolUserId = await getOrCreateProtocolSystemUserId(client);
  const protocolSymbol = "EMBER";
  const protocolEventPrefix = `personal:${Date.now()}`;
  const safeProtocolEvent = async (type, message, tx = null, options = {}) => {
    try {
      await insertEvent(
        client,
        protocolUserId,
        null,
        protocolSymbol,
        type,
        message,
        tx,
        { moduleType: MODULE_TYPES.personalBurn, ...options }
      );
    } catch (error) {
      console.warn(`[personal-burn] protocol event insert failed: ${error?.message || error}`);
    }
  };

  const connection = getConnection();
  const signer = Keypair.fromSecretKey(config.devWalletPrivateKey);
  const reserveLamports = toLamports(Math.max(0.001, Number(config.devWalletSolReserve || 0.01)));
  const beforeClaimBalance = await connection.getBalance(signer.publicKey, "confirmed");
  let txCreated = 0;
  let claimSuccess = 0;
  let claimFailures = 0;
  let claimSkipped = 0;
  let burnedAmount = 0;
  const claimSignatures = [];
  const estimatedClaimFeeSol = Math.max(0.00015, Number(config.basePriorityFeeSol || 0.0005) + 0.0001);
  const defaultClaimMinSol = Math.max(0.001, Number((estimatedClaimFeeSol * 1.5).toFixed(6)));
  const claimMinSol = Math.max(defaultClaimMinSol, Number(config.personalClaimMinSol || 0));
  const claimMinLamports = toLamports(claimMinSol);

  for (const mint of claimMints) {
    try {
      const preview = await getCreatorRewardsPreview({
        connection,
        mint,
        wallet: signer.publicKey.toBase58(),
      });
      const previewLamports = Math.max(0, Number(preview?.totalLamports || 0));
      if (previewLamports < claimMinLamports) {
        claimSkipped += 1;
        console.log(
          `[personal-burn] claim skipped for ${mint}: preview ${fromLamports(previewLamports).toFixed(6)} < min ${claimMinSol.toFixed(6)} SOL`
        );
        continue;
      }
      const claimSig = await pumpPortalCollectCreatorFee({
        connection,
        signer,
        mint,
        pool: "auto",
      });
      txCreated += 1;
      claimSuccess += 1;
      if (claimSig) claimSignatures.push(claimSig);
    } catch (error) {
      claimFailures += 1;
      console.warn(`[personal-burn] claim failed for ${mint}: ${error?.message || error}`);
    }
  }

  const afterClaim = await connection.getBalance(signer.publicKey, "confirmed");
  const claimedLamports = Math.max(0, afterClaim - beforeClaimBalance);
  if (claimSuccess > 0 && claimedLamports > 0) {
    await safeProtocolEvent(
      "claim",
      `Protocol creator rewards claimed (${fromLamports(claimedLamports).toFixed(6)} SOL)`,
      claimSignatures[claimSignatures.length - 1] || null,
      {
        amount: fromLamports(claimedLamports),
        idempotencyKey: `${protocolEventPrefix}:claim`,
      }
    );
  }
  const spendable = Math.max(0, afterClaim - reserveLamports);
  const beforeSnapshot = await getOwnerTokenBalanceSnapshot(
    connection,
    signer.publicKey,
    config.emberTokenMint
  );
  const burnableBefore = Number(beforeSnapshot.totalUi || 0);
  const hasGasForBurn = afterClaim > getTxFeeSafetyLamports();
  console.log(
    `[personal-burn] mint=${config.emberTokenMint} token_balance=${burnableBefore.toFixed(
      6
    )} token_accounts=${beforeSnapshot.accountCount} spendable=${fromLamports(spendable).toFixed(6)}`
  );
  if (spendable <= toLamports(0.0005)) {
    if (burnableBefore > 0 && hasGasForBurn) {
      try {
        const carryBurnSig = await sendTokenToIncinerator(
          connection,
          signer,
          config.emberTokenMint,
          burnableBefore
        );
        const afterCarryBurn = await getOwnerTokenBalanceSnapshot(
          connection,
          signer.publicKey,
          config.emberTokenMint
        );
        const carryBurned = Math.max(0, burnableBefore - Number(afterCarryBurn.totalUi || 0));
        if (carryBurned > 0) {
          txCreated += 1;
          burnedAmount += carryBurned;
          await safeProtocolEvent(
            "burn",
            `Protocol incinerated ${fmtInt(carryBurned.toFixed(2))} EMBER`,
            carryBurnSig,
            {
              amount: carryBurned,
              idempotencyKey: `${protocolEventPrefix}:carry-burn`,
            }
          );
        }
        console.log(
          `[personal-burn] carry-burn before=${burnableBefore.toFixed(6)} after=${Number(
            afterCarryBurn.totalUi || 0
          ).toFixed(6)} burned=${carryBurned.toFixed(6)}`
        );
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
      ).toFixed(6)}) claims_ok=${claimSuccess} claims_skip=${claimSkipped} claims_fail=${claimFailures}`
    );
    return { ran: true, txCreated, claimSuccess, claimFailures, spendableLamports: spendable };
  }

  const feeCreditConsumption = await consumeProtocolFeeCredits(client, spendable);
  const consumedFeeLamports = Math.max(0, Math.floor(Number(feeCreditConsumption.consumedLamports || 0)));
  const consumedFeeBurnLamports = Math.max(0, consumedFeeLamports - Math.floor(consumedFeeLamports * 0.5));

  const treasuryShare = Math.floor(spendable * 0.5);
  const burnShare = spendable - treasuryShare;
  if (treasuryShare > 0) {
    const transferResult = await sendSolTransfer(connection, signer, config.treasuryWallet, treasuryShare);
    txCreated += 1;
    await safeProtocolEvent(
      "transfer",
      `Protocol treasury transfer (${fromLamports(treasuryShare).toFixed(6)} SOL)`,
      transferResult?.signature || null,
      {
        amount: fromLamports(treasuryShare),
        idempotencyKey: `${protocolEventPrefix}:treasury`,
      }
    );
  }
  if (burnShare > 0) {
    const buySig = await pumpPortalTrade({
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
    await safeProtocolEvent(
      "buyback",
      `Protocol buyback executed (${fromLamports(burnShare).toFixed(6)} SOL)`,
      buySig || null,
      {
        amount: fromLamports(burnShare),
        idempotencyKey: `${protocolEventPrefix}:buyback`,
      }
    );
  }

  const burnable = await getOwnerTokenBalanceUi(connection, signer.publicKey, config.emberTokenMint);
  if (burnable > 0 && hasGasForBurn) {
    try {
      const burnSig = await sendTokenToIncinerator(connection, signer, config.emberTokenMint, burnable);
      const afterBurn = await getOwnerTokenBalanceSnapshot(
        connection,
        signer.publicKey,
        config.emberTokenMint
      );
      const burnedNow = Math.max(0, burnable - Number(afterBurn.totalUi || 0));
      if (burnedNow > 0) {
        txCreated += 1;
        burnedAmount += burnedNow;

        let attributedBurnTotal = 0;
        if (burnShare > 0 && consumedFeeBurnLamports > 0 && Array.isArray(feeCreditConsumption.allocations)) {
          const ratio = Math.max(0, Math.min(1, consumedFeeBurnLamports / burnShare));
          attributedBurnTotal = Math.max(0, Math.min(burnedNow, burnedNow * ratio));
          if (attributedBurnTotal > 0) {
            const allocs = feeCreditConsumption.allocations
              .map((item) => {
                const srcLamports = Math.max(0, Math.floor(Number(item?.lamports || 0)));
                const srcBurnLamports = Math.max(0, srcLamports - Math.floor(srcLamports * 0.5));
                return {
                  tokenId: item?.tokenId ? String(item.tokenId) : null,
                  symbol: String(item?.symbol || "UNKNOWN").trim().toUpperCase() || "UNKNOWN",
                  lamports: srcLamports,
                  burnLamports: srcBurnLamports,
                };
              })
              .filter((item) => item.burnLamports > 0);

            const allocBurnLamportsTotal = allocs.reduce((sum, item) => sum + item.burnLamports, 0);
            if (allocBurnLamportsTotal > 0) {
              let emitted = 0;
              for (let i = 0; i < allocs.length; i += 1) {
                const item = allocs[i];
                const isLast = i === allocs.length - 1;
                const piece = isLast
                  ? Math.max(0, attributedBurnTotal - emitted)
                  : Math.max(0, attributedBurnTotal * (item.burnLamports / allocBurnLamportsTotal));
                if (piece <= 0) continue;
                emitted += piece;
                await insertEvent(
                  client,
                  protocolUserId,
                  item.tokenId,
                  item.symbol,
                  "burn",
                  `Protocol fee burn attributed: ${fmtInt(piece.toFixed(2))} EMBER`,
                  burnSig,
                  {
                    moduleType: MODULE_TYPES.personalBurn,
                    amount: piece,
                    idempotencyKey: `${protocolEventPrefix}:burn:fee:${i + 1}`,
                    metadata: {
                      attribution: "fee_credit",
                      sourceLamports: item.lamports,
                      sourceBurnLamports: item.burnLamports,
                    },
                  }
                );
              }
              attributedBurnTotal = Math.max(0, emitted);
            } else {
              attributedBurnTotal = 0;
            }
          }
        }

        const protocolBurnRemainder = Math.max(0, burnedNow - attributedBurnTotal);
        if (protocolBurnRemainder > 0) {
          await safeProtocolEvent(
            "burn",
            `Protocol incinerated ${fmtInt(protocolBurnRemainder.toFixed(2))} EMBER`,
            burnSig,
            {
              amount: protocolBurnRemainder,
              idempotencyKey: `${protocolEventPrefix}:burn`,
            }
          );
        }
      }
      console.log(
        `[personal-burn] burn before=${burnable.toFixed(6)} after=${Number(afterBurn.totalUi || 0).toFixed(
          6
        )} burned=${burnedNow.toFixed(6)}`
      );
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
    `[personal-burn] executed tx=${txCreated} claims_ok=${claimSuccess} claims_skip=${claimSkipped} claims_fail=${claimFailures} spendable=${fromLamports(
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
    if (row.module_type === MODULE_TYPES.dca) {
      return runDcaExecutor(client, row);
    }
    if (row.module_type === MODULE_TYPES.rekindle) {
      return runRekindleExecutor(client, row);
    }
    if (row.module_type === MODULE_TYPES.marketMaker) {
      return runMarketMakerExecutor(client, row);
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

async function processTelegramConnectUpdates() {
  if (!config.telegramBotToken || telegramUpdatesInFlight) return;
  telegramUpdatesInFlight = true;
  try {
    const url = new URL(`https://api.telegram.org/bot${config.telegramBotToken}/getUpdates`);
    url.searchParams.set("timeout", "0");
    url.searchParams.set("allowed_updates", JSON.stringify(["message"]));
    if (telegramUpdateOffset > 0) {
      url.searchParams.set("offset", String(telegramUpdateOffset));
    }
    const res = await fetch(url.toString());
    const data = await res.json().catch(() => ({}));
    const updates = Array.isArray(data?.result) ? data.result : [];
    for (const update of updates) {
      const updateId = Number(update?.update_id || 0);
      if (updateId > 0) telegramUpdateOffset = updateId + 1;
      const message = update?.message;
      const text = String(message?.text || "").trim();
      const chatType = String(message?.chat?.type || "").trim().toLowerCase();
      if (!text.startsWith("/start") || chatType !== "private") continue;
      const connectToken = text.split(/\s+/, 2)[1]?.trim();
      if (!connectToken) {
        await sendTelegramDirectMessage(
          String(message?.chat?.id || ""),
          "EMBER: open your dashboard and use the Telegram connect link there first."
        );
        continue;
      }

      const result = await withTx(async (client) => {
        const tokenRes = await client.query(
          `
            SELECT user_id
            FROM user_telegram_connect_tokens
            WHERE token = $1
              AND consumed_at IS NULL
              AND expires_at > NOW()
            LIMIT 1
            FOR UPDATE
          `,
          [connectToken]
        );
        if (!tokenRes.rowCount) {
          return { ok: false, reason: "invalid" };
        }
        const userId = Number(tokenRes.rows[0].user_id || 0);
        const chatId = String(message?.chat?.id || "").trim();
        const telegramUsername = String(message?.from?.username || "").trim();
        const firstName = String(message?.from?.first_name || "").trim();
        const lastName = String(message?.from?.last_name || "").trim();

        await client.query(
          `
            INSERT INTO user_telegram_links (
              user_id, chat_id, telegram_username, first_name, last_name, is_connected, connected_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, TRUE, NOW(), NOW())
            ON CONFLICT (user_id) DO UPDATE
            SET
              chat_id = EXCLUDED.chat_id,
              telegram_username = EXCLUDED.telegram_username,
              first_name = EXCLUDED.first_name,
              last_name = EXCLUDED.last_name,
              is_connected = TRUE,
              connected_at = COALESCE(user_telegram_links.connected_at, NOW()),
              updated_at = NOW()
          `,
          [userId, chatId, telegramUsername || null, firstName || null, lastName || null]
        );
        await client.query(
          `
            INSERT INTO user_telegram_alert_prefs (user_id, enabled)
            VALUES ($1, TRUE)
            ON CONFLICT (user_id) DO UPDATE
            SET enabled = TRUE, updated_at = NOW()
          `,
          [userId]
        );
        await client.query(
          "UPDATE user_telegram_connect_tokens SET consumed_at = NOW() WHERE token = $1",
          [connectToken]
        );
        return { ok: true };
      });

      if (result.ok) {
        await sendTelegramDirectMessage(
          String(message?.chat?.id || ""),
          [
            "EMBER alerts connected.",
            "You will now receive direct alerts for the dashboard account linked from the site.",
            "Adjust alert settings inside the EMBER dashboard anytime.",
          ].join("\n")
        );
      } else {
        await sendTelegramDirectMessage(
          String(message?.chat?.id || ""),
          "EMBER: that connect link is invalid or expired. Generate a fresh one from your dashboard."
        );
      }
    }
  } catch (error) {
    console.warn(`[telegram] update poll failed: ${error?.message || error}`);
  } finally {
    telegramUpdatesInFlight = false;
  }
}

function formatTelegramImmediateAlert(row) {
  const token = String(row?.token_symbol || "Protocol");
  const moduleLabel = moduleTypeLabel(String(row?.module_type || ""));
  const event = String(row?.event_type || "").toUpperCase();
  const lines = [
    "EMBER Alert",
    `Token: ${token}`,
    moduleLabel ? `Bot: ${moduleLabel}` : "",
    `Event: ${event}`,
    `Message: ${escapeTelegramText(row?.message || "")}`,
    row?.amount ? `Amount: ${trimNumber(row.amount)}${["claim", "deposit", "fee", "withdraw"].includes(String(row?.event_type || "")) ? " SOL" : ""}` : "",
    row?.tx ? `Tx: ${row.tx}` : "",
    `Time: ${new Date(row?.created_at || Date.now()).toISOString().replace("T", " ").replace("Z", " UTC")}`,
  ].filter(Boolean);
  return lines.join("\n");
}

function formatTelegramDigestMessage(rows = []) {
  const sorted = [...rows].sort((a, b) => new Date(a.created_at).getTime() - new Date(b.created_at).getTime());
  const byToken = new Map();
  for (const row of sorted) {
    const key = String(row.token_symbol || "Protocol");
    const bucket = byToken.get(key) || { count: 0, claims: 0, burns: 0, trades: 0, deposits: 0, errors: 0 };
    bucket.count += 1;
    const type = String(row.event_type || "");
    if (type === "claim") bucket.claims += 1;
    else if (type === "burn") bucket.burns += 1;
    else if (type === "deposit") bucket.deposits += 1;
    else if (type === "error") bucket.errors += 1;
    else bucket.trades += 1;
    byToken.set(key, bucket);
  }
  const lines = ["EMBER Digest"];
  for (const [token, bucket] of byToken.entries()) {
    const parts = [];
    if (bucket.deposits) parts.push(`deposits ${bucket.deposits}`);
    if (bucket.claims) parts.push(`claims ${bucket.claims}`);
    if (bucket.burns) parts.push(`burns ${bucket.burns}`);
    if (bucket.trades) parts.push(`trades ${bucket.trades}`);
    if (bucket.errors) parts.push(`errors ${bucket.errors}`);
    lines.push(`${token}: ${parts.join(" | ")}`);
  }
  lines.push(`Events: ${sorted.length}`);
  lines.push(`Window end: ${new Date(sorted[sorted.length - 1]?.scheduled_at || Date.now()).toISOString().replace("T", " ").replace("Z", " UTC")}`);
  return lines.join("\n");
}

async function deliverPendingTelegramAlerts() {
  if (!config.telegramBotToken || telegramAlertDeliveryInFlight) return;
  telegramAlertDeliveryInFlight = true;
  try {
    const dueRes = await pool.query(
      `
        SELECT q.id, q.user_id, q.token_id, q.token_symbol, q.module_type, q.event_type, q.amount, q.message, q.tx,
               q.delivery_kind, q.digest_key, q.scheduled_at, q.attempts, q.created_at, l.chat_id
        FROM user_telegram_alert_queue q
        JOIN user_telegram_links l ON l.user_id = q.user_id AND l.is_connected = TRUE
        WHERE q.status = 'pending'
          AND q.scheduled_at <= NOW()
        ORDER BY q.created_at ASC
        LIMIT 200
      `
    );
    const rows = dueRes.rows || [];
    if (!rows.length) return;

    const immediate = rows.filter((row) => String(row.delivery_kind) === "immediate");
    for (const row of immediate) {
      const ok = await sendTelegramDirectMessage(String(row.chat_id || ""), formatTelegramImmediateAlert(row));
      if (ok) {
        await pool.query(
          `UPDATE user_telegram_alert_queue SET status = 'sent', sent_at = NOW(), error = NULL WHERE id = $1`,
          [row.id]
        );
      } else {
        const attempts = Number(row.attempts || 0) + 1;
        await pool.query(
          `
            UPDATE user_telegram_alert_queue
            SET attempts = $2,
                scheduled_at = NOW() + INTERVAL '5 minutes',
                error = 'telegram send failed',
                status = CASE WHEN $2 >= 3 THEN 'failed' ELSE status END
            WHERE id = $1
          `,
          [row.id, attempts]
        );
      }
    }

    const digests = rows.filter((row) => String(row.delivery_kind) === "digest");
    const digestGroups = new Map();
    for (const row of digests) {
      const key = `${row.user_id}:${row.digest_key}`;
      const group = digestGroups.get(key) || [];
      group.push(row);
      digestGroups.set(key, group);
    }
    for (const groupRows of digestGroups.values()) {
      const chatId = String(groupRows[0]?.chat_id || "");
      const ids = groupRows.map((row) => Number(row.id));
      const ok = await sendTelegramDirectMessage(chatId, formatTelegramDigestMessage(groupRows));
      if (ok) {
        await pool.query(
          `UPDATE user_telegram_alert_queue SET status = 'sent', sent_at = NOW(), error = NULL WHERE id = ANY($1::bigint[])`,
          [ids]
        );
      } else {
        const maxAttempts = Math.max(...groupRows.map((row) => Number(row.attempts || 0))) + 1;
        await pool.query(
          `
            UPDATE user_telegram_alert_queue
            SET attempts = $2,
                scheduled_at = NOW() + INTERVAL '5 minutes',
                error = 'telegram digest send failed',
                status = CASE WHEN $2 >= 3 THEN 'failed' ELSE status END
            WHERE id = ANY($1::bigint[])
          `,
          [ids, maxAttempts]
        );
      }
    }
  } catch (error) {
    console.warn(`[telegram] alert delivery failed: ${error?.message || error}`);
  } finally {
    telegramAlertDeliveryInFlight = false;
  }
}

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

  await processTelegramConnectUpdates();
  await deliverPendingTelegramAlerts();

  return {
    dueTokens: scheduleSummary.dueCount,
    eventsCreated,
    enqueuedJobs: scheduleSummary.enqueued,
    executedJobs,
  };
}


