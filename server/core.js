import crypto from "node:crypto";
import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import { createRequire } from "node:module";
import os from "node:os";
import path from "node:path";
import WebSocket from "ws";
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
  createAssociatedTokenAccountIdempotentInstruction,
  createBurnCheckedInstruction,
  createTransferCheckedInstruction,
  getAssociatedTokenAddressSync,
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

function normalizeReferralCode(value) {
  const code = String(value || "").trim().toUpperCase();
  if (!code) return "";
  if (!/^[A-Z0-9_]{4,40}$/.test(code)) {
    throw new Error("Referral code is invalid.");
  }
  return code;
}

function defaultReferralCodeForUserId(userId) {
  const id = Math.max(0, Math.floor(Number(userId || 0)));
  if (!id) return "";
  return `EMBER${id.toString(16).toUpperCase()}`;
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

const TOOL_TYPES = {
  holderPooler: "holder_pooler",
  reactionManager: "reaction_manager",
  smartSell: "smart_sell",
  bundleManager: "bundle_manager",
};

const TOOL_STATUSES = {
  awaitingFunds: "awaiting_funds",
  ready: "ready",
  active: "active",
  paused: "paused",
  provisioning: "provisioning",
  completed: "completed",
  archived: "archived",
};

const TOOL_FUNDING_MODES = Object.freeze({
  direct: "direct",
  ember: "ember",
});

function moduleTypeLabel(moduleType) {
  if (moduleType === MODULE_TYPES.volume) return "Volume Bot";
  if (moduleType === MODULE_TYPES.marketMaker) return "Market Maker Bot";
  if (moduleType === MODULE_TYPES.dca) return "DCA Bot";
  if (moduleType === MODULE_TYPES.rekindle) return "Rekindle Bot";
  if (moduleType === MODULE_TYPES.personalBurn) return "Personal Burn Bot";
  return "Burn Bot";
}

function normalizeToolType(value, fallback = TOOL_TYPES.holderPooler) {
  const raw = String(value || "").trim().toLowerCase();
  if (raw === TOOL_TYPES.reactionManager) return TOOL_TYPES.reactionManager;
  if (raw === TOOL_TYPES.smartSell) return TOOL_TYPES.smartSell;
  if (raw === TOOL_TYPES.bundleManager) return TOOL_TYPES.bundleManager;
  if (raw === TOOL_TYPES.holderPooler) return TOOL_TYPES.holderPooler;
  return fallback;
}

function toolTypeLabel(toolType) {
  if (toolType === TOOL_TYPES.reactionManager) return "Reaction Manager";
  if (toolType === TOOL_TYPES.smartSell) return "Smart Sell";
  if (toolType === TOOL_TYPES.bundleManager) return "Bundle Manager";
  return "Holder Pooler";
}

function normalizeToolFundingMode(value, fallback = TOOL_FUNDING_MODES.direct) {
  const raw = String(value || "").trim().toLowerCase();
  if (raw === TOOL_FUNDING_MODES.ember || raw === "provider" || raw === "splitnow") {
    return TOOL_FUNDING_MODES.ember;
  }
  if (raw === TOOL_FUNDING_MODES.direct) return TOOL_FUNDING_MODES.direct;
  return fallback;
}

function toolFundingModeLabel(mode) {
  return normalizeToolFundingMode(mode, TOOL_FUNDING_MODES.direct) === TOOL_FUNDING_MODES.ember
    ? "EMBER Funding"
    : "Direct Funding";
}

function defaultToolReserveLamports(toolType) {
  if (toolType === TOOL_TYPES.bundleManager) return toLamports(0.006);
  if (toolType === TOOL_TYPES.smartSell) return toLamports(0.005);
  if (toolType === TOOL_TYPES.holderPooler) return toLamports(0.004);
  return toLamports(0.001);
}

function defaultToolConfig(toolType) {
  if (toolType === TOOL_TYPES.reactionManager) {
    return {
      reactionType: "rocket",
      targetCount: 1000,
    };
  }
  if (toolType === TOOL_TYPES.smartSell) {
    return {
      triggerMode: "every_buy",
      sellPct: 25,
      thresholdSol: 0,
      timingMode: "randomized",
      walletMode: "managed",
      fundingMode: TOOL_FUNDING_MODES.direct,
      walletCount: 6,
      walletReserveSol: 0.0015,
      stealthMode: true,
    };
  }
  if (toolType === TOOL_TYPES.bundleManager) {
    return {
      walletCount: 10,
      sideBias: 50,
      importedWallets: [],
      walletMode: "managed",
      fundingMode: TOOL_FUNDING_MODES.direct,
      fundingStaggerMode: true,
    };
  }
  return {
    walletCount: 10,
    tokenAmountPerWallet: 1,
    solAmountPerWallet: 0,
    walletReserveSol: 0,
    walletMode: "managed",
    stealthMode: true,
  };
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

const REFERRAL_COOLDOWN_MS = 6 * 60 * 60 * 1000;
const REFERRAL_MIN_GROSS_FEE_LAMPORTS = toLamports(0.001);

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
    const cachedCap = Math.max(0, Number(readCachedMarketCapUsd(mint) || 0));
    const rowCap = Math.max(0, Number(row?.market_cap || 0));
    const marketCap = cachedCap > 0 ? cachedCap : rowCap;
    byMint.set(mint, marketCap);
    if (marketCap > 0) {
      const entry = getMarketCapEntry(mint);
      entry.value = marketCap;
      entry.at = Date.now();
      if (!Number(entry.nextRefreshAt || 0)) {
        entry.nextRefreshAt = Date.now() + MARKET_CAP_CACHE_MS;
      }
    }
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

async function sendTelegramDirectMessage(chatId, text, options = {}) {
  if (!config.telegramBotToken || !chatId || !text) return false;
  try {
    const res = await fetch(`https://api.telegram.org/bot${config.telegramBotToken}/sendMessage`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        chat_id: chatId,
        text: String(text || "").slice(0, 4096),
        disable_web_page_preview: true,
        ...(options.replyMarkup ? { reply_markup: options.replyMarkup } : {}),
      }),
    });
    return res.ok;
  } catch {
    return false;
  }
}

async function editTelegramMessage(chatId, messageId, text, options = {}) {
  if (!config.telegramBotToken || !chatId || !messageId || !text) return false;
  try {
    const res = await fetch(`https://api.telegram.org/bot${config.telegramBotToken}/editMessageText`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        chat_id: chatId,
        message_id: Number(messageId),
        text: String(text || "").slice(0, 4096),
        disable_web_page_preview: true,
        ...(options.replyMarkup ? { reply_markup: options.replyMarkup } : {}),
      }),
    });
    return res.ok;
  } catch {
    return false;
  }
}

async function answerTelegramCallbackQuery(callbackQueryId, text = "") {
  if (!config.telegramBotToken || !callbackQueryId) return false;
  try {
    const res = await fetch(`https://api.telegram.org/bot${config.telegramBotToken}/answerCallbackQuery`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        callback_query_id: callbackQueryId,
        ...(text ? { text: String(text).slice(0, 200) } : {}),
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

function shouldUseVanityWallets(value, fallback = true) {
  if (value === undefined || value === null || value === "") return fallback;
  if (typeof value === "boolean") return value;
  const raw = String(value).trim().toLowerCase();
  if (["false", "0", "no", "regular", "random"].includes(raw)) return false;
  if (["true", "1", "yes", "vanity", "branded", "ember", "embr"].includes(raw)) return true;
  return fallback;
}

async function reserveRegularDepositAddresses(userId, count) {
  return withTx(async (client) => {
    const reserved = [];
    for (let i = 0; i < count; i += 1) {
      const generated = generateNonVanityDeposit();
      const reservationId = makeId("dep");
      await client.query(
        `
          INSERT INTO token_deposit_pool (
            prefix,
            deposit_pubkey,
            secret_key_base58,
            status,
            reservation_id,
            reserved_user_id,
            reserved_at
          )
          VALUES ($1, $2, $3, 'reserved', $4, $5, NOW())
        `,
        [null, generated.pubkey, encryptDepositSecret(generated.secretKeyBase58), reservationId, userId]
      );
      reserved.push({
        pendingDepositId: reservationId,
        deposit: generated.pubkey,
        vanity: false,
      });
    }
    return reserved;
  });
}

export async function reserveDepositAddresses(userId, countInput = 1, options = {}) {
  const count = Math.max(1, Math.min(DEPOSIT_POOL_MAX, Math.floor(Number(countInput) || 1)));
  const useVanity = shouldUseVanityWallets(options?.useVanity, true);
  if (!useVanity) {
    return reserveRegularDepositAddresses(userId, count);
  }
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
        vanity: true,
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

export async function generatePendingDepositAddress(userId, countInput = 1, options = {}) {
  const scope = await resolveUserAccessScopeFromPool(userId);
  const count = Math.max(1, Math.min(DEPOSIT_POOL_MAX, Math.floor(Number(countInput) || 1)));
  const reserved = await reserveDepositAddresses(scope.ownerUserId, count, options);
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
    vanity: String(row?.deposit_pubkey || "").startsWith("EMBR") || String(row?.deposit_pubkey || "").startsWith("EMBER"),
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
  const useVanity = shouldUseVanityWallets(payload?.useVanity, true);
  if (!Number.isFinite(initialBuySol) || initialBuySol <= 0) {
    throw new Error("Pump deploy requires an initial buy greater than 0 SOL.");
  }

  const requiredLamports = estimateDeployVanityRequiredLamports(initialBuySol);
  const expiresAt = deployVanityReservationExpiryDate();
  if (!useVanity) {
    const generated = generateNonVanityDeposit();
    const reservationId = makeId("dvw");
    const storedSecret = encryptDepositSecret(generated.secretKeyBase58);
    await pool.query(
      `
        INSERT INTO deploy_wallet_reservations (
          id, user_id, deposit_pubkey, secret_key_base58, required_lamports, expires_at
        )
        VALUES ($1, $2, $3, $4, $5, $6)
      `,
      [reservationId, deployUserId, generated.pubkey, storedSecret, requiredLamports, expiresAt]
    );
    return {
      reservationId,
      deposit: generated.pubkey,
      requiredLamports,
      requiredSol: fromLamports(requiredLamports),
      bufferSol: fromLamports(getDeployVanityBufferLamports()),
      balanceLamports: 0,
      balanceSol: 0,
      funded: false,
      shortfallLamports: requiredLamports,
      shortfallSol: fromLamports(requiredLamports),
      status: "reserved",
      expiresAt: new Date(expiresAt).toISOString(),
      deployedMint: "",
      deploySignature: "",
      lastError: "",
      privateKeyBase58: generated.secretKeyBase58,
      privateKeyArray: Array.from(keypairFromBase58(generated.secretKeyBase58).secretKey),
      vanity: false,
    };
  }
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
    vanity: true,
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

export async function registerUser(usernameInput, passwordInput, referralCodeInput = "", meta = {}) {
  const username = normalizeUsername(usernameInput);
  const password = normalizePassword(passwordInput);
  const referralCode = normalizeReferralCode(referralCodeInput);
  const passwordHash = await bcrypt.hash(password, 10);
  const signupIp = String(meta?.requestIp || "").trim().slice(0, 255) || null;

  const user = await withTx(async (client) => {
    const existing = await client.query("SELECT id FROM users WHERE username = $1", [username]);
    if (existing.rowCount > 0) {
      throw new Error("Username already exists.");
    }

    let referrerUserId = null;
    if (referralCode) {
      const referrerRes = await client.query(
        "SELECT id, COALESCE(is_og, FALSE) AS is_og FROM users WHERE referral_code = $1 LIMIT 1",
        [referralCode]
      );
      if (!referrerRes.rowCount) {
        throw new Error("Referral code was not found.");
      }
      if (Boolean(referrerRes.rows[0].is_og)) {
        throw new Error("OG accounts cannot be used as referrers.");
      }
      referrerUserId = Number(referrerRes.rows[0].id || 0) || null;
    }

    const inserted = await client.query(
      `
        INSERT INTO users (username, password_hash, referrer_user_id, signup_ip, last_login_ip)
        VALUES ($1, $2, $3, $4, $4)
        RETURNING id, username
      `,
      [username, passwordHash, referrerUserId, signupIp]
    );

    const insertedId = Number(inserted.rows[0].id || 0);
    await client.query(
      `UPDATE users SET referral_code = COALESCE(NULLIF(referral_code, ''), $1) WHERE id = $2`,
      [defaultReferralCodeForUserId(insertedId), insertedId]
    );

    return inserted.rows[0];
  });

  const sessionToken = await createSession(user.id);
  return {
    user: await getUserAuthProfile(user.id),
    sessionToken,
  };
}

export async function loginUser(usernameInput, passwordInput, meta = {}) {
  const username = normalizeUsername(usernameInput);
  const password = normalizePassword(passwordInput);

  const result = await pool.query(
    "SELECT id, username, password_hash, COALESCE(is_banned, FALSE) AS is_banned, COALESCE(banned_reason, '') AS banned_reason FROM users WHERE username = $1",
    [username]
  );

  if (!result.rowCount) {
    throw new Error("Invalid username or password.");
  }

  const user = result.rows[0];
  if (Boolean(user.is_banned)) {
    throw new Error(`Account suspended${user.banned_reason ? `: ${user.banned_reason}` : "."}`);
  }
  const ok = await bcrypt.compare(password, user.password_hash);
  if (!ok) {
    throw new Error("Invalid username or password.");
  }

  const loginIp = String(meta?.requestIp || "").trim().slice(0, 255) || null;
  if (loginIp) {
    await pool.query("UPDATE users SET last_login_ip = $2 WHERE id = $1", [Number(user.id || 0), loginIp]);
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
      SELECT u.id, u.username, COALESCE(u.is_banned, FALSE) AS is_banned
      FROM sessions s
      JOIN users u ON u.id = s.user_id
      WHERE s.token = $1 AND s.expires_at > NOW()
      LIMIT 1
    `,
    [token]
  );

  if (!result.rowCount) return null;
  if (Boolean(result.rows[0].is_banned)) return null;
  return getUserAuthProfile(Number(result.rows[0].id));
}

async function resolveUserAccessScope(client, actorUserId) {
  const actorId = Number(actorUserId || 0);
  if (!Number.isFinite(actorId) || actorId <= 0) {
    throw new Error("User is required.");
  }

  const actorRes = await client.query(
    `
      SELECT
        id,
        username,
        COALESCE(is_admin, FALSE) AS is_admin,
        COALESCE(is_og, FALSE) AS is_og,
        COALESCE(is_banned, FALSE) AS is_banned,
        COALESCE(banned_reason, '') AS banned_reason,
        COALESCE(referral_code, '') AS referral_code,
        fee_bps_override,
        referrer_user_id
      FROM users
      WHERE id = $1
      LIMIT 1
    `,
    [actorId]
  );
  if (!actorRes.rowCount) {
    throw new Error("User not found.");
  }
  const actor = actorRes.rows[0];

  const grantRes = await client.query(
    `
      SELECT
        g.owner_user_id,
        owner.username AS owner_username,
        COALESCE(owner.is_admin, FALSE) AS owner_is_admin,
        COALESCE(owner.is_og, FALSE) AS owner_is_og,
        COALESCE(owner.is_banned, FALSE) AS owner_is_banned,
        COALESCE(owner.referral_code, '') AS owner_referral_code,
        owner.fee_bps_override AS owner_fee_bps_override,
        owner.referrer_user_id AS owner_referrer_user_id
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
  const ownerIsAdmin = isOperator ? Boolean(grantRes.rows[0].owner_is_admin) : Boolean(actor.is_admin);
  const ownerIsOg = isOperator ? Boolean(grantRes.rows[0].owner_is_og) : Boolean(actor.is_og);
  const ownerIsBanned = isOperator ? Boolean(grantRes.rows[0].owner_is_banned) : Boolean(actor.is_banned);
  const ownerReferralCode = isOperator ? String(grantRes.rows[0].owner_referral_code || "") : String(actor.referral_code || "");
  const ownerFeeBpsOverride = isOperator
    ? (grantRes.rows[0].owner_fee_bps_override == null
        ? null
        : Math.max(0, Math.floor(Number(grantRes.rows[0].owner_fee_bps_override || 0))))
    : (actor.fee_bps_override == null ? null : Math.max(0, Math.floor(Number(actor.fee_bps_override || 0))));
  const ownerReferrerUserId = isOperator
    ? (grantRes.rows[0].owner_referrer_user_id == null
        ? null
        : Math.max(0, Math.floor(Number(grantRes.rows[0].owner_referrer_user_id || 0))))
    : (actor.referrer_user_id == null ? null : Math.max(0, Math.floor(Number(actor.referrer_user_id || 0))));

  return {
    actorUserId: actorId,
    actorUsername: String(actor.username || ""),
    actorIsAdmin: Boolean(actor.is_admin),
    actorIsOg: Boolean(actor.is_og),
    actorIsBanned: Boolean(actor.is_banned),
    actorBannedReason: String(actor.banned_reason || ""),
    actorReferralCode: String(actor.referral_code || ""),
    actorFeeBpsOverride:
      actor.fee_bps_override == null ? null : Math.max(0, Math.floor(Number(actor.fee_bps_override || 0))),
    actorReferrerUserId:
      actor.referrer_user_id == null ? null : Math.max(0, Math.floor(Number(actor.referrer_user_id || 0))),
    ownerUserId,
    ownerUsername,
    ownerIsAdmin,
    ownerIsOg,
    ownerIsBanned,
    ownerReferralCode,
    ownerFeeBpsOverride,
    ownerReferrerUserId,
    role: isOperator ? "manager" : "owner",
    isOperator,
    isOwner: !isOperator,
    isAdmin: Boolean(actor.is_admin) && !isOperator,
    isOg: ownerIsOg,
    ownerHasToolFeeWaiver: ownerIsAdmin || ownerIsOg,
    isBanned: ownerIsBanned || Boolean(actor.is_banned),
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

function assertAdminPermission(scope, message = "Admin access is required.") {
  if (!scope?.isAdmin) {
    throw new Error(message);
  }
}

async function recordAdminAudit(client, payload = {}) {
  const actorUserId = payload.actorUserId == null ? null : Math.max(0, Math.floor(Number(payload.actorUserId || 0))) || null;
  const targetUserId = payload.targetUserId == null ? null : Math.max(0, Math.floor(Number(payload.targetUserId || 0))) || null;
  const targetTokenId = payload.targetTokenId == null ? null : String(payload.targetTokenId || "").trim() || null;
  const action = String(payload.action || "").trim();
  if (!action) return;
  const details = payload.details && typeof payload.details === "object" ? payload.details : {};
  await client.query(
    `
      INSERT INTO admin_audit_log (actor_user_id, target_user_id, target_token_id, action, details_json)
      VALUES ($1, $2, $3, $4, $5::jsonb)
    `,
    [actorUserId, targetUserId, targetTokenId, action, JSON.stringify(details)]
  );
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
    isAdmin: scope.isAdmin,
    isOg: scope.isOg,
    isBanned: scope.isBanned,
    bannedReason: scope.actorBannedReason,
    referralCode: scope.ownerReferralCode,
    feeBpsOverride: scope.ownerFeeBpsOverride,
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
        JOIN users u ON u.id = t.user_id
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

function sanitizeToolTitle(value, fallback) {
  const clean = String(value || "").trim().replace(/\s+/g, " ");
  return (clean || fallback || "").slice(0, 80);
}

function normalizeToolTargetMint(value, label = "Token mint") {
  const clean = String(value || "").trim();
  if (!clean) throw new Error(`${label} is required.`);
  try {
    return new PublicKey(clean).toBase58();
  } catch {
    throw new Error(`${label} is invalid.`);
  }
}

function toolUnlockFeeLamportsForType(toolType, settings = DEFAULT_PROTOCOL_SETTINGS) {
  if (toolType === TOOL_TYPES.reactionManager) {
    return Math.max(0, Math.floor(Number(settings.toolFeeReactionManagerLamports || 0)));
  }
  if (toolType === TOOL_TYPES.smartSell) {
    return Math.max(0, Math.floor(Number(settings.toolFeeSmartSellLamports || 0)));
  }
  if (toolType === TOOL_TYPES.bundleManager) {
    return Math.max(0, Math.floor(Number(settings.toolFeeBundleManagerLamports || 0)));
  }
  return Math.max(0, Math.floor(Number(settings.toolFeeHolderPoolerLamports || 0)));
}

function toolRuntimeFeeLamportsForType(toolType, settings = DEFAULT_PROTOCOL_SETTINGS) {
  if (toolType === TOOL_TYPES.smartSell) {
    return Math.max(0, Math.floor(Number(settings.toolFeeSmartSellRuntimeLamports || 0)));
  }
  return 0;
}

function toolRuntimeWindowHoursForType(toolType, settings = DEFAULT_PROTOCOL_SETTINGS) {
  if (toolType === TOOL_TYPES.smartSell) {
    return Math.max(0, Math.floor(Number(settings.toolFeeSmartSellRuntimeWindowHours || 0)));
  }
  return 0;
}

function buildToolFeeProfile(toolType, settings = DEFAULT_PROTOCOL_SETTINGS, options = {}) {
  const waiveFees = Boolean(options.waiveFees);
  const unlockFeeLamports = waiveFees ? 0 : toolUnlockFeeLamportsForType(toolType, settings);
  const runtimeFeeLamports = waiveFees ? 0 : toolRuntimeFeeLamportsForType(toolType, settings);
  const runtimeFeeWindowHours = runtimeFeeLamports > 0 ? toolRuntimeWindowHoursForType(toolType, settings) : 0;
  return {
    unlockFeeLamports,
    runtimeFeeLamports,
    runtimeFeeWindowHours,
  };
}

function buildToolCatalog(settings = DEFAULT_PROTOCOL_SETTINGS, options = {}) {
  const safeSettings = settings || DEFAULT_PROTOCOL_SETTINGS;
  const holderFees = buildToolFeeProfile(TOOL_TYPES.holderPooler, safeSettings, options);
  const reactionFees = buildToolFeeProfile(TOOL_TYPES.reactionManager, safeSettings, options);
  const smartSellFees = buildToolFeeProfile(TOOL_TYPES.smartSell, safeSettings, options);
  const bundleFees = buildToolFeeProfile(TOOL_TYPES.bundleManager, safeSettings, options);
  return [
    {
      toolType: TOOL_TYPES.holderPooler,
      label: toolTypeLabel(TOOL_TYPES.holderPooler),
      description: "Distribute supply and SOL across managed holder wallets with reclaim-ready balances.",
      targetKind: "mint",
      simpleDefaults: defaultToolConfig(TOOL_TYPES.holderPooler),
      unlockFeeLamports: holderFees.unlockFeeLamports,
      reserveLamports: defaultToolReserveLamports(TOOL_TYPES.holderPooler),
      runtimeFeeLamports: holderFees.runtimeFeeLamports,
      runtimeFeeWindowHours: holderFees.runtimeFeeWindowHours,
    },
    {
      toolType: TOOL_TYPES.reactionManager,
      label: toolTypeLabel(TOOL_TYPES.reactionManager),
      description: "Run one-reaction DexScreener campaigns against a target pair page with tracked delivery.",
      targetKind: "url",
      simpleDefaults: defaultToolConfig(TOOL_TYPES.reactionManager),
      unlockFeeLamports: reactionFees.unlockFeeLamports,
      reserveLamports: defaultToolReserveLamports(TOOL_TYPES.reactionManager),
      runtimeFeeLamports: reactionFees.runtimeFeeLamports,
      runtimeFeeWindowHours: reactionFees.runtimeFeeWindowHours,
    },
    {
      toolType: TOOL_TYPES.smartSell,
      label: toolTypeLabel(TOOL_TYPES.smartSell),
      description: "React to buy flow with configurable sell routing, timing, and managed stealth-wallet rotation.",
      targetKind: "mint",
      simpleDefaults: defaultToolConfig(TOOL_TYPES.smartSell),
      unlockFeeLamports: smartSellFees.unlockFeeLamports,
      reserveLamports: defaultToolReserveLamports(TOOL_TYPES.smartSell),
      runtimeFeeLamports: smartSellFees.runtimeFeeLamports,
      runtimeFeeWindowHours: smartSellFees.runtimeFeeWindowHours,
    },
    {
      toolType: TOOL_TYPES.bundleManager,
      label: toolTypeLabel(TOOL_TYPES.bundleManager),
      description: "Coordinate managed and imported wallet bundles for buy/sell campaigns with reserve-aware funding.",
      targetKind: "mint",
      simpleDefaults: defaultToolConfig(TOOL_TYPES.bundleManager),
      unlockFeeLamports: bundleFees.unlockFeeLamports,
      reserveLamports: defaultToolReserveLamports(TOOL_TYPES.bundleManager),
      runtimeFeeLamports: bundleFees.runtimeFeeLamports,
      runtimeFeeWindowHours: bundleFees.runtimeFeeWindowHours,
    },
  ].map((entry) => ({
    ...entry,
    requiredLamports: entry.unlockFeeLamports + entry.reserveLamports,
  }));
}

function mapToolEvent(row) {
  return {
    id: Number(row.id || 0),
    toolInstanceId: String(row.tool_instance_id || ""),
    toolType: normalizeToolType(row.tool_type, TOOL_TYPES.holderPooler),
    eventType: String(row.event_type || ""),
    amount: Number(row.amount || 0),
    message: String(row.message || ""),
    tx: row.tx ? String(row.tx) : null,
    metadata: row.metadata_json && typeof row.metadata_json === "object" ? row.metadata_json : {},
    createdAt: row.created_at,
  };
}

function mapToolInstance(row, events = []) {
  const toolType = normalizeToolType(row.tool_type, TOOL_TYPES.holderPooler);
  const unlockFeeLamports = Math.max(0, Math.floor(Number(row.unlock_fee_lamports || 0)));
  const reserveLamports = Math.max(0, Math.floor(Number(row.reserve_lamports || 0)));
  const runtimeFeeLamports = Math.max(0, Math.floor(Number(row.runtime_fee_lamports || 0)));
  const runtimeFeeWindowHours = Math.max(0, Math.floor(Number(row.runtime_fee_window_hours || 0)));
  const balanceLamports = Math.max(0, Math.floor(Number(row.current_balance_lamports || 0)));
  const requiredLamports = unlockFeeLamports + reserveLamports;
  return {
    id: String(row.id || ""),
    toolType,
    label: toolTypeLabel(toolType),
    status: String(row.status || TOOL_STATUSES.awaitingFunds),
    simpleMode: Boolean(row.simple_mode),
    title: String(row.title || toolTypeLabel(toolType)),
    targetMint: String(row.target_mint || ""),
    targetUrl: String(row.target_url || ""),
    fundingWalletPubkey: String(row.funding_wallet_pubkey || ""),
    unlockFeeLamports,
    reserveLamports,
    requiredLamports,
    runtimeFeeLamports,
    runtimeFeeWindowHours,
    unlockFeeSol: fromLamports(unlockFeeLamports),
    reserveSol: fromLamports(reserveLamports),
    requiredSol: fromLamports(requiredLamports),
    runtimeFeeSol: fromLamports(runtimeFeeLamports),
    balanceLamports,
    balanceSol: fromLamports(balanceLamports),
    isFunded: balanceLamports >= requiredLamports,
    unlockTx: row.unlock_tx ? String(row.unlock_tx) : "",
    unlockedAt: row.unlocked_at,
    activatedAt: row.activated_at,
    lastRunAt: row.last_run_at,
    archivedAt: row.archived_at,
    lastError: String(row.last_error || ""),
    config: row.config_json && typeof row.config_json === "object" ? row.config_json : {},
    state: row.state_json && typeof row.state_json === "object" ? row.state_json : {},
    fundingState: mapFundingStateForDisplay(row.state_json || {}),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    events,
  };
}

async function insertToolEvent(client, ownerUserId, toolInstanceId, toolType, eventType, message, options = {}) {
  const amount = Number(options.amount || 0);
  const tx = options.tx ? String(options.tx) : null;
  const metadata = options.metadata && typeof options.metadata === "object" ? options.metadata : {};
  const res = await client.query(
    `
      INSERT INTO tool_events (
        tool_instance_id,
        owner_user_id,
        tool_type,
        event_type,
        amount,
        message,
        tx,
        metadata_json
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
      RETURNING id, tool_instance_id, tool_type, event_type, amount, message, tx, metadata_json, created_at
    `,
    [toolInstanceId, ownerUserId, toolType, eventType, amount, message, tx, JSON.stringify(metadata)]
  );
  return mapToolEvent(res.rows[0]);
}

async function applyToolUnlockFeeFlow(client, options = {}) {
  const connection = options.connection;
  const signer = options.signer;
  const ownerUserId = Math.max(0, Math.floor(Number(options.ownerUserId || 0)));
  const toolInstanceId = String(options.toolInstanceId || "").trim();
  const toolType = normalizeToolType(options.toolType, TOOL_TYPES.holderPooler);
  const totalLamports = Math.max(0, Math.floor(Number(options.totalLamports || 0)));
  if (!connection || !signer || !ownerUserId || !toolInstanceId || totalLamports <= 0) {
    return { feeLamports: 0, treasuryLamports: 0, burnLamports: 0, treasurySig: null, burnSig: null };
  }

  const settings = await getProtocolSettings(client);
  const treasuryBps = Math.max(0, Math.floor(Number(settings.defaultTreasuryBps || 0)));
  const burnBps = Math.max(0, Math.floor(Number(settings.defaultBurnBps || 0)));
  const split = splitLamportsByBps(totalLamports, { treasury: treasuryBps, burn: burnBps });
  let treasuryLamports = Math.max(0, Math.floor(Number(split.treasury || 0)));
  let burnLamports = Math.max(0, Math.floor(Number(split.burn || 0)));
  if (burnLamports > 0 && !config.devWalletPublicKey) {
    treasuryLamports += burnLamports;
    burnLamports = 0;
  }

  let treasurySig = null;
  let burnSig = null;
  if (treasuryLamports > 0) {
    treasurySig = await sendSolTransfer(connection, signer, config.treasuryWallet, treasuryLamports);
  }
  if (burnLamports > 0 && config.devWalletPublicKey) {
    burnSig = await sendSolTransfer(connection, signer, config.devWalletPublicKey, burnLamports);
    await addProtocolFeeCredit(client, {
      userId: ownerUserId,
      symbol: "TOOLS",
      lamports: burnLamports,
    });
  }

  await client.query(
    `
      UPDATE protocol_metrics
      SET fees_taken_sol = fees_taken_sol + $2,
          updated_at = NOW()
      WHERE id = $1
    `,
    [1, fromLamports(totalLamports)]
  );

  if (treasuryLamports > 0) {
    await insertToolEvent(
      client,
      ownerUserId,
      toolInstanceId,
      toolType,
      "fee",
      `Tool fee sent to treasury (${fromLamports(treasuryLamports).toFixed(6)} SOL)`,
      { tx: treasurySig, amount: fromLamports(treasuryLamports), metadata: { allocation: "treasury" } }
    );
  }
  if (burnLamports > 0) {
    await insertToolEvent(
      client,
      ownerUserId,
      toolInstanceId,
      toolType,
      "fee",
      `Tool fee sent to dev burn wallet (${fromLamports(burnLamports).toFixed(6)} SOL)`,
      { tx: burnSig, amount: fromLamports(burnLamports), metadata: { allocation: "burn", source: "Tools" } }
    );
  }

  return {
    feeLamports: totalLamports,
    treasuryLamports,
    burnLamports,
    treasurySig,
    burnSig,
  };
}

async function applyTelegramTradeFeeFlow(options = {}) {
  const connection = options.connection;
  const signer = options.signer;
  const ownerUserId = Math.max(0, Math.floor(Number(options.ownerUserId || 0)));
  const walletId = String(options.walletId || "").trim();
  const direction = String(options.direction || "").trim().toLowerCase();
  const totalLamports = Math.max(0, Math.floor(Number(options.totalLamports || 0)));
  if (!connection || !signer || !ownerUserId || !walletId || totalLamports <= 0) {
    return { feeLamports: 0, treasuryLamports: 0, burnLamports: 0, treasurySig: null, burnSig: null };
  }

  const settings = await getProtocolSettings(pool);
  const treasuryBps = Math.max(0, Math.floor(Number(settings.defaultTreasuryBps || 0)));
  const burnBps = Math.max(0, Math.floor(Number(settings.defaultBurnBps || 0)));
  const split = splitLamportsByBps(totalLamports, { treasury: treasuryBps, burn: burnBps });
  let treasuryLamports = Math.max(0, Math.floor(Number(split.treasury || 0)));
  let burnLamports = Math.max(0, Math.floor(Number(split.burn || 0)));
  if (burnLamports > 0 && !config.devWalletPublicKey) {
    treasuryLamports += burnLamports;
    burnLamports = 0;
  }

  let treasurySig = null;
  let burnSig = null;
  if (treasuryLamports > 0) {
    treasurySig = await sendSolTransfer(connection, signer, config.treasuryWallet, treasuryLamports);
  }
  if (burnLamports > 0 && config.devWalletPublicKey) {
    burnSig = await sendSolTransfer(connection, signer, config.devWalletPublicKey, burnLamports);
    await addProtocolFeeCredit(pool, {
      userId: ownerUserId,
      symbol: "TRADE",
      lamports: burnLamports,
    });
  }

  await pool.query(
    `
      UPDATE protocol_metrics
      SET fees_taken_sol = fees_taken_sol + $2,
          updated_at = NOW()
      WHERE id = $1
    `,
    [1, fromLamports(totalLamports)]
  );

  if (treasuryLamports > 0) {
    await insertTelegramTradeEvent(
      ownerUserId,
      walletId,
      "fee",
      `Trading fee sent to treasury (${fromLamports(treasuryLamports).toFixed(6)} SOL).`,
      { amountSol: fromLamports(treasuryLamports), tx: treasurySig, mint: direction }
    );
  }
  if (burnLamports > 0) {
    await insertTelegramTradeEvent(
      ownerUserId,
      walletId,
      "fee",
      `Trading fee sent to dev burn wallet (${fromLamports(burnLamports).toFixed(6)} SOL).`,
      { amountSol: fromLamports(burnLamports), tx: burnSig, mint: direction }
    );
  }

  return {
    feeLamports: totalLamports,
    treasuryLamports,
    burnLamports,
    treasurySig,
    burnSig,
  };
}

async function ownerHasToolFeeWaiver(client, ownerUserId) {
  const ownerId = Math.max(0, Math.floor(Number(ownerUserId || 0)));
  if (!ownerId) return false;
  const res = await client.query(
    `
      SELECT COALESCE(is_admin, FALSE) AS is_admin, COALESCE(is_og, FALSE) AS is_og
      FROM users
      WHERE id = $1
      LIMIT 1
    `,
    [ownerId]
  );
  if (!res.rowCount) return false;
  return Boolean(res.rows[0].is_admin) || Boolean(res.rows[0].is_og);
}

async function syncToolBillingForOwner(client, row) {
  const current = row && typeof row === "object" ? row : null;
  if (!current?.id) return current;
  const waiveFees = await ownerHasToolFeeWaiver(client, Number(current.owner_user_id || 0));
  const toolType = normalizeToolType(current.tool_type, TOOL_TYPES.holderPooler);
  const currentUnlock = Math.max(0, Math.floor(Number(current.unlock_fee_lamports || 0)));
  const currentRuntime = Math.max(0, Math.floor(Number(current.runtime_fee_lamports || 0)));
  const currentWindow = Math.max(0, Math.floor(Number(current.runtime_fee_window_hours || 0)));
  const targetProfile = waiveFees
    ? { unlockFeeLamports: 0, runtimeFeeLamports: 0, runtimeFeeWindowHours: 0 }
    : buildToolFeeProfile(toolType, await getProtocolSettings(client));
  if (
    currentUnlock === targetProfile.unlockFeeLamports &&
    currentRuntime === targetProfile.runtimeFeeLamports &&
    currentWindow === targetProfile.runtimeFeeWindowHours
  ) {
    return current;
  }
  const updateRes = await client.query(
    `
      UPDATE tool_instances
      SET
        unlock_fee_lamports = $2,
        runtime_fee_lamports = $3,
        runtime_fee_window_hours = $4,
        updated_at = NOW()
      WHERE id = $1
      RETURNING *
    `,
    [
      String(current.id || ""),
      targetProfile.unlockFeeLamports,
      targetProfile.runtimeFeeLamports,
      targetProfile.runtimeFeeWindowHours,
    ]
  );
  return updateRes.rows[0] || current;
}

async function syncToolInstanceFundingState(client, row) {
  const billedRow = await syncToolBillingForOwner(client, row);
  const current = billedRow && typeof billedRow === "object" ? billedRow : {};
  const fundingPubkey = String(current.funding_wallet_pubkey || "").trim();
  const secretBase58 = String(current.funding_wallet_secret_key_base58 || "").trim();
  const unlockFeeLamports = Math.max(0, Math.floor(Number(current.unlock_fee_lamports || 0)));
  const reserveLamports = Math.max(0, Math.floor(Number(current.reserve_lamports || 0)));
  const requiredLamports = unlockFeeLamports + reserveLamports;
  let balanceLamports = 0;
  if (fundingPubkey) {
    try {
      balanceLamports = await getConnection().getBalance(new PublicKey(fundingPubkey), "confirmed");
    } catch {
      balanceLamports = 0;
    }
  }

  let nextRow = { ...current, current_balance_lamports: balanceLamports };
  if (!current.unlocked_at && balanceLamports >= requiredLamports && secretBase58) {
    try {
      const signer = keypairFromBase58(decryptDepositSecret(secretBase58));
      const feeResult = await applyToolUnlockFeeFlow(client, {
        connection: getConnection(),
        signer,
        ownerUserId: Number(current.owner_user_id || 0),
        toolInstanceId: String(current.id || ""),
        toolType: normalizeToolType(current.tool_type, TOOL_TYPES.holderPooler),
        totalLamports: unlockFeeLamports,
      });
      const unlockTx = feeResult.treasurySig || feeResult.burnSig || null;
      const updatedRes = await client.query(
        `
          UPDATE tool_instances
          SET
            status = $2,
            unlocked_at = COALESCE(unlocked_at, NOW()),
            unlock_tx = COALESCE($3, unlock_tx),
            last_error = NULL
          WHERE id = $1
          RETURNING *
        `,
        [String(current.id || ""), TOOL_STATUSES.ready, unlockTx]
      );
      nextRow = {
        ...(updatedRes.rows[0] || current),
        current_balance_lamports: await getConnection()
          .getBalance(new PublicKey(fundingPubkey), "confirmed")
          .catch(() => balanceLamports),
      };
      await insertToolEvent(
        client,
        Number(current.owner_user_id || 0),
        String(current.id || ""),
        normalizeToolType(current.tool_type, TOOL_TYPES.holderPooler),
        "status",
        `${toolTypeLabel(current.tool_type)} unlocked and ready.`,
        { tx: unlockTx, amount: fromLamports(unlockFeeLamports) }
      );
    } catch (error) {
      const message = String(error?.message || error || "Unlock failed.");
      const updatedRes = await client.query(
        `
          UPDATE tool_instances
          SET last_error = $2
          WHERE id = $1
          RETURNING *
        `,
        [String(current.id || ""), message]
      );
      nextRow = {
        ...(updatedRes.rows[0] || current),
        current_balance_lamports: balanceLamports,
      };
    }
  }

  return nextRow;
}

function buildToolCommandSummary(instance) {
  const target = instance.targetMint || instance.targetUrl || "unconfigured";
  const status = String(instance.status || TOOL_STATUSES.awaitingFunds).replace(/_/g, " ");
  return `${instance.label}: ${status} | ${target} | fee ${instance.unlockFeeSol.toFixed(3)} SOL`;
}

function getToolEmoji(toolType) {
  if (toolType === TOOL_TYPES.holderPooler) return "🪙";
  if (toolType === TOOL_TYPES.reactionManager) return "⚡";
  if (toolType === TOOL_TYPES.smartSell) return "🎯";
  if (toolType === TOOL_TYPES.bundleManager) return "📦";
  return "🧩";
}

function getStatusEmoji(status) {
  const normalized = String(status || "").trim().toLowerCase();
  if (normalized === TOOL_STATUSES.active) return "🟢";
  if (normalized === TOOL_STATUSES.ready) return "✅";
  if (normalized === TOOL_STATUSES.awaitingFunds) return "🟠";
  if (normalized === TOOL_STATUSES.provisioning) return "🔄";
  if (normalized === TOOL_STATUSES.paused) return "⏸";
  if (normalized === TOOL_STATUSES.failed) return "⚠";
  return "⚪";
}

function getBotEmoji(botType) {
  const normalized = String(botType || "").trim().toLowerCase();
  if (normalized === MODULE_TYPES.burn) return "🔥";
  if (normalized === MODULE_TYPES.volume) return "📈";
  if (normalized === MODULE_TYPES.marketMaker) return "📊";
  if (normalized === MODULE_TYPES.dca) return "🪙";
  if (normalized === MODULE_TYPES.rekindle) return "⚡";
  return "🤖";
}

export async function getTelegramToolOverview(userId) {
  const workspace = await getToolsWorkspace(userId);
  return {
    catalog: workspace.catalog.map((tool) => ({
      toolType: tool.toolType,
      label: tool.label,
      unlockFeeSol: tool.unlockFeeSol,
      reserveSol: tool.reserveSol,
    })),
    instances: workspace.instances.map((instance) => ({
      id: instance.id,
      toolType: instance.toolType,
      summary: buildToolCommandSummary(instance),
      fundingWalletPubkey: instance.fundingWalletPubkey,
      status: instance.status,
    })),
  };
}

function formatTelegramToolDetails(details) {
  const tool = details?.tool || {};
  const funding = details?.funding || {};
  const fundingState = details?.tool?.fundingState || null;
  const wallets = Array.isArray(details?.wallets) ? details.wallets : [];
  const events = Array.isArray(tool?.events) ? tool.events : [];
  const lines = [
    `${getToolEmoji(tool.toolType)} ${tool.label || "Tool"}`,
    `Status: ${getStatusEmoji(tool.status)} ${String(tool.status || "").replace(/_/g, " ")}`,
    tool.targetMint ? `Mint: ${tool.targetMint}` : tool.targetUrl ? `Target: ${tool.targetUrl}` : "",
    `Funding wallet: ${funding.walletPubkey || tool.fundingWalletPubkey || "-"}`,
    `Funding SOL: ${Number(funding.solBalance || 0).toFixed(6)}`,
  ];
  if (fundingState?.mode) {
    lines.push(`Funding mode: ${fundingState.modeLabel}`);
    if (fundingState.orderId) {
      lines.push(`EMBER Funding: ${fundingState.statusText || fundingState.statusShort || "pending"} (${fundingState.orderId})`);
    }
  }
  if (tool.toolType === TOOL_TYPES.holderPooler) {
    lines.push(`Funding token: ${Number(funding.tokenBalance || 0).toFixed(6)}`);
    lines.push(`Holder wallets: ${wallets.length}`);
    lines.push(`Mode: one-way holder distribution only.`);
    lines.push(`Important: recipient wallets are not reclaimable later.`);
  } else if (tool.toolType === TOOL_TYPES.reactionManager) {
    lines.push(`Reaction target: ${tool.targetUrl || "-"}`);
    lines.push(`Order: ${tool.state?.reactionOrderId || "Not started"}`);
    lines.push(`Remaining: ${Number(tool.state?.reactionRemains || 0)}`);
  } else if (tool.toolType === TOOL_TYPES.smartSell) {
    lines.push(`Funding token: ${Number(funding.tokenBalance || 0).toFixed(6)}`);
    lines.push(`Sell wallets: ${wallets.length}`);
    lines.push(`Pending signals: ${Array.isArray(tool.state?.pendingSignals) ? tool.state.pendingSignals.length : 0}`);
  } else if (tool.toolType === TOOL_TYPES.bundleManager) {
    lines.push(`Funding token: ${Number(funding.tokenBalance || 0).toFixed(6)}`);
    lines.push(`Bundle wallets: ${wallets.length}`);
    lines.push(`Mode: managed or imported wallet bundle.`);
  }
  if (events.length) {
    lines.push("");
    lines.push("Recent:");
    for (const event of events.slice(0, 3)) {
      lines.push(`- ${event.message}`);
    }
  }
  return lines.filter(Boolean).join("\n");
}

function buildTelegramToolsOverviewText(overview) {
  const catalogLines = overview.catalog.map(
    (item) => `${getToolEmoji(item.toolType)} ${item.label}: ${item.unlockFeeSol.toFixed(3)} SOL`
  );
  const instanceLines = overview.instances.length
    ? overview.instances.slice(0, 8).map((item) => {
        const state = item?.fundingState || null;
        const fundingSuffix =
          item.status === TOOL_STATUSES.awaitingFunds
            ? ` | fund ${Number(item.requiredSol || 0).toFixed(3)} SOL`
            : state?.mode === TOOL_FUNDING_MODES.ember && state?.statusText
              ? ` | ${state.statusText}`
              : "";
        return `${getStatusEmoji(item.status)} ${item.summary}${fundingSuffix}`;
      })
    : ['\u{1F4ED} No tool instances yet.'];
  const activeTools = (overview.instances || []).filter((item) => item.status === TOOL_STATUSES.active).length;
  return [
    '\u{1F9E9} EMBER Tools',
    '',
    'Create and control paid tool instances directly from Telegram.',
    `${activeTools}/${Number(overview.instances?.length || 0)} active`,
    '',
    'Catalog:',
    ...catalogLines,
    '',
    'Your instances:',
    ...instanceLines,
  ].join('\n');
}

function buildTelegramToolsKeyboard(overview) {
  const rows = (overview.instances || [])
    .slice(0, 8)
    .map((item) => [{ text: `${getToolEmoji(item.toolType)} ${item.summary.slice(0, 40)}`, callback_data: `tool:${item.id}` }]);
  rows.push([{ text: '\u2728 Create New Tool', callback_data: 'tools:new' }]);
  rows.push([{ text: '\u{1F504} Refresh Tools', callback_data: 'tools' }]);
  rows.push([{ text: '\u{1F3E0} Main Menu', callback_data: 'menu' }]);
  return { inline_keyboard: rows };
}

function buildTelegramToolKeyboard(details) {
  const tool = details?.tool || {};
  const fundingState = details?.tool?.fundingState || null;
  const permissions = details?.permissions || {};
  const wallets = Array.isArray(details?.wallets) ? details.wallets : [];
  const config = tool?.config || {};
  const rows = [];
  if (tool.toolType === TOOL_TYPES.holderPooler) {
    rows.push([{ text: 'Run Distribution', callback_data: `confirm:holder_run:${tool.id}` }]);
  } else if (tool.toolType === TOOL_TYPES.reactionManager) {
    rows.push([
      { text: 'Start Campaign', callback_data: `act:reaction_run:${tool.id}` },
      { text: 'Refresh Status', callback_data: `act:reaction_status:${tool.id}` },
    ]);
  } else if (tool.toolType === TOOL_TYPES.smartSell) {
    rows.push([
      { text: 'Arm', callback_data: `act:smart_arm:${tool.id}` },
      { text: 'Pause', callback_data: `act:smart_pause:${tool.id}` },
    ]);
    rows.push([{ text: `Funding: ${toolFundingModeLabel(config.fundingMode)}`, callback_data: `cfg:tool_funding:${tool.id}:toggle` }]);
    rows.push([{ text: 'Reclaim', callback_data: `confirm:smart_reclaim:${tool.id}` }]);
  } else if (tool.toolType === TOOL_TYPES.bundleManager) {
    rows.push([
      { text: 'Run Campaign', callback_data: `act:bundle_run:${tool.id}` },
      { text: 'Reclaim', callback_data: `confirm:bundle_reclaim:${tool.id}` },
    ]);
    rows.push([{ text: `Funding: ${toolFundingModeLabel(config.fundingMode)}`, callback_data: `cfg:tool_funding:${tool.id}:toggle` }]);
  }
  if (fundingState?.mode === TOOL_FUNDING_MODES.ember && fundingState?.orderId) {
    rows.push([{ text: 'Refresh EMBER Funding', callback_data: `act:tool_funding_refresh:${tool.id}` }]);
  }
  rows.push([{ text: '\u{1F4B0} Funding Wallet', callback_data: `tool:funding:${tool.id}` }]);
  if (wallets.length) {
    rows.push([{ text: '\u{1F45B} Managed Wallets', callback_data: `tool:wallets:${tool.id}` }]);
  }
  if (Array.isArray(tool?.events) && tool.events.length) {
    rows.push([{ text: '\u{1F4DC} Activity', callback_data: `tool:events:${tool.id}` }]);
  }
  if (permissions.canManageFunds) {
    rows.push([{ text: '\u{1F511} Funding Key', callback_data: `tool:key:${tool.id}` }]);
  }
  rows.push([{ text: '\u2699\uFE0F Settings', callback_data: `tool_settings:${tool.id}` }]);
  rows.push([
    { text: '\u{1F504} Refresh', callback_data: `tool:${tool.id}` },
    { text: '\u2B05 Back', callback_data: 'tools' },
  ]);
  return { inline_keyboard: rows };
}

function buildTelegramToolFundingText(details = {}) {
  const tool = details?.tool || {};
  const funding = details?.funding || {};
  const fundingState = tool?.fundingState || null;
  const lines = [
    `${getToolEmoji(tool.toolType)} ${tool.label || "Tool"} Funding`,
    "",
    `Wallet: ${funding.walletPubkey || tool.fundingWalletPubkey || "-"}`,
    `SOL: ${Number(funding.solBalance || 0).toFixed(6)}`,
  ];
  if (tool.targetMint) {
    lines.push(`Token: ${Number(funding.tokenBalance || 0).toFixed(6)}`);
  }
  lines.push(`Required unlock: ${Number(tool.requiredSol || 0).toFixed(6)} SOL`);
  if (fundingState?.mode) {
    lines.push(`Funding mode: ${fundingState.modeLabel || toolFundingModeLabel(fundingState.mode)}`);
  }
  if (fundingState?.orderId) {
    lines.push(`EMBER Funding: ${fundingState.statusText || fundingState.statusShort || "pending"} (${fundingState.orderId})`);
    if (fundingState.depositWalletAddress) {
      lines.push(`EMBER deposit: ${fundingState.depositWalletAddress}`);
    }
    if (fundingState.depositAmountSol > 0) {
      lines.push(`EMBER amount: ${Number(fundingState.depositAmountSol || 0).toFixed(6)} SOL`);
    }
    if (fundingState.sentSol > 0) {
      lines.push(`Sent: ${Number(fundingState.sentSol || 0).toFixed(6)} SOL`);
    }
    if (fundingState.recipientCount > 0) {
      lines.push(`Recipients: ${Number(fundingState.recipientCount || 0)}`);
    }
  }
  if (tool.lastError) {
    lines.push("");
    lines.push(`Last error: ${tool.lastError}`);
  }
  return lines.filter(Boolean).join("\n");
}

function buildTelegramToolFundingKeyboard(details = {}) {
  const tool = details?.tool || {};
  const fundingState = tool?.fundingState || null;
  const permissions = details?.permissions || {};
  const rows = [];
  if (fundingState?.mode === TOOL_FUNDING_MODES.ember && fundingState?.orderId) {
    rows.push([{ text: '\u{1F504} Refresh EMBER Funding', callback_data: `act:tool_funding_refresh:${tool.id}` }]);
  }
  if (permissions.canManageFunds) {
    rows.push([{ text: '\u{1F511} Show Funding Key', callback_data: `tool:key:${tool.id}` }]);
  }
  if (fundingState?.depositWalletAddress) {
    rows.push([{ text: '\u{1F517} EMBER Deposit', url: `https://solscan.io/account/${fundingState.depositWalletAddress}` }]);
  }
  const fundingWallet = String(details?.funding?.walletPubkey || tool.fundingWalletPubkey || "").trim();
  if (fundingWallet) {
    rows.push([{ text: '\u{1F517} View on Solscan', url: `https://solscan.io/account/${fundingWallet}` }]);
  }
  rows.push([
    { text: '\u{1F504} Refresh', callback_data: `tool:${tool.id}` },
    { text: '\u2B05 Back to Tool', callback_data: `tool:${tool.id}` },
  ]);
  return { inline_keyboard: rows };
}

function buildTelegramToolWalletsText(details = {}) {
  const tool = details?.tool || {};
  const wallets = Array.isArray(details?.wallets) ? details.wallets : [];
  if (!wallets.length) {
    return [
      `${getToolEmoji(tool.toolType)} ${tool.label || "Tool"} Wallets`,
      "",
      "No managed wallets are active yet.",
    ].join("\n");
  }
  const rows = wallets.slice(0, 12).map((wallet) => {
    const label = String(wallet.label || "Wallet").trim();
    const pubkey = String(wallet.publicKey || wallet.walletPubkey || "").trim();
    const sol = Number(wallet.solBalance || 0).toFixed(4);
    const token = Number(wallet.tokenBalance || 0).toFixed(4);
    const flags = [
      wallet.imported ? "imported" : "managed",
      wallet.active === false ? "paused" : "",
    ].filter(Boolean).join(", ");
    return `- ${label} | ${sol} SOL | ${token} TOK${flags ? ` | ${flags}` : ""}${pubkey ? `\n  ${pubkey}` : ""}`;
  });
  return [
    `${getToolEmoji(tool.toolType)} ${tool.label || "Tool"} Wallets`,
    "",
    ...rows,
  ].join("\n");
}

function buildTelegramToolWalletsKeyboard(details = {}) {
  const tool = details?.tool || {};
  const wallets = Array.isArray(details?.wallets) ? details.wallets : [];
  const permissions = details?.permissions || {};
  const rows = wallets.slice(0, 10).map((wallet) => {
    const pubkey = String(wallet.publicKey || wallet.walletPubkey || "").trim();
    return [{
      text: `\u{1F517} ${String(wallet.label || "Wallet").trim().slice(0, 20)}`,
      url: `https://solscan.io/account/${pubkey}`,
    }];
  });
  if (permissions.isOwner && wallets.length) {
    rows.push([{ text: '\u{1F511} Show Wallet Keys', callback_data: `tool:wallet_keys:${tool.id}` }]);
  }
  rows.push([
    { text: '\u{1F504} Refresh', callback_data: `tool:${tool.id}` },
    { text: '\u2B05 Back to Tool', callback_data: `tool:${tool.id}` },
  ]);
  return { inline_keyboard: rows };
}

function buildTelegramToolEventsText(details = {}) {
  const tool = details?.tool || {};
  const events = Array.isArray(tool?.events) ? tool.events : [];
  if (!events.length) {
    return [
      `${getToolEmoji(tool.toolType)} ${tool.label || "Tool"} Activity`,
      "",
      "No activity yet.",
    ].join("\n");
  }
  return [
    `${getToolEmoji(tool.toolType)} ${tool.label || "Tool"} Activity`,
    "",
    ...events.slice(0, 10).map((event) => `- ${event.message}`),
  ].join("\n");
}

function buildTelegramToolEventsKeyboard(details = {}) {
  const tool = details?.tool || {};
  return {
    inline_keyboard: [[
      { text: '\u{1F504} Refresh', callback_data: `tool:${tool.id}` },
      { text: '\u2B05 Back to Tool', callback_data: `tool:${tool.id}` },
    ]],
  };
}

async function getToolFundingSecretForOwner(userId, toolId) {
  return withTx(async (client) => {
    const { scope, row } = await readToolInstanceForScope(client, userId, toolId, { forUpdate: false });
    assertOwnerPermission(scope, "Managers cannot reveal tool funding keys.");
    return {
      id: String(row.id || ""),
      label: String(row.title || toolTypeLabel(row.tool_type || TOOL_TYPES.holderPooler)),
      toolType: normalizeToolType(row.tool_type, TOOL_TYPES.holderPooler),
      publicKey: String(row.funding_wallet_pubkey || "").trim(),
      secretKeyBase58: decryptDepositSecret(String(row.funding_wallet_secret_key_base58 || "")),
    };
  });
}

async function getToolManagedWalletSecretsForOwner(userId, toolId) {
  return withTx(async (client) => {
    const { scope, row } = await readToolInstanceForScope(client, userId, toolId, { forUpdate: false });
    assertOwnerPermission(scope, "Managers cannot reveal tool wallet keys.");
    const walletsRes = await client.query(
      `
        SELECT id, label, wallet_pubkey, secret_key_base58, imported
        FROM tool_managed_wallets
        WHERE tool_instance_id = $1
        ORDER BY position ASC, created_at ASC
      `,
      [String(row.id || "")]
    );
    return {
      id: String(row.id || ""),
      label: String(row.title || toolTypeLabel(row.tool_type || TOOL_TYPES.holderPooler)),
      toolType: normalizeToolType(row.tool_type, TOOL_TYPES.holderPooler),
      wallets: (walletsRes.rows || []).map((walletRow) => ({
        id: String(walletRow.id || ""),
        label: String(walletRow.label || "").trim(),
        publicKey: String(walletRow.wallet_pubkey || "").trim(),
        secretKeyBase58: decryptDepositSecret(String(walletRow.secret_key_base58 || "")),
        imported: Boolean(walletRow.imported),
      })),
    };
  });
}

function buildTelegramToolFundingKeyText(fundingWallet = {}) {
  return [
    `\u{1F511} ${getToolEmoji(fundingWallet.toolType)} ${fundingWallet.label || "Tool"} Funding Key`,
    "",
    `Wallet: ${fundingWallet.publicKey || "-"}`,
    "",
    "This private key controls the funding wallet for this tool instance.",
    "Anyone with this key can move the funds.",
    "",
    `${fundingWallet.secretKeyBase58 || "-"}`,
  ].join("\n");
}

function buildTelegramToolWalletKeysText(payload = {}) {
  const wallets = Array.isArray(payload?.wallets) ? payload.wallets : [];
  if (!wallets.length) {
    return [
      `\u{1F511} ${getToolEmoji(payload?.toolType)} ${payload?.label || "Tool"} Wallet Keys`,
      "",
      "No managed wallets are attached to this tool yet.",
    ].join("\n");
  }
  return [
    `\u{1F511} ${getToolEmoji(payload?.toolType)} ${payload?.label || "Tool"} Wallet Keys`,
    "",
    ...wallets.flatMap((wallet) => [
      `${wallet.label || "Wallet"}${wallet.imported ? " (Imported)" : ""}`,
      `Address: ${wallet.publicKey || "-"}`,
      `Key: ${wallet.secretKeyBase58 || "-"}`,
      "",
    ]),
  ].join("\n").trim();
}

function formatReactionTypeLabel(reactionType) {
  const type = String(reactionType || 'rocket').trim().toLowerCase();
  if (type === 'fire') return 'Fire';
  if (type === 'poop') return 'Poop';
  if (type === 'flag') return 'Flag';
  return 'Rocket';
}

function stepTelegramDecimal(currentValue, step, minValue, maxValue, direction, digits = 4) {
  const current = Number(currentValue || 0);
  const delta = direction === 'dec' ? -Math.abs(Number(step || 0)) : Math.abs(Number(step || 0));
  const next = Math.max(Number(minValue || 0), Math.min(Number(maxValue || 0), current + delta));
  return Number(next.toFixed(digits));
}

function cycleTelegramValue(currentValue, options = []) {
  const list = Array.isArray(options) ? options.filter(Boolean) : [];
  if (!list.length) return currentValue;
  const index = Math.max(0, list.indexOf(currentValue));
  return list[(index + 1) % list.length];
}

function buildTelegramToolSettingsText(details) {
  const tool = details?.tool || {};
  const config = tool?.config || {};
  const lines = [
    `\u2699\uFE0F ${tool.label || 'Tool'} Settings`,
    '',
    `Title: ${tool.title || tool.label || 'Tool'}`,
  ];
  if (tool.toolType === TOOL_TYPES.holderPooler) {
    lines.push(`Wallets: ${Math.max(1, Number(config.walletCount || 10))}`);
    lines.push(`Tokens Per Wallet: ${Number(config.tokenAmountPerWallet || 1).toFixed(2)}`);
    lines.push('This is a one-way holder spread. Those recipient wallets are not reclaimed later.');
  } else if (tool.toolType === TOOL_TYPES.reactionManager) {
    lines.push(`Reaction: ${formatReactionTypeLabel(config.reactionType)}`);
    lines.push(`Target Count: ${Math.max(1000, Number(config.targetCount || 1000))}`);
    lines.push('One reaction type runs against the chosen DexScreener link until the target count is reached.');
  } else if (tool.toolType === TOOL_TYPES.smartSell) {
    lines.push(`Funding: ${toolFundingModeLabel(config.fundingMode)}`);
    lines.push(`Trigger: ${String(config.triggerMode || 'every_buy').replace('_', ' ')}`);
    lines.push(`Sell %: ${Math.max(1, Number(config.sellPct || 25))}%`);
    lines.push(`Threshold: ${Number(config.thresholdSol || 0).toFixed(3)} SOL`);
    lines.push(`Timing: ${String(config.timingMode || 'randomized')}`);
    lines.push(`Wallets: ${Math.max(1, Number(config.walletCount || 6))}`);
    lines.push('Smart Sell reacts to buy flow and rotates through the managed sell wallets.');
  } else if (tool.toolType === TOOL_TYPES.bundleManager) {
    lines.push(`Funding: ${toolFundingModeLabel(config.fundingMode)}`);
    lines.push(`Wallet Mode: ${String(config.walletMode || 'managed')}`);
    lines.push(`Wallets: ${Math.max(1, Number(config.walletCount || 10))}`);
    lines.push(`Buy SOL Per Wallet: ${Number(config.buySolPerWallet || 0.01).toFixed(3)}`);
    lines.push(`Sell % Per Wallet: ${Math.max(1, Number(config.sellPctPerWallet || 25))}%`);
    lines.push(`Buy/Sell Bias: ${Math.max(0, Number(config.sideBias || 50))}% / ${100 - Math.max(0, Number(config.sideBias || 50))}%`);
    lines.push(`Reserve: ${Number(config.walletReserveSol || 0.0015).toFixed(4)} SOL`);
    if (String(config.walletMode || 'managed') === 'imported') {
      lines.push('Imported wallets can be added from Telegram.');
    }
    lines.push('Bundle Manager spreads buys and sells across the configured wallet set.');
  }
  lines.push('');
  lines.push('Use the buttons below to tune the live behavior.');
  return lines.join('\n');
}

function buildTelegramToolSettingsKeyboard(details) {
  const tool = details?.tool || {};
  const config = tool?.config || {};
  const rows = [];
  if (tool.toolType === TOOL_TYPES.holderPooler) {
    rows.push([
      { text: '\u2796 Fewer Wallets', callback_data: `cfg:tool_setting:${tool.id}:wallets:dec` },
      { text: '\u2795 More Wallets', callback_data: `cfg:tool_setting:${tool.id}:wallets:inc` },
    ]);
    rows.push([
      { text: '\u2796 Less Per Wallet', callback_data: `cfg:tool_setting:${tool.id}:tokens:dec` },
      { text: '\u2795 More Per Wallet', callback_data: `cfg:tool_setting:${tool.id}:tokens:inc` },
    ]);
  } else if (tool.toolType === TOOL_TYPES.reactionManager) {
    rows.push([{ text: `Reaction: ${formatReactionTypeLabel(config.reactionType)}`, callback_data: `cfg:tool_setting:${tool.id}:reaction:cycle` }]);
    rows.push([
      { text: '\u2796 Lower Target', callback_data: `cfg:tool_setting:${tool.id}:target:dec` },
      { text: '\u2795 Higher Target', callback_data: `cfg:tool_setting:${tool.id}:target:inc` },
    ]);
  } else if (tool.toolType === TOOL_TYPES.smartSell) {
    rows.push([{ text: `Funding: ${toolFundingModeLabel(config.fundingMode)}`, callback_data: `cfg:tool_setting:${tool.id}:funding:toggle` }]);
    rows.push([{ text: `Trigger: ${String(config.triggerMode || 'every_buy').replace('_', ' ')}`, callback_data: `cfg:tool_setting:${tool.id}:trigger:cycle` }]);
    rows.push([
      { text: '\u2796 Lower Sell %', callback_data: `cfg:tool_setting:${tool.id}:sellpct:dec` },
      { text: '\u2795 Higher Sell %', callback_data: `cfg:tool_setting:${tool.id}:sellpct:inc` },
    ]);
    rows.push([
      { text: '\u2796 Lower Threshold', callback_data: `cfg:tool_setting:${tool.id}:threshold:dec` },
      { text: '\u2795 Higher Threshold', callback_data: `cfg:tool_setting:${tool.id}:threshold:inc` },
    ]);
    rows.push([{ text: `Timing: ${String(config.timingMode || 'randomized')}`, callback_data: `cfg:tool_setting:${tool.id}:timing:cycle` }]);
    rows.push([
      { text: '\u2796 Fewer Wallets', callback_data: `cfg:tool_setting:${tool.id}:wallets:dec` },
      { text: '\u2795 More Wallets', callback_data: `cfg:tool_setting:${tool.id}:wallets:inc` },
    ]);
  } else if (tool.toolType === TOOL_TYPES.bundleManager) {
    rows.push([{ text: `Funding: ${toolFundingModeLabel(config.fundingMode)}`, callback_data: `cfg:tool_setting:${tool.id}:funding:toggle` }]);
    rows.push([{ text: `Wallet Mode: ${String(config.walletMode || 'managed')}`, callback_data: `cfg:tool_setting:${tool.id}:walletmode:cycle` }]);
    rows.push([
      { text: '\u2796 Fewer Wallets', callback_data: `cfg:tool_setting:${tool.id}:wallets:dec` },
      { text: '\u2795 More Wallets', callback_data: `cfg:tool_setting:${tool.id}:wallets:inc` },
    ]);
    rows.push([
      { text: '\u2796 Smaller Buys', callback_data: `cfg:tool_setting:${tool.id}:buysol:dec` },
      { text: '\u2795 Larger Buys', callback_data: `cfg:tool_setting:${tool.id}:buysol:inc` },
    ]);
    rows.push([
      { text: '\u2796 Lower Sell %', callback_data: `cfg:tool_setting:${tool.id}:sellpct:dec` },
      { text: '\u2795 Higher Sell %', callback_data: `cfg:tool_setting:${tool.id}:sellpct:inc` },
    ]);
    rows.push([
      { text: '\u2796 Less Buy Bias', callback_data: `cfg:tool_setting:${tool.id}:bias:dec` },
      { text: '\u2795 More Buy Bias', callback_data: `cfg:tool_setting:${tool.id}:bias:inc` },
    ]);
    rows.push([
      { text: '\u2796 Lower Reserve', callback_data: `cfg:tool_setting:${tool.id}:reserve:dec` },
      { text: '\u2795 Higher Reserve', callback_data: `cfg:tool_setting:${tool.id}:reserve:inc` },
    ]);
    if (String(config.walletMode || 'managed') === 'imported') {
      rows.push([{ text: '\u{1F4E5} Add Imported Wallets', callback_data: `cfg:tool_setting:${tool.id}:imports:prompt` }]);
    }
  }
  rows.push([
    { text: '\u{1F504} Refresh', callback_data: `tool_settings:${tool.id}` },
    { text: '\u2B05 Back to Tool', callback_data: `tool:${tool.id}` },
  ]);
  return { inline_keyboard: rows };
}

function buildTelegramMainKeyboard(stats = null) {
  const toolLabel = stats ? `🧩 Tools (${Number(stats.toolsActive || 0)}/${Number(stats.toolsTotal || 0)})` : '🧩 Tools';
  const botLabel = stats ? `🤖 Bots (${Number(stats.tokensActive || 0)}/${Number(stats.tokensTotal || 0)})` : '🤖 Bots';
  const tradeLabel = stats ? `💱 Trade (${Number(stats.tradeWallets || 0)})` : '💱 Trade';
  return {
    inline_keyboard: [
      [
        { text: toolLabel, callback_data: 'tools' },
        { text: '\u2728 Create Tool', callback_data: 'tools:new' },
      ],
      [
        { text: botLabel, callback_data: 'menu:bots' },
        { text: '\u{1F680} Deploy', callback_data: 'menu:deploy' },
      ],
      [{ text: tradeLabel, callback_data: 'menu:trade' }],
    ],
  };
}

async function getTelegramMainMenuStats(userId) {
  const scope = await resolveUserAccessScopeFromPool(userId);
  const [tokensRes, toolsRes, tradeRes] = await Promise.all([
    pool.query(
      `
        SELECT COUNT(*)::int AS total, COUNT(*) FILTER (WHERE active = TRUE)::int AS active
        FROM tokens
        WHERE user_id = $1
      `,
      [scope.ownerUserId]
    ),
    pool.query(
      `
        SELECT COUNT(*)::int AS total, COUNT(*) FILTER (WHERE status = $2)::int AS active
        FROM tool_instances
        WHERE owner_user_id = $1
      `,
      [scope.ownerUserId, TOOL_STATUSES.active]
    ),
    pool.query(
      `
        SELECT COUNT(*)::int AS total
        FROM telegram_trade_wallet_slots
        WHERE user_id = $1
      `,
      [Number(userId || 0)]
    ),
  ]);
  return {
    tokensTotal: Number(tokensRes.rows?.[0]?.total || 0),
    tokensActive: Number(tokensRes.rows?.[0]?.active || 0),
    toolsTotal: Number(toolsRes.rows?.[0]?.total || 0),
    toolsActive: Number(toolsRes.rows?.[0]?.active || 0),
    tradeWallets: Number(tradeRes.rows?.[0]?.total || 0),
  };
}

function buildTelegramMainMenuText(stats = null) {
  const lines = [
    '\u{1F525} EMBER Telegram',
    '',
    'Choose what you want to control.',
    'This menu is built for button-first control.',
    'Tools, Bots, Deploy, and Trade are live here.',
    'Use Create Tool to launch a new paid tool instance directly from chat.',
  ];
  if (stats) {
    lines.push('');
    lines.push(`Bots: ${Number(stats.tokensActive || 0)}/${Number(stats.tokensTotal || 0)} active`);
    lines.push(`Tools: ${Number(stats.toolsActive || 0)}/${Number(stats.toolsTotal || 0)} active`);
    lines.push(`Trade wallets: ${Number(stats.tradeWallets || 0)}`);
  }
  return lines.join('\n');
}

export async function getTelegramBotOverview(userId) {
  const dashboard = await getDashboard(userId);
  const tokens = Array.isArray(dashboard?.tokens) ? dashboard.tokens : [];
  return {
    tokens: tokens.slice(0, 12).map((token) => ({
      id: String(token.id || ''),
      symbol: String(token.symbol || token.name || 'TOKEN'),
      mint: String(token.mint || ''),
      selectedBot: String(token.selectedBot || 'burn'),
      active: Boolean(token.active),
      moduleEnabled: Boolean(token.moduleEnabled),
      txCount: Number(token.txCount || 0),
    })),
  };
}

function buildTelegramBotOverviewText(overview) {
  const tokenLines = overview.tokens.length
    ? overview.tokens.map((token) => {
        const status = token.active ? '\u{1F7E2} active' : '\u23F8 paused';
        return `${getBotEmoji(token.selectedBot)} ${token.symbol}: ${String(token.selectedBot || 'burn').toUpperCase()} (${status})`;
      })
    : ['\u{1F4ED} No attached tokens yet.'];
  return [
    '\u{1F916} EMBER Bots',
    '',
    'Attach and control tokens directly from Telegram.',
    '',
    'Your tokens:',
    ...tokenLines,
  ].join('\n');
}

function buildTelegramBotsKeyboard(overview) {
  const rows = overview.tokens.map((token) => [
    { text: `${getBotEmoji(token.selectedBot)} ${token.active ? '\u{1F7E2}' : '\u23F8'} ${token.symbol}`, callback_data: `bot:${token.id}` },
  ]);
  rows.push([{ text: '\u{1F517} Attach Existing Token', callback_data: 'bots:attach' }]);
  rows.push([{ text: '\u{1F504} Refresh Bots', callback_data: 'menu:bots' }]);
  rows.push([{ text: '\u{1F3E0} Main Menu', callback_data: 'menu' }]);
  return { inline_keyboard: rows };
}

async function getTelegramBotDetails(userId, tokenId) {
  const dashboard = await getDashboard(userId);
  const token = (Array.isArray(dashboard?.tokens) ? dashboard.tokens : []).find((item) => String(item.id || '') === String(tokenId || ''));
  if (!token) throw new Error('Token not found.');
  const scope = await resolveUserAccessScopeFromPool(userId);
  return {
    ...token,
    permissions: {
      role: scope.role,
      isOwner: scope.isOwner,
      canManageFunds: scope.canManageFunds,
    },
  };
}

function formatTelegramBotDetails(token) {
  return [
    `${getBotEmoji(token.selectedBot)} ${String(token.symbol || token.name || 'Token')} Bot`,
    `Mint: ${String(token.mint || '-')}`,
    `Deposit: ${String(token.deposit || '-')}`,
    `Mode: ${String(token.selectedBot || 'burn').toUpperCase()}`,
    `Status: ${Boolean(token.active) ? '\u{1F7E2} active' : '\u23F8 paused'}`,
    `Transactions: ${Number(token.txCount || 0)}`,
  ].join('\n');
}

function formatTelegramEventAge(createdAt) {
  const ts = new Date(createdAt || 0).getTime();
  if (!Number.isFinite(ts) || ts <= 0) return 'just now';
  const seconds = Math.max(0, Math.floor((Date.now() - ts) / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

async function getTelegramBotLiveSnapshot(userId, tokenId) {
  const live = await getTokenLiveDetails(userId, tokenId);
  const scope = await resolveUserAccessScopeFromPool(userId);
  const eventsRes = await pool.query(
    `
      SELECT event_type, message, tx, created_at
      FROM token_events
      WHERE user_id = $1 AND token_id = $2
      ORDER BY created_at DESC
      LIMIT 6
    `,
    [scope.ownerUserId, String(tokenId || '').trim()]
  );
  return {
    live,
    events: (eventsRes.rows || []).map((row) => ({
      type: String(row.event_type || '').trim(),
      message: String(row.message || '').trim(),
      tx: String(row.tx || '').trim(),
      createdAt: row.created_at,
    })),
  };
}

async function getTelegramBotWalletSecrets(userId, tokenId) {
  const scope = await resolveUserAccessScopeFromPool(userId);
  assertOwnerPermission(scope, "Managers cannot reveal bot wallet keys.");
  const tokenRes = await pool.query(
    `
      SELECT id, symbol, name, mint
      FROM tokens
      WHERE user_id = $1
        AND id = $2
      LIMIT 1
    `,
    [scope.ownerUserId, String(tokenId || '').trim()]
  );
  if (!tokenRes.rowCount) throw new Error('Token not found.');
  const token = tokenRes.rows[0];
  const walletRows = [];
  const depositRes = await pool.query(
    `
      SELECT deposit_pubkey AS wallet_pubkey, secret_key_base58, 'deposit' AS wallet_type
      FROM token_deposit_keys
      WHERE token_id = $1
      LIMIT 1
    `,
    [String(tokenId || '').trim()]
  );
  walletRows.push(...(depositRes.rows || []));
  const tradeRes = await pool.query(
    `
      SELECT wallet_pubkey, secret_key_base58, 'trade' AS wallet_type, label
      FROM volume_trade_wallets
      WHERE token_id = $1
      ORDER BY created_at ASC
    `,
    [String(tokenId || '').trim()]
  );
  walletRows.push(...(tradeRes.rows || []));
  return {
    token: {
      id: String(token.id || ''),
      symbol: String(token.symbol || token.name || 'TOKEN'),
      mint: String(token.mint || ''),
    },
    wallets: walletRows.map((row, index) => ({
      walletType: String(row.wallet_type || '').trim() || (index === 0 ? 'deposit' : 'trade'),
      label: String(row.label || (String(row.wallet_type || '').trim() === 'trade' ? `Trade ${index}` : 'Deposit')).trim(),
      publicKey: String(row.wallet_pubkey || '').trim(),
      secretKeyBase58: decryptDepositSecret(String(row.secret_key_base58 || '')),
    })),
  };
}

function buildTelegramBotLiveText(token, snapshot = {}) {
  const live = snapshot?.live || {};
  const addresses = Array.isArray(live.addresses) ? live.addresses : [];
  const totals = live.totals || {};
  const creatorRewards = live.creatorRewards || {};
  const events = Array.isArray(snapshot?.events) ? snapshot.events : [];
  const eventLines = events.length
    ? events.slice(0, 5).map((event) => `\u2022 ${event.message} (${formatTelegramEventAge(event.createdAt)})`)
    : ['\u2022 No recent activity yet.'];
  return [
    `${getBotEmoji(token?.selectedBot)} ${String(token?.symbol || token?.name || 'Token')} Live`,
    '',
    `Bot: ${String(token?.selectedBot || 'burn').toUpperCase()}`,
    `Status: ${Boolean(token?.active) ? '\u{1F7E2} active' : '\u23F8 paused'}`,
    `Wallets tracked: ${addresses.length}`,
    `Total SOL: ${Number(totals.sol || 0).toFixed(6)}`,
    `Total Token: ${Number(totals.token || 0).toFixed(6)}`,
    `Creator Rewards Preview: ${Number(creatorRewards.totalSol || 0).toFixed(6)} SOL`,
    '',
    'Recent activity:',
    ...eventLines,
  ].join('\n');
}

function buildTelegramBotLiveKeyboard(tokenId) {
  return {
    inline_keyboard: [
      [{ text: '\u{1F4BC} Wallets', callback_data: `bot:wallets:${tokenId}` }],
      [
        { text: '\u{1F504} Refresh Live', callback_data: `bot:live:${tokenId}` },
        { text: '\u2B05 Back to Bot', callback_data: `bot:${tokenId}` },
      ],
    ],
  };
}

function buildTelegramBotWalletsText(token, snapshot = {}) {
  const addresses = Array.isArray(snapshot?.live?.addresses) ? snapshot.live.addresses : [];
  if (!addresses.length) {
    return [
      `${getBotEmoji(token?.selectedBot)} ${String(token?.symbol || token?.name || 'Token')} Wallets`,
      '',
      'No tracked wallets found for this token.',
    ].join('\n');
  }
  return [
    `${getBotEmoji(token?.selectedBot)} ${String(token?.symbol || token?.name || 'Token')} Wallets`,
    '',
    ...addresses.map((entry) => {
      const typeLabel = String(entry.type || '').trim().toLowerCase() === 'trade' ? 'Trade' : 'Deposit';
      return `${typeLabel}: ${entry.label} \u2022 ${entry.pubkey}\nSOL ${Number(entry.solBalance || 0).toFixed(6)} \u2022 TOK ${Number(entry.tokenBalance || 0).toFixed(6)}`;
    }),
  ].join('\n');
}

function buildTelegramBotWalletsKeyboard(token, snapshot = {}) {
  const tokenId = String(token?.id || '').trim();
  const addresses = Array.isArray(snapshot?.live?.addresses) ? snapshot.live.addresses : [];
  const permissions = token?.permissions || {};
  const rows = addresses.slice(0, 8).map((entry) => [
    { text: `${entry.type === 'trade' ? '\u{1F45B}' : '\u{1F4E5}'} ${entry.label}`, url: `https://solscan.io/account/${entry.pubkey}` },
  ]);
  if (permissions.canManageFunds) {
    rows.push([{ text: '\u{1F511} Show Wallet Keys', callback_data: `bot:keys:${tokenId}` }]);
  }
  rows.push([
    { text: '\u{1F504} Refresh Wallets', callback_data: `bot:wallets:${tokenId}` },
    { text: '\u2B05 Back to Bot', callback_data: `bot:${tokenId}` },
  ]);
  return { inline_keyboard: rows };
}

function buildTelegramBotWalletKeysText(payload = {}) {
  const token = payload?.token || {};
  const wallets = Array.isArray(payload?.wallets) ? payload.wallets : [];
  if (!wallets.length) {
    return [
      `\u{1F511} ${String(token.symbol || 'TOKEN')} Wallet Keys`,
      '',
      'No wallet keys are available for this token.',
    ].join('\n');
  }
  return [
    `\u{1F511} ${String(token.symbol || 'TOKEN')} Wallet Keys`,
    '',
    ...wallets.map((wallet) => [
      `${wallet.walletType === 'trade' ? '\u{1F45B}' : '\u{1F4E5}'} ${wallet.label}`,
      `Wallet: ${wallet.publicKey}`,
      `${wallet.secretKeyBase58}`,
      '',
    ].join('\n')).slice(0, 6),
  ].join('\n');
}

function buildTelegramBotWalletKeysKeyboard(tokenId) {
  return {
    inline_keyboard: [[
      { text: '\u2B05 Back to Wallets', callback_data: `bot:wallets:${tokenId}` },
      { text: '\u2B05 Back to Bot', callback_data: `bot:${tokenId}` },
    ]],
  };
}

function buildTelegramBotSettingsText(token) {
  const configJson = token?.moduleConfig && typeof token.moduleConfig === 'object' ? token.moduleConfig : {};
  const selectedBot = normalizeModuleType(token?.selectedBot, MODULE_TYPES.burn);
  const lines = [
    `\u2699\uFE0F ${String(token?.symbol || token?.name || 'Token')} Settings`,
    '',
    `Bot: ${String(selectedBot || 'burn').toUpperCase()}`,
    `Claim Every: ${Math.max(60, Number(token?.claimSec || 120))}s`,
  ];
  if (selectedBot === MODULE_TYPES.burn) {
    lines.push(`Burn Every: ${Math.max(60, Number(token?.burnSec || 300))}s`);
    lines.push(`Split Buys: ${Math.max(1, Number(token?.splits || 1))}`);
  } else {
    lines.push(`Wallets: ${Math.max(1, Number(configJson.tradeWalletCount || 1))}`);
    lines.push(`Aggression: ${Math.max(0, Math.min(100, Number(configJson.aggression || 35)))}`);
    lines.push(`Min Trade: ${sanitizeSol(configJson.minTradeSol, 0.01, 0.001, 10).toFixed(4)} SOL`);
    if (selectedBot === MODULE_TYPES.marketMaker) {
      lines.push(`Inventory Target: ${Math.max(20, Math.min(80, Number(configJson.targetInventoryPct || 50)))}%`);
    }
  }
  lines.push('');
  lines.push('Use the buttons below to tune this bot.');
  return lines.join('\n');
}

function buildTelegramBotSettingsKeyboard(token) {
  const selectedBot = normalizeModuleType(token?.selectedBot, MODULE_TYPES.burn);
  const rows = [
    [
      { text: '\u2796 Claim', callback_data: `cfg:bot_setting:${token.id}:claim:dec` },
      { text: '\u2795 Claim', callback_data: `cfg:bot_setting:${token.id}:claim:inc` },
    ],
  ];
  if (selectedBot === MODULE_TYPES.burn) {
    rows.push([
      { text: '\u2796 Burn', callback_data: `cfg:bot_setting:${token.id}:burn:dec` },
      { text: '\u2795 Burn', callback_data: `cfg:bot_setting:${token.id}:burn:inc` },
    ]);
    rows.push([{ text: '\u{1F504} Split Buys', callback_data: `cfg:bot_setting:${token.id}:splits:cycle` }]);
  } else {
    rows.push([
      { text: '\u2796 Aggro', callback_data: `cfg:bot_setting:${token.id}:aggression:dec` },
      { text: '\u2795 Aggro', callback_data: `cfg:bot_setting:${token.id}:aggression:inc` },
    ]);
    rows.push([
      { text: '\u2796 Min Buy', callback_data: `cfg:bot_setting:${token.id}:min_trade:dec` },
      { text: '\u2795 Min Buy', callback_data: `cfg:bot_setting:${token.id}:min_trade:inc` },
    ]);
    rows.push([{ text: '\u{1F45B} Wallets', callback_data: `cfg:bot_setting:${token.id}:wallets:cycle` }]);
    if (selectedBot === MODULE_TYPES.marketMaker) {
      rows.push([{ text: '\u{1F4CF} Inventory Target', callback_data: `cfg:bot_setting:${token.id}:inventory:cycle` }]);
    }
  }
  rows.push([
    { text: '\u{1F504} Refresh', callback_data: `bot_settings:${token.id}` },
    { text: '\u2B05 Back to Bot', callback_data: `bot:${token.id}` },
  ]);
  return { inline_keyboard: rows };
}

function buildTelegramBotFieldPrompt(field, token = {}) {
  const symbol = String(token?.symbol || token?.name || 'Token');
  const selectedBot = normalizeModuleType(token?.selectedBot, MODULE_TYPES.burn);
  if (field === 'withdraw_destination') {
    return [
      `${getBotEmoji(selectedBot)} ${symbol} ${selectedBot === MODULE_TYPES.burn ? 'Withdraw' : 'Deposit Withdraw'}`,
      '',
      'Send the destination Solana wallet address.',
      selectedBot === MODULE_TYPES.burn
        ? 'Any remaining token in the burn wallet will be cleaned up before the SOL withdrawal.'
        : 'This withdraws spendable SOL from the deposit wallet after the bot is paused.',
    ].join('\n');
  }
  return [
    `${getBotEmoji(selectedBot)} ${symbol}`,
    '',
    'Send the value you want to use.',
  ].join('\n');
}

function buildTelegramBotActionResultText(token = {}, action = '', result = {}) {
  const symbol = String(token?.symbol || token?.name || 'Token');
  const selectedBot = normalizeModuleType(token?.selectedBot, MODULE_TYPES.burn);
  if (action === 'sweep') {
    return [
      `${getBotEmoji(selectedBot)} ${symbol} Sweep Complete`,
      '',
      `Wallets swept: ${Number(result.walletsSwept || 0)} / ${Number(result.walletsTotal || 0)}`,
      `Tokens sold: ${Number(result.soldTokens || 0).toFixed(6)}`,
      `SOL moved: ${Number(result.sweptSol || 0).toFixed(6)}`,
      `Transactions: ${Number(result.txCreated || 0)}`,
    ].join('\n');
  }
  if (action === 'withdraw') {
    const lines = [
      `${getBotEmoji(selectedBot)} ${symbol} Withdraw Complete`,
      '',
      `Sent: ${Number(result.sentSol || 0).toFixed(6)} SOL`,
      `Remaining: ${Number(result.remainingSol || 0).toFixed(6)} SOL`,
    ];
    if (Number(result.sentTokenBurned || 0) > 0) {
      lines.push(`Cleanup burned: ${Number(result.sentTokenBurned || 0).toFixed(6)} ${symbol}`);
    }
    if (result.signature) {
      lines.push(`Tx: ${String(result.signature || '')}`);
    }
    return lines.join('\n');
  }
  return `${getBotEmoji(selectedBot)} ${symbol}\n\nAction completed.`;
}

function buildTelegramConfirmationText(kind = "", context = {}) {
  const label = String(context?.label || context?.symbol || "item");
  if (kind === "holder_run") {
    return [
      `⚠️ ${label} Distribution`,
      "",
      "This holder distribution is one-way.",
      "Recipient wallets are not reclaimable later.",
      "Continue only if you want to send the selected token amount permanently.",
    ].join("\n");
  }
  if (kind === "smart_reclaim") {
    return [
      `⚠️ ${label} Reclaim`,
      "",
      "This will pull managed Smart Sell balances back into the funding wallet.",
      "Use it when you want to unwind the active wallet set.",
    ].join("\n");
  }
  if (kind === "bundle_reclaim") {
    return [
      `⚠️ ${label} Reclaim`,
      "",
      "This will reclaim managed bundle wallet balances back into the funding wallet.",
      "Imported wallets stay controlled by their own keys.",
    ].join("\n");
  }
  if (kind === "bot_sweep") {
    return [
      `⚠️ ${label} Sweep Wallets`,
      "",
      "This will sell remaining token from trade wallets where needed and move spendable SOL back to the deposit wallet.",
      "Keep the bot paused during this cleanup.",
    ].join("\n");
  }
  if (kind === "bot_withdraw") {
    return [
      `⚠️ ${label} Withdraw`,
      "",
      "The next step will ask for the destination wallet.",
      "Use this only when the bot is paused and you want funds moved out.",
    ].join("\n");
  }
  if (kind === "deploy_reset") {
    return [
      "⚠️ Reset Deploy",
      "",
      "This clears the current deploy setup in Telegram.",
      "Use it only if you want to start the deploy flow over.",
    ].join("\n");
  }
  return [
    `⚠️ ${label}`,
    "",
    "Confirm this action to continue.",
  ].join("\n");
}

function buildTelegramConfirmationKeyboard(confirmData, backData = "menu") {
  return {
    inline_keyboard: [
      [{ text: "✅ Confirm", callback_data: confirmData }],
      [{ text: "⬅ Back", callback_data: backData }],
    ],
  };
}

function buildTelegramBotKeyboard(token) {
  const selectedBot = normalizeModuleType(token?.selectedBot, MODULE_TYPES.burn);
  const permissions = token?.permissions || {};
  return {
    inline_keyboard: [
      [
        {
          text: Boolean(token.active) ? '\u23F8 Pause Bot' : '\u25B6 Start Bot',
          callback_data: `act:bot_toggle:${token.id}:${token.active ? 'pause' : 'start'}`,
        },
      ],
      [
        {
          text: `\u{1F501} Mode: ${String(token.selectedBot || 'burn').toUpperCase()}`,
          callback_data: `cfg:bot_mode:${token.id}`,
        },
      ],
      [{ text: '\u{1F4CA} Live Status', callback_data: `bot:live:${token.id}` }],
      [{ text: '\u2699\uFE0F Settings', callback_data: `bot_settings:${token.id}` }],
      ...(
        permissions.canManageFunds && !token.active
          ? (
              selectedBot === MODULE_TYPES.burn
                ? [[{ text: '\u{1F4B8} Withdraw Burn Wallet', callback_data: `confirm:bot_withdraw:${token.id}` }]]
                : isTradeBotModuleType(selectedBot)
                  ? [[
                      { text: '\u{1F9F9} Sweep Wallets', callback_data: `confirm:bot_sweep:${token.id}` },
                      { text: '\u{1F4B8} Withdraw Deposit', callback_data: `confirm:bot_withdraw:${token.id}` },
                    ]]
                  : []
            )
          : []
      ),
      [
        { text: '\u{1F504} Refresh', callback_data: `bot:${token.id}` },
        { text: '\u2B05 Back', callback_data: 'menu:bots' },
      ],
    ],
  };
}

async function getTelegramLinkedUserId(chatId) {
  const linkRes = await pool.query(
    `
      SELECT user_id
      FROM user_telegram_links
      WHERE chat_id = $1
        AND is_connected = TRUE
      LIMIT 1
    `,
    [String(chatId || '').trim()]
  );
  return linkRes.rowCount ? Number(linkRes.rows[0].user_id || 0) : 0;
}

async function getTelegramFlowState(userId) {
  const res = await pool.query(
    `
      SELECT flow_type, step, state_json
      FROM telegram_flow_state
      WHERE user_id = $1
      LIMIT 1
    `,
    [Number(userId || 0)]
  );
  if (!res.rowCount) return null;
  return {
    flowType: String(res.rows[0].flow_type || ''),
    step: String(res.rows[0].step || ''),
    state: res.rows[0].state_json && typeof res.rows[0].state_json === 'object' ? res.rows[0].state_json : {},
  };
}

async function upsertTelegramFlowState(userId, flowType, step, state = {}) {
  await pool.query(
    `
      INSERT INTO telegram_flow_state (user_id, flow_type, step, state_json, updated_at)
      VALUES ($1, $2, $3, $4::jsonb, NOW())
      ON CONFLICT (user_id) DO UPDATE
      SET flow_type = EXCLUDED.flow_type,
          step = EXCLUDED.step,
          state_json = EXCLUDED.state_json,
          updated_at = NOW()
    `,
    [Number(userId || 0), String(flowType || ''), String(step || ''), JSON.stringify(state || {})]
  );
}

async function clearTelegramFlowState(userId) {
  await pool.query('DELETE FROM telegram_flow_state WHERE user_id = $1', [Number(userId || 0)]);
}

function defaultTelegramDeployState() {
  return {
    walletMode: 'vanity',
    attachBot: false,
    selectedBot: 'burn',
    initialBuySol: 0.1,
    name: '',
    symbol: '',
    description: 'Deployed on EMBER.nexus',
    twitter: '',
    telegram: '',
    website: '',
    imageDataUri: '',
    imageFileName: 'token',
    bannerDataUri: '',
    bannerFileName: 'banner',
    reservationId: '',
    deposit: '',
    requiredSol: 0,
    balanceSol: 0,
    shortfallSol: 0,
    status: 'reserved',
    lastError: '',
    funded: false,
  };
}

async function getTelegramDeployReservationSecret(userId, reservationId) {
  const reservation = await getDeployWalletReservationRow(pool, reservationId);
  const reservationUserId = Number(reservation?.user_id || 0);
  const actorUserId = Number(userId || 0);
  if (reservationUserId > 0 && actorUserId > 0 && reservationUserId !== actorUserId) {
    throw new Error("Deploy wallet reservation does not belong to this Telegram account.");
  }
  const secretKeyBase58 = decryptDepositSecret(String(reservation.secret_key_base58 || ""));
  return {
    ...toDeployWalletReservation(reservation),
    privateKeyBase58: secretKeyBase58,
    privateKeyArray: Array.from(keypairFromBase58(secretKeyBase58).secretKey),
  };
}

function defaultTelegramAttachState() {
  return {
    mint: '',
    walletMode: 'vanity',
    selectedBot: 'burn',
    name: '',
    symbol: '',
    pictureUrl: '',
    pendingDepositId: '',
    deposit: '',
  };
}

function cycleTelegramBotMode(currentMode) {
  const order = [
    MODULE_TYPES.burn,
    MODULE_TYPES.volume,
    MODULE_TYPES.marketMaker,
    MODULE_TYPES.dca,
    MODULE_TYPES.rekindle,
  ];
  const normalized = normalizeModuleType(currentMode, MODULE_TYPES.burn);
  const currentIndex = Math.max(0, order.indexOf(normalized));
  return order[(currentIndex + 1) % order.length];
}

function stepTelegramPresetValue(currentValue, presets, direction) {
  const values = Array.isArray(presets) ? presets.map((item) => Number(item)).filter((item) => Number.isFinite(item)) : [];
  if (!values.length) return Number(currentValue || 0);
  const current = Number(currentValue || values[0]);
  let index = values.findIndex((value) => value >= current);
  if (index < 0) index = values.length - 1;
  if (direction === 'dec') {
    return values[Math.max(0, index - 1)];
  }
  return values[Math.min(values.length - 1, index + (values[index] === current ? 1 : 0))];
}

function isTelegramDeployReady(flowState = {}) {
  return Boolean(
    String(flowState.name || '').trim() &&
      String(flowState.symbol || '').trim() &&
      String(flowState.description || '').trim() &&
      String(flowState.imageDataUri || '').trim() &&
      String(flowState.reservationId || '').trim() &&
      Boolean(flowState.funded)
  );
}

function buildTelegramToolCreateText() {
  return [
    '\u2728 Create a Tool',
    '',
    'Pick the tool you want to set up.',
    'I will guide you through the target and funding steps here in Telegram.',
  ].join('\n');
}

function buildTelegramToolCreateKeyboard() {
  return {
    inline_keyboard: [
      [{ text: '\u{1FA99} Holder Pooler', callback_data: `tool_create:${TOOL_TYPES.holderPooler}` }],
      [{ text: '\u26A1 Reaction Manager', callback_data: `tool_create:${TOOL_TYPES.reactionManager}` }],
      [{ text: '\u{1F3AF} Smart Sell', callback_data: `tool_create:${TOOL_TYPES.smartSell}` }],
      [{ text: '\u{1F4E6} Bundle Manager', callback_data: `tool_create:${TOOL_TYPES.bundleManager}` }],
      [{ text: '\u2B05 Back to Tools', callback_data: 'tools' }],
    ],
  };
}

function buildTelegramToolCreatePrompt(toolType) {
  if (toolType === TOOL_TYPES.reactionManager) {
    return [
      '\u26A1 Reaction Manager',
      '',
      'Send the DexScreener URL you want this campaign to target.',
      'Example: https://dexscreener.com/solana/...',
      '',
      'Reply with the link now, or tap Cancel.',
    ].join('\n');
  }
  return [
    `${getToolEmoji(toolType)} ${toolTypeLabel(toolType)}`,
    '',
    'Send the token mint this tool should target.',
    '',
    'Reply with the mint now, or tap Cancel.',
  ].join('\n');
}

function buildTelegramToolMintCreateText(toolType) {
  return [
    `${getToolEmoji(toolType)} ${toolTypeLabel(toolType)}`,
    '',
    'Pick one of your attached tokens or paste a mint manually.',
    'Using your attached token list is faster and avoids copy mistakes.',
  ].join('\n');
}

function buildTelegramToolMintCreateKeyboard(toolType) {
  return {
    inline_keyboard: [
      [{ text: '\u{1FA99} My Attached Tokens', callback_data: `tool_create_attached:${toolType}` }],
      [{ text: '\u270F\uFE0F Paste Mint', callback_data: `tool_create_paste:${toolType}` }],
      [{ text: '\u2B05 Back to Tools', callback_data: 'tools:new' }],
    ],
  };
}

async function getTelegramPendingDepositReservationSecret(userId, pendingDepositId) {
  const scope = await resolveUserAccessScopeFromPool(userId);
  const reservationId = String(pendingDepositId || '').trim();
  if (!reservationId) {
    throw new Error('Reserve a deposit wallet first.');
  }
  const res = await pool.query(
    `
      SELECT reservation_id, deposit_pubkey, secret_key_base58
      FROM token_deposit_pool
      WHERE reservation_id = $1
        AND reserved_user_id = $2
        AND status = 'reserved'
      LIMIT 1
    `,
    [reservationId, scope.ownerUserId]
  );
  if (!res.rowCount) {
    throw new Error('Reserved deposit wallet not found or expired.');
  }
  return {
    pendingDepositId: reservationId,
    deposit: String(res.rows[0].deposit_pubkey || '').trim(),
    privateKeyBase58: decryptDepositSecret(String(res.rows[0].secret_key_base58 || '')),
  };
}

function buildTelegramAttachKeyText(reservation = {}) {
  return [
    '\u{1F511} EMBER Deposit Wallet',
    '',
    `Wallet: ${String(reservation.deposit || '').trim() || '-'}`,
    '',
    'This is the deposit wallet that will be attached to the token.',
    'Save this key if you want direct control over that wallet.',
    '',
    `Private Key (base58): ${String(reservation.privateKeyBase58 || '').trim() || '-'}`,
  ].join('\n');
}

function buildTelegramToolTargetPickerText(toolType, overview = {}) {
  const tokens = Array.isArray(overview?.tokens) ? overview.tokens : [];
  const tokenLines = tokens.length
    ? tokens.slice(0, 12).map((token) => `${getBotEmoji(token.selectedBot)} ${token.symbol} \u2022 ${token.mint}`)
    : ['\u{1F4ED} No attached tokens available yet.'];
  return [
    `${getToolEmoji(toolType)} ${toolTypeLabel(toolType)}`,
    '',
    'Pick the attached token this tool should target.',
    '',
    ...tokenLines,
  ].join('\n');
}

function buildTelegramToolTargetPickerKeyboard(toolType, overview = {}) {
  const tokens = Array.isArray(overview?.tokens) ? overview.tokens : [];
  const rows = tokens.slice(0, 12).map((token) => [
    {
      text: `${getBotEmoji(token.selectedBot)} ${token.symbol}`,
      callback_data: `tool_create_target:${toolType}:${token.id}`,
    },
  ]);
  rows.push([{ text: '\u270F\uFE0F Paste Mint Instead', callback_data: `tool_create_paste:${toolType}` }]);
  rows.push([{ text: '\u2B05 Back', callback_data: `tool_create:${toolType}` }]);
  return { inline_keyboard: rows };
}

function buildTelegramFieldPromptKeyboard(backAction = 'menu') {
  return {
    inline_keyboard: [[{ text: '\u2B05 Cancel', callback_data: backAction }]],
  };
}

function buildTelegramDeployFieldKeyboard(field) {
  const optional = ["twitter", "telegram", "website", "banner"].includes(String(field || "").trim().toLowerCase());
  const rows = [];
  if (optional) {
    rows.push([{ text: '\u23ED Skip', callback_data: `deploy:skip:${field}` }]);
  }
  rows.push([{ text: '\u2B05 Cancel', callback_data: 'deploy:cancel_field' }]);
  return { inline_keyboard: rows };
}

function buildTelegramTradeFieldPrompt(field) {
  if (field === 'mint') {
    return [
      '\u{1F4B1} EMBER Trade',
      '',
      'Reply with the token mint you want this trading wallet to use.',
      '',
      'Tap cancel if you want to go back.',
    ].join('\n');
  }
  if (field === 'buy_amount') {
    return [
      '\u{1F4B1} EMBER Trade',
      '',
      'Reply with the SOL amount you want to buy with.',
      '',
      'Example: 0.15',
    ].join('\n');
  }
  if (field === 'sell_percent') {
    return [
      '\u{1F4B1} EMBER Trade',
      '',
      'Reply with the percent of your selected token balance you want to sell.',
      '',
      'Example: 37',
    ].join('\n');
  }
  if (field === 'withdraw_destination') {
    return [
      '\u{1F4B8} Withdraw Trade SOL',
      '',
      'Reply with the wallet address that should receive the spendable SOL from your selected trading wallet.',
      'The wallet reserve stays behind so the trading wallet is not fully drained.',
    ].join('\n');
  }
  if (field === 'import_wallet') {
    return [
      '\u{1F4E5} Import Trading Wallet',
      '',
      'Reply with the base58 private key for the wallet you want to import into Telegram Trade.',
      'The imported wallet will become your selected wallet immediately.',
      '',
      'Tap cancel if you want to go back.',
    ].join('\n');
  }
  return [
    '\u{1F4B1} EMBER Trade',
    '',
    'Reply with the value.',
  ].join('\n');
}

function buildTelegramBundleImportPrompt() {
  return [
    '\u{1F4E5} Bundle Manager Imports',
    '',
    'Reply with one base58 private key per line.',
    'These wallets will be added to the current imported bundle.',
    '',
    'Tap cancel if you want to go back.',
  ].join('\n');
}

function buildTelegramTradeText() {
  return [
    '\u{1F4B1} EMBER Trade',
    '',
    'Use a managed EMBER trading wallet directly in Telegram.',
    'Set a mint, buy, sell, and review recent activity without leaving chat.',
  ].join('\n');
}

function buildTelegramTradeKeyboard() {
  return {
    inline_keyboard: [
      [{ text: '\u{1F4B1} Open Trade', callback_data: 'menu:trade' }],
      [{ text: '\u{1F3E0} Main Menu', callback_data: 'menu' }],
    ],
  };
}

function buildTelegramTradeWalletInfoText(details = {}) {
  const wallet = details?.selectedWallet || details?.wallet || null;
  if (!wallet) {
    return [
      '\u{1F45B} Trading Wallet',
      '',
      'Create a trading wallet first.',
    ].join('\n');
  }
  return [
    '\u{1F45B} Trading Wallet',
    '',
    `Wallet: ${wallet.label || 'Wallet'}`,
    `Type: ${wallet.imported ? 'Imported' : 'Managed'}`,
    `Address: ${wallet.publicKey}`,
    `SOL: ${Number(wallet.solBalance || 0).toFixed(6)}`,
    wallet.selectedMint ? `Token: ${wallet.selectedSymbol || wallet.selectedMint}` : 'Token: not selected',
    wallet.selectedMint ? `Token Balance: ${Number(wallet.tokenBalance || 0).toFixed(6)}` : '',
    '',
    'Fund this wallet directly if you want to trade from it in Telegram.',
  ].filter(Boolean).join('\n');
}

function buildTelegramTradeWalletInfoKeyboard(details = {}) {
  const wallet = details?.selectedWallet || details?.wallet || null;
  const rows = [];
  if (wallet?.publicKey) {
    rows.push([{ text: '\u{1F50E} View Wallet', url: `https://solscan.io/account/${wallet.publicKey}` }]);
  }
  rows.push([{ text: '\u{1F511} Show Wallet Key', callback_data: 'trade:wallet_key' }]);
  rows.push([{ text: '\u2B05 Back to Trade', callback_data: 'menu:trade' }]);
  return { inline_keyboard: rows };
}

function buildTelegramTradeTokenLinksText(details = {}) {
  const wallet = details?.selectedWallet || details?.wallet || null;
  if (!wallet?.selectedMint) {
    return [
      '\u{1FA99} Token Links',
      '',
      'Pick a token first.',
    ].join('\n');
  }
  return [
    '\u{1FA99} Token Links',
    '',
    `${wallet.selectedSymbol || 'Token'}`,
    `Mint: ${wallet.selectedMint}`,
    '',
    'Open the token page or explorer using the buttons below.',
  ].join('\n');
}

function buildTelegramTradeTokenLinksKeyboard(details = {}) {
  const wallet = details?.selectedWallet || details?.wallet || null;
  const mint = String(wallet?.selectedMint || '').trim();
  const rows = [];
  if (mint) {
    rows.push([{ text: '\u{1F4C8} Open on pump.fun', url: `https://pump.fun/coin/${mint}` }]);
    rows.push([{ text: '\u{1F50E} View Token', url: `https://solscan.io/token/${mint}` }]);
  }
  rows.push([{ text: '\u2B05 Back to Trade', callback_data: 'menu:trade' }]);
  return { inline_keyboard: rows };
}

function buildTelegramTradeWalletKeyText(wallet = {}) {
  return [
    '\u{1F511} Trading Wallet Key',
    '',
    `Wallet: ${String(wallet.label || 'Wallet').trim() || 'Wallet'}`,
    `Address: ${String(wallet.publicKey || '').trim() || '-'}`,
    '',
    'Save this key if you want full external control over the wallet.',
    'Anyone with this key controls the wallet and anything funded into it.',
    '',
    `Private Key (base58): ${String(wallet.secretKeyBase58 || '').trim() || '-'}`,
  ].join('\n');
}

function countTelegramDeployCoreFields(flowState = defaultTelegramDeployState()) {
  let completed = 0;
  if (String(flowState.name || '').trim()) completed += 1;
  if (String(flowState.symbol || '').trim()) completed += 1;
  if (String(flowState.description || '').trim()) completed += 1;
  if (String(flowState.imageDataUri || '').trim()) completed += 1;
  return completed;
}

function buildTelegramDeploySuccessKeyboard(result = {}) {
  const rows = [];
  if (result?.pumpfunUrl) {
    rows.push([{ text: '\u{1F4C8} View on pump.fun', url: result.pumpfunUrl }]);
  }
  if (result?.solscanTx) {
    rows.push([{ text: '\u{1F50E} View Deploy Tx', url: result.solscanTx }]);
  }
  if (result?.solscanMint) {
    rows.push([{ text: '\u{1F9FE} View Token', url: result.solscanMint }]);
  }
  rows.push([{ text: '\u{1F3E0} Main Menu', callback_data: 'menu' }]);
  return { inline_keyboard: rows };
}

function isTelegramAttachReady(flowState = defaultTelegramAttachState()) {
  return Boolean(
    String(flowState.mint || '').trim() &&
      String(flowState.pendingDepositId || '').trim() &&
      String(flowState.deposit || '').trim()
  );
}

function buildTelegramAttachText(flowState = defaultTelegramAttachState()) {
  const walletMode = String(flowState.walletMode || 'vanity');
  const lines = [
    '\u{1F517} EMBER Attach Token',
    '',
    `Mint: ${String(flowState.mint || '').trim() || 'Not set'}`,
    `Token: ${String(flowState.symbol || flowState.name || '').trim() || 'Not resolved yet'}`,
    `Wallet: ${walletMode === 'regular' ? 'Regular' : 'EMBER/EMBR'}`,
    `Bot: ${String(flowState.selectedBot || 'burn').toUpperCase()}`,
  ];
  if (flowState.deposit) {
    lines.push(`Deposit Wallet: ${String(flowState.deposit || '').trim()}`);
    lines.push('Deposit Key: ready to view');
  } else {
    lines.push('Deposit Wallet: not reserved');
  }
  lines.push('');
  lines.push('Set the mint, choose the wallet style and bot mode, reserve a deposit wallet, then attach.');
  return lines.join('\n');
}

function buildTelegramAttachKeyboard(flowState = defaultTelegramAttachState()) {
  const rows = [
    [{ text: '\u{1FA99} Set Mint', callback_data: 'attach:field:mint' }],
    [
      {
        text: `Wallet: ${String(flowState.walletMode || 'vanity') === 'regular' ? 'Regular' : 'EMBER/EMBR'}`,
        callback_data: 'attach:wallet',
      },
      {
        text: `Bot: ${String(flowState.selectedBot || 'burn').toUpperCase()}`,
        callback_data: 'attach:bot',
      },
    ],
    [
      {
        text: flowState.pendingDepositId ? '\u{1F504} Refresh Deposit' : '\u{1F4BC} Reserve Deposit Wallet',
        callback_data: 'attach:reserve',
      },
    ],
  ];
  if (flowState.pendingDepositId) {
    rows.push([{ text: '\u{1F511} Show Deposit Key', callback_data: 'attach:key' }]);
  }
  if (isTelegramAttachReady(flowState)) {
    rows.push([{ text: '\u2705 Attach Token', callback_data: 'attach:submit' }]);
  }
  rows.push([{ text: '\u2B05 Back to Bots', callback_data: 'menu:bots' }]);
  return { inline_keyboard: rows };
}

function buildTelegramAttachFieldPrompt(field) {
  if (field === 'mint') {
    return [
      '\u{1F517} EMBER Attach Token',
      '',
      'Reply with the token mint you want to attach.',
      '',
      'Tap cancel if you want to go back.',
    ].join('\n');
  }
  return 'Reply with a value or tap cancel.';
}

function buildTelegramDeployText(flowState = defaultTelegramDeployState()) {
  const walletMode = String(flowState.walletMode || 'vanity');
  const attachBot = Boolean(flowState.attachBot);
  const selectedBot = String(flowState.selectedBot || 'burn');
  const initialBuySol = Number(flowState.initialBuySol || 0.1);
  const coreCompleted = countTelegramDeployCoreFields(flowState);
  const coreTotal = 4;
  const requiredSol =
    Number(flowState.requiredSol || 0) > 0
      ? Number(flowState.requiredSol || 0)
      : initialBuySol + fromLamports(getDeployVanityBufferLamports());
  const funded = Boolean(flowState.funded);
  const lines = [
    '\u{1F680} EMBER Deploy',
    '',
    `Core Setup: ${coreCompleted}/${coreTotal} complete`,
    `Wallet: ${walletMode === 'regular' ? 'Regular' : 'EMBER/EMBR'}`,
    `Initial Buy: ${initialBuySol.toFixed(3)} SOL`,
    `Attach Bot: ${attachBot ? `${getBotEmoji(selectedBot)} ${selectedBot.toUpperCase()}` : 'Off'}`,
    `Funding Required: ${requiredSol.toFixed(6)} SOL`,
    '',
    'Core fields:',
    `${String(flowState.name || '').trim() ? '\u2705' : '\u26AA'} Name: ${String(flowState.name || '').trim() || 'Not set'}`,
    `${String(flowState.symbol || '').trim() ? '\u2705' : '\u26AA'} Symbol: ${String(flowState.symbol || '').trim() || 'Not set'}`,
    `${String(flowState.description || '').trim() ? '\u2705' : '\u26AA'} Description: ${String(flowState.description || '').trim() ? 'Set' : 'Not set'}`,
    `${String(flowState.imageDataUri || '').trim() ? '\u2705' : '\u26AA'} Image: ${String(flowState.imageDataUri || '').trim() ? 'Uploaded' : 'Missing'}`,
    `${String(flowState.bannerDataUri || '').trim() ? '\u2705' : '\u26AA'} Banner: ${String(flowState.bannerDataUri || '').trim() ? 'Uploaded' : 'Skip'}`,
    '',
    'Optional links:',
    `${String(flowState.twitter || '').trim() ? '\u2705' : '\u26AA'} X: ${String(flowState.twitter || '').trim() ? 'Set' : 'Skip'}`,
    `${String(flowState.telegram || '').trim() ? '\u2705' : '\u26AA'} Telegram: ${String(flowState.telegram || '').trim() ? 'Set' : 'Skip'}`,
    `${String(flowState.website || '').trim() ? '\u2705' : '\u26AA'} Website: ${String(flowState.website || '').trim() ? 'Set' : 'Skip'}`,
  ];
  if (flowState.reservationId && flowState.deposit) {
    lines.push('');
    lines.push(`Funding Wallet: ${flowState.deposit}`);
    lines.push(`Status: ${funded ? '\u{1F7E2} Funded' : '\u{1F7E0} Waiting for funding'}`);
    lines.push(`Balance: ${Number(flowState.balanceSol || 0).toFixed(6)} SOL`);
    if (!funded) {
      lines.push(`Still needed: ${Number(flowState.shortfallSol || requiredSol).toFixed(6)} SOL`);
    }
    lines.push('Deployer Key: ready to view');
  } else {
    lines.push('');
    lines.push('Reserve a deploy wallet to continue.');
  }
  if (isTelegramDeployReady(flowState)) {
    lines.push('');
    lines.push('\u2705 Deploy is ready to submit.');
  }
  if (String(flowState.lastError || '').trim()) {
    lines.push('');
    lines.push(`Last Error: ${String(flowState.lastError || '').trim()}`);
  }
  lines.push('');
  lines.push('Tap a field below, then reply in chat. Use Show Deployer Key after reserving the wallet.');
  return lines.join('\n');
}

function buildTelegramDeployKeyboard(flowState = defaultTelegramDeployState()) {
  const attachBot = Boolean(flowState.attachBot);
  const nameReady = Boolean(String(flowState.name || '').trim());
  const symbolReady = Boolean(String(flowState.symbol || '').trim());
  const descriptionReady = Boolean(String(flowState.description || '').trim());
  const imageReady = Boolean(String(flowState.imageDataUri || '').trim());
  const bannerReady = Boolean(String(flowState.bannerDataUri || '').trim());
  const twitterReady = Boolean(String(flowState.twitter || '').trim());
  const telegramReady = Boolean(String(flowState.telegram || '').trim());
  const websiteReady = Boolean(String(flowState.website || '').trim());
  const fundingReady = Boolean(flowState.reservationId) && Boolean(flowState.funded);
  const rows = [
    [
      {
        text: `Wallet: ${String(flowState.walletMode || 'vanity') === 'regular' ? 'Regular' : 'EMBER/EMBR'}`,
        callback_data: 'deploy:wallet',
      },
      {
        text: `Bot: ${attachBot ? 'On' : 'Off'}`,
        callback_data: 'deploy:attach',
      },
    ],
    [
      { text: '\u2796 Buy', callback_data: 'deploy:buy:dec' },
      { text: `\u{1F4B0} ${Number(flowState.initialBuySol || 0.1).toFixed(2)} SOL`, callback_data: 'noop' },
      { text: '\u2795 Buy', callback_data: 'deploy:buy:inc' },
    ],
    [
      { text: `${nameReady ? '\u2705' : '\u270F'} Name`, callback_data: 'deploy:field:name' },
      { text: `${symbolReady ? '\u2705' : '\u{1F524}'} Symbol`, callback_data: 'deploy:field:symbol' },
    ],
    [
      { text: `${descriptionReady ? '\u2705' : '\u{1F4DD}'} Description`, callback_data: 'deploy:field:description' },
      { text: `${imageReady ? '\u2705' : '\u{1F5BC}'} Image`, callback_data: 'deploy:field:image' },
    ],
    [{ text: `${bannerReady ? '\u2705' : '\u{1F39E}'} Banner`, callback_data: 'deploy:field:banner' }],
    [
      { text: `${twitterReady ? '\u2705' : 'X'} URL`, callback_data: 'deploy:field:twitter' },
      { text: `${telegramReady ? '\u2705' : '\u{1F4E3}'} Telegram`, callback_data: 'deploy:field:telegram' },
    ],
    [{ text: `${websiteReady ? '\u2705' : '\u{1F310}'} Website`, callback_data: 'deploy:field:website' }],
  ];
  if (attachBot) {
    rows.push([{ text: `\u{1F916} Bot Mode: ${String(flowState.selectedBot || 'burn').toUpperCase()}`, callback_data: 'deploy:bot' }]);
  }
  rows.push([
    {
      text: !flowState.reservationId
        ? '\u{1F4B3} Reserve Wallet'
        : fundingReady
          ? '\u2705 Funding Ready'
          : '\u{1F504} Refresh Funding',
      callback_data: 'deploy:reserve',
    },
    ...(flowState.reservationId ? [{ text: '\u{1F511} Show Deployer Key', callback_data: 'deploy:key' }] : []),
  ]);
  if (flowState.deposit) {
    rows.push([{ text: '\u{1F517} View Funding Wallet', url: `https://solscan.io/account/${String(flowState.deposit || '').trim()}` }]);
  }
  rows.push([{ text: '\u{1F9F9} Reset Deploy', callback_data: 'confirm:deploy_reset' }]);
  if (isTelegramDeployReady(flowState)) {
    rows.push([{ text: '\u{1F680} Deploy Token', callback_data: 'deploy:submit' }]);
  }
  rows.push([{ text: '\u2B05 Main Menu', callback_data: 'menu' }]);
  return { inline_keyboard: rows };
}

function buildTelegramDeployFieldPrompt(field) {
  if (field === 'name') return '\u270F Reply with the token name.';
  if (field === 'symbol') return '\u{1F524} Reply with the token symbol.';
  if (field === 'description') return '\u{1F4DD} Reply with the token description.';
  if (field === 'twitter') return 'Reply with the X URL, or tap Skip.';
  if (field === 'telegram') return '\u{1F4E3} Reply with the Telegram URL, or tap Skip.';
  if (field === 'website') return '\u{1F310} Reply with the website URL, or tap Skip.';
  if (field === 'image') return '\u{1F5BC} Send the token image as a photo or image document.';
  if (field === 'banner') return '\u{1F39E} Send the token banner image, or tap Skip.';
  return 'Reply with the value.';
}

function buildTelegramDeployKeyText(reservation = {}) {
  return [
    '\u{1F511} EMBER Deploy Wallet',
    '',
    `Funding Wallet: ${String(reservation.deposit || '').trim() || '-'}`,
    `Required: ${Number(reservation.requiredSol || 0).toFixed(6)} SOL`,
    `Current: ${Number(reservation.balanceSol || 0).toFixed(6)} SOL`,
    '',
    'This wallet is the real on-chain deployer/creator wallet for the token.',
    'Developer trades from this wallet can show on chart as creator activity.',
    'Save this key before you deploy. Anyone with it controls the wallet and any leftover SOL.',
    '',
    `Private Key (base58): ${String(reservation.privateKeyBase58 || '').trim() || '-'}`,
  ].join('\n');
}

async function fetchTelegramFileDataUri(fileId, fallbackName = "token") {

  if (!config.telegramBotToken || !fileId) throw new Error("Telegram file id is required.");
  const metaRes = await fetch(`https://api.telegram.org/bot${config.telegramBotToken}/getFile`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ file_id: String(fileId) }),
  });
  const metaJson = await metaRes.json().catch(() => ({}));
  const filePath = String(metaJson?.result?.file_path || "").trim();
  if (!metaRes.ok || !filePath) {
    throw new Error("Unable to fetch Telegram file.");
  }
  const fileRes = await fetch(`https://api.telegram.org/file/bot${config.telegramBotToken}/${filePath}`);
  if (!fileRes.ok) {
    throw new Error("Unable to download Telegram file.");
  }
  const contentType = String(fileRes.headers.get("content-type") || "").trim().toLowerCase() || "image/jpeg";
  if (!contentType.startsWith("image/")) {
    throw new Error("Telegram upload must be an image.");
  }
  const buffer = Buffer.from(await fileRes.arrayBuffer());
  const base64 = buffer.toString("base64");
  return {
    dataUri: `data:${contentType};base64,${base64}`,
    fileName: String(fallbackName || "token").trim().slice(0, 80) || "token",
  };
}

const TELEGRAM_TRADE_MAX_WALLETS = 10;

async function syncTelegramTradeLegacyWallet(userId, slot) {
  if (!slot) return;
  await pool.query(
    `
      INSERT INTO telegram_trade_wallets (
        user_id, public_key, secret_key_base58, selected_mint, selected_symbol, updated_at, last_used_at
      )
      VALUES ($1, $2, $3, $4, $5, NOW(), $6)
      ON CONFLICT (user_id) DO UPDATE
      SET public_key = EXCLUDED.public_key,
          secret_key_base58 = EXCLUDED.secret_key_base58,
          selected_mint = EXCLUDED.selected_mint,
          selected_symbol = EXCLUDED.selected_symbol,
          updated_at = NOW(),
          last_used_at = EXCLUDED.last_used_at
    `,
    [
      Number(userId || 0),
      String(slot.public_key || '').trim(),
      String(slot.secret_key_base58 || '').trim(),
      String(slot.selected_mint || '').trim(),
      String(slot.selected_symbol || '').trim(),
      slot.last_used_at || null,
    ]
  );
}

async function ensureTelegramTradeWalletSlots(userId, { forUpdate = false } = {}) {
  const userIdNum = Number(userId || 0);
  const lockClause = forUpdate ? 'FOR UPDATE' : '';
  let slotRes = await pool.query(
    `
      SELECT *
      FROM telegram_trade_wallet_slots
      WHERE user_id = $1
      ORDER BY position ASC
      ${lockClause}
    `,
    [userIdNum]
  );
  if (!slotRes.rowCount) {
    const legacyRes = await pool.query(
      `
        SELECT *
        FROM telegram_trade_wallets
        WHERE user_id = $1
        LIMIT 1
        ${lockClause}
      `,
      [userIdNum]
    );
    const source =
      legacyRes.rowCount > 0
        ? legacyRes.rows[0]
        : (() => {
            const generated = generateNonVanityDeposit();
            return {
              public_key: generated.pubkey,
              secret_key_base58: encryptDepositSecret(generated.secretKeyBase58),
              selected_mint: '',
              selected_symbol: '',
              last_used_at: null,
            };
          })();
    const inserted = await pool.query(
      `
        INSERT INTO telegram_trade_wallet_slots (
          id, user_id, public_key, secret_key_base58, label, position, selected_mint, selected_symbol, is_selected, last_used_at
        )
        VALUES ($1, $2, $3, $4, $5, 1, $6, $7, TRUE, $8)
        RETURNING *
      `,
      [
        makeId('tgw'),
        userIdNum,
        String(source.public_key || '').trim(),
        String(source.secret_key_base58 || '').trim(),
        'Wallet 1',
        String(source.selected_mint || '').trim(),
        String(source.selected_symbol || '').trim(),
        source.last_used_at || null,
      ]
    );
    await syncTelegramTradeLegacyWallet(userIdNum, inserted.rows[0]);
    return { rows: inserted.rows, selected: inserted.rows[0], created: true, migrated: legacyRes.rowCount > 0 };
  }

  let rows = slotRes.rows;
  let selected = rows.find((row) => Boolean(row.is_selected)) || null;
  if (!selected && rows.length) {
    selected = rows[0];
    await pool.query(
      `
        UPDATE telegram_trade_wallet_slots
        SET is_selected = (id = $2), updated_at = NOW()
        WHERE user_id = $1
      `,
      [userIdNum, String(selected.id || '')]
    );
    rows = rows.map((row) => ({ ...row, is_selected: row.id === selected.id }));
  }
  if (selected) {
    await syncTelegramTradeLegacyWallet(userIdNum, selected);
  }
  return { rows, selected, created: false, migrated: false };
}

async function createTelegramTradeWalletSlot(userId) {
  const userIdNum = Number(userId || 0);
  const current = await ensureTelegramTradeWalletSlots(userIdNum, { forUpdate: true });
  if (current.rows.length >= TELEGRAM_TRADE_MAX_WALLETS) {
    throw new Error(`Telegram Trade supports up to ${TELEGRAM_TRADE_MAX_WALLETS} wallets.`);
  }
  const generated = generateNonVanityDeposit();
  const position = current.rows.length + 1;
  const inserted = await pool.query(
    `
      INSERT INTO telegram_trade_wallet_slots (
        id, user_id, public_key, secret_key_base58, imported, label, position, selected_mint, selected_symbol, is_selected
      )
      VALUES ($1, $2, $3, $4, FALSE, $5, $6, '', '', TRUE)
      RETURNING *
    `,
    [makeId('tgw'), userIdNum, generated.pubkey, encryptDepositSecret(generated.secretKeyBase58), `Wallet ${position}`, position]
  );
  const created = inserted.rows[0];
  await pool.query(
    `
      UPDATE telegram_trade_wallet_slots
      SET is_selected = CASE WHEN id = $2 THEN TRUE ELSE FALSE END,
          updated_at = NOW()
      WHERE user_id = $1
    `,
    [userIdNum, String(created.id || '')]
  );
  await syncTelegramTradeLegacyWallet(userIdNum, created);
  await insertTelegramTradeEvent(userIdNum, created.id, 'wallet', `${created.label} created for Telegram trading.`, {});
  return getTelegramTradeDetails(userIdNum);
}

async function importTelegramTradeWalletSlot(userId, secretKeyBase58) {
  const userIdNum = Number(userId || 0);
  const current = await ensureTelegramTradeWalletSlots(userIdNum, { forUpdate: true });
  if (current.rows.length >= TELEGRAM_TRADE_MAX_WALLETS) {
    throw new Error(`Telegram Trade supports up to ${TELEGRAM_TRADE_MAX_WALLETS} wallets.`);
  }
  const signer = keypairFromBase58(String(secretKeyBase58 || "").trim());
  const publicKey = signer.publicKey.toBase58();
  if (current.rows.some((row) => String(row.public_key || '').trim() === publicKey)) {
    throw new Error('That wallet is already imported into Telegram Trade.');
  }
  const position = current.rows.length + 1;
  const inserted = await pool.query(
    `
      INSERT INTO telegram_trade_wallet_slots (
        id, user_id, public_key, secret_key_base58, imported, label, position, selected_mint, selected_symbol, is_selected
      )
      VALUES ($1, $2, $3, $4, TRUE, $5, $6, '', '', TRUE)
      RETURNING *
    `,
    [
      makeId('tgw'),
      userIdNum,
      publicKey,
      encryptDepositSecret(String(secretKeyBase58 || '').trim()),
      `Imported ${position}`,
      position,
    ]
  );
  const created = inserted.rows[0];
  await pool.query(
    `
      UPDATE telegram_trade_wallet_slots
      SET is_selected = CASE WHEN id = $2 THEN TRUE ELSE FALSE END,
          updated_at = NOW()
      WHERE user_id = $1
    `,
    [userIdNum, String(created.id || '')]
  );
  await syncTelegramTradeLegacyWallet(userIdNum, created);
  await insertTelegramTradeEvent(userIdNum, created.id, 'wallet', `${created.label} imported into Telegram Trade.`, {});
  return getTelegramTradeDetails(userIdNum);
}

async function selectTelegramTradeWallet(userId, walletId) {
  const userIdNum = Number(userId || 0);
  const current = await ensureTelegramTradeWalletSlots(userIdNum, { forUpdate: true });
  const target = current.rows.find((row) => String(row.id || '') === String(walletId || ''));
  if (!target) {
    throw new Error('Trading wallet not found.');
  }
  await pool.query(
    `
      UPDATE telegram_trade_wallet_slots
      SET is_selected = CASE WHEN id = $2 THEN TRUE ELSE FALSE END,
          updated_at = NOW()
      WHERE user_id = $1
    `,
    [userIdNum, String(target.id || '')]
  );
  await syncTelegramTradeLegacyWallet(userIdNum, target);
  return getTelegramTradeDetails(userIdNum);
}

async function insertTelegramTradeEvent(userId, walletId, eventType, message, options = {}) {
  await pool.query(
    `
      INSERT INTO telegram_trade_events (
        user_id, wallet_id, event_type, mint, amount_sol, amount_token, tx, message
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    `,
    [
      Number(userId || 0),
      String(walletId || '').trim(),
      String(eventType || '').trim(),
      String(options.mint || '').trim(),
      Number(options.amountSol || 0),
      Number(options.amountToken || 0),
      String(options.tx || '').trim() || null,
      String(message || '').trim(),
    ]
  );
}

export async function getTelegramTradeDetails(userId) {
  const userIdNum = Number(userId || 0);
  const scope = await resolveUserAccessScopeFromPool(userIdNum);
  const { rows: slotRows, selected } = await ensureTelegramTradeWalletSlots(userIdNum);
  const settings = await getProtocolSettings(pool).catch(() => DEFAULT_PROTOCOL_SETTINGS);
  const eventRes = await pool.query(
    `
      SELECT wallet_id, event_type, mint, amount_sol, amount_token, tx, message, created_at
      FROM telegram_trade_events
      WHERE user_id = $1
      ORDER BY created_at DESC
      LIMIT 12
    `,
    [userIdNum]
  );
  if (!slotRows.length) {
    return {
      wallet: null,
      selectedWallet: null,
      wallets: [],
      maxWallets: TELEGRAM_TRADE_MAX_WALLETS,
      events: eventRes.rows || [],
      permissions: {
        role: scope.role,
        isOwner: scope.isOwner,
        canManageFunds: scope.canManageFunds,
      },
      fees: {
        buyFeeSol: fromLamports(Number(settings.telegramTradeBuyFeeLamports || 0)),
        sellFeeSol: fromLamports(Number(settings.telegramTradeSellFeeLamports || 0)),
      },
    };
  }

  const connection = getConnection();
  const wallets = await Promise.all(
    slotRows.map(async (row) => {
      const walletPk = new PublicKey(String(row.public_key || ''));
      const solLamports = await connection.getBalance(walletPk, 'confirmed').catch(() => 0);
      let tokenBalance = 0;
      let mintMeta = null;
      const selectedMint = String(row.selected_mint || '').trim();
      if (selectedMint) {
        tokenBalance = await getOwnerTokenBalanceUi(connection, walletPk, selectedMint).catch(() => 0);
        mintMeta = await resolveMintMetadata(selectedMint).catch(() => null);
      }
      return {
        id: String(row.id || ''),
        label: String(row.label || '').trim() || `Wallet ${Number(row.position || 1)}`,
        position: Number(row.position || 1),
        imported: Boolean(row.imported),
        isSelected: Boolean(row.is_selected),
        publicKey: String(row.public_key || ''),
        selectedMint,
        selectedSymbol: String(row.selected_symbol || mintMeta?.symbol || '').trim(),
        solBalance: fromLamports(solLamports),
        spendableSol: Math.max(
          0,
          fromLamports(
            Math.max(0, Number(solLamports || 0) - toLamports(Math.max(0.003, Number(config.botSolReserve || 0.005))) - getTxFeeSafetyLamports())
          )
        ),
        tokenBalance,
      };
    })
  );
  const selectedWallet =
    wallets.find((wallet) => wallet.id === String(selected?.id || '')) ||
    wallets.find((wallet) => wallet.isSelected) ||
    wallets[0] ||
    null;
  const walletLabelMap = new Map(wallets.map((wallet) => [wallet.id, wallet.label]));

  return {
    wallet: selectedWallet,
    selectedWallet,
    wallets,
    maxWallets: TELEGRAM_TRADE_MAX_WALLETS,
    events: (eventRes.rows || []).map((event) => ({
      ...event,
      walletLabel: walletLabelMap.get(String(event.wallet_id || '')) || '',
    })),
    permissions: {
      role: scope.role,
      isOwner: scope.isOwner,
      canManageFunds: scope.canManageFunds,
    },
    fees: {
      buyFeeSol: fromLamports(Number(settings.telegramTradeBuyFeeLamports || 0)),
      sellFeeSol: fromLamports(Number(settings.telegramTradeSellFeeLamports || 0)),
    },
  };
}

async function getTelegramTradeWalletSecret(userId, walletId = '') {
  const { rows, selected } = await ensureTelegramTradeWalletSlots(userId);
  const target = rows.find((row) => String(row.id || '') === String(walletId || '')) || selected;
  if (!target) {
    throw new Error('Trading wallet not found.');
  }
  return {
    id: String(target.id || ''),
    label: String(target.label || '').trim() || 'Wallet',
    publicKey: String(target.public_key || '').trim(),
    selectedMint: String(target.selected_mint || '').trim(),
    selectedSymbol: String(target.selected_symbol || '').trim(),
    secretKeyBase58: decryptDepositSecret(String(target.secret_key_base58 || '')),
  };
}

export async function setTelegramTradeMint(userId, mintInput, walletId = '') {
  const metadata = await resolveMintMetadata(mintInput);
  const { rows, selected } = await ensureTelegramTradeWalletSlots(userId, { forUpdate: true });
  const target = rows.find((row) => String(row.id || '') === String(walletId || '')) || selected;
  if (!target) {
    throw new Error('Trading wallet not found.');
  }
  await pool.query(
    `
      UPDATE telegram_trade_wallet_slots
      SET selected_mint = $2,
          selected_symbol = $3,
          updated_at = NOW()
      WHERE id = $1
    `,
    [String(target.id || ''), metadata.mint, metadata.symbol]
  );
  await syncTelegramTradeLegacyWallet(Number(userId || 0), {
    ...target,
    selected_mint: metadata.mint,
    selected_symbol: metadata.symbol,
  });
  await insertTelegramTradeEvent(
    userId,
    target.id,
    'select_mint',
    `Selected ${metadata.symbol} for ${target.label || 'this wallet'}.`,
    { mint: metadata.mint }
  );
  return getTelegramTradeDetails(userId);
}

export async function executeTelegramTradeBuy(userId, amountSol) {
  const scope = await resolveUserAccessScopeFromPool(userId);
  const { selected } = await ensureTelegramTradeWalletSlots(userId, { forUpdate: true });
  if (!selected) {
    throw new Error('Create a trading wallet first.');
  }
  const selectedMint = String(selected.selected_mint || '').trim();
  if (!selectedMint) {
    throw new Error('Pick a token first.');
  }
  const spendSol = sanitizeSol(amountSol, 0.05, 0.001, 100);
  const connection = getConnection();
  const settings = await getProtocolSettings(pool).catch(() => DEFAULT_PROTOCOL_SETTINGS);
  const signer = keypairFromBase58(decryptDepositSecret(String(selected.secret_key_base58 || '')));
  const balanceLamports = await connection.getBalance(signer.publicKey, 'confirmed').catch(() => 0);
  const reserveLamports = toLamports(Math.max(0.003, Number(config.botSolReserve || 0.005)));
  const feeSafetyLamports = getTxFeeSafetyLamports();
  const tradeFeeLamports = Math.max(0, Math.floor(Number(settings.telegramTradeBuyFeeLamports || 0)));
  const spendableLamports = Math.max(0, Number(balanceLamports || 0) - reserveLamports - feeSafetyLamports - tradeFeeLamports);
  const requestedLamports = toLamports(spendSol);
  if (requestedLamports > spendableLamports) {
    throw new Error(
      `Not enough spendable SOL. Available: ${fromLamports(spendableLamports).toFixed(6)} SOL after reserve and fees.`
    );
  }
  const sig = await pumpPortalTrade({
    connection,
    signer,
    mint: selectedMint,
    action: 'buy',
    amount: spendSol,
    denominatedInSol: true,
    slippage: 10,
    pool: 'auto',
  });
  const feeResult = await applyTelegramTradeFeeFlow({
    connection,
    signer,
    ownerUserId: scope.ownerUserId,
    walletId: selected.id,
    direction: "buy",
    totalLamports: tradeFeeLamports,
  });
  await pool.query(
    `UPDATE telegram_trade_wallet_slots SET last_used_at = NOW(), updated_at = NOW() WHERE id = $1`,
    [String(selected.id || '')]
  );
  await syncTelegramTradeLegacyWallet(Number(userId || 0), { ...selected, last_used_at: new Date().toISOString() });
  await insertTelegramTradeEvent(
    userId,
    selected.id,
    'buy',
    `Bought ${spendSol.toFixed(3)} SOL of ${selected.selected_symbol || selectedMint} on ${selected.label || 'selected wallet'}.`,
    {
      mint: selectedMint,
      amountSol: spendSol,
      tx: sig,
    }
  );
  if (feeResult.feeLamports > 0) {
    await insertTelegramTradeEvent(
      userId,
      selected.id,
      'fee_summary',
      `Buy fee processed (${fromLamports(feeResult.feeLamports).toFixed(6)} SOL).`,
      {
        mint: selectedMint,
        amountSol: fromLamports(feeResult.feeLamports),
        tx: feeResult.treasurySig || feeResult.burnSig || null,
      }
    );
  }
  return getTelegramTradeDetails(userId);
}

export async function executeTelegramTradeSellPct(userId, percent) {
  const scope = await resolveUserAccessScopeFromPool(userId);
  const { selected } = await ensureTelegramTradeWalletSlots(userId, { forUpdate: true });
  if (!selected) {
    throw new Error('Create a trading wallet first.');
  }
  const selectedMint = String(selected.selected_mint || '').trim();
  if (!selectedMint) {
    throw new Error('Pick a token first.');
  }
  const sellPct = Math.max(1, Math.min(100, Math.floor(Number(percent || 0))));
  const connection = getConnection();
  const settings = await getProtocolSettings(pool).catch(() => DEFAULT_PROTOCOL_SETTINGS);
  const signer = keypairFromBase58(decryptDepositSecret(String(selected.secret_key_base58 || '')));
  const balanceLamports = await connection.getBalance(signer.publicKey, 'confirmed').catch(() => 0);
  const reserveLamports = toLamports(Math.max(0.003, Number(config.botSolReserve || 0.005)));
  const feeSafetyLamports = getTxFeeSafetyLamports();
  const tradeFeeLamports = Math.max(0, Math.floor(Number(settings.telegramTradeSellFeeLamports || 0)));
  const spendableLamports = Math.max(0, Number(balanceLamports || 0) - reserveLamports - feeSafetyLamports);
  if (tradeFeeLamports > spendableLamports) {
    throw new Error(
      `Not enough spendable SOL to cover the sell fee. Top up this wallet with at least ${fromLamports(
        Math.max(0, tradeFeeLamports - spendableLamports)
      ).toFixed(6)} SOL.`
    );
  }
  const tokenBalance = await getOwnerTokenBalanceUi(connection, signer.publicKey, selectedMint).catch(() => 0);
  if (tokenBalance <= 0.0000001) {
    throw new Error('No token balance available to sell.');
  }
  const amountToken = Number((tokenBalance * (sellPct / 100)).toFixed(6));
  if (amountToken <= 0.0000001) {
    throw new Error('Sell amount is too small.');
  }
  const sig = await pumpPortalTrade({
    connection,
    signer,
    mint: selectedMint,
    action: 'sell',
    amount: amountToken,
    denominatedInSol: false,
    slippage: 10,
    pool: 'auto',
  });
  const feeResult = await applyTelegramTradeFeeFlow({
    connection,
    signer,
    ownerUserId: scope.ownerUserId,
    walletId: selected.id,
    direction: "sell",
    totalLamports: tradeFeeLamports,
  });
  await pool.query(
    `UPDATE telegram_trade_wallet_slots SET last_used_at = NOW(), updated_at = NOW() WHERE id = $1`,
    [String(selected.id || '')]
  );
  await syncTelegramTradeLegacyWallet(Number(userId || 0), { ...selected, last_used_at: new Date().toISOString() });
  await insertTelegramTradeEvent(
    userId,
    selected.id,
    'sell',
    `Sold ${sellPct}% of ${selected.selected_symbol || selectedMint} from ${selected.label || 'selected wallet'}.`,
    { mint: selectedMint, amountToken, tx: sig }
  );
  if (feeResult.feeLamports > 0) {
    await insertTelegramTradeEvent(
      userId,
      selected.id,
      'fee_summary',
      `Sell fee processed (${fromLamports(feeResult.feeLamports).toFixed(6)} SOL).`,
      {
        mint: selectedMint,
        amountSol: fromLamports(feeResult.feeLamports),
        tx: feeResult.treasurySig || feeResult.burnSig || null,
      }
    );
  }
  return getTelegramTradeDetails(userId);
}

export async function executeTelegramTradeWithdraw(userId, destination, walletId = '') {
  const scope = await resolveUserAccessScopeFromPool(userId);
  assertOwnerPermission(scope, "Managers cannot withdraw Telegram trade wallet balances.");
  const { rows, selected } = await ensureTelegramTradeWalletSlots(userId, { forUpdate: true });
  const target = rows.find((row) => String(row.id || '') === String(walletId || '')) || selected;
  if (!target) {
    throw new Error('Trading wallet not found.');
  }
  let destinationPubkey;
  try {
    destinationPubkey = new PublicKey(String(destination || '').trim());
  } catch {
    throw new Error('Destination wallet is invalid.');
  }
  const destinationText = destinationPubkey.toBase58();
  const connection = getConnection();
  const signer = keypairFromBase58(decryptDepositSecret(String(target.secret_key_base58 || '')));
  const balanceLamports = await connection.getBalance(signer.publicKey, 'confirmed').catch(() => 0);
  const reserveLamports = toLamports(Math.max(0.003, Number(config.botSolReserve || 0.005)));
  const feeSafetyLamports = getTxFeeSafetyLamports();
  const withdrawableLamports = Math.max(0, Number(balanceLamports || 0) - reserveLamports - feeSafetyLamports);
  if (withdrawableLamports <= 0) {
    throw new Error(`No spendable SOL available. Current reserve floor is ${fromLamports(reserveLamports).toFixed(6)} SOL.`);
  }
  const signature = await signAndSendLegacyInstructions(connection, signer, [
    SystemProgram.transfer({
      fromPubkey: signer.publicKey,
      toPubkey: destinationPubkey,
      lamports: withdrawableLamports,
    }),
  ]);
  await pool.query(
    `UPDATE telegram_trade_wallet_slots SET last_used_at = NOW(), updated_at = NOW() WHERE id = $1`,
    [String(target.id || '')]
  );
  await syncTelegramTradeLegacyWallet(Number(userId || 0), { ...target, last_used_at: new Date().toISOString() });
  await insertTelegramTradeEvent(
    userId,
    target.id,
    'withdraw',
    `Withdrew ${fromLamports(withdrawableLamports).toFixed(6)} SOL from ${target.label || 'selected wallet'} to ${destinationText}.`,
    {
      amountSol: fromLamports(withdrawableLamports),
      tx: signature,
    }
  );
  return getTelegramTradeDetails(userId);
}

function buildTelegramTradeDetailsText(details = {}) {
  const wallet = details?.selectedWallet || details?.wallet || null;
  const wallets = Array.isArray(details?.wallets) ? details.wallets : [];
  const events = Array.isArray(details?.events) ? details.events : [];
  const fees = details?.fees || {};
  if (!wallet) {
    return [
      '\u{1F4B1} EMBER Trade',
      '',
      'Create a managed trading wallet to start buying and selling from Telegram.',
      'You can keep multiple wallets here and switch between them with buttons.',
      '',
      `Fees: Buy ${Number(fees.buyFeeSol || 0).toFixed(6)} SOL | Sell ${Number(fees.sellFeeSol || 0).toFixed(6)} SOL`,
    ].join('\n');
  }
  const eventLines = events.length
    ? events.slice(0, 5).map((event) => `\u2022 ${event.walletLabel ? `${event.walletLabel}: ` : ''}${event.message} (${formatTelegramEventAge(event.created_at)})`)
    : ['\u2022 No trade activity yet.'];
  const walletLines = wallets.length
    ? wallets
        .slice(0, 6)
        .map(
          (entry) =>
            `${entry.isSelected ? '\u{1F7E2}' : '\u26AA'} ${entry.label}${entry.imported ? ' (Imported)' : ''} \u2022 ${Number(entry.solBalance || 0).toFixed(4)} SOL`
        )
    : ['\u2022 No trading wallets yet.'];
  const spendableSol = Math.max(0, Number(wallet.spreadableSol || wallet.spendableSol || 0));
  return [
    '\u{1F4B1} EMBER Trade',
    '',
    `Selected: ${wallet.label || 'Wallet'}`,
    `Type: ${wallet.imported ? 'Imported' : 'Managed'}`,
    `Address: ${wallet.publicKey}`,
    `SOL: ${Number(wallet.solBalance || 0).toFixed(6)}`,
    `Spendable: ${spendableSol.toFixed(6)} SOL`,
    `Token: ${wallet.selectedSymbol || 'Not set'}`,
    wallet.selectedMint ? `Mint: ${wallet.selectedMint}` : 'Mint: not selected',
    wallet.selectedMint ? `Balance: ${Number(wallet.tokenBalance || 0).toFixed(6)}` : '',
    `Fees: Buy ${Number(fees.buyFeeSol || 0).toFixed(6)} SOL | Sell ${Number(fees.sellFeeSol || 0).toFixed(6)} SOL`,
    '',
    `Wallets (${wallets.length}/${Number(details?.maxWallets || TELEGRAM_TRADE_MAX_WALLETS)}):`,
    ...walletLines,
    '',
    'Recent activity:',
    ...eventLines,
  ].filter(Boolean).join('\n');
}

function buildTelegramTradeTokenPickerText(overview = {}) {
  const tokens = Array.isArray(overview?.tokens) ? overview.tokens : [];
  const tokenLines = tokens.length
    ? tokens.slice(0, 12).map((token) => `${getBotEmoji(token.selectedBot)} ${token.symbol} \u2022 ${token.mint}`)
    : ['\u{1F4ED} No attached tokens available yet.'];
  return [
    '\u{1FA99} Pick a Trading Token',
    '',
    'Tap one of your attached tokens below or go back and paste a mint manually.',
    '',
    ...tokenLines,
  ].join('\n');
}

function buildTelegramTradeTokenPickerKeyboard(overview = {}) {
  const tokens = Array.isArray(overview?.tokens) ? overview.tokens : [];
  const rows = tokens.slice(0, 12).map((token) => [
    {
      text: `${getBotEmoji(token.selectedBot)} ${token.symbol}`,
      callback_data: `trade:token:${token.id}`,
    },
  ]);
  rows.push([{ text: '\u2B05 Back to Trade', callback_data: 'menu:trade' }]);
  return { inline_keyboard: rows };
}

function buildTelegramTradeWalletPickerText(details = {}) {
  const wallets = Array.isArray(details?.wallets) ? details.wallets : [];
  if (!wallets.length) {
    return [
      '\u{1F45B} Trading Wallets',
      '',
      'You do not have any Telegram trading wallets yet.',
      'Create one to start buying and selling from chat.',
    ].join('\n');
  }
  return [
    '\u{1F45B} Trading Wallets',
    '',
    'Choose the wallet you want to trade from.',
    '',
    ...wallets.map(
      (wallet) =>
        `${wallet.isSelected ? '\u{1F7E2}' : '\u26AA'} ${wallet.label}${wallet.imported ? ' (Imported)' : ''} \u2022 ${Number(wallet.solBalance || 0).toFixed(4)} SOL${
          wallet.selectedSymbol ? ` \u2022 ${wallet.selectedSymbol}` : ''
        }`
    ),
  ].join('\n');
}

function buildTelegramTradeWalletPickerKeyboard(details = {}) {
  const wallets = Array.isArray(details?.wallets) ? details.wallets : [];
  const rows = wallets.map((wallet) => [
    {
      text: `${wallet.isSelected ? '\u25B6 ' : ''}${wallet.label}${wallet.imported ? ' (Imp)' : ''}`,
      callback_data: `trade:wallet:${wallet.id}`,
    },
  ]);
  if (wallets.length < Number(details?.maxWallets || TELEGRAM_TRADE_MAX_WALLETS)) {
    rows.push([{ text: '\u2795 Add Wallet', callback_data: 'trade:create_wallet' }]);
    rows.push([{ text: '\u{1F4E5} Import Wallet', callback_data: 'trade:import_wallet' }]);
  }
  rows.push([{ text: '\u2B05 Back to Trade', callback_data: 'menu:trade' }]);
  return { inline_keyboard: rows };
}

function buildTelegramTradeActionKeyboard(details = {}) {
  const wallet = details?.selectedWallet || details?.wallet || null;
  const wallets = Array.isArray(details?.wallets) ? details.wallets : [];
  const permissions = details?.permissions || {};
  if (!wallet) {
    return {
      inline_keyboard: [
        [{ text: '\u2795 Create Trading Wallet', callback_data: 'trade:create_wallet' }],
        [{ text: '\u{1F4E5} Import Wallet', callback_data: 'trade:import_wallet' }],
        [{ text: '\u{1F3E0} Main Menu', callback_data: 'menu' }],
      ],
    };
  }
  const tokenButtonLabel = wallet.selectedMint
    ? `\u{1FA99} ${String(wallet.selectedSymbol || 'Token').slice(0, 14)}`
    : '\u{1FA99} My Tokens';
  const rows = [
    [
      { text: `\u{1F45B} ${wallet.label} (${wallets.length})`, callback_data: 'trade:wallets' },
      wallets.length < Number(details?.maxWallets || TELEGRAM_TRADE_MAX_WALLETS)
        ? { text: '\u2795 Add Wallet', callback_data: 'trade:create_wallet' }
        : { text: '\u{1F504} Refresh', callback_data: 'menu:trade' },
    ],
    [
      { text: '\u{1F50E} Wallet Info', callback_data: 'trade:wallet_info' },
      { text: '\u{1F511} Show Key', callback_data: 'trade:wallet_key' },
    ],
    [
      { text: tokenButtonLabel, callback_data: 'trade:pick_attached' },
      { text: '\u270F\uFE0F Paste Mint', callback_data: 'trade:set_mint' },
    ],
    [{ text: '\u{1F504} Refresh', callback_data: 'menu:trade' }],
    [
      { text: 'Buy 0.01', callback_data: 'trade:buy:0.01' },
      { text: 'Buy 0.05', callback_data: 'trade:buy:0.05' },
      { text: 'Buy 0.10', callback_data: 'trade:buy:0.10' },
    ],
    [{ text: 'Buy 0.25', callback_data: 'trade:buy:0.25' }],
    [{ text: 'Custom Buy', callback_data: 'trade:buy_custom' }],
  ];
  if (wallet.selectedMint) {
    rows.push([{ text: '\u{1F517} Token Links', callback_data: 'trade:token_links' }]);
    rows.push([
      { text: 'Sell 25%', callback_data: 'trade:sell:25' },
      { text: 'Sell 50%', callback_data: 'trade:sell:50' },
      { text: 'Sell 100%', callback_data: 'trade:sell:100' },
    ]);
    rows.push([{ text: 'Custom Sell %', callback_data: 'trade:sell_custom' }]);
  }
  if (permissions.isOwner) {
    rows.push([{ text: '\u{1F4B8} Withdraw SOL', callback_data: 'trade:withdraw' }]);
  }
  if (wallets.length < Number(details?.maxWallets || TELEGRAM_TRADE_MAX_WALLETS)) {
    rows.push([{ text: '\u{1F4E5} Import Wallet', callback_data: 'trade:import_wallet' }]);
  }
  rows.push([{ text: '\u{1F3E0} Main Menu', callback_data: 'menu' }]);
  return { inline_keyboard: rows };
}

export async function getToolsWorkspace(userId) {


  const scope = await resolveUserAccessScopeFromPool(userId);
  const settings = await getProtocolSettings(pool);
  const catalog = buildToolCatalog(settings, { waiveFees: Boolean(scope.ownerHasToolFeeWaiver) });
  const instancesRes = await pool.query(
    `
      SELECT *
      FROM tool_instances
      WHERE owner_user_id = $1
      ORDER BY created_at DESC
      LIMIT 200
    `,
    [scope.ownerUserId]
  );
  const syncedRows = [];
  for (const row of instancesRes.rows) {
    syncedRows.push(await syncToolInstanceFundingState(pool, row));
  }
  const eventRes = await pool.query(
    `
      SELECT id, tool_instance_id, tool_type, event_type, amount, message, tx, metadata_json, created_at
      FROM tool_events
      WHERE owner_user_id = $1
      ORDER BY created_at DESC
      LIMIT 600
    `,
    [scope.ownerUserId]
  );
  const eventMap = new Map();
  for (const row of eventRes.rows) {
    const key = String(row.tool_instance_id || "");
    if (!eventMap.has(key)) eventMap.set(key, []);
    if (eventMap.get(key).length < 12) {
      eventMap.get(key).push(mapToolEvent(row));
    }
  }
  return {
    catalog: catalog.map((entry) => ({
      ...entry,
      unlockFeeSol: fromLamports(entry.unlockFeeLamports),
      reserveSol: fromLamports(entry.reserveLamports),
      requiredSol: fromLamports(entry.requiredLamports),
      runtimeFeeSol: fromLamports(entry.runtimeFeeLamports),
    })),
    instances: syncedRows.map((row) => mapToolInstance(row, eventMap.get(String(row.id || "")) || [])),
    permissions: {
      role: scope.role,
      isOwner: scope.isOwner,
      canManageFunds: scope.canManageFunds,
    },
  };
}

export async function createToolInstance(userId, payload = {}) {
  return withTx(async (client) => {
    const scope = await resolveUserAccessScope(client, userId);
    const settings = await getProtocolSettings(client);
    const toolType = normalizeToolType(payload.toolType, TOOL_TYPES.holderPooler);
    const catalogEntry = buildToolCatalog(settings, { waiveFees: Boolean(scope.ownerHasToolFeeWaiver) }).find(
      (entry) => entry.toolType === toolType
    );
    if (!catalogEntry) {
      throw new Error("Tool type is invalid.");
    }

    let targetMint = "";
    let targetUrl = "";
    if (catalogEntry.targetKind === "url") {
      targetUrl = normalizeHttpUrl(payload.targetUrl || "", "DexScreener URL");
    } else {
      targetMint = normalizeToolTargetMint(payload.targetMint || "", "Token mint");
    }

    const fundingWallet = generateNonVanityDeposit();
    const toolId = makeId("tool");
    const title = sanitizeToolTitle(payload.title, toolTypeLabel(toolType));
    const simpleMode =
      payload.simpleMode === undefined ? true : Boolean(payload.simpleMode);
    const configJson = {
      ...defaultToolConfig(toolType),
      ...(payload.config && typeof payload.config === "object" ? payload.config : {}),
    };

    const insertRes = await client.query(
      `
        INSERT INTO tool_instances (
          id,
          owner_user_id,
          created_by_user_id,
          tool_type,
          status,
          simple_mode,
          title,
          target_mint,
          target_url,
          funding_wallet_pubkey,
          funding_wallet_secret_key_base58,
          unlock_fee_lamports,
          reserve_lamports,
          runtime_fee_lamports,
          runtime_fee_window_hours,
          config_json,
          state_json
        )
        VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9,
          $10, $11, $12, $13, $14, $15, $16::jsonb, '{}'::jsonb
        )
        RETURNING *
      `,
      [
        toolId,
        scope.ownerUserId,
        scope.actorUserId,
        toolType,
        TOOL_STATUSES.awaitingFunds,
        simpleMode,
        title,
        targetMint,
        targetUrl,
        fundingWallet.pubkey,
        encryptDepositSecret(fundingWallet.secretKeyBase58),
        catalogEntry.unlockFeeLamports,
        catalogEntry.reserveLamports,
        catalogEntry.runtimeFeeLamports,
        catalogEntry.runtimeFeeWindowHours,
        JSON.stringify(configJson),
      ]
    );

    await insertToolEvent(
      client,
      scope.ownerUserId,
      toolId,
      toolType,
      "status",
      `${toolTypeLabel(toolType)} created. Fund ${fromLamports(catalogEntry.requiredLamports).toFixed(6)} SOL to unlock.`,
      {
        amount: fromLamports(catalogEntry.requiredLamports),
        metadata: {
          createdBy: scope.actorUsername,
          simpleMode,
          targetMint,
          targetUrl,
        },
      }
    );

    const row = {
      ...(insertRes.rows[0] || {}),
      current_balance_lamports: 0,
    };
    return mapToolInstance(row, []);
  });
}

function mapToolManagedWallet(row) {
  return {
    id: String(row.id || ""),
    toolInstanceId: String(row.tool_instance_id || ""),
    walletPubkey: String(row.wallet_pubkey || ""),
    label: String(row.label || ""),
    position: Math.max(0, Number(row.position || 0)),
    imported: Boolean(row.imported),
    active: Boolean(row.active),
    state: row.state_json && typeof row.state_json === "object" ? row.state_json : {},
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function normalizeHolderPoolerConfig(rawConfig = {}, currentConfig = {}) {
  const merged = {
    ...defaultToolConfig(TOOL_TYPES.holderPooler),
    ...(currentConfig && typeof currentConfig === "object" ? currentConfig : {}),
    ...(rawConfig && typeof rawConfig === "object" ? rawConfig : {}),
  };
  return {
    walletCount: Math.max(1, Math.min(50, Math.floor(Number(merged.walletCount || 10)))),
    tokenAmountPerWallet: sanitizeSol(merged.tokenAmountPerWallet, 0, 0, 1_000_000_000_000),
    solAmountPerWallet: sanitizeSol(merged.solAmountPerWallet, 0, 0, 10_000),
    walletReserveSol: 0,
    walletMode: String(merged.walletMode || "managed").trim().toLowerCase() === "imported" ? "imported" : "managed",
    stealthMode: Boolean(merged.stealthMode),
  };
}

function normalizeBundleManagerConfig(rawConfig = {}, currentConfig = {}) {
  const merged = {
    ...defaultToolConfig(TOOL_TYPES.bundleManager),
    ...(currentConfig && typeof currentConfig === "object" ? currentConfig : {}),
    ...(rawConfig && typeof rawConfig === "object" ? rawConfig : {}),
  };
  const importedWallets = Array.isArray(rawConfig?.importedWallets)
    ? rawConfig.importedWallets
    : Array.isArray(currentConfig?.importedWallets)
      ? currentConfig.importedWallets
      : [];
  const fundingMode = normalizeToolFundingMode(
    merged.fundingMode,
    Boolean(merged.stealthMode) ? TOOL_FUNDING_MODES.ember : TOOL_FUNDING_MODES.direct
  );
  return {
    walletCount: Math.max(1, Math.min(50, Math.floor(Number(merged.walletCount || 10)))),
    walletMode: String(merged.walletMode || "managed").trim().toLowerCase() === "imported" ? "imported" : "managed",
    fundingMode,
    sideBias: Math.max(0, Math.min(100, Number(merged.sideBias ?? 50) || 50)),
    buySolPerWallet: sanitizeSol(merged.buySolPerWallet, 0.01, 0, 100),
    sellPctPerWallet: Math.max(1, Math.min(100, Number(merged.sellPctPerWallet ?? 25) || 25)),
    walletReserveSol: sanitizeSol(merged.walletReserveSol, 0.0015, 0.0005, 0.02),
    stealthMode: fundingMode === TOOL_FUNDING_MODES.ember,
    fundingStaggerMode: merged.fundingStaggerMode === undefined ? true : Boolean(merged.fundingStaggerMode),
    importedWallets: importedWallets
      .map((value) => String(value || "").trim())
      .filter(Boolean)
      .slice(0, 50),
  };
}

const REACTION_MANAGER_SERVICE_BY_TYPE = Object.freeze({
  rocket: 1,
  fire: 7,
  poop: 4,
  flag: 6,
});

function normalizeReactionManagerConfig(rawConfig = {}, currentConfig = {}) {
  const merged = {
    ...defaultToolConfig(TOOL_TYPES.reactionManager),
    ...(currentConfig && typeof currentConfig === "object" ? currentConfig : {}),
    ...(rawConfig && typeof rawConfig === "object" ? rawConfig : {}),
  };
  const reactionTypeRaw = String(merged.reactionType || "rocket").trim().toLowerCase();
  const reactionType = Object.prototype.hasOwnProperty.call(REACTION_MANAGER_SERVICE_BY_TYPE, reactionTypeRaw)
    ? reactionTypeRaw
    : "rocket";
  return {
    reactionType,
    targetCount: Math.max(1000, Math.min(500000, Math.floor(Number(merged.targetCount || 1000)))),
  };
}

function normalizeSmartSellConfig(rawConfig = {}, currentConfig = {}) {
  const merged = {
    ...defaultToolConfig(TOOL_TYPES.smartSell),
    ...(currentConfig && typeof currentConfig === "object" ? currentConfig : {}),
    ...(rawConfig && typeof rawConfig === "object" ? rawConfig : {}),
  };
  const triggerMode = String(merged.triggerMode || "every_buy").trim().toLowerCase() === "threshold"
    ? "threshold"
    : "every_buy";
  const timingModeRaw = String(merged.timingMode || "randomized").trim().toLowerCase();
  const timingMode =
    timingModeRaw === "instant" || timingModeRaw === "split"
      ? timingModeRaw
      : "randomized";
  const fundingMode = normalizeToolFundingMode(
    merged.fundingMode,
    Boolean(merged.stealthMode) ? TOOL_FUNDING_MODES.ember : TOOL_FUNDING_MODES.direct
  );
  return {
    triggerMode,
    sellPct: Math.max(1, Math.min(100, Number(merged.sellPct ?? 25) || 25)),
    thresholdSol: sanitizeSol(merged.thresholdSol, 0, 0, 1000),
    timingMode,
    walletMode: "managed",
    fundingMode,
    walletCount: Math.max(1, Math.min(50, Math.floor(Number(merged.walletCount || 6)))),
    walletReserveSol: sanitizeSol(merged.walletReserveSol, 0.0015, 0.0005, 0.02),
    stealthMode: fundingMode === TOOL_FUNDING_MODES.ember,
  };
}

function buildFundingPlanKey(recipients = []) {
  const stable = recipients
    .map((item) => ({
      walletId: String(item.walletId || ""),
      address: String(item.address || ""),
      lamports: Math.max(0, Math.floor(Number(item.lamports || 0))),
    }))
    .sort((a, b) => a.address.localeCompare(b.address));
  return JSON.stringify(stable);
}

function allocatePctBips(recipients = []) {
  const cleaned = recipients
    .map((item) => ({
      ...item,
      lamports: Math.max(0, Math.floor(Number(item.lamports || 0))),
    }))
    .filter((item) => item.lamports > 0);
  const total = cleaned.reduce((sum, item) => sum + item.lamports, 0);
  if (!total) return [];
  let allocated = 0;
  return cleaned.map((item, index) => {
    const toPctBips =
      index === cleaned.length - 1
        ? 10000 - allocated
        : Math.max(1, Math.floor((item.lamports / total) * 10000));
    allocated += toPctBips;
    return { ...item, toPctBips };
  });
}

async function callEmberFundingApi(path, { method = "GET", body } = {}) {
  if (!config.emberFundingApiKey) {
    throw new Error("EMBER Funding is not configured.");
  }
  const headers = {
    "x-api-key": String(config.emberFundingApiKey || ""),
  };
  const init = { method, headers };
  if (body !== undefined) {
    headers["Content-Type"] = "application/json";
    init.body = JSON.stringify(body);
  }
  const res = await fetch(`${config.emberFundingApiUrl}${path}`, init);
  const text = await res.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    throw new Error(`EMBER Funding returned invalid response (${text.slice(0, 180)})`);
  }
  if (!res.ok || data?.error) {
    throw new Error(data?.message || `EMBER Funding request failed (${res.status})`);
  }
  return data;
}

async function createEmberFundingOrder({ recipients = [], staggerMode = true }) {
  const outputs = allocatePctBips(recipients);
  const totalLamports = outputs.reduce((sum, item) => sum + item.lamports, 0);
  if (!outputs.length || totalLamports <= 0) {
    throw new Error("EMBER Funding requires at least one recipient.");
  }
  const fromAmount = Number(fromLamports(totalLamports).toFixed(9));
  const quoteCreate = await callEmberFundingApi("/quotes/", {
    method: "POST",
    body: {
      type: "mixed_rate",
      quoteInput: {
        fromAmount,
        fromAssetId: "sol",
        fromNetworkId: "solana",
      },
      quoteOutputs: outputs.map((item) => ({
        toPctBips: item.toPctBips,
        toAssetId: "sol",
        toNetworkId: "solana",
      })),
      customSignature: "EMBER Funding",
    },
  });
  const quoteId = String(quoteCreate?.data || "").trim();
  if (!quoteId) {
    throw new Error("EMBER Funding did not return a quote id.");
  }
  const quote = await callEmberFundingApi(`/quotes/${quoteId}`, { method: "GET" });
  const legs = Array.isArray(quote?.data?.quoteLegs) ? quote.data.quoteLegs : [];
  const orderOutputs = outputs.map((item, index) => ({
    toAddress: item.address,
    toPctBips: item.toPctBips,
    toAssetId: "sol",
    toNetworkId: "solana",
    toExchangerId:
      String(legs[index]?.quoteLegOutput?.toExchangerId || legs[0]?.quoteLegOutput?.toExchangerId || "splitotc").trim(),
  }));
  const orderCreate = await callEmberFundingApi("/orders/", {
    method: "POST",
    body: {
      type: "mixed_rate",
      quoteId,
      orderInput: {
        fromAmount,
        fromAssetId: "sol",
        fromNetworkId: "solana",
      },
      orderOutputs,
      staggerMode: Boolean(staggerMode),
      staggerMinMs: staggerMode ? 30000 : 0,
      staggerMaxMs: staggerMode ? 60000 : 0,
      customSignature: "EMBER Funding",
    },
  });
  const orderId = String(orderCreate?.data?.orderId || "").trim();
  if (!orderId) {
    throw new Error("EMBER Funding did not return an order id.");
  }
  const order = await callEmberFundingApi(`/orders/${orderId}`, { method: "GET" });
  return {
    quoteId,
    orderId,
    order: order?.data || null,
    recipients: outputs,
  };
}

async function getEmberFundingOrder(orderId) {
  const id = String(orderId || "").trim();
  if (!id) throw new Error("EMBER Funding order id is required.");
  const response = await callEmberFundingApi(`/orders/${id}`, { method: "GET" });
  return response?.data || null;
}

function mapFundingStateForDisplay(stateJson = {}) {
  const funding = stateJson?.funding && typeof stateJson.funding === "object" ? stateJson.funding : null;
  if (!funding) return null;
  return {
    mode: normalizeToolFundingMode(funding.mode, TOOL_FUNDING_MODES.direct),
    modeLabel: toolFundingModeLabel(funding.mode),
    orderId: String(funding.orderId || ""),
    quoteId: String(funding.quoteId || ""),
    planKey: String(funding.planKey || ""),
    status: String(funding.status || ""),
    statusShort: String(funding.statusShort || ""),
    statusText: String(funding.statusText || ""),
    depositWalletAddress: String(funding.depositWalletAddress || ""),
    depositAmountLamports: Math.max(0, Math.floor(Number(funding.depositAmountLamports || 0))),
    depositAmountSol: fromLamports(Math.max(0, Math.floor(Number(funding.depositAmountLamports || 0)))),
    sentLamports: Math.max(0, Math.floor(Number(funding.sentLamports || 0))),
    sentSol: fromLamports(Math.max(0, Math.floor(Number(funding.sentLamports || 0)))),
    sentTx: String(funding.sentTx || ""),
    recipientCount: Math.max(0, Math.floor(Number(funding.recipientCount || 0))),
    lastCheckedAt: funding.lastCheckedAt || null,
    completedAt: funding.completedAt || null,
  };
}

async function refreshToolFundingState(client, row, { emitEvents = true } = {}) {
  const state = row.state_json && typeof row.state_json === "object" ? row.state_json : {};
  const funding = state.funding && typeof state.funding === "object" ? state.funding : null;
  if (!funding || normalizeToolFundingMode(funding.mode, TOOL_FUNDING_MODES.direct) !== TOOL_FUNDING_MODES.ember) {
    return row;
  }
  const statusShort = String(funding.statusShort || "").trim().toLowerCase();
  if (!funding.orderId || ["completed", "failed", "refunded", "halted", "expired"].includes(statusShort)) {
    return row;
  }
  const order = await getEmberFundingOrder(funding.orderId);
  const nextFunding = {
    ...funding,
    status: String(order?.status || ""),
    statusShort: String(order?.statusShort || ""),
    statusText: String(order?.statusText || ""),
    depositWalletAddress: String(order?.depositWalletAddress || funding.depositWalletAddress || ""),
    depositAmountLamports: toLamports(Number(order?.depositAmount || fromLamports(funding.depositAmountLamports || 0))),
    lastCheckedAt: new Date().toISOString(),
  };
  if (nextFunding.statusShort === "completed") {
    nextFunding.completedAt = nextFunding.completedAt || new Date().toISOString();
  }
  const nextState = {
    ...state,
    funding: nextFunding,
  };
  const nextStatus =
    nextFunding.statusShort === "completed"
      ? TOOL_STATUSES.ready
      : String(row.status || "") === TOOL_STATUSES.active
        ? TOOL_STATUSES.active
        : TOOL_STATUSES.provisioning;
  const updateRes = await client.query(
    `
      UPDATE tool_instances
      SET status = $2, state_json = $3::jsonb, updated_at = NOW()
      WHERE id = $1
      RETURNING *
    `,
    [String(row.id || ""), nextStatus, JSON.stringify(nextState)]
  );
  const updatedRow = updateRes.rows[0] || row;
  if (emitEvents && nextFunding.statusText && nextFunding.statusText !== funding.statusText) {
    await insertToolEvent(
      client,
      Number(updatedRow.owner_user_id || 0),
      String(updatedRow.id || ""),
      normalizeToolType(updatedRow.tool_type, TOOL_TYPES.bundleManager),
      "status",
      `EMBER Funding status: ${nextFunding.statusText}.`,
      {
        amount: fromLamports(nextFunding.sentLamports || 0),
        metadata: {
          fundingMode: TOOL_FUNDING_MODES.ember,
          orderId: nextFunding.orderId,
          statusShort: nextFunding.statusShort,
        },
      }
    );
  }
  return updatedRow;
}

async function ensureEmberFundingPlan(client, row, recipients, { staggerMode = true, pendingStatus = TOOL_STATUSES.provisioning } = {}) {
  const state = row.state_json && typeof row.state_json === "object" ? row.state_json : {};
  const planKey = buildFundingPlanKey(recipients);
  const existing = state.funding && typeof state.funding === "object" ? state.funding : null;
  if (existing && normalizeToolFundingMode(existing.mode, TOOL_FUNDING_MODES.direct) === TOOL_FUNDING_MODES.ember) {
    const existingShort = String(existing.statusShort || "").trim().toLowerCase();
    if (existing.planKey && existing.planKey !== planKey && !["completed", "failed", "refunded", "halted", "expired"].includes(existingShort)) {
      throw new Error("EMBER Funding is already processing another wallet funding batch. Refresh it before changing the plan.");
    }
    if (existing.orderId && existing.planKey === planKey && !["completed", "failed", "refunded", "halted", "expired"].includes(existingShort)) {
      const refreshedRow = await refreshToolFundingState(client, row, { emitEvents: true });
      const refreshedFunding = refreshedRow.state_json?.funding || {};
      if (String(refreshedFunding.statusShort || "").trim().toLowerCase() !== "completed") {
        return { ready: false, row: refreshedRow };
      }
      return { ready: true, row: refreshedRow };
    }
  }

  const fundingSigner = keypairFromBase58(
    decryptDepositSecret(String(row.funding_wallet_secret_key_base58 || ""))
  );
  const created = await createEmberFundingOrder({ recipients, staggerMode });
  const depositWalletAddress = String(created.order?.depositWalletAddress || "").trim();
  const depositAmountLamports = toLamports(Number(created.order?.depositAmount || 0));
  if (!depositWalletAddress || depositAmountLamports <= 0) {
    throw new Error("EMBER Funding did not return a deposit wallet.");
  }
  const connection = getConnection();
  const sendResult = await sendSolTransfer(connection, fundingSigner, depositWalletAddress, depositAmountLamports);
  const nextFunding = {
    mode: TOOL_FUNDING_MODES.ember,
    quoteId: created.quoteId,
    orderId: created.orderId,
    planKey,
    status: String(created.order?.status || ""),
    statusShort: String(created.order?.statusShort || ""),
    statusText: String(created.order?.statusText || ""),
    depositWalletAddress,
    depositAmountLamports,
    recipientCount: recipients.length,
    sentLamports: depositAmountLamports,
    sentTx: sendResult || "",
    createdAt: new Date().toISOString(),
    lastCheckedAt: new Date().toISOString(),
  };
  const nextState = {
    ...state,
    funding: nextFunding,
  };
  const updateRes = await client.query(
    `
      UPDATE tool_instances
      SET
        status = $2,
        state_json = $3::jsonb,
        last_error = NULL,
        updated_at = NOW()
      WHERE id = $1
      RETURNING *
    `,
    [String(row.id || ""), pendingStatus, JSON.stringify(nextState)]
  );
  const updatedRow = updateRes.rows[0] || row;
  await insertToolEvent(
    client,
    Number(updatedRow.owner_user_id || 0),
    String(updatedRow.id || ""),
    normalizeToolType(updatedRow.tool_type, TOOL_TYPES.bundleManager),
    "status",
    `EMBER Funding started for ${recipients.length} wallets (${fromLamports(depositAmountLamports).toFixed(6)} SOL).`,
    {
      amount: fromLamports(depositAmountLamports),
      tx: sendResult || null,
      metadata: {
        fundingMode: TOOL_FUNDING_MODES.ember,
        orderId: created.orderId,
        recipientCount: recipients.length,
      },
    }
  );
  return { ready: false, row: updatedRow };
}

async function callReactionManagerApi(params) {
  if (!config.reactionManagerApiKey) {
    throw new Error("Reaction Manager provider is not configured.");
  }
  const body = new URLSearchParams({
    key: String(config.reactionManagerApiKey || ""),
    ...Object.fromEntries(
      Object.entries(params || {}).map(([key, value]) => [key, String(value ?? "")])
    ),
  });
  const res = await fetch(config.reactionManagerApiUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body,
  });
  const text = await res.text();
  let data = null;
  try {
    data = JSON.parse(text);
  } catch {
    throw new Error(`Reaction provider returned invalid response (${text.slice(0, 180)})`);
  }
  if (!res.ok) {
    throw new Error(data?.message || `Reaction provider request failed (${res.status})`);
  }
  if (data?.error) {
    throw new Error(String(data.error || "Reaction provider error"));
  }
  return data;
}

async function ensureProviderReactionSession(client, ownerUserId) {
  const existingRes = await client.query(
    `
      SELECT *
      FROM reaction_sessions
      WHERE owner_user_id = $1
        AND session_type = $2
        AND provider_name = $3
      ORDER BY last_used_at DESC NULLS LAST, created_at DESC
      LIMIT 1
    `,
    [ownerUserId, "provider", REACTION_PROVIDER_NAME]
  );
  if (existingRes.rowCount) {
    const updateRes = await client.query(
      `
        UPDATE reaction_sessions
        SET status = 'active', last_error = NULL, last_used_at = NOW()
        WHERE id = $1
        RETURNING *
      `,
      [String(existingRes.rows[0].id || "")]
    );
    return updateRes.rows[0];
  }
  const insertRes = await client.query(
    `
      INSERT INTO reaction_sessions (
        id,
        owner_user_id,
        session_type,
        provider_name,
        label,
        status,
        state_json,
        last_used_at
      )
      VALUES ($1, $2, 'provider', $3, $4, 'active', '{}'::jsonb, NOW())
      RETURNING *
    `,
    [makeId("rsess"), ownerUserId, REACTION_PROVIDER_NAME, "Reaction Provider Session"]
  );
  return insertRes.rows[0];
}

async function insertReactionCampaign(client, toolRow, configJson, options = {}) {
  const targetInfo = parseDexPairTargetUrl(toolRow.target_url);
  const executorType = normalizeReactionExecutorType(options.executorType, REACTION_EXECUTOR_TYPES.provider);
  const providerName = executorType === REACTION_EXECUTOR_TYPES.provider ? REACTION_PROVIDER_NAME : "";
  const insertRes = await client.query(
    `
      INSERT INTO reaction_campaigns (
        id,
        tool_instance_id,
        owner_user_id,
        target_url,
        target_chain,
        target_pair_id,
        reaction_type,
        target_count,
        delivered_count,
        failed_count,
        executor_type,
        provider_name,
        status,
        pacing_mode,
        config_json,
        state_json,
        started_at,
        last_checked_at
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8,
        0, 0, $9, $10, 'queued', 'linear', $11::jsonb, '{}'::jsonb, NOW(), NOW()
      )
      RETURNING *
    `,
    [
      makeId("rc"),
      String(toolRow.id || ""),
      Number(toolRow.owner_user_id || 0),
      targetInfo.targetUrl,
      targetInfo.targetChain,
      targetInfo.targetPairId,
      configJson.reactionType,
      Math.max(0, Math.floor(Number(configJson.targetCount || 0))),
      executorType,
      providerName,
      JSON.stringify({
        reactionType: configJson.reactionType,
        targetCount: configJson.targetCount,
      }),
    ]
  );
  return insertRes.rows[0];
}

async function insertReactionJob(client, campaignRow, sessionRow, configJson, options = {}) {
  const insertRes = await client.query(
    `
      INSERT INTO reaction_jobs (
        id,
        campaign_id,
        tool_instance_id,
        owner_user_id,
        reaction_session_id,
        ordinal,
        reaction_type,
        target_url,
        target_chain,
        target_pair_id,
        quantity,
        executor_type,
        provider_name,
        status,
        metadata_json,
        started_at
      )
      VALUES (
        $1, $2, $3, $4, $5, 1, $6, $7, $8, $9, $10, $11, $12, 'queued', $13::jsonb, NOW()
      )
      RETURNING *
    `,
    [
      makeId("rjob"),
      String(campaignRow.id || ""),
      String(campaignRow.tool_instance_id || ""),
      Number(campaignRow.owner_user_id || 0),
      sessionRow?.id ? String(sessionRow.id) : null,
      configJson.reactionType,
      String(campaignRow.target_url || ""),
      String(campaignRow.target_chain || ""),
      String(campaignRow.target_pair_id || ""),
      Math.max(0, Math.floor(Number(configJson.targetCount || 0))),
      normalizeReactionExecutorType(options.executorType, REACTION_EXECUTOR_TYPES.provider),
      String(options.providerName || (sessionRow?.provider_name || "")),
      JSON.stringify({ toolInstanceId: String(campaignRow.tool_instance_id || "") }),
    ]
  );
  return insertRes.rows[0];
}

async function loadReactionBackendState(client, toolInstanceId, { campaignId = "" } = {}) {
  const campaignRes = await client.query(
    `
      SELECT *
      FROM reaction_campaigns
      WHERE tool_instance_id = $1
        ${campaignId ? "AND id = $2" : ""}
      ORDER BY created_at DESC
      LIMIT 1
    `,
    campaignId ? [String(toolInstanceId || ""), String(campaignId || "")] : [String(toolInstanceId || "")]
  );
  const campaignRow = campaignRes.rows[0] || null;
  if (!campaignRow) {
    return {
      currentCampaign: null,
      jobs: [],
      sessions: [],
    };
  }
  const jobsRes = await client.query(
    `
      SELECT *
      FROM reaction_jobs
      WHERE campaign_id = $1
      ORDER BY ordinal ASC, created_at ASC
      LIMIT 50
    `,
    [String(campaignRow.id || "")]
  );
  const sessionIds = [...new Set((jobsRes.rows || []).map((row) => String(row.reaction_session_id || "")).filter(Boolean))];
  let sessions = [];
  if (sessionIds.length) {
    const sessionRes = await client.query(
      `
        SELECT *
        FROM reaction_sessions
        WHERE id = ANY($1::text[])
        ORDER BY created_at DESC
      `,
      [sessionIds]
    );
    sessions = (sessionRes.rows || []).map((row) => mapReactionSession(row));
  }
  return {
    currentCampaign: mapReactionCampaign(campaignRow),
    jobs: (jobsRes.rows || []).map((row) => mapReactionJob(row)),
    sessions,
  };
}

async function readToolInstanceForScope(client, userId, toolId, { forUpdate = false } = {}) {
  const scope = await resolveUserAccessScope(client, userId);
  const lockClause = forUpdate ? "FOR UPDATE" : "";
  const res = await client.query(
    `
      SELECT *
      FROM tool_instances
      WHERE owner_user_id = $1
        AND id = $2
      LIMIT 1
      ${lockClause}
    `,
    [scope.ownerUserId, String(toolId || "").trim()]
  );
  if (!res.rowCount) throw new Error("Tool instance not found.");
  const row = await syncToolInstanceFundingState(client, res.rows[0]);
  return { scope, row };
}

async function getToolManagedWalletRows(client, toolInstanceId, { activeOnly = false } = {}) {
  const params = [String(toolInstanceId || "").trim()];
  const where = ["tool_instance_id = $1"];
  if (activeOnly) where.push("active = TRUE");
  const res = await client.query(
    `
      SELECT *
      FROM tool_managed_wallets
      WHERE ${where.join(" AND ")}
      ORDER BY position ASC, created_at ASC
    `,
    params
  );
  return res.rows || [];
}

async function ensureHolderPoolerWallets(client, toolRow, configJson) {
  const desiredCount = Math.max(1, Math.min(50, Math.floor(Number(configJson.walletCount || 10))));
  const existingRows = await getToolManagedWalletRows(client, toolRow.id, { activeOnly: true });
  if (existingRows.length >= desiredCount) {
    return existingRows.slice(0, desiredCount);
  }

  const created = [...existingRows];
  for (let index = existingRows.length; index < desiredCount; index += 1) {
    const generated = generateNonVanityDeposit();
    const insertRes = await client.query(
      `
        INSERT INTO tool_managed_wallets (
          id,
          tool_instance_id,
          owner_user_id,
          wallet_pubkey,
          secret_key_base58,
          label,
          position,
          imported,
          active,
          state_json
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, FALSE, TRUE, '{}'::jsonb)
        RETURNING *
      `,
      [
        makeId("toolw"),
        String(toolRow.id || ""),
        Number(toolRow.owner_user_id || 0),
        generated.pubkey,
        encryptDepositSecret(generated.secretKeyBase58),
        `Holder-${index + 1}`,
        index + 1,
      ]
    );
    created.push(insertRes.rows[0]);
  }
  return created;
}

async function ensureBundleManagerManagedWallets(client, toolRow, configJson) {
  const desiredCount = Math.max(1, Math.min(50, Math.floor(Number(configJson.walletCount || 10))));
  const existingRows = (await getToolManagedWalletRows(client, toolRow.id, { activeOnly: true }))
    .filter((row) => !row.imported);
  if (existingRows.length >= desiredCount) {
    return existingRows.slice(0, desiredCount);
  }

  const created = [...existingRows];
  for (let index = existingRows.length; index < desiredCount; index += 1) {
    const generated = generateNonVanityDeposit();
    const insertRes = await client.query(
      `
        INSERT INTO tool_managed_wallets (
          id,
          tool_instance_id,
          owner_user_id,
          wallet_pubkey,
          secret_key_base58,
          label,
          position,
          imported,
          active,
          state_json
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, FALSE, TRUE, '{}'::jsonb)
        RETURNING *
      `,
      [
        makeId("toolw"),
        String(toolRow.id || ""),
        Number(toolRow.owner_user_id || 0),
        generated.pubkey,
        encryptDepositSecret(generated.secretKeyBase58),
        `Bundle-${index + 1}`,
        index + 1,
      ]
    );
    created.push(insertRes.rows[0]);
  }
  return created;
}

async function ensureSmartSellWallets(client, toolRow, configJson) {
  const desiredCount = Math.max(1, Math.min(50, Math.floor(Number(configJson.walletCount || 6))));
  const existingRows = (await getToolManagedWalletRows(client, toolRow.id, { activeOnly: true }))
    .filter((row) => !row.imported);
  if (existingRows.length >= desiredCount) {
    return existingRows.slice(0, desiredCount);
  }

  const created = [...existingRows];
  for (let index = existingRows.length; index < desiredCount; index += 1) {
    const generated = generateNonVanityDeposit();
    const insertRes = await client.query(
      `
        INSERT INTO tool_managed_wallets (
          id,
          tool_instance_id,
          owner_user_id,
          wallet_pubkey,
          secret_key_base58,
          label,
          position,
          imported,
          active,
          state_json
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, FALSE, TRUE, '{}'::jsonb)
        RETURNING *
      `,
      [
        makeId("toolw"),
        String(toolRow.id || ""),
        Number(toolRow.owner_user_id || 0),
        generated.pubkey,
        encryptDepositSecret(generated.secretKeyBase58),
        `Sell-${index + 1}`,
        index + 1,
      ]
    );
    created.push(insertRes.rows[0]);
  }
  return created;
}

async function addBundleManagerImportedWallets(client, toolRow, importedSecrets = []) {
  const cleanSecrets = importedSecrets
    .map((value) => String(value || "").trim())
    .filter(Boolean)
    .slice(0, 50);
  if (!cleanSecrets.length) {
    return (await getToolManagedWalletRows(client, toolRow.id, { activeOnly: true })).filter((row) => row.imported);
  }

  const existingRows = await getToolManagedWalletRows(client, toolRow.id);
  const existingByPubkey = new Map(
    existingRows
      .filter((row) => row.imported)
      .map((row) => [String(row.wallet_pubkey || "").trim(), row])
  );
  let nextPosition = existingRows.reduce((max, row) => Math.max(max, Number(row.position || 0)), 0);
  for (const secret of cleanSecrets) {
    const signer = keypairFromBase58(secret);
    const pubkey = signer.publicKey.toBase58();
    const existing = existingByPubkey.get(pubkey);
    if (existing) {
      if (!existing.active) {
        await client.query(
          `
            UPDATE tool_managed_wallets
            SET active = TRUE, updated_at = NOW()
            WHERE id = $1
          `,
          [String(existing.id || "")]
        );
      }
      continue;
    }
    nextPosition += 1;
    await client.query(
      `
        INSERT INTO tool_managed_wallets (
          id,
          tool_instance_id,
          owner_user_id,
          wallet_pubkey,
          secret_key_base58,
          label,
          position,
          imported,
          active,
          state_json
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE, TRUE, '{}'::jsonb)
      `,
      [
        makeId("toolw"),
        String(toolRow.id || ""),
        Number(toolRow.owner_user_id || 0),
        pubkey,
        encryptDepositSecret(secret),
        `Imported-${nextPosition}`,
        nextPosition,
      ]
    );
  }
  return (await getToolManagedWalletRows(client, toolRow.id, { activeOnly: true })).filter((row) => row.imported);
}

async function ensureBundleManagerWallets(client, toolRow, configJson) {
  if (configJson.walletMode === "imported") {
    return addBundleManagerImportedWallets(client, toolRow, configJson.importedWallets || []);
  }
  return ensureBundleManagerManagedWallets(client, toolRow, configJson);
}

async function readToolWalletBalances(connection, targetMint, walletRows = []) {
  const mintText = String(targetMint || "").trim();
  return Promise.all(
    walletRows.map(async (row) => {
      const walletPubkey = String(row.wallet_pubkey || "").trim();
      let solLamports = 0;
      let tokenBalance = 0;
      if (walletPubkey) {
        try {
          solLamports = await connection.getBalance(new PublicKey(walletPubkey), "confirmed");
        } catch {
          solLamports = 0;
        }
        if (mintText) {
          try {
            tokenBalance = await getOwnerTokenBalanceUi(connection, walletPubkey, mintText);
          } catch {
            tokenBalance = 0;
          }
        }
      }
      return {
        ...mapToolManagedWallet(row),
        solLamports,
        solBalance: fromLamports(solLamports),
        tokenBalance: Number(tokenBalance || 0),
      };
    })
  );
}

async function transferManagedTokensToOwner(connection, signer, mintAddress, destinationOwnerAddress, uiAmount) {
  const amountUi = Number(uiAmount || 0);
  if (!Number.isFinite(amountUi) || amountUi <= 0) {
    return { signature: null, sentUiAmount: 0 };
  }

  const mint = new PublicKey(String(mintAddress || "").trim());
  const destinationOwner = new PublicKey(String(destinationOwnerAddress || "").trim());
  const sources = await getOwnerTokenAccountBalancesForMint(connection, signer.publicKey, mint, null);
  if (!sources.length) {
    return { signature: null, sentUiAmount: 0 };
  }

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

  const filteredSources = sources.filter((item) => item.programId?.equals?.(tokenProgramId));
  if (!filteredSources.length) {
    return { signature: null, sentUiAmount: 0 };
  }

  const decimals = Number(filteredSources[0]?.decimals || 0);
  const totalRawBalance = filteredSources.reduce((sum, item) => sum + BigInt(item.amountRaw || 0), 0n);
  if (totalRawBalance <= 0n) {
    return { signature: null, sentUiAmount: 0 };
  }

  const unit = 10 ** Math.max(0, decimals);
  const requestedRaw = BigInt(Math.floor(amountUi * unit));
  const rawAmount = requestedRaw > totalRawBalance ? totalRawBalance : requestedRaw;
  if (rawAmount <= 0n) {
    return { signature: null, sentUiAmount: 0 };
  }

  const destinationAta = getAssociatedTokenAddressSync(
    mint,
    destinationOwner,
    false,
    tokenProgramId
  );
  const instructions = [
    createAssociatedTokenAccountIdempotentInstruction(
      signer.publicKey,
      destinationAta,
      destinationOwner,
      mint,
      tokenProgramId
    ),
  ];

  let remaining = rawAmount;
  filteredSources.sort((a, b) => (a.amountRaw > b.amountRaw ? -1 : a.amountRaw < b.amountRaw ? 1 : 0));
  for (const source of filteredSources) {
    if (remaining <= 0n) break;
    const take = source.amountRaw > remaining ? remaining : source.amountRaw;
    if (take <= 0n) continue;
    instructions.push(
      createTransferCheckedInstruction(
        source.pubkey,
        mint,
        destinationAta,
        signer.publicKey,
        take,
        decimals,
        [],
        tokenProgramId
      )
    );
    remaining -= take;
  }

  if (remaining > 0n) {
    return { signature: null, sentUiAmount: 0 };
  }

  const signature = await signAndSendLegacyInstructions(connection, signer, instructions);
  const sentUiAmount = Number(rawAmount) / unit;
  return { signature, sentUiAmount };
}

async function buildToolInstanceDetails(client, row) {
  const mapped = mapToolInstance(row, []);
  const connection = getConnection();
  const fundingPubkey = String(row.funding_wallet_pubkey || "").trim();
  const fundingLamports = fundingPubkey
    ? await connection.getBalance(new PublicKey(fundingPubkey), "confirmed").catch(() => 0)
    : 0;
  const fundingTokenBalance = mapped.targetMint
    ? await getOwnerTokenBalanceUi(connection, fundingPubkey, mapped.targetMint).catch(() => 0)
    : 0;

  let wallets = [];
  if (
    mapped.toolType === TOOL_TYPES.holderPooler ||
    mapped.toolType === TOOL_TYPES.bundleManager ||
    mapped.toolType === TOOL_TYPES.smartSell
  ) {
    const walletRows = await getToolManagedWalletRows(client, row.id);
    wallets = await readToolWalletBalances(connection, row.target_mint, walletRows);
  }

  const eventsRes = await client.query(
    `
      SELECT id, tool_instance_id, tool_type, event_type, amount, message, tx, metadata_json, created_at
      FROM tool_events
      WHERE tool_instance_id = $1
      ORDER BY created_at DESC
      LIMIT 20
    `,
    [String(row.id || "")]
  );

  let reaction = null;
  if (mapped.toolType === TOOL_TYPES.reactionManager) {
    reaction = await loadReactionBackendState(client, row.id, {
      campaignId: String(row.state_json?.reactionCampaignId || ""),
    });
  }

  return {
    tool: {
      ...mapped,
      events: (eventsRes.rows || []).map((eventRow) => mapToolEvent(eventRow)),
      balanceLamports: fundingLamports,
      balanceSol: fromLamports(fundingLamports),
      isFunded: fundingLamports >= mapped.requiredLamports,
    },
    funding: {
      walletPubkey: fundingPubkey,
      solLamports: fundingLamports,
      solBalance: fromLamports(fundingLamports),
      tokenBalance: Number(fundingTokenBalance || 0),
    },
    wallets,
    reaction,
    permissions: {
      canManageFunds: true,
    },
  };
}

const REACTION_EXECUTOR_TYPES = Object.freeze({
  provider: "provider",
  manual: "manual",
});

const REACTION_PROVIDER_NAME = "dexmoji";

function normalizeReactionExecutorType(value, fallback = REACTION_EXECUTOR_TYPES.provider) {
  const raw = String(value || "").trim().toLowerCase();
  if (raw === REACTION_EXECUTOR_TYPES.manual) return REACTION_EXECUTOR_TYPES.manual;
  if (raw === REACTION_EXECUTOR_TYPES.provider) return REACTION_EXECUTOR_TYPES.provider;
  return fallback;
}

function parseDexPairTargetUrl(rawUrl = "") {
  const value = String(rawUrl || "").trim();
  if (!value) {
    return {
      targetUrl: "",
      targetChain: "",
      targetPairId: "",
    };
  }
  try {
    const parsed = new URL(value);
    const parts = parsed.pathname.split("/").filter(Boolean);
    return {
      targetUrl: parsed.toString(),
      targetChain: parts[0] ? String(parts[0]) : "",
      targetPairId: parts[1] ? String(parts[1]) : "",
    };
  } catch {
    return {
      targetUrl: value,
      targetChain: "",
      targetPairId: "",
    };
  }
}

function mapReactionCampaign(row) {
  if (!row) return null;
  return {
    id: String(row.id || ""),
    toolInstanceId: String(row.tool_instance_id || ""),
    ownerUserId: Math.max(0, Number(row.owner_user_id || 0)),
    targetUrl: String(row.target_url || ""),
    targetChain: String(row.target_chain || ""),
    targetPairId: String(row.target_pair_id || ""),
    reactionType: String(row.reaction_type || ""),
    targetCount: Math.max(0, Number(row.target_count || 0)),
    deliveredCount: Math.max(0, Number(row.delivered_count || 0)),
    failedCount: Math.max(0, Number(row.failed_count || 0)),
    executorType: normalizeReactionExecutorType(row.executor_type),
    providerName: String(row.provider_name || ""),
    providerOrderId: String(row.provider_order_id || ""),
    status: String(row.status || "draft"),
    pacingMode: String(row.pacing_mode || "linear"),
    config: row.config_json && typeof row.config_json === "object" ? row.config_json : {},
    state: row.state_json && typeof row.state_json === "object" ? row.state_json : {},
    lastError: String(row.last_error || ""),
    startedAt: row.started_at,
    completedAt: row.completed_at,
    lastCheckedAt: row.last_checked_at,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function mapReactionJob(row) {
  if (!row) return null;
  return {
    id: String(row.id || ""),
    campaignId: String(row.campaign_id || ""),
    toolInstanceId: String(row.tool_instance_id || ""),
    ownerUserId: Math.max(0, Number(row.owner_user_id || 0)),
    reactionSessionId: String(row.reaction_session_id || ""),
    ordinal: Math.max(0, Number(row.ordinal || 0)),
    reactionType: String(row.reaction_type || ""),
    targetUrl: String(row.target_url || ""),
    targetChain: String(row.target_chain || ""),
    targetPairId: String(row.target_pair_id || ""),
    quantity: Math.max(0, Number(row.quantity || 0)),
    executorType: normalizeReactionExecutorType(row.executor_type),
    providerName: String(row.provider_name || ""),
    providerOrderId: String(row.provider_order_id || ""),
    providerJobRef: String(row.provider_job_ref || ""),
    status: String(row.status || "queued"),
    attemptCount: Math.max(0, Number(row.attempt_count || 0)),
    metadata: row.metadata_json && typeof row.metadata_json === "object" ? row.metadata_json : {},
    lastError: String(row.last_error || ""),
    startedAt: row.started_at,
    finishedAt: row.finished_at,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function mapReactionSession(row) {
  if (!row) return null;
  return {
    id: String(row.id || ""),
    ownerUserId: Math.max(0, Number(row.owner_user_id || 0)),
    sessionType: String(row.session_type || "provider"),
    providerName: String(row.provider_name || ""),
    label: String(row.label || ""),
    status: String(row.status || "idle"),
    proxyLabel: String(row.proxy_label || ""),
    fingerprint: String(row.fingerprint || ""),
    state: row.state_json && typeof row.state_json === "object" ? row.state_json : {},
    lastError: String(row.last_error || ""),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    lastUsedAt: row.last_used_at,
  };
}

export async function getToolInstanceDetails(userId, toolId) {
  return withTx(async (client) => {
    const { scope, row } = await readToolInstanceForScope(client, userId, toolId, { forUpdate: true });
    let refreshedRow = row;
    try {
      refreshedRow = await refreshToolFundingState(client, row, { emitEvents: false });
    } catch {
      refreshedRow = row;
    }
    const details = await buildToolInstanceDetails(client, refreshedRow);
    details.permissions = {
      role: scope.role,
      isOwner: scope.isOwner,
      canManageFunds: scope.canManageFunds,
    };
    return details;
  });
}

export async function revealToolFundingSecret(userId, toolId) {
  return getToolFundingSecretForOwner(userId, toolId);
}

export async function revealToolManagedWalletSecrets(userId, toolId) {
  return getToolManagedWalletSecretsForOwner(userId, toolId);
}

export async function refreshToolFunding(userId, toolId) {
  return withTx(async (client) => {
    const { scope, row } = await readToolInstanceForScope(client, userId, toolId, { forUpdate: true });
    assertOwnerPermission(scope, "Managers cannot refresh EMBER Funding.");
    const updatedRow = await refreshToolFundingState(client, row, { emitEvents: true });
    return buildToolInstanceDetails(client, updatedRow);
  });
}

export async function updateToolInstance(userId, toolId, payload = {}) {
  return withTx(async (client) => {
    const { scope, row } = await readToolInstanceForScope(client, userId, toolId, { forUpdate: true });
    assertOwnerPermission(scope, "Managers cannot update tool funding configurations.");
    const toolType = normalizeToolType(row.tool_type, TOOL_TYPES.holderPooler);
    let nextConfig = row.config_json && typeof row.config_json === "object" ? row.config_json : {};
    if (toolType === TOOL_TYPES.holderPooler) {
      nextConfig = normalizeHolderPoolerConfig(payload.config || {}, nextConfig);
    } else if (toolType === TOOL_TYPES.reactionManager) {
      nextConfig = normalizeReactionManagerConfig(payload.config || {}, nextConfig);
    } else if (toolType === TOOL_TYPES.smartSell) {
      nextConfig = normalizeSmartSellConfig(payload.config || {}, nextConfig);
    } else if (toolType === TOOL_TYPES.bundleManager) {
      nextConfig = normalizeBundleManagerConfig(payload.config || {}, nextConfig);
      if (nextConfig.walletMode === "imported") {
        await addBundleManagerImportedWallets(client, row, nextConfig.importedWallets || []);
      }
      delete nextConfig.importedWallets;
    } else {
      nextConfig = {
        ...(nextConfig || {}),
        ...(payload.config && typeof payload.config === "object" ? payload.config : {}),
      };
    }

    const title = sanitizeToolTitle(payload.title, row.title || toolTypeLabel(toolType));
    const simpleMode = payload.simpleMode === undefined ? Boolean(row.simple_mode) : Boolean(payload.simpleMode);
    const updatedRes = await client.query(
      `
        UPDATE tool_instances
        SET
          title = $2,
          simple_mode = $3,
          config_json = $4::jsonb,
          last_error = NULL
        WHERE id = $1
        RETURNING *
      `,
      [String(toolId || ""), title, simpleMode, JSON.stringify(nextConfig)]
    );
    await insertToolEvent(
      client,
      Number(row.owner_user_id || 0),
      String(row.id || ""),
      toolType,
      "status",
      `${toolTypeLabel(toolType)} settings updated.`,
      {
        metadata: {
          updatedBy: scope.actorUsername,
          simpleMode,
        },
      }
    );
    return buildToolInstanceDetails(client, updatedRes.rows[0]);
  });
}

export async function armSmartSell(userId, toolId) {
  return withTx(async (client) => {
    const { scope, row } = await readToolInstanceForScope(client, userId, toolId, { forUpdate: true });
    assertOwnerPermission(scope, "Managers cannot arm Smart Sell.");
    const toolType = normalizeToolType(row.tool_type, TOOL_TYPES.smartSell);
    if (toolType !== TOOL_TYPES.smartSell) {
      throw new Error("This action is only available for Smart Sell.");
    }
    if (!row.unlocked_at) {
      throw new Error("Fund and unlock Smart Sell before arming it.");
    }
    const configJson = normalizeSmartSellConfig(row.config_json || {});
    const walletRows = await ensureSmartSellWallets(client, row, configJson);
    const connection = getConnection();
    const fundingSigner = keypairFromBase58(
      decryptDepositSecret(String(row.funding_wallet_secret_key_base58 || ""))
    );
    let balances = await readToolWalletBalances(connection, row.target_mint, walletRows);
    const fundingLamports = await connection.getBalance(fundingSigner.publicKey, "confirmed").catch(() => 0);
    const fundingTokenBalance = row.target_mint
      ? await getOwnerTokenBalanceUi(connection, fundingSigner.publicKey, row.target_mint).catch(() => 0)
      : 0;
    const reserveLamports = toLamports(configJson.walletReserveSol || 0.0015);
    const feeSafetyLamports = getTxFeeSafetyLamports();
    let fundingNeededLamports = 0;
    const fundingRecipients = [];
    for (const wallet of balances) {
      const needed = Math.max(0, reserveLamports - Number(wallet.solLamports || 0));
      fundingNeededLamports += needed;
      if (needed > 0) {
        fundingRecipients.push({
          walletId: wallet.id,
          address: wallet.walletPubkey,
          lamports: needed,
        });
      }
    }
    const spendableFundingLamports = Math.max(
      0,
      Number(fundingLamports || 0) - Math.max(0, Number(row.reserve_lamports || 0)) - feeSafetyLamports
    );
    if (fundingNeededLamports > spendableFundingLamports) {
      throw new Error(
        `Smart Sell needs ${fromLamports(fundingNeededLamports).toFixed(6)} SOL for wallet reserve but only ${fromLamports(
          spendableFundingLamports
        ).toFixed(6)} SOL is spendable after reserve.`
      );
    }

    let txCreated = 0;
    let distributedLamports = 0;
    if (configJson.fundingMode === TOOL_FUNDING_MODES.ember && fundingRecipients.length) {
      const fundingResult = await ensureEmberFundingPlan(client, row, fundingRecipients, {
        staggerMode: true,
        pendingStatus: TOOL_STATUSES.provisioning,
      });
      if (!fundingResult.ready) {
        return buildToolInstanceDetails(client, fundingResult.row);
      }
      balances = await readToolWalletBalances(connection, row.target_mint, walletRows);
    } else {
      for (const wallet of balances) {
        const needed = Math.max(0, reserveLamports - Number(wallet.solLamports || 0));
        if (needed > 0) {
          const sig = await sendSolTransfer(connection, fundingSigner, wallet.walletPubkey, needed);
          if (sig) {
            txCreated += 1;
            distributedLamports += needed;
          }
        }
      }
    }

    const fundingTokenUi = Number(fundingTokenBalance || 0);
    let distributedTokens = 0;
    if (fundingTokenUi > 0.0000001 && balances.length > 0) {
      const perWalletToken = fundingTokenUi / balances.length;
      for (const wallet of balances) {
        if (perWalletToken <= 0.0000001) break;
        const transfer = await transferManagedTokensToOwner(
          connection,
          fundingSigner,
          row.target_mint,
          wallet.walletPubkey,
          perWalletToken
        );
        if (transfer.signature) {
          txCreated += 1;
          distributedTokens += Number(transfer.sentUiAmount || 0);
        }
      }
    }

    const nextState = {
      ...(row.state_json && typeof row.state_json === "object" ? row.state_json : {}),
      smartSellArmedAt: new Date().toISOString(),
      nextWalletIndex: 0,
      processedTradeKeys: Array.isArray(row.state_json?.processedTradeKeys) ? row.state_json.processedTradeKeys.slice(-200) : [],
      pendingSignals: Array.isArray(row.state_json?.pendingSignals) ? row.state_json.pendingSignals : [],
      runtimeFeeLastPaidAt: row.state_json?.runtimeFeeLastPaidAt || null,
      stealthMode: configJson.stealthMode,
      fundingMode: configJson.fundingMode,
    };
    const updatedRes = await client.query(
      `
        UPDATE tool_instances
        SET
          status = $2,
          activated_at = COALESCE(activated_at, NOW()),
          last_run_at = NOW(),
          state_json = $3::jsonb,
          last_error = NULL
        WHERE id = $1
        RETURNING *
      `,
      [String(row.id || ""), TOOL_STATUSES.active, JSON.stringify(nextState)]
    );
    await insertToolEvent(
      client,
      Number(row.owner_user_id || 0),
      String(row.id || ""),
      toolType,
      "status",
      `Smart Sell armed across ${balances.length} wallets (${fromLamports(distributedLamports).toFixed(6)} SOL reserve, ${distributedTokens.toFixed(6)} tokens distributed).`,
      {
        amount: fromLamports(distributedLamports),
        metadata: {
          walletCount: balances.length,
          reserveSol: configJson.walletReserveSol,
          distributedTokens,
          stealthMode: configJson.stealthMode,
          fundingMode: configJson.fundingMode,
        },
      }
    );
    return buildToolInstanceDetails(client, updatedRes.rows[0]);
  });
}

export async function pauseSmartSell(userId, toolId) {
  return withTx(async (client) => {
    const { scope, row } = await readToolInstanceForScope(client, userId, toolId, { forUpdate: true });
    assertOwnerPermission(scope, "Managers cannot pause Smart Sell.");
    const toolType = normalizeToolType(row.tool_type, TOOL_TYPES.smartSell);
    if (toolType !== TOOL_TYPES.smartSell) {
      throw new Error("This action is only available for Smart Sell.");
    }
    const updatedRes = await client.query(
      `
        UPDATE tool_instances
        SET status = $2, last_error = NULL
        WHERE id = $1
        RETURNING *
      `,
      [String(row.id || ""), TOOL_STATUSES.paused]
    );
    await insertToolEvent(
      client,
      Number(row.owner_user_id || 0),
      String(row.id || ""),
      toolType,
      "status",
      "Smart Sell paused.",
      {}
    );
    return buildToolInstanceDetails(client, updatedRes.rows[0]);
  });
}

export async function reclaimSmartSell(userId, toolId) {
  return withTx(async (client) => {
    const { scope, row } = await readToolInstanceForScope(client, userId, toolId, { forUpdate: true });
    assertOwnerPermission(scope, "Managers cannot reclaim Smart Sell wallets.");
    const toolType = normalizeToolType(row.tool_type, TOOL_TYPES.smartSell);
    if (toolType !== TOOL_TYPES.smartSell) {
      throw new Error("This action is only available for Smart Sell.");
    }

    const connection = getConnection();
    const fundingSigner = keypairFromBase58(
      decryptDepositSecret(String(row.funding_wallet_secret_key_base58 || ""))
    );
    const walletRows = await getToolManagedWalletRows(client, row.id, { activeOnly: true });
    let txCreated = 0;
    let reclaimedSolLamports = 0;
    let reclaimedTokens = 0;

    for (const wallet of walletRows) {
      const signer = keypairFromBase58(decryptDepositSecret(String(wallet.secret_key_base58 || "")));
      if (row.target_mint) {
        const tokenBal = await getOwnerTokenBalanceUi(connection, signer.publicKey, row.target_mint).catch(() => 0);
        if (tokenBal > 0.0000001) {
          const transfer = await transferManagedTokensToOwner(
            connection,
            signer,
            row.target_mint,
            fundingSigner.publicKey.toBase58(),
            tokenBal
          );
          if (transfer.signature) {
            txCreated += 1;
            reclaimedTokens += Number(transfer.sentUiAmount || 0);
          }
        }
      }
      const transferResult = await sendAllSolTransfer(connection, signer, fundingSigner.publicKey.toBase58());
      if (transferResult?.signature) {
        txCreated += 1;
        reclaimedSolLamports += Math.max(0, Number(transferResult.sentLamports || 0));
      }
    }

    const nextState = {
      ...(row.state_json && typeof row.state_json === "object" ? row.state_json : {}),
      reclaimedAt: new Date().toISOString(),
      pendingSignals: [],
    };
    const updatedRes = await client.query(
      `
        UPDATE tool_instances
        SET
          status = $2,
          last_run_at = NOW(),
          state_json = $3::jsonb,
          last_error = NULL
        WHERE id = $1
        RETURNING *
      `,
      [String(row.id || ""), TOOL_STATUSES.ready, JSON.stringify(nextState)]
    );
    await insertToolEvent(
      client,
      Number(row.owner_user_id || 0),
      String(row.id || ""),
      toolType,
      "reclaim",
      `Smart Sell reclaimed ${fromLamports(reclaimedSolLamports).toFixed(6)} SOL and ${reclaimedTokens.toFixed(6)} tokens back to funding wallet.`,
      {
        amount: fromLamports(reclaimedSolLamports),
        metadata: {
          txCreated,
          tokenAmount: reclaimedTokens,
        },
      }
    );
    return buildToolInstanceDetails(client, updatedRes.rows[0]);
  });
}

export async function runReactionManagerCampaign(userId, toolId) {
  return withTx(async (client) => {
    const { scope, row } = await readToolInstanceForScope(client, userId, toolId, { forUpdate: true });
    assertOwnerPermission(scope, "Managers cannot run Reaction Manager.");
    const toolType = normalizeToolType(row.tool_type, TOOL_TYPES.reactionManager);
    if (toolType !== TOOL_TYPES.reactionManager) {
      throw new Error("This action is only available for Reaction Manager.");
    }
    if (!row.unlocked_at) {
      throw new Error("Fund and unlock the Reaction Manager before running it.");
    }

    const configJson = normalizeReactionManagerConfig(row.config_json || {});
    const service = REACTION_MANAGER_SERVICE_BY_TYPE[configJson.reactionType];
    const sessionRow = await ensureProviderReactionSession(client, Number(row.owner_user_id || 0));
    const campaignRow = await insertReactionCampaign(client, row, configJson, {
      executorType: REACTION_EXECUTOR_TYPES.provider,
    });
    const jobRow = await insertReactionJob(client, campaignRow, sessionRow, configJson, {
      executorType: REACTION_EXECUTOR_TYPES.provider,
      providerName: REACTION_PROVIDER_NAME,
    });
    const response = await callReactionManagerApi({
      action: "add",
      service,
      link: String(row.target_url || "").trim(),
      quantity: configJson.targetCount,
    });
    const orderId = String(response?.order ?? "");
    if (!orderId) {
      throw new Error("Reaction provider did not return an order id.");
    }

    const deliveredCount = 0;
    const failedCount = 0;
    await client.query(
      `
        UPDATE reaction_campaigns
        SET
          status = 'active',
          provider_order_id = $2,
          delivered_count = $3,
          failed_count = $4,
          state_json = $5::jsonb,
          last_error = NULL,
          started_at = COALESCE(started_at, NOW()),
          last_checked_at = NOW()
        WHERE id = $1
      `,
      [
        String(campaignRow.id || ""),
        orderId,
        deliveredCount,
        failedCount,
        JSON.stringify({
          providerResponse: response,
          providerStatus: "active",
          remains: configJson.targetCount,
        }),
      ]
    );
    await client.query(
      `
        UPDATE reaction_jobs
        SET
          status = 'active',
          provider_order_id = $2,
          provider_job_ref = $2,
          attempt_count = attempt_count + 1,
          metadata_json = $3::jsonb,
          last_error = NULL,
          started_at = COALESCE(started_at, NOW())
        WHERE id = $1
      `,
      [
        String(jobRow.id || ""),
        orderId,
        JSON.stringify({
          providerResponse: response,
          remains: configJson.targetCount,
          deliveredCount,
        }),
      ]
    );

    const nextState = {
      ...(row.state_json && typeof row.state_json === "object" ? row.state_json : {}),
      reactionOrderId: orderId,
      reactionCampaignId: String(campaignRow.id || ""),
      reactionJobId: String(jobRow.id || ""),
      reactionSessionId: String(sessionRow.id || ""),
      reactionStatus: "created",
      reactionRemains: configJson.targetCount,
      reactionTargetCount: configJson.targetCount,
      reactionDeliveredCount: 0,
      reactionReactionType: configJson.reactionType,
      reactionStartedAt: new Date().toISOString(),
      reactionLastProviderResponse: response,
    };
    const updatedRes = await client.query(
      `
        UPDATE tool_instances
        SET
          status = $2,
          activated_at = COALESCE(activated_at, NOW()),
          last_run_at = NOW(),
          state_json = $3::jsonb,
          last_error = NULL
        WHERE id = $1
        RETURNING *
      `,
      [String(row.id || ""), TOOL_STATUSES.active, JSON.stringify(nextState)]
    );
    await insertToolEvent(
      client,
      Number(row.owner_user_id || 0),
      String(row.id || ""),
      toolType,
      "campaign",
      `Reaction Manager placed ${configJson.targetCount} ${configJson.reactionType.replace(/_/g, " ")} reactions.`,
      {
        amount: 0,
        metadata: {
          orderId,
          campaignId: String(campaignRow.id || ""),
          jobId: String(jobRow.id || ""),
          sessionId: String(sessionRow.id || ""),
          targetCount: configJson.targetCount,
          reactionType: configJson.reactionType,
        },
      }
    );
    return buildToolInstanceDetails(client, updatedRes.rows[0]);
  });
}

export async function refreshReactionManagerStatus(userId, toolId) {
  return withTx(async (client) => {
    const { row } = await readToolInstanceForScope(client, userId, toolId, { forUpdate: true });
    const toolType = normalizeToolType(row.tool_type, TOOL_TYPES.reactionManager);
    if (toolType !== TOOL_TYPES.reactionManager) {
      throw new Error("This action is only available for Reaction Manager.");
    }
    const state = row.state_json && typeof row.state_json === "object" ? row.state_json : {};
    const orderId = String(state.reactionOrderId || "").trim();
    if (!orderId) {
      throw new Error("No reaction order is active for this instance.");
    }
    const response = await callReactionManagerApi({
      action: "status",
      order: orderId,
    });
    const remains = Math.max(0, Math.floor(Number(response?.remains ?? state.reactionRemains ?? 0)));
    const statusText = remains <= 0 ? "completed" : "active";
    const targetCount = Math.max(0, Math.floor(Number(state.reactionTargetCount || row.config_json?.targetCount || 0)));
    const deliveredCount = Math.max(0, targetCount - remains);
    const campaignId = String(state.reactionCampaignId || "").trim();
    const jobId = String(state.reactionJobId || "").trim();
    const sessionId = String(state.reactionSessionId || "").trim();
    if (campaignId) {
      await client.query(
        `
          UPDATE reaction_campaigns
          SET
            status = $2,
            delivered_count = $3,
            failed_count = CASE WHEN $2 = 'failed' THEN target_count - $3 ELSE failed_count END,
            state_json = COALESCE(state_json, '{}'::jsonb) || $4::jsonb,
            last_error = NULL,
            last_checked_at = NOW(),
            completed_at = CASE WHEN $2 = 'completed' THEN COALESCE(completed_at, NOW()) ELSE completed_at END
          WHERE id = $1
        `,
        [
          campaignId,
          statusText,
          deliveredCount,
          JSON.stringify({
            providerResponse: response,
            providerStatus: statusText,
            remains,
          }),
        ]
      );
    }
    if (jobId) {
      await client.query(
        `
          UPDATE reaction_jobs
          SET
            status = $2,
            metadata_json = COALESCE(metadata_json, '{}'::jsonb) || $3::jsonb,
            last_error = NULL,
            finished_at = CASE WHEN $2 = 'completed' THEN COALESCE(finished_at, NOW()) ELSE finished_at END
          WHERE id = $1
        `,
        [
          jobId,
          statusText,
          JSON.stringify({
            providerResponse: response,
            remains,
            deliveredCount,
          }),
        ]
      );
    }
    if (sessionId) {
      await client.query(
        `
          UPDATE reaction_sessions
          SET status = $2, last_error = NULL, last_used_at = NOW()
          WHERE id = $1
        `,
        [sessionId, remains <= 0 ? "idle" : "active"]
      );
    }
    const nextState = {
      ...state,
      reactionStatus: statusText,
      reactionRemains: remains,
      reactionDeliveredCount: deliveredCount,
      reactionLastCheckedAt: new Date().toISOString(),
      reactionLastProviderResponse: response,
    };
    const updatedRes = await client.query(
      `
        UPDATE tool_instances
        SET
          status = $2,
          state_json = $3::jsonb,
          last_error = NULL
        WHERE id = $1
        RETURNING *
      `,
      [String(row.id || ""), remains <= 0 ? TOOL_STATUSES.ready : TOOL_STATUSES.active, JSON.stringify(nextState)]
    );
    await insertToolEvent(
      client,
      Number(row.owner_user_id || 0),
      String(row.id || ""),
      toolType,
      "status",
      `Reaction Manager status refreshed: ${remains} reactions remaining.`,
      {
        amount: 0,
        metadata: {
          orderId,
          remains,
          providerStatus: statusText,
        },
      }
    );
    return buildToolInstanceDetails(client, updatedRes.rows[0]);
  });
}

export async function runBundleManagerCampaign(userId, toolId) {
  return withTx(async (client) => {
    const { scope, row } = await readToolInstanceForScope(client, userId, toolId, { forUpdate: true });
    assertOwnerPermission(scope, "Managers cannot run Bundle Manager.");
    const toolType = normalizeToolType(row.tool_type, TOOL_TYPES.bundleManager);
    if (toolType !== TOOL_TYPES.bundleManager) {
      throw new Error("This action is only available for Bundle Manager.");
    }
    if (!row.unlocked_at) {
      throw new Error("Fund and unlock the Bundle Manager before running it.");
    }

    const configJson = normalizeBundleManagerConfig(row.config_json || {});
    const walletRows = await ensureBundleManagerWallets(client, row, configJson);
    if (!walletRows.length) {
      throw new Error("Bundle Manager has no wallets available.");
    }

    const connection = getConnection();
    const fundingSigner = keypairFromBase58(
      decryptDepositSecret(String(row.funding_wallet_secret_key_base58 || ""))
    );
    let balances = await readToolWalletBalances(connection, row.target_mint, walletRows);
    const fundingLamports = await connection.getBalance(fundingSigner.publicKey, "confirmed").catch(() => 0);
    const feeSafetyLamports = getTxFeeSafetyLamports();
    const walletReserveLamports = toLamports(configJson.walletReserveSol || 0.0015);
    const buyLamportsPerWallet = toLamports(configJson.buySolPerWallet || 0);
    const spendableFundingLamports = Math.max(
      0,
      Number(fundingLamports || 0) - Math.max(0, Number(row.reserve_lamports || 0)) - feeSafetyLamports
    );

    const buyCount = Math.max(0, Math.min(balances.length, Math.round((balances.length * Number(configJson.sideBias || 0)) / 100)));
    const runIndex = Math.max(0, Number(row.state_json?.bundleRunCount || 0));
    const ordered = balances
      .slice()
      .sort((a, b) => (a.position > b.position ? 1 : a.position < b.position ? -1 : 0));
    const rotated = ordered.map((_, idx) => ordered[(idx + (runIndex % ordered.length)) % ordered.length]);
    const buyWalletIds = new Set(rotated.slice(0, buyCount).map((wallet) => String(wallet.id || "")));

    let totalFundingNeededLamports = 0;
    const fundingRecipients = [];
    for (const wallet of balances) {
      if (!buyWalletIds.has(String(wallet.id || ""))) continue;
      const needed = Math.max(0, buyLamportsPerWallet + walletReserveLamports - Number(wallet.solLamports || 0));
      totalFundingNeededLamports += needed;
      if (needed > 0) {
        fundingRecipients.push({
          walletId: wallet.id,
          address: wallet.walletPubkey,
          lamports: needed,
        });
      }
    }
    if (totalFundingNeededLamports > spendableFundingLamports) {
      throw new Error(
        `Bundle Manager needs ${fromLamports(totalFundingNeededLamports).toFixed(6)} SOL but only ${fromLamports(
          spendableFundingLamports
        ).toFixed(6)} SOL is spendable after reserve.`
      );
    }

    let buyTx = 0;
    let sellTx = 0;
    let fundedTx = 0;
    let buyVolumeLamports = 0;
    let soldTokens = 0;
    if (configJson.fundingMode === TOOL_FUNDING_MODES.ember && fundingRecipients.length) {
      const fundingResult = await ensureEmberFundingPlan(client, row, fundingRecipients, {
        staggerMode: configJson.fundingStaggerMode !== false,
        pendingStatus: TOOL_STATUSES.provisioning,
      });
      if (!fundingResult.ready) {
        return buildToolInstanceDetails(client, fundingResult.row);
      }
      balances = await readToolWalletBalances(connection, row.target_mint, walletRows);
    }
    for (const wallet of balances) {
      const signer = keypairFromBase58(decryptDepositSecret(String(walletRows.find((rowItem) => String(rowItem.id || "") === String(wallet.id || ""))?.secret_key_base58 || "")));
      const isBuyWallet = buyWalletIds.has(String(wallet.id || ""));
      if (isBuyWallet && buyLamportsPerWallet > 0) {
        const needed = Math.max(0, buyLamportsPerWallet + walletReserveLamports - Number(wallet.solLamports || 0));
        if (needed > 0 && configJson.fundingMode !== TOOL_FUNDING_MODES.ember) {
          const fundingSig = await sendSolTransfer(connection, fundingSigner, signer.publicKey.toBase58(), needed);
          if (fundingSig) {
            fundedTx += 1;
            wallet.solLamports += needed;
            wallet.solBalance = fromLamports(wallet.solLamports);
          }
        }
        if (wallet.solLamports > walletReserveLamports + feeSafetyLamports && buyLamportsPerWallet > 0) {
          const sig = await pumpPortalTrade({
            connection,
            signer,
            mint: row.target_mint,
            action: "buy",
            amount: fromLamports(buyLamportsPerWallet),
            denominatedInSol: true,
            slippage: 10,
            pool: "auto",
          });
          if (sig) {
            buyTx += 1;
            buyVolumeLamports += buyLamportsPerWallet;
          }
        }
        continue;
      }

      const tokenBalance = Number(wallet.tokenBalance || 0);
      if (tokenBalance <= 0.0000001) continue;
      const availableSol = Number(wallet.solLamports || 0);
      if (availableSol <= walletReserveLamports + feeSafetyLamports) continue;
      const sellAmount = Number((tokenBalance * (Number(configJson.sellPctPerWallet || 0) / 100)).toFixed(6));
      if (sellAmount <= 0.0000001) continue;
      const sig = await pumpPortalTrade({
        connection,
        signer,
        mint: row.target_mint,
        action: "sell",
        amount: sellAmount,
        denominatedInSol: false,
        slippage: 10,
        pool: "auto",
      });
      if (sig) {
        sellTx += 1;
        soldTokens += sellAmount;
      }
    }

    const nextState = {
      ...(row.state_json && typeof row.state_json === "object" ? row.state_json : {}),
      bundleRunCount: runIndex + 1,
      lastRunAt: new Date().toISOString(),
      sideBias: configJson.sideBias,
      walletMode: configJson.walletMode,
      stealthMode: configJson.stealthMode,
      fundingMode: configJson.fundingMode,
      lastBuyWallets: buyCount,
      lastSellWallets: Math.max(0, balances.length - buyCount),
    };
    const updatedRes = await client.query(
      `
        UPDATE tool_instances
        SET
          status = $2,
          activated_at = COALESCE(activated_at, NOW()),
          last_run_at = NOW(),
          state_json = $3::jsonb,
          last_error = NULL
        WHERE id = $1
        RETURNING *
      `,
      [String(row.id || ""), TOOL_STATUSES.active, JSON.stringify(nextState)]
    );
    await insertToolEvent(
      client,
      Number(row.owner_user_id || 0),
      String(row.id || ""),
      toolType,
      "campaign",
      `Bundle Manager ran ${buyTx} buys and ${sellTx} sells across ${balances.length} wallets.`,
      {
        amount: fromLamports(buyVolumeLamports),
        metadata: {
          buyTx,
          sellTx,
          fundedTx,
          buySol: fromLamports(buyVolumeLamports),
          soldTokens,
          sideBias: configJson.sideBias,
          walletMode: configJson.walletMode,
          stealthMode: configJson.stealthMode,
          fundingMode: configJson.fundingMode,
        },
      }
    );
    return buildToolInstanceDetails(client, updatedRes.rows[0]);
  });
}

export async function reclaimBundleManager(userId, toolId) {
  return withTx(async (client) => {
    const { scope, row } = await readToolInstanceForScope(client, userId, toolId, { forUpdate: true });
    assertOwnerPermission(scope, "Managers cannot reclaim Bundle Manager wallets.");
    const toolType = normalizeToolType(row.tool_type, TOOL_TYPES.bundleManager);
    if (toolType !== TOOL_TYPES.bundleManager) {
      throw new Error("This action is only available for Bundle Manager.");
    }

    const connection = getConnection();
    const fundingSigner = keypairFromBase58(
      decryptDepositSecret(String(row.funding_wallet_secret_key_base58 || ""))
    );
    const walletRows = await getToolManagedWalletRows(client, row.id, { activeOnly: true });
    let txCreated = 0;
    let reclaimedSolLamports = 0;
    let reclaimedTokens = 0;

    for (const wallet of walletRows) {
      const signer = keypairFromBase58(decryptDepositSecret(String(wallet.secret_key_base58 || "")));
      if (row.target_mint) {
        const tokenBal = await getOwnerTokenBalanceUi(connection, signer.publicKey, row.target_mint).catch(() => 0);
        if (tokenBal > 0.0000001) {
          const transfer = await transferManagedTokensToOwner(
            connection,
            signer,
            row.target_mint,
            fundingSigner.publicKey.toBase58(),
            tokenBal
          );
          if (transfer.signature) {
            txCreated += 1;
            reclaimedTokens += Number(transfer.sentUiAmount || 0);
          }
        }
      }
      const transferResult = await sendAllSolTransfer(connection, signer, fundingSigner.publicKey.toBase58());
      if (transferResult?.signature) {
        txCreated += 1;
        reclaimedSolLamports += Math.max(0, Number(transferResult.sentLamports || 0));
      }
    }

    const nextState = {
      ...(row.state_json && typeof row.state_json === "object" ? row.state_json : {}),
      reclaimedAt: new Date().toISOString(),
    };
    const updatedRes = await client.query(
      `
        UPDATE tool_instances
        SET
          status = $2,
          last_run_at = NOW(),
          state_json = $3::jsonb,
          last_error = NULL
        WHERE id = $1
        RETURNING *
      `,
      [String(row.id || ""), TOOL_STATUSES.ready, JSON.stringify(nextState)]
    );
    await insertToolEvent(
      client,
      Number(row.owner_user_id || 0),
      String(row.id || ""),
      toolType,
      "reclaim",
      `Bundle Manager reclaimed ${fromLamports(reclaimedSolLamports).toFixed(6)} SOL and ${reclaimedTokens.toFixed(6)} tokens back to funding wallet.`,
      {
        amount: fromLamports(reclaimedSolLamports),
        metadata: {
          txCreated,
          tokenAmount: reclaimedTokens,
        },
      }
    );
    return buildToolInstanceDetails(client, updatedRes.rows[0]);
  });
}

export async function runHolderPoolerDistribution(userId, toolId) {
  return withTx(async (client) => {
    const { scope, row } = await readToolInstanceForScope(client, userId, toolId, { forUpdate: true });
    assertOwnerPermission(scope, "Managers cannot run Holder Pooler.");
    const toolType = normalizeToolType(row.tool_type, TOOL_TYPES.holderPooler);
    if (toolType !== TOOL_TYPES.holderPooler) {
      throw new Error("This action is only available for Holder Pooler.");
    }
    if (!row.unlocked_at) {
      throw new Error("Fund and unlock the Holder Pooler before running it.");
    }

    const configJson = normalizeHolderPoolerConfig(row.config_json || {});
    const wallets = await ensureHolderPoolerWallets(client, row, configJson);
    const connection = getConnection();
    const fundingSigner = keypairFromBase58(
      decryptDepositSecret(String(row.funding_wallet_secret_key_base58 || ""))
    );
    const balances = await readToolWalletBalances(connection, row.target_mint, wallets);
    const fundingLamports = await connection.getBalance(fundingSigner.publicKey, "confirmed");
    const fundingTokenBalance = row.target_mint
      ? await getOwnerTokenBalanceUi(connection, fundingSigner.publicKey, row.target_mint).catch(() => 0)
      : 0;
    const feeSafetyLamports = getTxFeeSafetyLamports();
    const desiredTokenPerWallet = Number(configJson.tokenAmountPerWallet || 0);

    let tokenNeeded = 0;
    for (const wallet of balances) {
      if (desiredTokenPerWallet > 0) {
        tokenNeeded += Math.max(0, desiredTokenPerWallet - Number(wallet.tokenBalance || 0));
      }
    }
    if (tokenNeeded > Number(fundingTokenBalance || 0) + 0.0000001) {
      throw new Error(
        `Funding wallet needs ${tokenNeeded.toFixed(6)} tokens but only ${Number(
          fundingTokenBalance || 0
        ).toFixed(6)} are available.`
      );
    }

    let txCreated = 0;
    let walletsTouched = 0;
    let totalTokensDistributed = 0;
    for (const wallet of balances) {
      if (desiredTokenPerWallet > 0) {
        const tokenShortfall = Math.max(0, desiredTokenPerWallet - Number(wallet.tokenBalance || 0));
        if (tokenShortfall > 0.0000001) {
          const transfer = await transferManagedTokensToOwner(
            connection,
            fundingSigner,
            row.target_mint,
            wallet.walletPubkey,
            tokenShortfall
          );
          if (transfer.signature) {
            txCreated += 1;
            walletsTouched += 1;
            totalTokensDistributed += Number(transfer.sentUiAmount || 0);
          }
        }
      }
    }

    const nextState = {
      ...(row.state_json && typeof row.state_json === "object" ? row.state_json : {}),
      distributedAt: new Date().toISOString(),
      distributedWallets: balances.length,
      walletMode: configJson.walletMode,
      stealthMode: configJson.stealthMode,
      irreversibleDistribution: true,
    };
    const updatedRes = await client.query(
      `
        UPDATE tool_instances
        SET
          status = $2,
          activated_at = COALESCE(activated_at, NOW()),
          last_run_at = NOW(),
          state_json = $3::jsonb,
          last_error = NULL
        WHERE id = $1
        RETURNING *
      `,
      [String(row.id || ""), TOOL_STATUSES.active, JSON.stringify(nextState)]
    );
    await insertToolEvent(
      client,
      Number(row.owner_user_id || 0),
      String(row.id || ""),
      toolType,
      "distribution",
      `Holder Pooler distributed ${totalTokensDistributed.toFixed(6)} tokens across ${balances.length} holder wallets.`,
      {
        amount: totalTokensDistributed,
        metadata: {
          txCreated,
          walletsTouched,
          tokenAmount: totalTokensDistributed,
          stealthMode: configJson.stealthMode,
          irreversibleDistribution: true,
        },
      }
    );
    return buildToolInstanceDetails(client, updatedRes.rows[0]);
  });
}

export async function reclaimHolderPooler(userId, toolId) {
  throw new Error("Holder Pooler distributions are permanent and cannot be reclaimed.");
}

export async function sellHolderPooler(userId, toolId) {
  throw new Error("Holder Pooler distributions are permanent and do not support sell-back.");
}

export async function getReferralAccountSummary(userId) {
  const scope = await resolveUserAccessScopeFromPool(userId);
  const [profileRes, referredRes, eventsRes, balanceRes] = await Promise.all([
    pool.query(
      `
        SELECT
          id,
          username,
          COALESCE(referral_code, '') AS referral_code,
          COALESCE(is_og, FALSE) AS is_og,
          referrer_user_id
        FROM users
        WHERE id = $1
        LIMIT 1
      `,
      [scope.ownerUserId]
    ),
    pool.query(
      `
        SELECT
          u.id,
          u.username,
          u.created_at,
          COALESCE(rb.total_earned_lamports, 0) AS total_earned_lamports,
          COALESCE(SUM(re.referral_fee_lamports), 0) AS earned_from_user_lamports
        FROM users u
        LEFT JOIN referral_events re
          ON re.user_id = u.id
         AND re.referrer_user_id = $1
        LEFT JOIN referral_balances rb
          ON rb.referrer_user_id = $1
        WHERE u.referrer_user_id = $1
        GROUP BY u.id, u.username, u.created_at, rb.total_earned_lamports
        ORDER BY u.created_at DESC
      `,
      [scope.ownerUserId]
    ),
    pool.query(
      `
        SELECT
          re.id,
          re.user_id,
          u.username,
          re.token_id,
          re.token_symbol,
          re.module_type,
          re.referral_fee_lamports,
          re.gross_fee_lamports,
          re.tx,
          re.created_at
        FROM referral_events re
        JOIN users u ON u.id = re.user_id
        WHERE re.referrer_user_id = $1
        ORDER BY re.created_at DESC
        LIMIT 100
      `,
      [scope.ownerUserId]
    ),
    pool.query(
      `
        SELECT
          COALESCE(total_earned_lamports, 0) AS total_earned_lamports,
          COALESCE(pending_lamports, 0) AS pending_lamports,
          COALESCE(claimed_lamports, 0) AS claimed_lamports
        FROM referral_balances
        WHERE referrer_user_id = $1
        LIMIT 1
      `,
      [scope.ownerUserId]
    ),
  ]);
  const referralSignalMap = await getReferralSignalMap(
    pool,
    referredRes.rows.map((row) => ({
      userId: Number(row.id || 0),
      referrerUserId: scope.ownerUserId,
    }))
  );

  const profile = profileRes.rows[0] || {};
  const balance = balanceRes.rows[0] || {};
  const currentReferralCode = String(profile.referral_code || "");
  const defaultReferralCode = defaultReferralCodeForUserId(scope.ownerUserId);
  return {
    ownerUserId: scope.ownerUserId,
    ownerUsername: scope.ownerUsername,
    role: scope.role,
    canClaim: Boolean(scope.canManageFunds),
    isOg: Boolean(profile.is_og),
    referralCode: currentReferralCode,
    defaultReferralCode,
    canCustomizeReferralCode: Boolean(scope.canManageFunds) && currentReferralCode === defaultReferralCode,
    referredByUserId:
      profile.referrer_user_id == null ? null : Math.max(0, Math.floor(Number(profile.referrer_user_id || 0))),
    totals: {
      totalEarnedSol: fromLamports(balance.total_earned_lamports),
      pendingSol: fromLamports(balance.pending_lamports),
      claimedSol: fromLamports(balance.claimed_lamports),
    },
    referredUsers: referredRes.rows.map((row) => ({
      ...(function () {
        const signals =
          referralSignalMap.get(`${Number(row.id || 0)}:${scope.ownerUserId}`) || {};
        const risk = describeReferralRisk(signals, REFERRAL_MIN_GROSS_FEE_LAMPORTS);
        return {
          referralStatus: risk.statusLabel,
          referralFlags: risk.flags,
          cooldownEndsAt: signals.cooldownEndsAt || null,
          hasWalletOverlap: Boolean(signals.walletOverlap),
          hasIpOverlap: Boolean(signals.ipOverlap),
        };
      })(),
      userId: Number(row.id || 0),
      username: String(row.username || ""),
      createdAt: row.created_at,
      earnedSol: fromLamports(row.earned_from_user_lamports),
    })),
    events: eventsRes.rows.map((row) => ({
      id: Number(row.id || 0),
      userId: Number(row.user_id || 0),
      username: String(row.username || ""),
      tokenId: row.token_id ? String(row.token_id) : null,
      tokenSymbol: String(row.token_symbol || "UNKNOWN"),
      moduleType: String(row.module_type || ""),
      referralSol: fromLamports(row.referral_fee_lamports),
      grossFeeSol: fromLamports(row.gross_fee_lamports),
      tx: row.tx ? String(row.tx) : null,
      createdAt: row.created_at,
    })),
  };
}

export async function claimReferralEarnings(userId, payload = {}) {
  return withTx(async (client) => {
    const scope = await resolveUserAccessScope(client, userId);
    assertOwnerPermission(scope, "Managers cannot claim referral earnings.");

    const destination = normalizePubkeyString(payload.destinationWallet || payload.destination || "");
    if (!destination) {
      throw new Error("A valid Solana wallet address is required.");
    }

    const balanceRes = await client.query(
      `
        SELECT pending_lamports, total_earned_lamports, claimed_lamports
        FROM referral_balances
        WHERE referrer_user_id = $1
        LIMIT 1
        FOR UPDATE
      `,
      [scope.ownerUserId]
    );
    if (!balanceRes.rowCount) {
      throw new Error("No referral earnings are available.");
    }

    const pendingLamports = Math.max(0, Math.floor(Number(balanceRes.rows[0].pending_lamports || 0)));
    if (pendingLamports <= 0) {
      throw new Error("No claimable referral earnings are available.");
    }
    if (!config.treasuryWalletPrivateKey) {
      throw new Error("Treasury signer is not configured.");
    }

    const connection = getConnection();
    const treasurySigner = Keypair.fromSecretKey(config.treasuryWalletPrivateKey);
    const treasuryBalance = await connection.getBalance(treasurySigner.publicKey, "confirmed");
    const maxSpendable = Math.max(0, treasuryBalance - getTxFeeSafetyLamports());
    if (maxSpendable < pendingLamports) {
      throw new Error("Treasury balance is too low to claim referral earnings right now.");
    }

    const sig = await sendSolTransfer(connection, treasurySigner, destination, pendingLamports);
    await client.query(
      `
        UPDATE referral_balances
        SET
          pending_lamports = pending_lamports - $2,
          claimed_lamports = claimed_lamports + $2,
          updated_at = NOW()
        WHERE referrer_user_id = $1
      `,
      [scope.ownerUserId, pendingLamports]
    );
    await client.query(
      `
        INSERT INTO referral_claims (
          referrer_user_id,
          destination_wallet,
          amount_lamports,
          tx
        )
        VALUES ($1, $2, $3, $4)
      `,
      [scope.ownerUserId, destination, pendingLamports, sig]
    );
    return {
      ok: true,
      signature: sig,
      claimedSol: fromLamports(pendingLamports),
      destinationWallet: destination,
    };
  });
}

export async function updateOwnReferralCode(userId, nextReferralCodeInput = "") {
  return withTx(async (client) => {
    const scope = await resolveUserAccessScope(client, userId);
    assertOwnerPermission(scope, "Managers cannot change referral codes.");
    const nextReferralCode = normalizeReferralCode(nextReferralCodeInput);
    if (!nextReferralCode) {
      throw new Error("A referral code is required.");
    }

    const profileRes = await client.query(
      `
        SELECT id, COALESCE(referral_code, '') AS referral_code
        FROM users
        WHERE id = $1
        LIMIT 1
        FOR UPDATE
      `,
      [scope.ownerUserId]
    );
    if (!profileRes.rowCount) throw new Error("User not found.");
    const row = profileRes.rows[0];
    const currentReferralCode = String(row.referral_code || "");
    const defaultReferralCode = defaultReferralCodeForUserId(scope.ownerUserId);
    if (currentReferralCode !== defaultReferralCode) {
      throw new Error("Referral code can only be customized once.");
    }
    if (nextReferralCode === currentReferralCode) {
      return {
        ok: true,
        referralCode: currentReferralCode,
        canCustomizeReferralCode: false,
      };
    }

    const existingRes = await client.query(
      `
        SELECT id
        FROM users
        WHERE referral_code = $1 AND id <> $2
        LIMIT 1
      `,
      [nextReferralCode, scope.ownerUserId]
    );
    if (existingRes.rowCount) {
      throw new Error("Referral code is already taken.");
    }

    await client.query(
      `UPDATE users SET referral_code = $2 WHERE id = $1`,
      [scope.ownerUserId, nextReferralCode]
    );

    return {
      ok: true,
      referralCode: nextReferralCode,
      canCustomizeReferralCode: false,
    };
  });
}

export async function getAdminOverview(userId) {
  const scope = await resolveUserAccessScopeFromPool(userId);
  assertAdminPermission(scope, "Only the admin account can access admin controls.");

  const [settings, usersRes, tokensRes, referralsRes, countsRes, errorRes, auditRes, depositPool, treasuryBalanceLamports, devBalanceLamports] = await Promise.all([
    getProtocolSettings(pool),
    pool.query(
      `
        SELECT
          u.id,
          u.username,
          COALESCE(u.is_admin, FALSE) AS is_admin,
          COALESCE(u.is_og, FALSE) AS is_og,
          COALESCE(u.is_banned, FALSE) AS is_banned,
          COALESCE(u.banned_reason, '') AS banned_reason,
          COALESCE(u.referral_code, '') AS referral_code,
          u.referrer_user_id,
          ref.username AS referrer_username,
          COALESCE(ref.referral_code, '') AS referrer_code,
          COALESCE(rb.total_earned_lamports, 0) AS total_earned_lamports,
          COALESCE(rb.pending_lamports, 0) AS pending_lamports,
          COUNT(t.id)::bigint AS token_count,
          MAX(u.created_at) AS created_at
        FROM users u
        LEFT JOIN users ref ON ref.id = u.referrer_user_id
        LEFT JOIN referral_balances rb ON rb.referrer_user_id = u.id
        LEFT JOIN tokens t ON t.user_id = u.id
        WHERE u.username <> $1
        GROUP BY u.id, u.username, u.is_admin, u.is_og, u.banned_reason, u.referral_code, u.referrer_user_id, ref.username, ref.referral_code, rb.total_earned_lamports, rb.pending_lamports
        ORDER BY u.created_at DESC, u.id DESC
        LIMIT 250
      `,
      [PROTOCOL_SYSTEM_USERNAME]
    ),
    pool.query(
      `
        SELECT
          t.id,
          t.user_id,
          u.username,
          t.symbol,
          t.mint,
          t.selected_bot,
          t.active,
          t.disconnected,
          COALESCE(t.hidden_from_public, FALSE) AS hidden_from_public,
          COALESCE(t.pinned_rank, 0) AS pinned_rank,
          t.deposit,
          t.created_at
        FROM tokens t
        JOIN users u ON u.id = t.user_id
        ORDER BY t.created_at DESC
        LIMIT 250
      `
    ),
    pool.query(
      `
        SELECT
          re.referrer_user_id,
          u.username,
          COALESCE(SUM(re.referral_fee_lamports), 0) AS earned_lamports,
          COUNT(*)::bigint AS event_count
        FROM referral_events re
        JOIN users u ON u.id = re.referrer_user_id
        GROUP BY re.referrer_user_id, u.username
        ORDER BY earned_lamports DESC
        LIMIT 100
      `
    ),
    pool.query(
      `
        SELECT
          (SELECT COUNT(*)::bigint FROM users WHERE username <> $1) AS user_count,
          (SELECT COUNT(*)::bigint FROM users WHERE username <> $1 AND COALESCE(is_banned, FALSE) = TRUE) AS banned_user_count,
          (SELECT COUNT(*)::bigint FROM tokens) AS token_count,
          (SELECT COUNT(*)::bigint FROM tokens WHERE COALESCE(disconnected, FALSE) = TRUE) AS archived_token_count,
          (SELECT COUNT(*)::bigint FROM tokens WHERE COALESCE(hidden_from_public, FALSE) = TRUE) AS hidden_token_count,
          (SELECT COUNT(*)::bigint FROM tokens WHERE COALESCE(active, FALSE) = TRUE AND COALESCE(disconnected, FALSE) = FALSE) AS active_token_count,
          (SELECT COUNT(*)::bigint FROM bot_modules WHERE COALESCE(enabled, FALSE) = TRUE) AS enabled_module_count
      `,
      [PROTOCOL_SYSTEM_USERNAME]
    ),
    pool.query(
      `
        SELECT COUNT(*)::bigint AS error_count_24h
        FROM token_events
        WHERE event_type = 'error'
          AND created_at >= NOW() - INTERVAL '24 hours'
      `
    ),
    pool.query(
      `
        SELECT
          aal.id,
          aal.action,
          aal.target_token_id,
          aal.details_json,
          aal.created_at,
          actor.username AS actor_username,
          target.username AS target_username
        FROM admin_audit_log aal
        LEFT JOIN users actor ON actor.id = aal.actor_user_id
        LEFT JOIN users target ON target.id = aal.target_user_id
        ORDER BY aal.created_at DESC
        LIMIT 80
      `
    ).catch(() => ({ rows: [] })),
    getDepositPoolStatus().catch(() => ({ byPrefix: [], summary: "Pool status unavailable", total: 0, target: 0 })),
    (async () => {
      if (!config.treasuryWalletPublicKey) return 0;
      try {
        return await getConnection().getBalance(new PublicKey(config.treasuryWalletPublicKey), "confirmed");
      } catch {
        return 0;
      }
    })(),
    (async () => {
      if (!config.devWalletPublicKey) return 0;
      try {
        return await getConnection().getBalance(new PublicKey(config.devWalletPublicKey), "confirmed");
      } catch {
        return 0;
      }
    })(),
  ]);

  const counts = countsRes.rows[0] || {};
  const errors24h = errorRes.rows[0] || {};
  const adminReferralSignalMap = await getReferralSignalMap(
    pool,
    usersRes.rows.map((row) => ({
      userId: Number(row.id || 0),
      referrerUserId: Math.max(0, Math.floor(Number(row.referrer_user_id || 0))),
    }))
  );
  return {
    settings,
    health: {
      depositPoolSummary: String(depositPool?.summary || ""),
      depositPoolByPrefix: Array.isArray(depositPool?.byPrefix) ? depositPool.byPrefix : [],
      userCount: Number(counts.user_count || 0),
      bannedUserCount: Number(counts.banned_user_count || 0),
      tokenCount: Number(counts.token_count || 0),
      archivedTokenCount: Number(counts.archived_token_count || 0),
      hiddenTokenCount: Number(counts.hidden_token_count || 0),
      activeTokenCount: Number(counts.active_token_count || 0),
      enabledModuleCount: Number(counts.enabled_module_count || 0),
      errorCount24h: Number(errors24h.error_count_24h || 0),
      treasurySol: fromLamports(treasuryBalanceLamports),
      devWalletSol: fromLamports(devBalanceLamports),
    },
    users: usersRes.rows.map((row) => ({
      ...(function () {
        const referrerUserId = Math.max(0, Math.floor(Number(row.referrer_user_id || 0)));
        const signals = referrerUserId
          ? adminReferralSignalMap.get(`${Number(row.id || 0)}:${referrerUserId}`) || {}
          : {};
        const risk = describeReferralRisk(signals, REFERRAL_MIN_GROSS_FEE_LAMPORTS);
        return {
          referralStatus: referrerUserId ? risk.statusLabel : "",
          referralFlags: referrerUserId ? risk.flags : [],
          hasWalletOverlap: Boolean(signals.walletOverlap),
          hasIpOverlap: Boolean(signals.ipOverlap),
          cooldownEndsAt: signals.cooldownEndsAt || null,
        };
      })(),
      id: Number(row.id || 0),
      username: String(row.username || ""),
      isAdmin: Boolean(row.is_admin),
      isOg: Boolean(row.is_og),
      isBanned: Boolean(row.is_banned),
      bannedReason: String(row.banned_reason || ""),
      referralCode: String(row.referral_code || ""),
      referrerUsername: String(row.referrer_username || ""),
      referrerCode: String(row.referrer_code || ""),
      totalReferralEarnedSol: fromLamports(row.total_earned_lamports),
      pendingReferralSol: fromLamports(row.pending_lamports),
      tokenCount: Number(row.token_count || 0),
      createdAt: row.created_at,
    })),
    tokens: tokensRes.rows.map((row) => ({
      id: String(row.id || ""),
      userId: Number(row.user_id || 0),
      username: String(row.username || ""),
      symbol: String(row.symbol || ""),
      mint: String(row.mint || ""),
      selectedBot: String(row.selected_bot || "burn"),
      active: Boolean(row.active),
      disconnected: Boolean(row.disconnected),
      hiddenFromPublic: Boolean(row.hidden_from_public),
      pinnedRank: Number(row.pinned_rank || 0),
      deposit: String(row.deposit || ""),
      createdAt: row.created_at,
    })),
    referralLeaders: referralsRes.rows.map((row) => ({
      userId: Number(row.referrer_user_id || 0),
      username: String(row.username || ""),
      earnedSol: fromLamports(row.earned_lamports),
      eventCount: Number(row.event_count || 0),
    })),
    auditTrail: auditRes.rows.map((row) => ({
      id: Number(row.id || 0),
      action: String(row.action || ""),
      actorUsername: String(row.actor_username || ""),
      targetUsername: String(row.target_username || ""),
      targetTokenId: row.target_token_id ? String(row.target_token_id) : null,
      details: row.details_json && typeof row.details_json === "object" ? row.details_json : {},
      createdAt: row.created_at,
    })),
  };
}

export async function updateAdminProtocolSettings(userId, payload = {}) {
  return withTx(async (client) => {
    const scope = await resolveUserAccessScope(client, userId);
    assertAdminPermission(scope, "Only the admin account can update protocol settings.");
    const current = await getProtocolSettings(client);
    const next = {
      defaultFeeBps:
        payload.defaultFeeBps === undefined ? current.defaultFeeBps : Math.max(0, Math.min(5000, Math.floor(Number(payload.defaultFeeBps || current.defaultFeeBps)))),
      defaultTreasuryBps:
        payload.defaultTreasuryBps === undefined ? current.defaultTreasuryBps : Math.max(0, Math.min(5000, Math.floor(Number(payload.defaultTreasuryBps || current.defaultTreasuryBps)))),
      defaultBurnBps:
        payload.defaultBurnBps === undefined ? current.defaultBurnBps : Math.max(0, Math.min(5000, Math.floor(Number(payload.defaultBurnBps || current.defaultBurnBps)))),
      referredTreasuryBps:
        payload.referredTreasuryBps === undefined ? current.referredTreasuryBps : Math.max(0, Math.min(5000, Math.floor(Number(payload.referredTreasuryBps || current.referredTreasuryBps)))),
      referredBurnBps:
        payload.referredBurnBps === undefined ? current.referredBurnBps : Math.max(0, Math.min(5000, Math.floor(Number(payload.referredBurnBps || current.referredBurnBps)))),
      referredReferralBps:
        payload.referredReferralBps === undefined ? current.referredReferralBps : Math.max(0, Math.min(5000, Math.floor(Number(payload.referredReferralBps || current.referredReferralBps)))),
      personalBotMode:
        payload.personalBotMode === undefined ? current.personalBotMode : normalizeModuleType(payload.personalBotMode, current.personalBotMode),
      personalBotEnabled:
        payload.personalBotEnabled === undefined ? current.personalBotEnabled : Boolean(payload.personalBotEnabled),
      personalBotIntensity:
        payload.personalBotIntensity === undefined ? current.personalBotIntensity : Math.max(0, Math.min(100, Math.floor(Number(payload.personalBotIntensity ?? current.personalBotIntensity)))),
      personalBotSafety:
        payload.personalBotSafety === undefined ? current.personalBotSafety : Math.max(0, Math.min(100, Math.floor(Number(payload.personalBotSafety ?? current.personalBotSafety)))),
      toolFeeHolderPoolerLamports:
        payload.toolFeeHolderPoolerLamports === undefined
          ? current.toolFeeHolderPoolerLamports
          : Math.max(0, Math.floor(Number(payload.toolFeeHolderPoolerLamports ?? current.toolFeeHolderPoolerLamports))),
      toolFeeReactionManagerLamports:
        payload.toolFeeReactionManagerLamports === undefined
          ? current.toolFeeReactionManagerLamports
          : Math.max(0, Math.floor(Number(payload.toolFeeReactionManagerLamports ?? current.toolFeeReactionManagerLamports))),
      toolFeeSmartSellLamports:
        payload.toolFeeSmartSellLamports === undefined
          ? current.toolFeeSmartSellLamports
          : Math.max(0, Math.floor(Number(payload.toolFeeSmartSellLamports ?? current.toolFeeSmartSellLamports))),
      toolFeeSmartSellRuntimeLamports:
        payload.toolFeeSmartSellRuntimeLamports === undefined
          ? current.toolFeeSmartSellRuntimeLamports
          : Math.max(0, Math.floor(Number(payload.toolFeeSmartSellRuntimeLamports ?? current.toolFeeSmartSellRuntimeLamports))),
      toolFeeSmartSellRuntimeWindowHours:
        payload.toolFeeSmartSellRuntimeWindowHours === undefined
          ? current.toolFeeSmartSellRuntimeWindowHours
          : Math.max(0, Math.floor(Number(payload.toolFeeSmartSellRuntimeWindowHours ?? current.toolFeeSmartSellRuntimeWindowHours))),
      toolFeeBundleManagerLamports:
        payload.toolFeeBundleManagerLamports === undefined
          ? current.toolFeeBundleManagerLamports
          : Math.max(0, Math.floor(Number(payload.toolFeeBundleManagerLamports ?? current.toolFeeBundleManagerLamports))),
      telegramTradeBuyFeeLamports:
        payload.telegramTradeBuyFeeLamports === undefined
          ? current.telegramTradeBuyFeeLamports
          : Math.max(0, Math.floor(Number(payload.telegramTradeBuyFeeLamports ?? current.telegramTradeBuyFeeLamports))),
      telegramTradeSellFeeLamports:
        payload.telegramTradeSellFeeLamports === undefined
          ? current.telegramTradeSellFeeLamports
          : Math.max(0, Math.floor(Number(payload.telegramTradeSellFeeLamports ?? current.telegramTradeSellFeeLamports))),
      maintenanceEnabled:
        payload.maintenanceEnabled === undefined ? current.maintenanceEnabled : Boolean(payload.maintenanceEnabled),
      maintenanceMode:
        payload.maintenanceMode === undefined
          ? current.maintenanceMode
          : ["soft", "hard"].includes(String(payload.maintenanceMode || "").trim().toLowerCase())
            ? String(payload.maintenanceMode).trim().toLowerCase()
            : current.maintenanceMode,
      maintenanceMessage:
        payload.maintenanceMessage === undefined
          ? current.maintenanceMessage
          : String(payload.maintenanceMessage || "").trim().slice(0, 240),
    };

    await client.query(
      `
        UPDATE protocol_settings
        SET
          default_fee_bps = $2,
          default_treasury_bps = $3,
          default_burn_bps = $4,
          referred_treasury_bps = $5,
          referred_burn_bps = $6,
          referred_referral_bps = $7,
          personal_bot_mode = $8,
          personal_bot_enabled = $9,
          personal_bot_intensity = $10,
          personal_bot_safety = $11,
          tool_fee_holder_pooler_lamports = $12,
          tool_fee_reaction_manager_lamports = $13,
          tool_fee_smart_sell_lamports = $14,
          tool_fee_smart_sell_runtime_lamports = $15,
          tool_fee_smart_sell_runtime_window_hours = $16,
          tool_fee_bundle_manager_lamports = $17,
          telegram_trade_buy_fee_lamports = $18,
          telegram_trade_sell_fee_lamports = $19,
          maintenance_enabled = $20,
          maintenance_mode = $21,
          maintenance_message = $22,
          updated_at = NOW()
        WHERE id = $1
      `,
      [
        1,
        next.defaultFeeBps,
        next.defaultTreasuryBps,
        next.defaultBurnBps,
        next.referredTreasuryBps,
        next.referredBurnBps,
        next.referredReferralBps,
        next.personalBotMode,
        next.personalBotEnabled,
        next.personalBotIntensity,
        next.personalBotSafety,
        next.toolFeeHolderPoolerLamports,
        next.toolFeeReactionManagerLamports,
        next.toolFeeSmartSellLamports,
        next.toolFeeSmartSellRuntimeLamports,
        next.toolFeeSmartSellRuntimeWindowHours,
        next.toolFeeBundleManagerLamports,
        next.telegramTradeBuyFeeLamports,
        next.telegramTradeSellFeeLamports,
        next.maintenanceEnabled,
        next.maintenanceMode,
        next.maintenanceMessage,
      ]
    );

    await recordAdminAudit(client, {
      actorUserId: scope.actorUserId,
      action: "protocol_settings_updated",
      details: next,
    });

    return getProtocolSettings(client);
  });
}

export async function adminSetUserOg(userId, targetUserId, enabled) {
  return withTx(async (client) => {
    const scope = await resolveUserAccessScope(client, userId);
    assertAdminPermission(scope, "Only the admin account can update OG accounts.");
    const targetId = Math.max(0, Math.floor(Number(targetUserId || 0)));
    if (!targetId) throw new Error("User id is required.");
    await client.query(
      `UPDATE users SET is_og = $2 WHERE id = $1`,
      [targetId, Boolean(enabled)]
    );
    await recordAdminAudit(client, {
      actorUserId: scope.actorUserId,
      targetUserId: targetId,
      action: Boolean(enabled) ? "user_marked_og" : "user_removed_og",
      details: { enabled: Boolean(enabled) },
    });
    return { ok: true };
  });
}

export async function adminSetUserReferrer(userId, targetUserId, referralCodeInput = "") {
  return withTx(async (client) => {
    const scope = await resolveUserAccessScope(client, userId);
    assertAdminPermission(scope, "Only the admin account can update referrals.");
    const targetId = Math.max(0, Math.floor(Number(targetUserId || 0)));
    if (!targetId) throw new Error("User id is required.");

    const referralCode = normalizeReferralCode(referralCodeInput);
    let referrerId = null;
    if (referralCode) {
      const refRes = await client.query(
        `SELECT id, COALESCE(is_og, FALSE) AS is_og FROM users WHERE referral_code = $1 LIMIT 1`,
        [referralCode]
      );
      if (!refRes.rowCount) throw new Error("Referral code not found.");
      if (Boolean(refRes.rows[0].is_og)) {
        throw new Error("OG accounts cannot be used as referrers.");
      }
      referrerId = Math.max(0, Math.floor(Number(refRes.rows[0].id || 0))) || null;
      if (referrerId === targetId) {
        throw new Error("Users cannot refer themselves.");
      }
    }

    await client.query(
      `UPDATE users SET referrer_user_id = $2 WHERE id = $1`,
      [targetId, referrerId]
    );
    await recordAdminAudit(client, {
      actorUserId: scope.actorUserId,
      targetUserId: targetId,
      action: "user_referrer_updated",
      details: { referralCode, referrerUserId: referrerId },
    });
    return { ok: true };
  });
}

export async function adminSetUserBan(userId, targetUserId, payload = {}) {
  return withTx(async (client) => {
    const scope = await resolveUserAccessScope(client, userId);
    assertAdminPermission(scope, "Only the admin account can manage suspensions.");
    const targetId = Math.max(0, Math.floor(Number(targetUserId || 0)));
    if (!targetId) throw new Error("User id is required.");
    if (targetId === scope.actorUserId) {
      throw new Error("Admin cannot suspend the active admin account.");
    }
    const enabled = Boolean(payload?.enabled);
    const reason = String(payload?.reason || "").trim().slice(0, 300);
    if (enabled && !reason) {
      throw new Error("A suspension reason is required.");
    }
    await client.query(
      `
        UPDATE users
        SET
          is_banned = $2,
          banned_reason = CASE WHEN $2 THEN $3 ELSE NULL END,
          banned_at = CASE WHEN $2 THEN NOW() ELSE NULL END,
          banned_by_user_id = CASE WHEN $2 THEN $4 ELSE NULL END
        WHERE id = $1
      `,
      [targetId, enabled, reason, scope.actorUserId]
    );
    if (enabled) {
      await client.query(
        `
          UPDATE tokens
          SET active = FALSE
          WHERE user_id = $1
        `,
        [targetId]
      );
      await client.query(
        `
          UPDATE bot_modules
          SET enabled = FALSE
          WHERE user_id = $1
        `,
        [targetId]
      );
      await client.query(`DELETE FROM sessions WHERE user_id = $1`, [targetId]);
      await client.query(
        `
          DELETE FROM sessions
          WHERE user_id IN (
            SELECT grantee_user_id
            FROM user_access_grants
            WHERE owner_user_id = $1
          )
        `,
        [targetId]
      );
    }
    await recordAdminAudit(client, {
      actorUserId: scope.actorUserId,
      targetUserId: targetId,
      action: enabled ? "user_suspended" : "user_unsuspended",
      details: { enabled, reason },
    });
    return { ok: true };
  });
}

export async function adminUpdateTokenPublicState(userId, tokenId, payload = {}) {
  return withTx(async (client) => {
    const scope = await resolveUserAccessScope(client, userId);
    assertAdminPermission(scope, "Only the admin account can update public token controls.");
    const tokenIdText = String(tokenId || "").trim();
    if (!tokenIdText) throw new Error("Token id is required.");
    const hiddenFromPublic = Boolean(payload.hiddenFromPublic);
    const pinnedRank = Math.max(0, Math.min(999, Math.floor(Number(payload.pinnedRank || 0))));
    await client.query(
      `
        UPDATE tokens
        SET
          hidden_from_public = $2,
          pinned_rank = $3
        WHERE id = $1
      `,
      [tokenIdText, hiddenFromPublic, pinnedRank]
    );
    await recordAdminAudit(client, {
      actorUserId: scope.actorUserId,
      targetTokenId: tokenIdText,
      action: "token_public_state_updated",
      details: { hiddenFromPublic, pinnedRank },
    });
    return { ok: true };
  });
}

export async function adminForcePauseToken(userId, tokenId) {
  return withTx(async (client) => {
    const scope = await resolveUserAccessScope(client, userId);
    assertAdminPermission(scope, "Only the admin account can force pause bots.");
    const tokenIdText = String(tokenId || "").trim();
    if (!tokenIdText) throw new Error("Token id is required.");

    const tokenRes = await client.query(
      `
        SELECT id, user_id, symbol, selected_bot, COALESCE(active, FALSE) AS active
        FROM tokens
        WHERE id = $1
        LIMIT 1
      `,
      [tokenIdText]
    );
    if (!tokenRes.rowCount) throw new Error("Token not found.");
    const row = tokenRes.rows[0];

    await client.query(`UPDATE tokens SET active = FALSE, updated_at = NOW() WHERE id = $1`, [tokenIdText]);
    await client.query(
      `UPDATE bot_modules SET enabled = FALSE, next_run_at = NOW() + INTERVAL '60 seconds', updated_at = NOW() WHERE token_id = $1`,
      [tokenIdText]
    );
    await insertEvent(
      client,
      Number(row.user_id || 0),
      tokenIdText,
      String(row.symbol || ""),
      "status",
      `Bot force-paused by admin.`,
      null,
      {
        moduleType: String(row.selected_bot || "burn"),
        idempotencyKey: `admin:force-pause:${tokenIdText}:${Date.now()}`,
      }
    );
    await recordAdminAudit(client, {
      actorUserId: scope.actorUserId,
      targetUserId: Number(row.user_id || 0),
      targetTokenId: tokenIdText,
      action: "token_force_paused",
      details: { symbol: String(row.symbol || ""), moduleType: String(row.selected_bot || "burn"), wasActive: Boolean(row.active) },
    });
    return { ok: true };
  });
}

export async function adminArchiveToken(userId, tokenId) {
  return withTx(async (client) => {
    const scope = await resolveUserAccessScope(client, userId);
    assertAdminPermission(scope, "Only the admin account can archive tokens.");
    const tokenIdText = String(tokenId || "").trim();
    if (!tokenIdText) throw new Error("Token id is required.");
    const tokenRes = await client.query(`SELECT user_id, symbol FROM tokens WHERE id = $1 LIMIT 1`, [tokenIdText]);
    if (!tokenRes.rowCount) throw new Error("Token not found.");
    await client.query(
      `UPDATE tokens SET active = FALSE, disconnected = TRUE WHERE id = $1`,
      [tokenIdText]
    );
    await client.query(
      `UPDATE bot_modules SET enabled = FALSE WHERE token_id = $1`,
      [tokenIdText]
    );
    await recordAdminAudit(client, {
      actorUserId: scope.actorUserId,
      targetUserId: Number(tokenRes.rows[0].user_id || 0),
      targetTokenId: tokenIdText,
      action: "token_archived",
      details: { symbol: String(tokenRes.rows[0].symbol || "") },
    });
    return { ok: true };
  });
}

export async function adminRestoreArchivedToken(userId, tokenId) {
  return withTx(async (client) => {
    const scope = await resolveUserAccessScope(client, userId);
    assertAdminPermission(scope, "Only the admin account can restore tokens.");
    const tokenIdText = String(tokenId || "").trim();
    if (!tokenIdText) throw new Error("Token id is required.");
    const tokenRes = await client.query(`SELECT user_id, symbol FROM tokens WHERE id = $1 LIMIT 1`, [tokenIdText]);
    if (!tokenRes.rowCount) throw new Error("Token not found.");
    await client.query(
      `UPDATE tokens SET active = FALSE, disconnected = FALSE WHERE id = $1`,
      [tokenIdText]
    );
    await recordAdminAudit(client, {
      actorUserId: scope.actorUserId,
      targetUserId: Number(tokenRes.rows[0].user_id || 0),
      targetTokenId: tokenIdText,
      action: "token_restored",
      details: { symbol: String(tokenRes.rows[0].symbol || "") },
    });
    return { ok: true };
  });
}

export async function adminPermanentlyDeleteArchivedToken(userId, tokenId) {
  const scope = await resolveUserAccessScopeFromPool(userId);
  assertAdminPermission(scope, "Only the admin account can permanently delete tokens.");
  const tokenIdText = String(tokenId || "").trim();
  if (!tokenIdText) throw new Error("Token id is required.");

  const tokenRes = await pool.query(
    `
      SELECT user_id
      FROM tokens
      WHERE id = $1
      LIMIT 1
    `,
    [tokenIdText]
  );
  if (!tokenRes.rowCount) throw new Error("Token not found.");
  const ownerUserId = Math.max(0, Math.floor(Number(tokenRes.rows[0].user_id || 0)));
  if (!ownerUserId) throw new Error("Token owner could not be resolved.");

  const tokenRowRes = await pool.query(
    `
      SELECT id, user_id, symbol, name, mint, picture_url, deposit, selected_bot, active, disconnected
      FROM tokens
      WHERE user_id = $1 AND id = $2
      LIMIT 1
    `,
    [ownerUserId, tokenIdText]
  );
  if (!tokenRowRes.rowCount) throw new Error("Token not found.");
  const tokenRow = tokenRowRes.rows[0];
  if (!Boolean(tokenRow.disconnected)) {
    throw new Error("Only archived tokens can be permanently deleted.");
  }

  const addresses = await withTx(async (client) =>
    readTokenWalletAddresses(client, ownerUserId, tokenRow)
  );
  const balances = await collectWalletBalances(tokenRow, addresses);
  const hasFunds = balances.some(
    (a) => Number(a.solBalance || 0) > 0.00005 || Number(a.tokenBalance || 0) > 0.000001
  );
  if (hasFunds) {
    throw new Error("Archived token cannot be permanently deleted while funds remain in its wallets.");
  }

  await withTx(async (client) => {
    await client.query(`DELETE FROM token_events WHERE token_id = $1`, [tokenIdText]);
    await client.query(`DELETE FROM referral_events WHERE token_id = $1`, [tokenIdText]);
    await client.query(`DELETE FROM protocol_fee_credits WHERE source_token_id = $1`, [tokenIdText]);
    await recordAdminAudit(client, {
      actorUserId: scope.actorUserId,
      targetUserId: ownerUserId,
      targetTokenId: tokenIdText,
      action: "token_permanently_deleted",
      details: { symbol: String(tokenRow.symbol || ""), mint: String(tokenRow.mint || "") },
    });
    await client.query(
      `
        DELETE FROM tokens
        WHERE user_id = $1 AND id = $2 AND disconnected = TRUE
      `,
      [ownerUserId, tokenIdText]
    );
  });

  return { ok: true };
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
        LEFT JOIN users u
          ON u.id = t.user_id
        WHERE m.id IS NOT NULL
          AND COALESCE(t.disconnected, false) = false
          AND COALESCE(t.hidden_from_public, false) = false
          AND COALESCE(u.is_banned, false) = false
        ORDER BY COALESCE(t.pinned_rank, 0) DESC, t.active DESC, t.updated_at DESC, t.created_at DESC
        LIMIT 200
      `
    ),
    pool.query(
      `
        SELECT e.id, e.token_id, e.token_symbol, e.module_type, e.event_type, e.amount, e.message, e.tx, e.created_at
        FROM token_events e
        LEFT JOIN tokens t ON t.id = e.token_id
        LEFT JOIN users u ON u.id = t.user_id
        WHERE (
            e.token_id IS NULL
            OR (
              COALESCE(t.disconnected, false) = false
              AND COALESCE(t.hidden_from_public, false) = false
              AND COALESCE(u.is_banned, false) = false
            )
          )
          AND (
            e.event_type IN ('burn', 'buyback')
           OR (
             e.module_type IN ('burn', 'personal_burn')
             AND e.event_type IN ('claim', 'status', 'transfer', 'error', 'withdraw', 'sell', 'buy')
           )
          )
        ORDER BY e.created_at DESC
        LIMIT 500
      `
    ),
    pool.query(
      `
        SELECT e.event_type, e.amount, e.message, e.created_at
        FROM token_events e
        LEFT JOIN tokens t ON t.id = e.token_id
        LEFT JOIN users u ON u.id = t.user_id
        WHERE e.event_type = 'burn'
          AND e.created_at >= NOW() - INTERVAL '7 days'
          AND (
            e.token_id IS NULL
            OR (
              COALESCE(t.disconnected, false) = false
              AND COALESCE(t.hidden_from_public, false) = false
              AND COALESCE(u.is_banned, false) = false
            )
          )
        ORDER BY e.created_at DESC
      `
    ),
    pool.query(
      `
        SELECT
          UPPER(COALESCE(NULLIF(e.token_symbol, ''), 'UNKNOWN')) AS symbol,
          COALESCE(SUM(e.amount), 0)::numeric AS amount
        FROM token_events e
        LEFT JOIN tokens t ON t.id = e.token_id
        LEFT JOIN users u ON u.id = t.user_id
        WHERE e.event_type = 'burn'
          AND (
            e.token_id IS NULL
            OR (
              COALESCE(t.disconnected, false) = false
              AND COALESCE(t.hidden_from_public, false) = false
              AND COALESCE(u.is_banned, false) = false
            )
          )
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

export async function permanentlyDeleteArchivedToken(userId, tokenId) {
  const scope = await resolveUserAccessScopeFromPool(userId);
  assertOwnerPermission(scope, "Managers cannot permanently delete archived bots.");

  const tokenRes = await pool.query(
    `
      SELECT id, user_id, symbol, name, mint, picture_url, deposit, selected_bot, active, disconnected
      FROM tokens
      WHERE user_id = $1 AND id = $2
      LIMIT 1
    `,
    [scope.ownerUserId, tokenId]
  );
  if (!tokenRes.rowCount) throw new Error("Token not found.");
  const tokenRow = tokenRes.rows[0];
  if (!Boolean(tokenRow.disconnected)) {
    throw new Error("Only archived tokens can be permanently deleted.");
  }

  const addresses = await withTx(async (client) =>
    readTokenWalletAddresses(client, scope.ownerUserId, tokenRow)
  );
  const balances = await collectWalletBalances(tokenRow, addresses);
  const hasFunds = balances.some(
    (a) => Number(a.solBalance || 0) > 0.00005 || Number(a.tokenBalance || 0) > 0.000001
  );
  if (hasFunds) {
    throw new Error("Archived token cannot be permanently deleted while funds remain in its wallets.");
  }

  await withTx(async (client) => {
    await client.query(`DELETE FROM token_events WHERE token_id = $1`, [String(tokenId)]);
    await client.query(`DELETE FROM referral_events WHERE token_id = $1`, [String(tokenId)]);
    await client.query(`DELETE FROM protocol_fee_credits WHERE source_token_id = $1`, [String(tokenId)]);
    await client.query(
      `
        DELETE FROM tokens
        WHERE user_id = $1 AND id = $2 AND disconnected = TRUE
      `,
      [scope.ownerUserId, String(tokenId)]
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
  const [tokenAggRes, eventAggRes, totalHolders, emberMarketCap, protocolAggRes, protocolSettings] = await Promise.all([
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
    getProtocolSettings(pool).catch(() => DEFAULT_PROTOCOL_SETTINGS),
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
    maintenanceEnabled: Boolean(protocolSettings?.maintenanceEnabled),
    maintenanceMode: String(protocolSettings?.maintenanceMode || "soft"),
    maintenanceMessage: String(protocolSettings?.maintenanceMessage || ""),
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

const DEFAULT_PROTOCOL_SETTINGS = Object.freeze({
  defaultFeeBps: 1000,
  defaultTreasuryBps: 500,
  defaultBurnBps: 500,
  referredTreasuryBps: 250,
  referredBurnBps: 250,
  referredReferralBps: 500,
  personalBotMode: MODULE_TYPES.burn,
  personalBotEnabled: true,
  personalBotIntensity: 45,
  personalBotSafety: 65,
  toolFeeHolderPoolerLamports: 60000000,
  toolFeeReactionManagerLamports: 40000000,
  toolFeeSmartSellLamports: 80000000,
  toolFeeSmartSellRuntimeLamports: 10000000,
  toolFeeSmartSellRuntimeWindowHours: 24,
  toolFeeBundleManagerLamports: 100000000,
  telegramTradeBuyFeeLamports: 1000000,
  telegramTradeSellFeeLamports: 1000000,
  maintenanceEnabled: false,
  maintenanceMode: "soft",
  maintenanceMessage: "",
});

export async function getProtocolSettings(client = pool) {
  const queryable = client?.query ? client : pool;
  const res = await queryable.query(
    `
      SELECT
        default_fee_bps,
        default_treasury_bps,
        default_burn_bps,
        referred_treasury_bps,
        referred_burn_bps,
        referred_referral_bps,
        personal_bot_mode,
        personal_bot_enabled,
        personal_bot_intensity,
        personal_bot_safety,
        tool_fee_holder_pooler_lamports,
        tool_fee_reaction_manager_lamports,
        tool_fee_smart_sell_lamports,
        tool_fee_smart_sell_runtime_lamports,
        tool_fee_smart_sell_runtime_window_hours,
        tool_fee_bundle_manager_lamports,
        telegram_trade_buy_fee_lamports,
        telegram_trade_sell_fee_lamports,
        maintenance_enabled,
        maintenance_mode,
        maintenance_message
      FROM protocol_settings
      WHERE id = 1
      LIMIT 1
    `
  ).catch(() => ({ rows: [] }));
  const row = res.rows?.[0] || {};
  return {
    defaultFeeBps: Math.max(0, Math.floor(Number(row.default_fee_bps || DEFAULT_PROTOCOL_SETTINGS.defaultFeeBps))),
    defaultTreasuryBps: Math.max(0, Math.floor(Number(row.default_treasury_bps || DEFAULT_PROTOCOL_SETTINGS.defaultTreasuryBps))),
    defaultBurnBps: Math.max(0, Math.floor(Number(row.default_burn_bps || DEFAULT_PROTOCOL_SETTINGS.defaultBurnBps))),
    referredTreasuryBps: Math.max(0, Math.floor(Number(row.referred_treasury_bps || DEFAULT_PROTOCOL_SETTINGS.referredTreasuryBps))),
    referredBurnBps: Math.max(0, Math.floor(Number(row.referred_burn_bps || DEFAULT_PROTOCOL_SETTINGS.referredBurnBps))),
    referredReferralBps: Math.max(0, Math.floor(Number(row.referred_referral_bps || DEFAULT_PROTOCOL_SETTINGS.referredReferralBps))),
    personalBotMode: normalizeModuleType(row.personal_bot_mode, DEFAULT_PROTOCOL_SETTINGS.personalBotMode),
    personalBotEnabled:
      typeof row.personal_bot_enabled === "boolean"
        ? row.personal_bot_enabled
      : DEFAULT_PROTOCOL_SETTINGS.personalBotEnabled,
    personalBotIntensity: Math.max(0, Math.min(100, Math.floor(Number(row.personal_bot_intensity ?? DEFAULT_PROTOCOL_SETTINGS.personalBotIntensity)))),
    personalBotSafety: Math.max(0, Math.min(100, Math.floor(Number(row.personal_bot_safety ?? DEFAULT_PROTOCOL_SETTINGS.personalBotSafety)))),
    toolFeeHolderPoolerLamports: Math.max(
      0,
      Math.floor(Number(row.tool_fee_holder_pooler_lamports ?? DEFAULT_PROTOCOL_SETTINGS.toolFeeHolderPoolerLamports))
    ),
    toolFeeReactionManagerLamports: Math.max(
      0,
      Math.floor(Number(row.tool_fee_reaction_manager_lamports ?? DEFAULT_PROTOCOL_SETTINGS.toolFeeReactionManagerLamports))
    ),
    toolFeeSmartSellLamports: Math.max(
      0,
      Math.floor(Number(row.tool_fee_smart_sell_lamports ?? DEFAULT_PROTOCOL_SETTINGS.toolFeeSmartSellLamports))
    ),
    toolFeeSmartSellRuntimeLamports: Math.max(
      0,
      Math.floor(Number(row.tool_fee_smart_sell_runtime_lamports ?? DEFAULT_PROTOCOL_SETTINGS.toolFeeSmartSellRuntimeLamports))
    ),
    toolFeeSmartSellRuntimeWindowHours: Math.max(
      0,
      Math.floor(Number(row.tool_fee_smart_sell_runtime_window_hours ?? DEFAULT_PROTOCOL_SETTINGS.toolFeeSmartSellRuntimeWindowHours))
    ),
    toolFeeBundleManagerLamports: Math.max(
      0,
      Math.floor(Number(row.tool_fee_bundle_manager_lamports ?? DEFAULT_PROTOCOL_SETTINGS.toolFeeBundleManagerLamports))
    ),
    telegramTradeBuyFeeLamports: Math.max(
      0,
      Math.floor(Number(row.telegram_trade_buy_fee_lamports ?? DEFAULT_PROTOCOL_SETTINGS.telegramTradeBuyFeeLamports))
    ),
    telegramTradeSellFeeLamports: Math.max(
      0,
      Math.floor(Number(row.telegram_trade_sell_fee_lamports ?? DEFAULT_PROTOCOL_SETTINGS.telegramTradeSellFeeLamports))
    ),
    maintenanceEnabled:
      typeof row.maintenance_enabled === "boolean"
        ? row.maintenance_enabled
        : DEFAULT_PROTOCOL_SETTINGS.maintenanceEnabled,
    maintenanceMode: ["soft", "hard"].includes(String(row.maintenance_mode || "").trim().toLowerCase())
      ? String(row.maintenance_mode).trim().toLowerCase()
      : DEFAULT_PROTOCOL_SETTINGS.maintenanceMode,
    maintenanceMessage: String(row.maintenance_message || DEFAULT_PROTOCOL_SETTINGS.maintenanceMessage || "").trim(),
  };
}

function deriveProtocolPersonalReserveSol(settings = {}) {
  const safety = Math.max(0, Math.min(100, Number(settings.personalBotSafety ?? DEFAULT_PROTOCOL_SETTINGS.personalBotSafety) || 0));
  return Number((0.003 + (safety / 100) * 0.009).toFixed(3));
}

function buildProtocolPersonalModuleConfig(moduleType, settings = {}) {
  const intensity = Math.max(0, Math.min(100, Number(settings.personalBotIntensity ?? DEFAULT_PROTOCOL_SETTINGS.personalBotIntensity) || 0));
  const reserveSol = deriveProtocolPersonalReserveSol(settings);
  if (moduleType === MODULE_TYPES.volume) {
    return deriveVolumeConfig({
      ...defaultVolumeModuleConfig(),
      speed: intensity,
      aggression: intensity,
      minTradeSol: Number((0.005 + (intensity / 100) * 0.015).toFixed(3)),
      maxTradeSol: Number((0.015 + (intensity / 100) * 0.05).toFixed(3)),
      reserveSol,
    });
  }
  if (moduleType === MODULE_TYPES.marketMaker) {
    return deriveMarketMakerConfig({
      ...defaultMarketMakerModuleConfig(),
      aggression: intensity,
      minTradeSol: Number((0.005 + (intensity / 100) * 0.012).toFixed(3)),
      maxTradeSol: Number((0.018 + (intensity / 100) * 0.05).toFixed(3)),
      reserveSol,
      targetInventoryPct: 50,
    });
  }
  if (moduleType === MODULE_TYPES.dca) {
    return deriveDcaConfig({
      ...defaultDcaModuleConfig(),
      aggression: intensity,
      minTradeSol: Number((0.005 + (intensity / 100) * 0.02).toFixed(3)),
      cycleIntervalSec: Math.max(20, 240 - Math.round(intensity * 2)),
      reserveSol,
    });
  }
  if (moduleType === MODULE_TYPES.rekindle) {
    return deriveRekindleConfig({
      ...defaultRekindleModuleConfig(),
      aggression: intensity,
      minTradeSol: Number((0.005 + (intensity / 100) * 0.018).toFixed(3)),
      reserveSol,
      cooldownSec: Math.max(30, 180 - Math.round(intensity * 1.2)),
    });
  }
  return {
    reserveSol,
  };
}

async function getProtocolRuntimeState(client, stateKey, fallback = {}) {
  const key = String(stateKey || "").trim();
  if (!key) return { ...(fallback || {}) };
  const res = await client.query(
    `
      SELECT state_json
      FROM protocol_runtime_state
      WHERE state_key = $1
      LIMIT 1
    `,
    [key]
  );
  return {
    ...(fallback || {}),
    ...((res.rows[0]?.state_json && typeof res.rows[0].state_json === "object") ? res.rows[0].state_json : {}),
  };
}

async function setProtocolRuntimeState(client, stateKey, state = {}) {
  const key = String(stateKey || "").trim();
  if (!key) return;
  await client.query(
    `
      INSERT INTO protocol_runtime_state (state_key, state_json, updated_at)
      VALUES ($1, $2::jsonb, NOW())
      ON CONFLICT (state_key) DO UPDATE
      SET state_json = EXCLUDED.state_json,
          updated_at = NOW()
    `,
    [key, JSON.stringify(state || {})]
  );
}

async function getUserBillingProfile(client, userId) {
  const res = await client.query(
    `
      SELECT
        id,
        username,
        COALESCE(is_og, FALSE) AS is_og,
        fee_bps_override,
        referrer_user_id,
        COALESCE(referral_code, '') AS referral_code
      FROM users
      WHERE id = $1
      LIMIT 1
    `,
    [userId]
  );
  if (!res.rowCount) {
    throw new Error("User not found.");
  }
  const row = res.rows[0];
  return {
    userId: Number(row.id || 0),
    username: String(row.username || ""),
    isOg: Boolean(row.is_og),
    feeBpsOverride:
      row.fee_bps_override == null ? null : Math.max(0, Math.floor(Number(row.fee_bps_override || 0))),
    referrerUserId:
      row.referrer_user_id == null ? null : Math.max(0, Math.floor(Number(row.referrer_user_id || 0))),
    referralCode: String(row.referral_code || ""),
  };
}

function intersectValues(left = [], right = []) {
  const rightSet = new Set(Array.isArray(right) ? right.filter(Boolean) : []);
  if (!rightSet.size) return [];
  return Array.from(new Set((Array.isArray(left) ? left : []).filter((value) => value && rightSet.has(value))));
}

function describeReferralRisk(signals = {}, grossFeeLamports = 0) {
  const flags = [];
  const reasons = [];
  const cooldownRemainingMs = Math.max(0, Number(signals.cooldownRemainingMs || 0));
  const grossLamports = Math.max(0, Math.floor(Number(grossFeeLamports || 0)));

  if (signals.walletOverlap) flags.push("wallet_overlap");
  if (signals.ipOverlap) flags.push("ip_overlap");
  if (signals.referrerIsOg) reasons.push("referrer_og");
  if (cooldownRemainingMs > 0) reasons.push("cooldown");
  if (grossLamports > 0 && grossLamports < REFERRAL_MIN_GROSS_FEE_LAMPORTS) reasons.push("below_min_fee");
  if (!signals.hasRealUsage) reasons.push("no_real_usage");

  return {
    flags,
    reasons,
    eligible: reasons.length === 0,
    statusLabel:
      reasons[0] === "referrer_og"
        ? "Referrer ineligible"
        : reasons[0] === "cooldown"
          ? "Cooling down"
          : reasons[0] === "below_min_fee"
            ? "Fee below minimum"
            : reasons[0] === "no_real_usage"
              ? "Waiting for real usage"
              : "Eligible",
  };
}

async function getReferralSignalMap(client, pairs = []) {
  const normalizedPairs = Array.from(
    new Map(
      (Array.isArray(pairs) ? pairs : [])
        .map((pair) => ({
          userId: Math.max(0, Math.floor(Number(pair?.userId || 0))),
          referrerUserId: Math.max(0, Math.floor(Number(pair?.referrerUserId || 0))),
        }))
        .filter((pair) => pair.userId > 0 && pair.referrerUserId > 0 && pair.userId !== pair.referrerUserId)
        .map((pair) => [`${pair.userId}:${pair.referrerUserId}`, pair])
    ).values()
  );
  const result = new Map();
  if (!normalizedPairs.length) return result;

  const userIds = Array.from(
    new Set(normalizedPairs.flatMap((pair) => [pair.userId, pair.referrerUserId]).filter((value) => value > 0))
  );

  const [usersRes, tokensRes, modulesRes, eventsRes, walletsRes] = await Promise.all([
    client.query(
      `
        SELECT
          id,
          created_at,
          COALESCE(is_og, FALSE) AS is_og,
          COALESCE(signup_ip, '') AS signup_ip,
          COALESCE(last_login_ip, '') AS last_login_ip
        FROM users
        WHERE id = ANY($1::bigint[])
      `,
      [userIds]
    ),
    client.query(
      `
        SELECT
          user_id,
          COUNT(*) FILTER (WHERE COALESCE(disconnected, FALSE) = FALSE)::bigint AS active_token_count
        FROM tokens
        WHERE user_id = ANY($1::bigint[])
        GROUP BY user_id
      `,
      [userIds]
    ),
    client.query(
      `
        SELECT
          t.user_id,
          COUNT(*)::bigint AS module_count
        FROM bot_modules bm
        JOIN tokens t ON t.id = bm.token_id
        WHERE t.user_id = ANY($1::bigint[])
        GROUP BY t.user_id
      `,
      [userIds]
    ),
    client.query(
      `
        SELECT
          user_id,
          COUNT(*)::bigint AS successful_event_count
        FROM token_events
        WHERE user_id = ANY($1::bigint[])
          AND tx IS NOT NULL
          AND event_type NOT IN ('status', 'transfer')
        GROUP BY user_id
      `,
      [userIds]
    ),
    client.query(
      `
        SELECT user_id, wallet
        FROM (
          SELECT user_id, deposit AS wallet
          FROM tokens
          WHERE user_id = ANY($1::bigint[]) AND deposit IS NOT NULL AND BTRIM(deposit) <> ''
          UNION
          SELECT user_id, deploy_wallet_pubkey AS wallet
          FROM tokens
          WHERE user_id = ANY($1::bigint[]) AND deploy_wallet_pubkey IS NOT NULL AND BTRIM(deploy_wallet_pubkey) <> ''
          UNION
          SELECT user_id, deposit_pubkey AS wallet
          FROM token_deposit_keys
          WHERE user_id = ANY($1::bigint[]) AND deposit_pubkey IS NOT NULL AND BTRIM(deposit_pubkey) <> ''
          UNION
          SELECT user_id, wallet_pubkey AS wallet
          FROM volume_trade_wallets
          WHERE user_id = ANY($1::bigint[]) AND wallet_pubkey IS NOT NULL AND BTRIM(wallet_pubkey) <> ''
          UNION
          SELECT user_id, source_wallet AS wallet
          FROM token_funding_sources
          WHERE user_id = ANY($1::bigint[]) AND source_wallet IS NOT NULL AND BTRIM(source_wallet) <> ''
          UNION
          SELECT referrer_user_id AS user_id, destination_wallet AS wallet
          FROM referral_claims
          WHERE referrer_user_id = ANY($1::bigint[]) AND destination_wallet IS NOT NULL AND BTRIM(destination_wallet) <> ''
        ) wallets
      `,
      [userIds]
    ),
  ]);

  const userMeta = new Map();
  usersRes.rows.forEach((row) => {
    userMeta.set(Number(row.id || 0), {
      createdAt: row.created_at ? new Date(row.created_at) : null,
      isOg: Boolean(row.is_og),
      ips: Array.from(
        new Set(
          [String(row.signup_ip || "").trim(), String(row.last_login_ip || "").trim()].filter(Boolean)
        )
      ),
    });
  });

  const activeTokenCounts = new Map(tokensRes.rows.map((row) => [Number(row.user_id || 0), Number(row.active_token_count || 0)]));
  const moduleCounts = new Map(modulesRes.rows.map((row) => [Number(row.user_id || 0), Number(row.module_count || 0)]));
  const eventCounts = new Map(eventsRes.rows.map((row) => [Number(row.user_id || 0), Number(row.successful_event_count || 0)]));
  const walletMap = new Map();
  walletsRes.rows.forEach((row) => {
    const userId = Number(row.user_id || 0);
    const wallet = String(row.wallet || "").trim();
    if (!userId || !wallet) return;
    if (!walletMap.has(userId)) walletMap.set(userId, new Set());
    walletMap.get(userId).add(wallet);
  });

  normalizedPairs.forEach((pair) => {
    const referred = userMeta.get(pair.userId) || { createdAt: null, isOg: false, ips: [] };
    const referrer = userMeta.get(pair.referrerUserId) || { createdAt: null, isOg: false, ips: [] };
    const overlapWallets = intersectValues(
      Array.from(walletMap.get(pair.userId) || []),
      Array.from(walletMap.get(pair.referrerUserId) || [])
    );
    const overlapIps = intersectValues(referred.ips, referrer.ips);
    const createdAtMs = referred.createdAt instanceof Date && Number.isFinite(referred.createdAt.getTime())
      ? referred.createdAt.getTime()
      : 0;
    const cooldownRemainingMs = createdAtMs > 0
      ? Math.max(0, createdAtMs + REFERRAL_COOLDOWN_MS - Date.now())
      : REFERRAL_COOLDOWN_MS;
    const activeTokenCount = Number(activeTokenCounts.get(pair.userId) || 0);
    const moduleCount = Number(moduleCounts.get(pair.userId) || 0);
    const successfulEventCount = Number(eventCounts.get(pair.userId) || 0);
    const hasRealUsage = activeTokenCount > 0 && moduleCount > 0;

    result.set(`${pair.userId}:${pair.referrerUserId}`, {
      userId: pair.userId,
      referrerUserId: pair.referrerUserId,
      referrerIsOg: Boolean(referrer.isOg),
      walletOverlap: overlapWallets.length > 0,
      ipOverlap: overlapIps.length > 0,
      overlapWallets: overlapWallets.slice(0, 5),
      overlapIps: overlapIps.slice(0, 5),
      cooldownRemainingMs,
      cooldownEndsAt: createdAtMs > 0 ? new Date(createdAtMs + REFERRAL_COOLDOWN_MS).toISOString() : null,
      hasRealUsage,
      activeTokenCount,
      moduleCount,
      successfulEventCount,
    });
  });

  return result;
}

function splitLamportsByBps(totalLamports, bpsMap = {}) {
  const total = Math.max(0, Math.floor(Number(totalLamports || 0)));
  const entries = Object.entries(bpsMap).map(([key, value]) => [key, Math.max(0, Math.floor(Number(value || 0)))]);
  const allocations = {};
  let remaining = total;
  let bpsRemaining = entries.reduce((sum, [, bps]) => sum + bps, 0);
  entries.forEach(([key, bps], index) => {
    if (index === entries.length - 1 || bpsRemaining <= 0) {
      allocations[key] = remaining;
      remaining = 0;
      return;
    }
    const piece = Math.min(remaining, Math.floor((total * bps) / 10000));
    allocations[key] = piece;
    remaining -= piece;
    bpsRemaining -= bps;
  });
  return allocations;
}

async function accrueReferralEarning(client, entry = {}) {
  const referrerUserId = Math.max(0, Math.floor(Number(entry.referrerUserId || 0)));
  const userId = Math.max(0, Math.floor(Number(entry.userId || 0)));
  const referralFeeLamports = Math.max(0, Math.floor(Number(entry.referralFeeLamports || 0)));
  if (!referrerUserId || !userId || referralFeeLamports <= 0) return;

  await client.query(
    `
      INSERT INTO referral_events (
        user_id,
        referrer_user_id,
        token_id,
        token_symbol,
        module_type,
        gross_fee_lamports,
        treasury_fee_lamports,
        burn_fee_lamports,
        referral_fee_lamports,
        tx,
        metadata
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    `,
    [
      userId,
      referrerUserId,
      entry.tokenId ? String(entry.tokenId) : null,
      String(entry.symbol || "").trim().toUpperCase() || "UNKNOWN",
      entry.moduleType ? String(entry.moduleType) : null,
      Math.max(0, Math.floor(Number(entry.grossFeeLamports || 0))),
      Math.max(0, Math.floor(Number(entry.treasuryFeeLamports || 0))),
      Math.max(0, Math.floor(Number(entry.burnFeeLamports || 0))),
      referralFeeLamports,
      entry.tx ? String(entry.tx) : null,
      entry.metadata && typeof entry.metadata === "object" ? JSON.stringify(entry.metadata) : null,
    ]
  );

  await client.query(
    `
      INSERT INTO referral_balances (
        referrer_user_id,
        total_earned_lamports,
        pending_lamports,
        claimed_lamports
      )
      VALUES ($1, $2, $2, 0)
      ON CONFLICT (referrer_user_id) DO UPDATE
      SET
        total_earned_lamports = referral_balances.total_earned_lamports + EXCLUDED.total_earned_lamports,
        pending_lamports = referral_balances.pending_lamports + EXCLUDED.pending_lamports,
        updated_at = NOW()
    `,
    [referrerUserId, referralFeeLamports]
  );
}

async function applyProtocolFeeFlow(client, options = {}) {
  const connection = options.connection;
  const signer = options.signer;
  const userId = Math.max(0, Math.floor(Number(options.userId || 0)));
  const tokenId = options.tokenId ? String(options.tokenId) : null;
  const symbol = String(options.symbol || "").trim().toUpperCase() || "UNKNOWN";
  const moduleType = options.moduleType ? String(options.moduleType) : null;
  const eventPrefix = String(options.eventPrefix || makeId("fee"));
  const totalLamports = Math.max(0, Math.floor(Number(options.totalLamports || 0)));
  const treasuryMessage = String(options.treasuryMessage || "Protocol fee sent to treasury");
  const burnMessage = String(options.burnMessage || "Protocol fee sent to dev burn wallet");
  const referralMessage = String(options.referralMessage || "Referral earnings accrued");
  if (!connection || !signer || !userId || totalLamports <= 0) {
    return {
      feeLamports: 0,
      treasuryLamports: 0,
      burnLamports: 0,
      referralLamports: 0,
      netLamports: Math.max(0, totalLamports),
      txCreated: 0,
    };
  }

  const protocolSettings = await getProtocolSettings(client);
  const billing = await getUserBillingProfile(client, userId);
  const baseFeeBps =
    billing.feeBpsOverride != null
      ? billing.feeBpsOverride
      : protocolSettings.defaultFeeBps;
  const referralSignals = billing.referrerUserId
    ? (await getReferralSignalMap(client, [{ userId, referrerUserId: billing.referrerUserId }])).get(
        `${userId}:${billing.referrerUserId}`
      ) || null
    : null;
  const referralRisk = referralSignals
    ? describeReferralRisk(referralSignals, Math.floor((Math.max(0, totalLamports) * Math.max(0, baseFeeBps)) / 10000))
    : null;
  const referralEligible =
    Boolean(billing.referrerUserId) &&
    Boolean(referralSignals) &&
    Boolean(referralRisk?.eligible) &&
    Boolean(tokenId) &&
    Boolean(moduleType);

  if (billing.isOg || baseFeeBps <= 0) {
    return {
      feeLamports: 0,
      treasuryLamports: 0,
      burnLamports: 0,
      referralLamports: 0,
      netLamports: totalLamports,
      txCreated: 0,
      billing,
    };
  }

  let splitBps;
  if (referralEligible) {
    const referredTotal =
      protocolSettings.referredTreasuryBps +
      protocolSettings.referredBurnBps +
      protocolSettings.referredReferralBps;
    const scale = referredTotal > 0 ? baseFeeBps / referredTotal : 0;
    splitBps = {
      treasury: Math.floor(protocolSettings.referredTreasuryBps * scale),
      burn: Math.floor(protocolSettings.referredBurnBps * scale),
      referral: Math.max(0, baseFeeBps) - Math.floor(protocolSettings.referredTreasuryBps * scale) - Math.floor(protocolSettings.referredBurnBps * scale),
    };
  } else {
    const defaultTotal = protocolSettings.defaultTreasuryBps + protocolSettings.defaultBurnBps;
    const scale = defaultTotal > 0 ? baseFeeBps / defaultTotal : 0;
    splitBps = {
      treasury: Math.floor(protocolSettings.defaultTreasuryBps * scale),
      burn: Math.max(0, baseFeeBps) - Math.floor(protocolSettings.defaultTreasuryBps * scale),
      referral: 0,
    };
  }

  const feeLamports = Math.min(totalLamports, Math.floor((totalLamports * baseFeeBps) / 10000));
  const allocations = splitLamportsByBps(feeLamports, splitBps);
  let treasuryLamports = Math.max(0, Math.floor(Number(allocations.treasury || 0)));
  let burnLamports = Math.max(0, Math.floor(Number(allocations.burn || 0)));
  const referralLamports = Math.max(0, Math.floor(Number(allocations.referral || 0)));
  if (burnLamports > 0 && !config.devWalletPublicKey) {
    treasuryLamports += burnLamports;
    burnLamports = 0;
  }
  const treasuryTransferLamports = treasuryLamports + referralLamports;
  let txCreated = 0;
  let treasurySig = null;
  let burnSig = null;

  if (treasuryTransferLamports > 0) {
    treasurySig = await sendSolTransfer(connection, signer, config.treasuryWallet, treasuryTransferLamports);
    txCreated += treasurySig ? 1 : 0;
  }
  if (burnLamports > 0 && config.devWalletPublicKey) {
    burnSig = await sendSolTransfer(connection, signer, config.devWalletPublicKey, burnLamports);
    txCreated += burnSig ? 1 : 0;
    await addProtocolFeeCredit(client, {
      tokenId,
      userId,
      symbol,
      lamports: burnLamports,
    });
  }

  if (treasuryLamports > 0) {
    await insertEvent(
      client,
      userId,
      tokenId,
      symbol,
      "fee",
      `${treasuryMessage} (${fromLamports(treasuryLamports).toFixed(6)} SOL)`,
      treasurySig,
      { moduleType, amount: fromLamports(treasuryLamports), idempotencyKey: `${eventPrefix}:fee:treasury` }
    );
  }

  if (burnLamports > 0) {
    await insertEvent(
      client,
      userId,
      tokenId,
      symbol,
      "fee",
      `${burnMessage} (${fromLamports(burnLamports).toFixed(6)} SOL)`,
      burnSig,
      { moduleType, amount: fromLamports(burnLamports), idempotencyKey: `${eventPrefix}:fee:burn` }
    );
  }

  if (referralEligible && referralLamports > 0) {
    await accrueReferralEarning(client, {
      userId,
      referrerUserId: billing.referrerUserId,
      tokenId,
      symbol,
      moduleType,
      grossFeeLamports: feeLamports,
      treasuryFeeLamports: treasuryLamports,
      burnFeeLamports: burnLamports,
      referralFeeLamports: referralLamports,
      tx: treasurySig,
      metadata: {
        allocation: "protocol_fee",
        walletOverlap: Boolean(referralSignals?.walletOverlap),
        ipOverlap: Boolean(referralSignals?.ipOverlap),
      },
    });
    await insertEvent(
      client,
      userId,
      tokenId,
      symbol,
      "fee",
      `${referralMessage} (${fromLamports(referralLamports).toFixed(6)} SOL)`,
      treasurySig,
      { moduleType, amount: fromLamports(referralLamports), idempotencyKey: `${eventPrefix}:fee:referral` }
    );
  }

  return {
    feeLamports,
    treasuryLamports,
    burnLamports,
    referralLamports,
    netLamports: Math.max(0, totalLamports - feeLamports),
    txCreated,
    billing,
    referralSignals,
    referralRisk,
  };
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
        AND NOT EXISTS (
          SELECT 1
          FROM bot_jobs j
          WHERE j.module_id = m.id
            AND j.status IN ('queued', 'running')
        )
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

  const feeResult = await applyProtocolFeeFlow(client, {
    connection,
    signer,
    userId: row.user_id,
    tokenId: row.token_id,
    symbol: row.symbol,
    moduleType: MODULE_TYPES.burn,
    eventPrefix,
    totalLamports: delta,
    treasuryMessage: "Protocol fee sent to treasury",
    burnMessage: "Protocol fee sent to dev burn wallet",
    referralMessage: "Referral reserve accrued",
  });
  txCreated += Number(feeResult.txCreated || 0);
  const netLamports = Math.max(0, Number(feeResult.netLamports || 0));

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
    const feeResult = await applyProtocolFeeFlow(client, {
      connection,
      signer: depositSigner,
      userId: row.user_id,
      tokenId: row.token_id,
      symbol: row.symbol,
      moduleType: MODULE_TYPES.volume,
      eventPrefix,
      totalLamports: effectiveDelta,
      treasuryMessage: "Volume fee to treasury",
      burnMessage: "Volume fee to dev burn wallet",
      referralMessage: "Volume referral reserve accrued",
    });
    txCreated += Number(feeResult.txCreated || 0);
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
    const feeResult = await applyProtocolFeeFlow(client, {
      connection,
      signer: depositSigner,
      userId: row.user_id,
      tokenId: row.token_id,
      symbol: row.symbol,
      moduleType,
      eventPrefix,
      totalLamports: effectiveDelta,
      treasuryMessage: `${moduleLabel} fee to treasury`,
      burnMessage: `${moduleLabel} fee to dev burn wallet`,
      referralMessage: `${moduleLabel} referral reserve accrued`,
    });
    txCreated += Number(feeResult.txCreated || 0);
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
    const feeResult = await applyProtocolFeeFlow(client, {
      connection,
      signer: depositSigner,
      userId: row.user_id,
      tokenId: row.token_id,
      symbol: row.symbol,
      moduleType,
      eventPrefix,
      totalLamports: effectiveDelta,
      treasuryMessage: `${moduleLabel} fee to treasury`,
      burnMessage: `${moduleLabel} fee to dev burn wallet`,
      referralMessage: `${moduleLabel} referral reserve accrued`,
    });
    txCreated += Number(feeResult.txCreated || 0);
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
    const feeResult = await applyProtocolFeeFlow(client, {
      connection,
      signer: depositSigner,
      userId: row.user_id,
      tokenId: row.token_id,
      symbol: row.symbol,
      moduleType,
      eventPrefix,
      totalLamports: effectiveDelta,
      treasuryMessage: `${moduleLabel} fee to treasury`,
      burnMessage: `${moduleLabel} fee to dev burn wallet`,
      referralMessage: `${moduleLabel} referral reserve accrued`,
    });
    txCreated += Number(feeResult.txCreated || 0);
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

function getProtocolPersonalClaimMints() {
  return [
    ...new Set(
      [config.emberTokenMint, ...(Array.isArray(config.personalCreatorMints) ? config.personalCreatorMints : [])]
        .map((value) => String(value || "").trim())
        .filter(Boolean)
    ),
  ];
}

function makeProtocolPersonalEventLogger(client, protocolUserId, personalMode, logPrefix = "personal") {
  return async (type, message, tx = null, options = {}) => {
    const metadata = {
      personalMode,
      ...(options?.metadata && typeof options.metadata === "object" ? options.metadata : {}),
    };
    try {
      await insertEvent(
        client,
        protocolUserId,
        null,
        "EMBER",
        type,
        message,
        tx,
        { ...options, moduleType: MODULE_TYPES.personalBurn, metadata }
      );
    } catch (error) {
      console.warn(`[${logPrefix}] protocol event insert failed: ${error?.message || error}`);
    }
  };
}

async function runPersonalCreatorClaimCycle({
  client,
  connection,
  signer,
  claimMints,
  moduleType,
  moduleLabel,
  eventPrefix,
  safeProtocolEvent,
  logPrefix,
  poolHint = "auto",
}) {
  const creatorMints = Array.isArray(claimMints) ? claimMints : [];
  const claimMinSol = Math.max(0, Number(config.personalClaimMinSol || 0));
  const estimatedClaimFeeSol = Math.max(0.00015, Number(config.basePriorityFeeSol || 0.0005) + 0.0001);
  let txCreated = 0;
  let claimSuccess = 0;
  let claimSkipped = 0;
  let claimFailures = 0;
  let claimedLamports = 0;

  for (const mint of creatorMints) {
    try {
      const previewLamports = await previewCreatorFeeLamports(connection, signer.publicKey, mint);
      const previewSol = fromLamports(previewLamports);
      if (claimMinSol > 0 && previewSol < Math.max(claimMinSol, estimatedClaimFeeSol)) {
        claimSkipped += 1;
        console.log(
          `[${logPrefix}] claim skipped for ${mint}: preview ${previewSol.toFixed(6)} < min ${Math.max(
            claimMinSol,
            estimatedClaimFeeSol
          ).toFixed(6)} SOL`
        );
        continue;
      }
      const claimSig = await pumpPortalCollectCreatorFee({
        connection,
        signer,
        mint,
        pool: poolHint,
      });
      txCreated += 1;
      claimSuccess += 1;
      const summary = await getClaimExecutionSummary(connection, claimSig, signer.publicKey.toBase58());
      const gross = Math.max(0, Number(summary.grossClaimLamports || 0));
      claimedLamports += gross;
      if (gross > 0) {
        await safeProtocolEvent(
          "claim",
          `${moduleLabel} claimed creator rewards (${fromLamports(gross).toFixed(6)} SOL)`,
          claimSig,
          {
            amount: fromLamports(gross),
            idempotencyKey: `${eventPrefix}:claim:${moduleType}:${mint}`,
          }
        );
      }
    } catch (error) {
      if (isSoftClaimError(error)) {
        claimSkipped += 1;
        console.log(`[${logPrefix}] claim skipped for ${mint}: ${error?.message || error}`);
      } else {
        claimFailures += 1;
        console.warn(`[${logPrefix}] claim failed for ${mint}: ${error?.message || error}`);
      }
    }
  }

  return {
    txCreated,
    claimSuccess,
    claimSkipped,
    claimFailures,
    claimedLamports,
  };
}

async function runPersonalBurnExecutor(client, protocolSettings = null) {
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
  const personalConfig = buildProtocolPersonalModuleConfig(MODULE_TYPES.burn, protocolSettings || {});
  const reserveLamports = toLamports(
    Math.max(0.001, Number(personalConfig.reserveSol || config.devWalletSolReserve || 0.01))
  );
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

async function runPersonalVolumeExecutor(client, protocolSettings = null) {
  if (!config.devWalletPrivateKey || !config.devWalletPublicKey || !config.emberTokenMint) {
    return { ran: false, txCreated: 0 };
  }

  const connection = getConnection();
  const signer = Keypair.fromSecretKey(config.devWalletPrivateKey);
  const protocolUserId = await getOrCreateProtocolSystemUserId(client);
  const protocolEventPrefix = `protocol:volume:${Date.now()}`;
  let txCreated = 0;
  let claimSuccess = 0;
  let claimSkipped = 0;
  let claimFailures = 0;
  let claimedLamports = 0;

  const safeProtocolEvent = async (type, message, tx = null, options = {}) => {
    try {
      await insertEvent(
        client,
        protocolUserId,
        null,
        "EMBER",
        type,
        message,
        tx,
        { moduleType: MODULE_TYPES.personalBurn, ...options }
      );
    } catch (error) {
      console.warn(`[personal-volume] protocol event insert failed: ${error?.message || error}`);
    }
  };

  const configJson = buildProtocolPersonalModuleConfig(MODULE_TYPES.volume, protocolSettings || {});
  const reserveLamports = toLamports(
    Math.max(0.001, Number(configJson.reserveSol || config.devWalletSolReserve || 0.01))
  );
  const claimMinSol = Math.max(0, Number(config.personalClaimMinSol || 0));
  const estimatedClaimFeeSol = Math.max(0.00015, Number(config.basePriorityFeeSol || 0.0005) + 0.0001);

  const creatorMints = [
    ...new Set(
      [
        config.emberTokenMint,
        ...(Array.isArray(config.personalCreatorMints) ? config.personalCreatorMints : []),
      ]
        .map((value) => String(value || "").trim())
        .filter(Boolean)
    ),
  ];

  for (const mint of creatorMints) {
    try {
      const previewLamports = await previewCreatorFeeLamports(connection, signer.publicKey, mint);
      const previewSol = fromLamports(previewLamports);
      if (claimMinSol > 0 && previewSol < Math.max(claimMinSol, estimatedClaimFeeSol)) {
        claimSkipped += 1;
        console.log(
          `[personal-volume] claim skipped for ${mint}: preview ${previewSol.toFixed(6)} < min ${Math.max(
            claimMinSol,
            estimatedClaimFeeSol
          ).toFixed(6)} SOL`
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
      const summary = await getClaimExecutionSummary(connection, claimSig, signer.publicKey.toBase58());
      const gross = Math.max(0, Number(summary.grossClaimLamports || 0));
      claimedLamports += gross;
      if (gross > 0) {
        await safeProtocolEvent(
          "claim",
          `Protocol volume claimed creator rewards (${fromLamports(gross).toFixed(6)} SOL)`,
          claimSig,
          {
            amount: fromLamports(gross),
            idempotencyKey: `${protocolEventPrefix}:claim:${mint}`,
          }
        );
      }
    } catch (error) {
      if (isSoftClaimError(error)) {
        claimSkipped += 1;
        console.log(`[personal-volume] claim skipped for ${mint}: ${error?.message || error}`);
      } else {
        claimFailures += 1;
        console.warn(`[personal-volume] claim failed for ${mint}: ${error?.message || error}`);
      }
    }
  }

  const balanceLamports = await connection.getBalance(signer.publicKey, "confirmed");
  const spendableLamports = Math.max(0, balanceLamports - reserveLamports);
  const tokenBalance = await getOwnerTokenBalanceUi(connection, signer.publicKey, config.emberTokenMint);

  if (spendableLamports <= toLamports(0.0005) && tokenBalance <= 0.000001) {
    await incrementProtocolMetrics(client, {
      totalBotTransactions: txCreated,
      rewardsProcessedSol: fromLamports(claimedLamports),
    });
    console.log(
      `[personal-volume] no actionable balance (${fromLamports(balanceLamports).toFixed(6)} total, reserve ${fromLamports(
        reserveLamports
      ).toFixed(6)}) claims_ok=${claimSuccess} claims_skip=${claimSkipped} claims_fail=${claimFailures}`
    );
    return { ran: true, txCreated, claimSuccess, claimFailures, spendableLamports };
  }

  let action =
    tokenBalance <= 0.000001 ? "buy" : spendableLamports <= toLamports(0.0005) ? "sell" : Math.random() < 0.55 ? "buy" : "sell";

  if (action === "buy") {
    const spendableSol = fromLamports(spendableLamports);
    const minTradeSol = Math.max(0.0005, Number(configJson.minTradeSol || 0.005));
    const maxTradeSol = Math.max(minTradeSol, Number(configJson.maxTradeSol || minTradeSol));
    const intensityRatio = Math.max(0.1, Math.min(1, Number(configJson.speed || configJson.aggression || 45) / 100));
    const desiredTradeSol = Math.max(
      minTradeSol,
      Math.min(maxTradeSol, spendableSol * (0.18 + intensityRatio * 0.42))
    );
    const tradeSol = Math.max(0.0005, Math.min(spendableSol * 0.72, desiredTradeSol));
    if (tradeSol > 0.0005) {
      const sig = await pumpPortalTrade({
        connection,
        signer,
        mint: config.emberTokenMint,
        action: "buy",
        amount: Number(tradeSol.toFixed(6)),
        denominatedInSol: true,
        slippage: Math.max(1, Math.floor(Number(configJson.slippageBps || 1200) / 100)),
        pool: "auto",
      });
      txCreated += 1;
      await safeProtocolEvent(
        "buy",
        `Protocol volume buy (${tradeSol.toFixed(6)} SOL)`,
        sig,
        {
          amount: tradeSol,
          idempotencyKey: `${protocolEventPrefix}:buy`,
        }
      );
    }
  } else if (tokenBalance > 0.000001) {
    const intensityRatio = Math.max(0.1, Math.min(1, Number(configJson.speed || configJson.aggression || 45) / 100));
    const sellAmount = Number((tokenBalance * (0.12 + intensityRatio * 0.36)).toFixed(6));
    if (sellAmount > 0) {
      const sig = await pumpPortalTrade({
        connection,
        signer,
        mint: config.emberTokenMint,
        action: "sell",
        amount: sellAmount,
        denominatedInSol: false,
        slippage: Math.max(1, Math.floor(Number(configJson.slippageBps || 1200) / 100)),
        pool: "auto",
      });
      txCreated += 1;
      await safeProtocolEvent(
        "sell",
        `Protocol volume sell (${sellAmount.toFixed(6)} EMBER)`,
        sig,
        {
          amount: sellAmount,
          idempotencyKey: `${protocolEventPrefix}:sell`,
        }
      );
    }
  }

  await incrementProtocolMetrics(client, {
    totalBotTransactions: txCreated,
    rewardsProcessedSol: fromLamports(claimedLamports),
  });
  console.log(
    `[personal-volume] executed tx=${txCreated} claims_ok=${claimSuccess} claims_skip=${claimSkipped} claims_fail=${claimFailures} spendable=${fromLamports(
      spendableLamports
    ).toFixed(6)} token_balance=${Number(tokenBalance || 0).toFixed(6)}`
  );
  return { ran: true, txCreated, claimSuccess, claimFailures, spendableLamports };
}

async function runPersonalDcaExecutor(client, protocolSettings = null) {
  if (!config.devWalletPrivateKey || !config.devWalletPublicKey || !config.emberTokenMint) {
    return { ran: false, txCreated: 0 };
  }

  const connection = getConnection();
  const signer = Keypair.fromSecretKey(config.devWalletPrivateKey);
  const protocolUserId = await getOrCreateProtocolSystemUserId(client);
  const protocolEventPrefix = `protocol:dca:${Date.now()}`;
  const safeProtocolEvent = makeProtocolPersonalEventLogger(
    client,
    protocolUserId,
    MODULE_TYPES.dca,
    "personal-dca"
  );
  const configJson = buildProtocolPersonalModuleConfig(MODULE_TYPES.dca, protocolSettings || {});
  const reserveLamports = toLamports(Math.max(0.001, Number(config.devWalletSolReserve || configJson.reserveSol || 0.01)));

  const claimResult = await runPersonalCreatorClaimCycle({
    client,
    connection,
    signer,
    claimMints: getProtocolPersonalClaimMints(),
    moduleType: MODULE_TYPES.dca,
    moduleLabel: "Protocol DCA",
    eventPrefix: protocolEventPrefix,
    safeProtocolEvent,
    logPrefix: "personal-dca",
  });

  let txCreated = Number(claimResult.txCreated || 0);
  const balanceLamports = await connection.getBalance(signer.publicKey, "confirmed");
  const spendableLamports = Math.max(0, balanceLamports - reserveLamports);
  const walletState = {
    wallet_pubkey: signer.publicKey.toBase58(),
    signer,
    solLamports: balanceLamports,
    sol: fromLamports(balanceLamports),
    reserveSol: fromLamports(reserveLamports),
    spendableSol: Math.max(0, fromLamports(spendableLamports) - 0.0005),
  };
  const plan = buildDcaPlan({
    configJson,
    walletStates: [walletState],
  });

  if (plan.wallet && plan.amountSol > 0.0005) {
    try {
      const marketState = await fetchMarketStateForMint(config.emberTokenMint);
      const sig = await pumpPortalTrade({
        connection,
        signer,
        mint: config.emberTokenMint,
        action: "buy",
        amount: plan.amountSol,
        denominatedInSol: true,
        slippage: Math.max(1, Math.floor(Number(configJson.slippageBps || 1000) / 100)),
        pool: getTradeBotPool(configJson, marketState),
      });
      txCreated += 1;
      await safeProtocolEvent("buy", `Protocol DCA buy (${plan.amountSol.toFixed(6)} SOL)`, sig, {
        amount: plan.amountSol,
        idempotencyKey: `${protocolEventPrefix}:buy`,
      });
    } catch (error) {
      console.warn(`[personal-dca] buy failed: ${error?.message || error}`);
      await safeProtocolEvent("error", `Protocol DCA buy failed: ${error?.message || error}`, null, {
        idempotencyKey: `${protocolEventPrefix}:buy:error`,
      });
    }
  } else {
    console.log(
      `[personal-dca] no spendable SOL after reserve (${fromLamports(balanceLamports).toFixed(6)} total, reserve ${fromLamports(
        reserveLamports
      ).toFixed(6)}) claims_ok=${claimResult.claimSuccess} claims_skip=${claimResult.claimSkipped} claims_fail=${claimResult.claimFailures}`
    );
  }

  await incrementProtocolMetrics(client, {
    totalBotTransactions: txCreated,
    rewardsProcessedSol: fromLamports(Number(claimResult.claimedLamports || 0)),
  });

  return {
    ran: true,
    txCreated,
    claimSuccess: claimResult.claimSuccess,
    claimFailures: claimResult.claimFailures,
    spendableLamports,
  };
}

async function runPersonalRekindleExecutor(client, protocolSettings = null) {
  if (!config.devWalletPrivateKey || !config.devWalletPublicKey || !config.emberTokenMint) {
    return { ran: false, txCreated: 0 };
  }

  const connection = getConnection();
  const signer = Keypair.fromSecretKey(config.devWalletPrivateKey);
  const protocolUserId = await getOrCreateProtocolSystemUserId(client);
  const protocolEventPrefix = `protocol:rekindle:${Date.now()}`;
  const safeProtocolEvent = makeProtocolPersonalEventLogger(
    client,
    protocolUserId,
    MODULE_TYPES.rekindle,
    "personal-rekindle"
  );
  const configJson = buildProtocolPersonalModuleConfig(MODULE_TYPES.rekindle, protocolSettings || {});
  const reserveLamports = toLamports(Math.max(0.001, Number(config.devWalletSolReserve || configJson.reserveSol || 0.01)));
  const now = Date.now();
  const state = await getProtocolRuntimeState(client, "personal:rekindle", {});

  const claimResult = await runPersonalCreatorClaimCycle({
    client,
    connection,
    signer,
    claimMints: getProtocolPersonalClaimMints(),
    moduleType: MODULE_TYPES.rekindle,
    moduleLabel: "Protocol Rekindle",
    eventPrefix: protocolEventPrefix,
    safeProtocolEvent,
    logPrefix: "personal-rekindle",
  });

  let txCreated = Number(claimResult.txCreated || 0);
  const balanceLamports = await connection.getBalance(signer.publicKey, "confirmed");
  const spendableLamports = Math.max(0, balanceLamports - reserveLamports);
  const walletState = {
    wallet_pubkey: signer.publicKey.toBase58(),
    signer,
    solLamports: balanceLamports,
    sol: fromLamports(balanceLamports),
    reserveSol: fromLamports(reserveLamports),
    spendableSol: Math.max(0, fromLamports(spendableLamports) - 0.0005),
  };
  const marketState = await fetchMarketStateForMint(config.emberTokenMint);
  const plan = buildRekindlePlan({
    configJson,
    marketState,
    walletStates: [walletState],
    state,
    now,
  });

  if (plan.wallet && plan.amountSol > 0.0005) {
    try {
      const sig = await pumpPortalTrade({
        connection,
        signer,
        mint: config.emberTokenMint,
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
      await safeProtocolEvent(
        "buy",
        `Protocol Rekindle buy (${plan.amountSol.toFixed(6)} SOL) after ${Number(plan.drawdownPct || 0).toFixed(1)}% pullback`,
        sig,
        {
          amount: plan.amountSol,
          idempotencyKey: `${protocolEventPrefix}:buy`,
        }
      );
    } catch (error) {
      state.nextDipBuyAt = now + Math.max(30, Number(configJson.cooldownSec || 135)) * 1000;
      console.warn(`[personal-rekindle] buy failed: ${error?.message || error}`);
      await safeProtocolEvent("error", `Protocol Rekindle buy failed: ${error?.message || error}`, null, {
        idempotencyKey: `${protocolEventPrefix}:buy:error`,
      });
    }
  } else if (Number(state.lastIdleAt || 0) + 300_000 <= now) {
    state.lastIdleAt = now;
    const reason = plan.coolingDown
      ? `cooldown active until ${new Date(Number(plan.coolingDownUntil || now)).toLocaleTimeString()}`
      : !plan.sellPressure
        ? "sell pressure not dominant yet"
        : `current pullback ${Number(plan.drawdownPct || 0).toFixed(1)}% vs trigger ${Number(
            plan.triggerPct || configJson.dipTriggerPct || 9
          ).toFixed(1)}%`;
    await safeProtocolEvent("status", `Protocol Rekindle waiting: ${reason}.`, null, {
      idempotencyKey: `${protocolEventPrefix}:idle`,
    });
  }

  state.peakPriceSol = Math.max(
    Number(state.peakPriceSol || 0),
    Number(plan.peakPriceSol || 0),
    Number(marketState?.priceSol || 0)
  );
  state.lastObservedPriceSol = Number(marketState?.priceSol || 0);
  state.lastObservedMarketCapUsd = Number(marketState?.marketCapUsd || 0);
  await setProtocolRuntimeState(client, "personal:rekindle", state);

  await incrementProtocolMetrics(client, {
    totalBotTransactions: txCreated,
    rewardsProcessedSol: fromLamports(Number(claimResult.claimedLamports || 0)),
  });

  return {
    ran: true,
    txCreated,
    claimSuccess: claimResult.claimSuccess,
    claimFailures: claimResult.claimFailures,
    spendableLamports,
  };
}

async function runPersonalMarketMakerExecutor(client, protocolSettings = null) {
  if (!config.devWalletPrivateKey || !config.devWalletPublicKey || !config.emberTokenMint) {
    return { ran: false, txCreated: 0 };
  }

  const connection = getConnection();
  const signer = Keypair.fromSecretKey(config.devWalletPrivateKey);
  const protocolUserId = await getOrCreateProtocolSystemUserId(client);
  const protocolEventPrefix = `protocol:mm:${Date.now()}`;
  const safeProtocolEvent = makeProtocolPersonalEventLogger(
    client,
    protocolUserId,
    MODULE_TYPES.marketMaker,
    "personal-mm"
  );
  const configJson = buildProtocolPersonalModuleConfig(MODULE_TYPES.marketMaker, protocolSettings || {});
  const reserveLamports = toLamports(Math.max(0.001, Number(config.devWalletSolReserve || configJson.reserveSol || 0.01)));
  const now = Date.now();
  const state = await getProtocolRuntimeState(client, "personal:mm", {});

  if (Number(state.cooldownUntil || 0) > now) {
    return { ran: true, txCreated: 0, coolingDown: true };
  }

  const claimResult = await runPersonalCreatorClaimCycle({
    client,
    connection,
    signer,
    claimMints: getProtocolPersonalClaimMints(),
    moduleType: MODULE_TYPES.marketMaker,
    moduleLabel: "Protocol MM",
    eventPrefix: protocolEventPrefix,
    safeProtocolEvent,
    logPrefix: "personal-mm",
  });

  let txCreated = Number(claimResult.txCreated || 0);
  const balanceLamports = await connection.getBalance(signer.publicKey, "confirmed");
  const tokenBalance = await getOwnerTokenBalanceUi(connection, signer.publicKey, config.emberTokenMint);
  const walletState = {
    wallet_pubkey: signer.publicKey.toBase58(),
    signer,
    solLamports: balanceLamports,
    sol: fromLamports(balanceLamports),
    reserveSol: fromLamports(reserveLamports),
    spendableSol: Math.max(0, fromLamports(Math.max(0, balanceLamports - reserveLamports)) - 0.0005),
    tokenBal: tokenBalance,
  };
  const marketState = await fetchMarketStateForMint(config.emberTokenMint);
  const plan = buildMarketMakerPlan({
    configJson,
    marketState,
    walletStates: [walletState],
    lastDirection: String(state.lastDirection || ""),
  });

  let executedSide = "";
  for (const action of plan.actions) {
    try {
      if (action.side === "buy" && action.amountSol > 0.0005) {
        const sig = await pumpPortalTrade({
          connection,
          signer,
          mint: config.emberTokenMint,
          action: "buy",
          amount: action.amountSol,
          denominatedInSol: true,
          slippage: Math.max(1, Math.floor(Number(configJson.slippageBps || 1200) / 100)),
          pool: plan.pool,
        });
        txCreated += 1;
        executedSide = "buy";
        await safeProtocolEvent("buy", `Protocol MM buy (${action.amountSol.toFixed(6)} SOL)`, sig, {
          amount: action.amountSol,
          idempotencyKey: `${protocolEventPrefix}:buy:${txCreated}`,
        });
      } else if (action.side === "sell" && action.amountToken > 0.000001) {
        const sig = await pumpPortalTrade({
          connection,
          signer,
          mint: config.emberTokenMint,
          action: "sell",
          amount: action.amountToken,
          denominatedInSol: false,
          slippage: Math.max(1, Math.floor(Number(configJson.slippageBps || 1200) / 100)),
          pool: plan.pool,
        });
        txCreated += 1;
        executedSide = "sell";
        await safeProtocolEvent("sell", `Protocol MM sell (${action.amountToken.toFixed(6)} EMBER)`, sig, {
          amount: action.amountToken,
          idempotencyKey: `${protocolEventPrefix}:sell:${txCreated}`,
        });
      }
    } catch (error) {
      state.cooldownUntil = now + Math.max(8, Number(configJson.cooldownSec || 16)) * 1000;
      console.warn(`[personal-mm] ${action.side} failed: ${error?.message || error}`);
      await safeProtocolEvent("error", `Protocol MM ${action.side} failed: ${error?.message || error}`, null, {
        idempotencyKey: `${protocolEventPrefix}:error:${action.side}:${txCreated}`,
      });
      break;
    }
  }

  if (!plan.actions.length && Number(state.lastIdlePostureAt || 0) + 300_000 <= now) {
    state.lastIdlePostureAt = now;
    await safeProtocolEvent(
      "status",
      `Protocol MM holding ${String(plan.posture || "neutral").replace("_", " ")} posture (${Number(
        plan.inventoryPct || 0
      ).toFixed(1)}% token inventory target ${Number(plan.targetInventoryPct || 0).toFixed(1)}%).`,
      null,
      { idempotencyKey: `${protocolEventPrefix}:idle` }
    );
  }

  if (executedSide) {
    state.lastDirection = executedSide;
    state.cooldownUntil = 0;
  }
  state.lastInventoryPct = Number(Number(plan.inventoryPct || 0).toFixed(3));
  state.lastPool = plan.pool;
  state.lastMarketCapUsd = Number(plan.marketCapUsd || 0);
  state.lastLiquidityUsd = Number(plan.liquidityUsd || 0);
  await setProtocolRuntimeState(client, "personal:mm", state);

  await incrementProtocolMetrics(client, {
    totalBotTransactions: txCreated,
    rewardsProcessedSol: fromLamports(Number(claimResult.claimedLamports || 0)),
  });

  return {
    ran: true,
    txCreated,
    claimSuccess: claimResult.claimSuccess,
    claimFailures: claimResult.claimFailures,
    spendableLamports: Math.max(0, balanceLamports - reserveLamports),
  };
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

const smartSellFeed = {
  ws: null,
  connecting: false,
  subscriptions: new Set(),
  tradesByMint: new Map(),
  reconnectAt: 0,
};

function parseSmartSellTradeMessage(raw) {
  let data = raw;
  if (typeof raw === "string") {
    try {
      data = JSON.parse(raw);
    } catch {
      return null;
    }
  }
  if (!data || typeof data !== "object") return null;
  const txType = String(data.txType || data.type || data.action || "").trim().toLowerCase();
  const mint = String(data.mint || data.tokenAddress || data.ca || data.address || "").trim();
  if (!txType || !mint) return null;
  const solAmount = Number(data.solAmount ?? data.baseAmount ?? data.nativeAmount ?? 0) || 0;
  const tokenAmount = Number(data.tokenAmount ?? data.quoteAmount ?? data.amount ?? 0) || 0;
  const signature = String(data.signature || data.tx || data.txHash || data.transaction || "").trim();
  const trader = String(data.publicKey || data.traderPublicKey || data.owner || "").trim();
  return {
    txType,
    mint,
    solAmount,
    tokenAmount,
    signature,
    trader,
    receivedAt: Date.now(),
  };
}

function recordSmartSellTrade(trade) {
  const mint = String(trade?.mint || "").trim();
  if (!mint) return;
  const arr = smartSellFeed.tradesByMint.get(mint) || [];
  arr.push(trade);
  if (arr.length > 300) {
    arr.splice(0, arr.length - 300);
  }
  smartSellFeed.tradesByMint.set(mint, arr);
}

function connectSmartSellFeed() {
  if (smartSellFeed.connecting || smartSellFeed.ws || Date.now() < smartSellFeed.reconnectAt) return;
  if (!smartSellFeed.subscriptions.size) return;
  smartSellFeed.connecting = true;
  const ws = new WebSocket("wss://pumpportal.fun/api/data");
  smartSellFeed.ws = ws;
  ws.on("open", () => {
    smartSellFeed.connecting = false;
    const keys = Array.from(smartSellFeed.subscriptions);
    if (keys.length) {
      ws.send(JSON.stringify({ method: "subscribeTokenTrade", keys }));
    }
  });
  ws.on("message", (buffer) => {
    const parsed = parseSmartSellTradeMessage(String(buffer || ""));
    if (!parsed || parsed.txType !== "buy") return;
    recordSmartSellTrade(parsed);
  });
  const handleClose = () => {
    smartSellFeed.ws = null;
    smartSellFeed.connecting = false;
    smartSellFeed.reconnectAt = Date.now() + 5_000;
  };
  ws.on("close", handleClose);
  ws.on("error", handleClose);
}

function ensureSmartSellFeedSubscriptions(mints = []) {
  const fresh = [];
  for (const mint of mints) {
    const clean = String(mint || "").trim();
    if (!clean) continue;
    if (!smartSellFeed.subscriptions.has(clean)) {
      smartSellFeed.subscriptions.add(clean);
      fresh.push(clean);
    }
  }
  connectSmartSellFeed();
  if (fresh.length && smartSellFeed.ws && smartSellFeed.ws.readyState === WebSocket.OPEN) {
    smartSellFeed.ws.send(JSON.stringify({ method: "subscribeTokenTrade", keys: fresh }));
  }
}

function getRecentSmartSellTrades(mint) {
  return smartSellFeed.tradesByMint.get(String(mint || "").trim()) || [];
}

async function maybeChargeSmartSellRuntimeFee(client, row) {
  row = await syncToolBillingForOwner(client, row);
  const runtimeFeeLamports = Math.max(0, Math.floor(Number(row.runtime_fee_lamports || 0)));
  const windowHours = Math.max(0, Math.floor(Number(row.runtime_fee_window_hours || 0)));
  if (runtimeFeeLamports <= 0 || windowHours <= 0) return row;
  const state = row.state_json && typeof row.state_json === "object" ? row.state_json : {};
  const lastPaidAt = state.runtimeFeeLastPaidAt ? Date.parse(state.runtimeFeeLastPaidAt) : 0;
  const now = Date.now();
  if (lastPaidAt && now - lastPaidAt < windowHours * 60 * 60 * 1000) {
    return row;
  }
  const fundingPubkey = String(row.funding_wallet_pubkey || "").trim();
  const balanceLamports = fundingPubkey
    ? await getConnection().getBalance(new PublicKey(fundingPubkey), "confirmed").catch(() => 0)
    : 0;
  const spendableLamports = Math.max(
    0,
    Number(balanceLamports || 0) - Math.max(0, Number(row.reserve_lamports || 0)) - getTxFeeSafetyLamports()
  );
  if (spendableLamports < runtimeFeeLamports) {
    const message = "Smart Sell paused: runtime fee funding is below the required threshold.";
    const updatedRes = await client.query(
      `
        UPDATE tool_instances
        SET status = $2, last_error = $3
        WHERE id = $1
        RETURNING *
      `,
      [String(row.id || ""), TOOL_STATUSES.paused, message]
    );
    await insertToolEvent(
      client,
      Number(row.owner_user_id || 0),
      String(row.id || ""),
      TOOL_TYPES.smartSell,
      "status",
      message,
      {}
    );
    return updatedRes.rows[0];
  }
  const signer = keypairFromBase58(decryptDepositSecret(String(row.funding_wallet_secret_key_base58 || "")));
  await applyToolUnlockFeeFlow(client, {
    connection: getConnection(),
    signer,
    ownerUserId: Number(row.owner_user_id || 0),
    toolInstanceId: String(row.id || ""),
    toolType: TOOL_TYPES.smartSell,
    totalLamports: runtimeFeeLamports,
  });
  const nextState = {
    ...state,
    runtimeFeeLastPaidAt: new Date(now).toISOString(),
  };
  const updatedRes = await client.query(
    `
      UPDATE tool_instances
      SET state_json = $2::jsonb, last_error = NULL
      WHERE id = $1
      RETURNING *
    `,
    [String(row.id || ""), JSON.stringify(nextState)]
  );
  return updatedRes.rows[0];
}

function buildSmartSellTradeKey(trade) {
  if (trade.signature) return trade.signature;
  return `${trade.mint}:${trade.trader}:${trade.solAmount}:${trade.tokenAmount}:${trade.receivedAt}`;
}

async function processSmartSellToolInstance(client, row) {
  row = await maybeChargeSmartSellRuntimeFee(client, row);
  if (String(row.status || "") !== TOOL_STATUSES.active) {
    return 0;
  }
  const configJson = normalizeSmartSellConfig(row.config_json || {});
  const walletRows = await getToolManagedWalletRows(client, row.id, { activeOnly: true });
  if (!walletRows.length) return 0;
  const connection = getConnection();
  const wallets = await readToolWalletBalances(connection, row.target_mint, walletRows);
  const state = row.state_json && typeof row.state_json === "object" ? row.state_json : {};
  const processedTradeKeys = Array.isArray(state.processedTradeKeys) ? state.processedTradeKeys.slice(-200) : [];
  const processedSet = new Set(processedTradeKeys);
  const pendingSignals = Array.isArray(state.pendingSignals) ? state.pendingSignals.filter((item) => item && typeof item === "object") : [];
  const now = Date.now();
  const recentTrades = getRecentSmartSellTrades(row.target_mint);
  for (const trade of recentTrades) {
    const key = buildSmartSellTradeKey(trade);
    if (processedSet.has(key)) continue;
    processedSet.add(key);
    processedTradeKeys.push(key);
    if (configJson.triggerMode === "threshold" && Number(trade.solAmount || 0) < Number(configJson.thresholdSol || 0)) {
      continue;
    }
    const baseTokenAmount = Math.max(0, Number(trade.tokenAmount || 0) * (Number(configJson.sellPct || 0) / 100));
    if (baseTokenAmount <= 0.0000001) continue;
    if (configJson.timingMode === "split") {
      pendingSignals.push({
        id: makeId("sssig"),
        tradeKey: key,
        executeAt: now + 10_000,
        tokenAmount: Number((baseTokenAmount / 2).toFixed(6)),
      });
      pendingSignals.push({
        id: makeId("sssig"),
        tradeKey: key,
        executeAt: now + 35_000,
        tokenAmount: Number((baseTokenAmount / 2).toFixed(6)),
      });
    } else if (configJson.timingMode === "instant") {
      pendingSignals.push({
        id: makeId("sssig"),
        tradeKey: key,
        executeAt: now,
        tokenAmount: Number(baseTokenAmount.toFixed(6)),
      });
    } else {
      pendingSignals.push({
        id: makeId("sssig"),
        tradeKey: key,
        executeAt: now + 5_000 + Math.floor(Math.random() * 40_000),
        tokenAmount: Number(baseTokenAmount.toFixed(6)),
      });
    }
  }

  const reserveLamports = toLamports(configJson.walletReserveSol || 0.0015);
  const feeSafetyLamports = getTxFeeSafetyLamports();
  let nextWalletIndex = Math.max(0, Math.floor(Number(state.nextWalletIndex || 0)));
  const remainingSignals = [];
  let soldTx = 0;
  let soldTokens = 0;
  for (const signal of pendingSignals) {
    if (Number(signal.executeAt || 0) > now) {
      remainingSignals.push(signal);
      continue;
    }
    let chosen = null;
    for (let offset = 0; offset < wallets.length; offset += 1) {
      const idx = (nextWalletIndex + offset) % wallets.length;
      const wallet = wallets[idx];
      if (Number(wallet.tokenBalance || 0) <= 0.0000001) continue;
      if (Number(wallet.solLamports || 0) <= reserveLamports + feeSafetyLamports) continue;
      chosen = { idx, wallet, row: walletRows[idx] };
      break;
    }
    if (!chosen) {
      remainingSignals.push(signal);
      continue;
    }
    const amount = Math.min(Number(chosen.wallet.tokenBalance || 0), Number(signal.tokenAmount || 0));
    if (amount <= 0.0000001) continue;
    try {
      const signer = keypairFromBase58(decryptDepositSecret(String(chosen.row.secret_key_base58 || "")));
      const sig = await pumpPortalTrade({
        connection,
        signer,
        mint: row.target_mint,
        action: "sell",
        amount: Number(amount.toFixed(6)),
        denominatedInSol: false,
        slippage: 10,
        pool: "auto",
      });
      if (sig) {
        soldTx += 1;
        soldTokens += amount;
        chosen.wallet.tokenBalance = Math.max(0, Number(chosen.wallet.tokenBalance || 0) - amount);
        nextWalletIndex = (chosen.idx + 1) % wallets.length;
      }
    } catch {
      remainingSignals.push(signal);
    }
  }

  const nextState = {
    ...state,
    nextWalletIndex,
    processedTradeKeys: processedTradeKeys.slice(-200),
    pendingSignals: remainingSignals.slice(-200),
  };
  await client.query(
    `
      UPDATE tool_instances
      SET state_json = $2::jsonb, last_run_at = NOW(), last_error = NULL
      WHERE id = $1
    `,
    [String(row.id || ""), JSON.stringify(nextState)]
  );
  if (soldTx > 0) {
    await insertToolEvent(
      client,
      Number(row.owner_user_id || 0),
      String(row.id || ""),
      TOOL_TYPES.smartSell,
      "sell",
      `Smart Sell executed ${soldTx} sells totaling ${soldTokens.toFixed(6)} tokens.`,
      {
        amount: soldTokens,
        metadata: {
          sellTx: soldTx,
          pendingSignals: remainingSignals.length,
        },
      }
    );
  }
  return soldTx;
}

async function processActiveSmartSellTools() {
  const activeRes = await pool.query(
    `
      SELECT *
      FROM tool_instances
      WHERE tool_type = $1
        AND unlocked_at IS NOT NULL
        AND status = $2
      ORDER BY updated_at ASC
      LIMIT 100
    `,
    [TOOL_TYPES.smartSell, TOOL_STATUSES.active]
  );
  const rows = activeRes.rows || [];
  if (!rows.length) return 0;
  ensureSmartSellFeedSubscriptions(rows.map((row) => row.target_mint));
  let txCreated = 0;
  for (const row of rows) {
    const client = await pool.connect();
    try {
      txCreated += await processSmartSellToolInstance(client, row);
    } catch (error) {
      const message = String(error?.message || error || "Smart Sell runtime failure");
      await client.query(
        `UPDATE tool_instances SET last_error = $2 WHERE id = $1`,
        [String(row.id || ""), message]
      );
      await insertToolEvent(
        client,
        Number(row.owner_user_id || 0),
        String(row.id || ""),
        TOOL_TYPES.smartSell,
        "error",
        `Smart Sell runtime failed: ${message}`,
        {}
      );
    } finally {
      client.release();
    }
  }
  return txCreated;
}

async function processTelegramConnectUpdates() {
  if (!config.telegramBotToken || telegramUpdatesInFlight) return;
  telegramUpdatesInFlight = true;
  try {
    const url = new URL(`https://api.telegram.org/bot${config.telegramBotToken}/getUpdates`);
    url.searchParams.set("timeout", "0");
    url.searchParams.set("allowed_updates", JSON.stringify(["message", "callback_query"]));
    if (telegramUpdateOffset > 0) {
      url.searchParams.set("offset", String(telegramUpdateOffset));
    }
    const res = await fetch(url.toString());
    const data = await res.json().catch(() => ({}));
    const updates = Array.isArray(data?.result) ? data.result : [];
    for (const update of updates) {
      const updateId = Number(update?.update_id || 0);
      if (updateId > 0) telegramUpdateOffset = updateId + 1;
      const callback = update?.callback_query;
      if (callback) {
        const callbackId = String(callback.id || "").trim();
        const dataKey = String(callback.data || "").trim();
        const chatId = String(callback?.message?.chat?.id || "").trim();
        const messageId = Number(callback?.message?.message_id || 0);
        const chatType = String(callback?.message?.chat?.type || "").trim().toLowerCase();
        if (chatType !== "private" || !chatId || !dataKey) {
          await answerTelegramCallbackQuery(callbackId);
          continue;
        }
        const linkedUserId = await getTelegramLinkedUserId(chatId);
        if (!linkedUserId) {
          await answerTelegramCallbackQuery(callbackId, "Connect your dashboard account first.");
          continue;
        }
        try {
          if (dataKey === "menu") {
            const stats = await getTelegramMainMenuStats(linkedUserId);
            await editTelegramMessage(chatId, messageId, buildTelegramMainMenuText(stats), {
              replyMarkup: buildTelegramMainKeyboard(stats),
            });
          } else if (dataKey === "noop") {
            await answerTelegramCallbackQuery(callbackId);
            continue;
          } else if (dataKey === "tools") {
            const overview = await getTelegramToolOverview(linkedUserId);
            await editTelegramMessage(chatId, messageId, buildTelegramToolsOverviewText(overview), {
              replyMarkup: buildTelegramToolsKeyboard(overview),
            });
          } else if (dataKey === "tools:new") {
            await editTelegramMessage(chatId, messageId, buildTelegramToolCreateText(), {
              replyMarkup: buildTelegramToolCreateKeyboard(),
            });
          } else if (dataKey === "menu:bots") {
            const overview = await getTelegramBotOverview(linkedUserId);
            await editTelegramMessage(chatId, messageId, buildTelegramBotOverviewText(overview), {
              replyMarkup: buildTelegramBotsKeyboard(overview),
            });
          } else if (dataKey === "bots:attach") {
            const currentFlow = await getTelegramFlowState(linkedUserId);
            const flow =
              currentFlow?.flowType === "attach"
                ? { ...defaultTelegramAttachState(), ...(currentFlow.state || {}) }
                : defaultTelegramAttachState();
            await upsertTelegramFlowState(linkedUserId, "attach", "config", flow);
            await editTelegramMessage(chatId, messageId, buildTelegramAttachText(flow), {
              replyMarkup: buildTelegramAttachKeyboard(flow),
            });
          } else if (dataKey.startsWith("confirm:")) {
            const parts = dataKey.split(":");
            const kind = String(parts[1] || "").trim();
            const targetId = String(parts[2] || "").trim();
            if (kind === "deploy_reset") {
              await editTelegramMessage(
                chatId,
                messageId,
                buildTelegramConfirmationText("deploy_reset"),
                { replyMarkup: buildTelegramConfirmationKeyboard("confirm_do:deploy_reset", "menu:deploy") }
              );
            } else if (kind === "holder_run" || kind === "smart_reclaim" || kind === "bundle_reclaim") {
              const details = await getToolInstanceDetails(linkedUserId, targetId);
              await editTelegramMessage(
                chatId,
                messageId,
                buildTelegramConfirmationText(kind, { label: details?.tool?.label || details?.tool?.title || "Tool" }),
                { replyMarkup: buildTelegramConfirmationKeyboard(`confirm_do:${kind}:${targetId}`, `tool:${targetId}`) }
              );
            } else if (kind === "bot_sweep" || kind === "bot_withdraw") {
              const token = await getTelegramBotDetails(linkedUserId, targetId);
              await editTelegramMessage(
                chatId,
                messageId,
                buildTelegramConfirmationText(kind, { symbol: token?.symbol || token?.name || "Token" }),
                { replyMarkup: buildTelegramConfirmationKeyboard(`confirm_do:${kind}:${targetId}`, `bot:${targetId}`) }
              );
            }
          } else if (dataKey.startsWith("confirm_do:")) {
            const parts = dataKey.split(":");
            const kind = String(parts[1] || "").trim();
            const targetId = String(parts[2] || "").trim();
            if (kind === "deploy_reset") {
              const nextFlow = defaultTelegramDeployState();
              await upsertTelegramFlowState(linkedUserId, "deploy", "config", nextFlow);
              await editTelegramMessage(chatId, messageId, buildTelegramDeployText(nextFlow), {
                replyMarkup: buildTelegramDeployKeyboard(nextFlow),
              });
            } else if (kind === "holder_run") {
              const details = await runHolderPoolerDistribution(linkedUserId, targetId);
              await editTelegramMessage(chatId, messageId, formatTelegramToolDetails(details), {
                replyMarkup: buildTelegramToolKeyboard(details),
              });
            } else if (kind === "smart_reclaim") {
              const details = await reclaimSmartSell(linkedUserId, targetId);
              await editTelegramMessage(chatId, messageId, formatTelegramToolDetails(details), {
                replyMarkup: buildTelegramToolKeyboard(details),
              });
            } else if (kind === "bundle_reclaim") {
              const details = await reclaimBundleManager(linkedUserId, targetId);
              await editTelegramMessage(chatId, messageId, formatTelegramToolDetails(details), {
                replyMarkup: buildTelegramToolKeyboard(details),
              });
            } else if (kind === "bot_sweep") {
              const token = await getTelegramBotDetails(linkedUserId, targetId);
              if (!token?.permissions?.canManageFunds) {
                throw new Error("Managers cannot sweep bot wallets.");
              }
              const result = await sweepVolumeWallets(linkedUserId, targetId);
              const refreshed = await getTelegramBotDetails(linkedUserId, targetId);
              await editTelegramMessage(chatId, messageId, formatTelegramBotDetails(refreshed), {
                replyMarkup: buildTelegramBotKeyboard(refreshed),
              });
              await sendTelegramDirectMessage(chatId, buildTelegramBotActionResultText(refreshed, "sweep", result), {
                replyMarkup: buildTelegramBotKeyboard(refreshed),
              });
            } else if (kind === "bot_withdraw") {
              const token = await getTelegramBotDetails(linkedUserId, targetId);
              if (!token?.permissions?.canManageFunds) {
                throw new Error("Managers cannot withdraw bot funds.");
              }
              await upsertTelegramFlowState(linkedUserId, "bot", "field:withdraw_destination", { tokenId: targetId });
              await editTelegramMessage(
                chatId,
                messageId,
                `${formatTelegramBotDetails(token)}\n\n${buildTelegramBotFieldPrompt("withdraw_destination", token)}`,
                { replyMarkup: buildTelegramFieldPromptKeyboard(`bot:${targetId}`) }
              );
            }
          } else if (dataKey === "menu:deploy") {
            const currentFlow = await getTelegramFlowState(linkedUserId);
            const flow =
              currentFlow?.flowType === "deploy"
                ? { ...defaultTelegramDeployState(), ...(currentFlow.state || {}) }
                : defaultTelegramDeployState();
            await upsertTelegramFlowState(linkedUserId, "deploy", "config", flow);
            await editTelegramMessage(chatId, messageId, buildTelegramDeployText(flow), {
              replyMarkup: buildTelegramDeployKeyboard(flow),
            });
          } else if (dataKey === "menu:trade") {
            const details = await getTelegramTradeDetails(linkedUserId);
            await editTelegramMessage(chatId, messageId, buildTelegramTradeDetailsText(details), {
              replyMarkup: buildTelegramTradeActionKeyboard(details),
            });
          } else if (dataKey.startsWith("tool_create_target:")) {
            const parts = dataKey.split(":");
            const toolType = normalizeToolType(String(parts[2] || "").trim());
            const tokenId = String(parts[3] || "").trim();
            const token = await getTelegramBotDetails(linkedUserId, tokenId);
            const tool = await createToolInstance(linkedUserId, {
              toolType,
              title: toolTypeLabel(toolType),
              targetMint: token.mint,
              simpleMode: true,
            });
            const details = await getToolInstanceDetails(linkedUserId, tool.id);
            await editTelegramMessage(
              chatId,
              messageId,
              [
                `\u2705 ${tool.label} created.`,
                `Target: ${String(token.symbol || token.name || "TOKEN")}`,
                `Funding wallet: ${tool.fundingWalletPubkey}`,
                `Required: ${tool.requiredSol.toFixed(6)} SOL`,
              ].join("\n"),
              { replyMarkup: buildTelegramToolKeyboard(details) }
            );
            if (details?.permissions?.canManageFunds) {
              const fundingWallet = await getToolFundingSecretForOwner(linkedUserId, tool.id);
              await sendTelegramDirectMessage(chatId, buildTelegramToolFundingKeyText(fundingWallet), {
                replyMarkup: buildTelegramFieldPromptKeyboard(`tool:${tool.id}`),
              });
            }
          } else if (dataKey.startsWith("tool_create_attached:")) {
            const toolType = normalizeToolType(String(dataKey.split(":")[2] || "").trim());
            const overview = await getTelegramBotOverview(linkedUserId);
            await editTelegramMessage(chatId, messageId, buildTelegramToolTargetPickerText(toolType, overview), {
              replyMarkup: buildTelegramToolTargetPickerKeyboard(toolType, overview),
            });
          } else if (dataKey.startsWith("tool_create_paste:")) {
            const toolType = normalizeToolType(String(dataKey.split(":")[2] || "").trim());
            await upsertTelegramFlowState(linkedUserId, "tool_create", "await_target", { toolType });
            await editTelegramMessage(chatId, messageId, buildTelegramToolCreatePrompt(toolType), {
              replyMarkup: buildTelegramFieldPromptKeyboard("tools:new"),
            });
          } else if (dataKey.startsWith("tool_create:")) {
            const toolType = normalizeToolType(String(dataKey.split(":")[1] || "").trim());
            if (toolType === TOOL_TYPES.reactionManager) {
              await upsertTelegramFlowState(linkedUserId, "tool_create", "await_target", { toolType });
              await editTelegramMessage(chatId, messageId, buildTelegramToolCreatePrompt(toolType), {
                replyMarkup: buildTelegramFieldPromptKeyboard("tools:new"),
              });
            } else {
              await editTelegramMessage(chatId, messageId, buildTelegramToolMintCreateText(toolType), {
                replyMarkup: buildTelegramToolMintCreateKeyboard(toolType),
              });
            }
          } else if (
            dataKey === "attach:wallet" ||
            dataKey === "attach:bot" ||
            dataKey === "attach:reserve" ||
            dataKey === "attach:key" ||
            dataKey === "attach:cancel_field" ||
            dataKey === "attach:submit"
          ) {
            const currentFlow = (await getTelegramFlowState(linkedUserId)) || {
              flowType: "attach",
              step: "config",
              state: defaultTelegramAttachState(),
            };
            let nextFlow = { ...defaultTelegramAttachState(), ...(currentFlow.state || {}) };
            if (dataKey === "attach:wallet") {
              nextFlow.walletMode = nextFlow.walletMode === "regular" ? "vanity" : "regular";
              nextFlow.pendingDepositId = "";
              nextFlow.deposit = "";
            } else if (dataKey === "attach:bot") {
              nextFlow.selectedBot = cycleTelegramBotMode(nextFlow.selectedBot);
            } else if (dataKey === "attach:reserve") {
              const reservation = await generatePendingDepositAddress(linkedUserId, 1, {
                useVanity: nextFlow.walletMode !== "regular",
              });
              nextFlow = {
                ...nextFlow,
                pendingDepositId: String(reservation?.pendingDepositId || "").trim(),
                deposit: String(reservation?.deposit || "").trim(),
              };
              const depositReservation = await getTelegramPendingDepositReservationSecret(linkedUserId, nextFlow.pendingDepositId);
              await sendTelegramDirectMessage(chatId, buildTelegramAttachKeyText(depositReservation), {
                replyMarkup: buildTelegramFieldPromptKeyboard("menu:bots"),
              });
            } else if (dataKey === "attach:key") {
              const depositReservation = await getTelegramPendingDepositReservationSecret(linkedUserId, nextFlow.pendingDepositId);
              await sendTelegramDirectMessage(chatId, buildTelegramAttachKeyText(depositReservation), {
                replyMarkup: buildTelegramFieldPromptKeyboard("menu:bots"),
              });
              await answerTelegramCallbackQuery(callbackId, "Deposit key sent.");
              continue;
            } else if (dataKey === "attach:cancel_field") {
              await upsertTelegramFlowState(linkedUserId, "attach", "config", nextFlow);
              await editTelegramMessage(chatId, messageId, buildTelegramAttachText(nextFlow), {
                replyMarkup: buildTelegramAttachKeyboard(nextFlow),
              });
              await answerTelegramCallbackQuery(callbackId);
              continue;
            } else if (dataKey === "attach:submit") {
              const token = await attachToken(linkedUserId, {
                mint: nextFlow.mint,
                name: nextFlow.name,
                symbol: nextFlow.symbol,
                pictureUrl: nextFlow.pictureUrl,
                selectedBot: nextFlow.selectedBot,
                pendingDepositId: nextFlow.pendingDepositId,
              });
              await clearTelegramFlowState(linkedUserId);
              const attached = await getTelegramBotDetails(linkedUserId, token.id);
              await editTelegramMessage(
                chatId,
                messageId,
                [
                  "\u2705 Token attached.",
                  `Symbol: ${String(attached.symbol || attached.name || "TOKEN")}`,
                  `Mint: ${String(attached.mint || "-")}`,
                  `Bot: ${String(attached.selectedBot || "burn").toUpperCase()}`,
                ].join("\n"),
                { replyMarkup: buildTelegramBotKeyboard(attached) }
              );
              await answerTelegramCallbackQuery(callbackId, "Token attached.");
              continue;
            }
            await upsertTelegramFlowState(linkedUserId, "attach", "config", nextFlow);
            await editTelegramMessage(chatId, messageId, buildTelegramAttachText(nextFlow), {
              replyMarkup: buildTelegramAttachKeyboard(nextFlow),
            });
          } else if (dataKey.startsWith("attach:field:")) {
            const field = String(dataKey.split(":")[2] || "").trim().toLowerCase();
            const currentFlow = (await getTelegramFlowState(linkedUserId)) || {
              flowType: "attach",
              step: "config",
              state: defaultTelegramAttachState(),
            };
            const nextFlow = { ...defaultTelegramAttachState(), ...(currentFlow.state || {}) };
            await upsertTelegramFlowState(linkedUserId, "attach", `field:${field}`, nextFlow);
            await editTelegramMessage(
              chatId,
              messageId,
              `${buildTelegramAttachText(nextFlow)}\n\n${buildTelegramAttachFieldPrompt(field)}`,
              { replyMarkup: buildTelegramFieldPromptKeyboard("attach:cancel_field") }
            );
          } else if (dataKey.startsWith("menu:")) {
            const section = String(dataKey.split(":")[1] || "").trim();
            await answerTelegramCallbackQuery(callbackId, `Opening ${section}.`);
            continue;
          } else if (
            dataKey === "deploy:wallet" ||
            dataKey === "deploy:attach" ||
            dataKey === "deploy:bot" ||
            dataKey.startsWith("deploy:buy:") ||
            dataKey === "deploy:reserve" ||
            dataKey === "deploy:key" ||
            dataKey === "deploy:reset" ||
            dataKey === "deploy:cancel_field" ||
            dataKey === "deploy:submit"
          ) {
            const currentFlow = (await getTelegramFlowState(linkedUserId)) || {
              flowType: "deploy",
              step: "config",
              state: defaultTelegramDeployState(),
            };
            let nextFlow = { ...defaultTelegramDeployState(), ...(currentFlow.state || {}) };
            if (dataKey === "deploy:wallet") {
              nextFlow.walletMode = nextFlow.walletMode === "regular" ? "vanity" : "regular";
              nextFlow.reservationId = "";
              nextFlow.deposit = "";
              nextFlow.requiredSol = 0;
              nextFlow.balanceSol = 0;
              nextFlow.shortfallSol = 0;
              nextFlow.status = 'reserved';
              nextFlow.lastError = '';
              nextFlow.funded = false;
            } else if (dataKey === "deploy:attach") {
              nextFlow.attachBot = !Boolean(nextFlow.attachBot);
            } else if (dataKey === "deploy:bot") {
              const order = ["burn", "volume", "market_maker", "dca", "rekindle"];
              const currentIndex = Math.max(0, order.indexOf(String(nextFlow.selectedBot || "burn")));
              nextFlow.selectedBot = order[(currentIndex + 1) % order.length];
            } else if (dataKey === "deploy:buy:inc") {
              nextFlow.initialBuySol = Math.min(100, Number((Number(nextFlow.initialBuySol || 0.1) + 0.05).toFixed(3)));
              nextFlow.reservationId = "";
              nextFlow.deposit = "";
              nextFlow.requiredSol = 0;
              nextFlow.balanceSol = 0;
              nextFlow.shortfallSol = 0;
              nextFlow.status = 'reserved';
              nextFlow.lastError = '';
              nextFlow.funded = false;
            } else if (dataKey === "deploy:buy:dec") {
              nextFlow.initialBuySol = Math.max(0.05, Number((Number(nextFlow.initialBuySol || 0.1) - 0.05).toFixed(3)));
              nextFlow.reservationId = "";
              nextFlow.deposit = "";
              nextFlow.requiredSol = 0;
              nextFlow.balanceSol = 0;
              nextFlow.shortfallSol = 0;
              nextFlow.status = 'reserved';
              nextFlow.lastError = '';
              nextFlow.funded = false;
            } else if (dataKey === "deploy:reserve") {
              if (!nextFlow.reservationId) {
                const reservation = await reserveVanityDeployWallet(linkedUserId, {
                  initialBuySol: nextFlow.initialBuySol,
                  useVanity: nextFlow.walletMode !== "regular",
                });
                nextFlow = {
                  ...nextFlow,
                  reservationId: reservation.reservationId,
                  deposit: reservation.deposit,
                  funded: Boolean(reservation.funded),
                  requiredSol: Number(reservation.requiredSol || 0),
                  balanceSol: Number(reservation.balanceSol || 0),
                  shortfallSol: Number(reservation.shortfallSol || 0),
                  status: String(reservation.status || 'reserved'),
                  lastError: String(reservation.lastError || ''),
                };
                await sendTelegramDirectMessage(chatId, buildTelegramDeployKeyText(reservation), {
                  replyMarkup: buildTelegramFieldPromptKeyboard("menu:deploy"),
                });
              } else {
                const status = await getVanityDeployWalletStatus(nextFlow.reservationId);
                nextFlow = {
                  ...nextFlow,
                  deposit: status.deposit,
                  funded: Boolean(status.funded),
                  requiredSol: Number(status.requiredSol || 0),
                  balanceSol: Number(status.balanceSol || 0),
                  shortfallSol: Number(status.shortfallSol || 0),
                  status: String(status.status || 'reserved'),
                  lastError: String(status.lastError || ''),
                };
              }
            } else if (dataKey === "deploy:key") {
              if (!nextFlow.reservationId) {
                throw new Error("Reserve a deploy wallet first.");
              }
              const reservation = await getTelegramDeployReservationSecret(linkedUserId, nextFlow.reservationId);
              await sendTelegramDirectMessage(chatId, buildTelegramDeployKeyText(reservation), {
                replyMarkup: buildTelegramFieldPromptKeyboard("menu:deploy"),
              });
              await answerTelegramCallbackQuery(callbackId, "Deployer key sent.");
              continue;
            } else if (dataKey === "deploy:reset") {
              nextFlow = defaultTelegramDeployState();
            } else if (dataKey === "deploy:cancel_field") {
              await upsertTelegramFlowState(linkedUserId, "deploy", "config", nextFlow);
              await editTelegramMessage(chatId, messageId, buildTelegramDeployText(nextFlow), {
                replyMarkup: buildTelegramDeployKeyboard(nextFlow),
              });
              await answerTelegramCallbackQuery(callbackId);
              continue;
            } else if (dataKey === "deploy:submit") {
              try {
                const result = await submitVanityDeploy(linkedUserId, {
                  reservationId: nextFlow.reservationId,
                  name: nextFlow.name,
                  symbol: nextFlow.symbol,
                  description: nextFlow.description,
                  initialBuySol: nextFlow.initialBuySol,
                  twitter: nextFlow.twitter,
                  telegram: nextFlow.telegram,
                  website: nextFlow.website,
                  imageDataUri: nextFlow.imageDataUri,
                  imageFileName: nextFlow.imageFileName || "token",
                  bannerDataUri: nextFlow.bannerDataUri,
                  bannerFileName: nextFlow.bannerFileName || "banner",
                  autoAttach: Boolean(nextFlow.attachBot),
                  selectedBot: nextFlow.selectedBot,
                });
                await clearTelegramFlowState(linkedUserId);
                const successLines = [
                  "\u2705 Token deployed.",
                  `Mint: ${String(result?.mint || "-")}`,
                  `Wallet: ${String(result?.deployWallet || "-")}`,
                  result?.autoAttached && result?.attachedToken?.selectedBot
                    ? `Bot attached: ${String(result.attachedToken.selectedBot || "").toUpperCase()}`
                    : "Bot attached: No",
                ];
                await editTelegramMessage(chatId, messageId, successLines.join("\n"), {
                  replyMarkup: buildTelegramDeploySuccessKeyboard(result),
                });
                await answerTelegramCallbackQuery(callbackId, "Deploy submitted.");
                continue;
              } catch (error) {
                nextFlow.lastError = String(error?.message || error || "Deploy failed.");
              }
            }
            await upsertTelegramFlowState(linkedUserId, "deploy", "config", nextFlow);
            await editTelegramMessage(chatId, messageId, buildTelegramDeployText(nextFlow), {
              replyMarkup: buildTelegramDeployKeyboard(nextFlow),
            });
          } else if (dataKey.startsWith("deploy:field:")) {
            const field = String(dataKey.split(":")[2] || "").trim().toLowerCase();
            const currentFlow = (await getTelegramFlowState(linkedUserId)) || {
              flowType: "deploy",
              step: "config",
              state: defaultTelegramDeployState(),
            };
            const nextFlow = { ...defaultTelegramDeployState(), ...(currentFlow.state || {}) };
            await upsertTelegramFlowState(linkedUserId, "deploy", `field:${field}`, nextFlow);
            await editTelegramMessage(
              chatId,
              messageId,
              `${buildTelegramDeployText(nextFlow)}\n\n${buildTelegramDeployFieldPrompt(field)}`,
              { replyMarkup: buildTelegramDeployFieldKeyboard(field) }
            );
          } else if (dataKey.startsWith("deploy:skip:")) {
            const field = String(dataKey.split(":")[2] || "").trim().toLowerCase();
            if (!["twitter", "telegram", "website", "banner"].includes(field)) {
              throw new Error("That field cannot be skipped.");
            }
            const currentFlow = (await getTelegramFlowState(linkedUserId)) || {
              flowType: "deploy",
              step: "config",
              state: defaultTelegramDeployState(),
            };
            const nextFlow = { ...defaultTelegramDeployState(), ...(currentFlow.state || {}) };
            if (field === "banner") {
              nextFlow.bannerDataUri = "";
              nextFlow.bannerFileName = "banner";
            } else {
              nextFlow[field] = "";
            }
            await upsertTelegramFlowState(linkedUserId, "deploy", "config", nextFlow);
            await editTelegramMessage(chatId, messageId, buildTelegramDeployText(nextFlow), {
              replyMarkup: buildTelegramDeployKeyboard(nextFlow),
            });
          } else if (dataKey === "trade:create_wallet") {
            const details = await createTelegramTradeWalletSlot(linkedUserId);
            const wallet = await getTelegramTradeWalletSecret(linkedUserId, details?.selectedWallet?.id || "");
            await editTelegramMessage(chatId, messageId, buildTelegramTradeDetailsText(details), {
              replyMarkup: buildTelegramTradeActionKeyboard(details),
            });
            await sendTelegramDirectMessage(chatId, buildTelegramTradeWalletKeyText(wallet), {
              replyMarkup: buildTelegramFieldPromptKeyboard("menu:trade"),
            });
          } else if (dataKey === "trade:import_wallet") {
            await upsertTelegramFlowState(linkedUserId, "trade", "field:import_wallet", {});
            await editTelegramMessage(chatId, messageId, buildTelegramTradeFieldPrompt("import_wallet"), {
              replyMarkup: buildTelegramFieldPromptKeyboard("menu:trade"),
            });
          } else if (dataKey === "trade:wallet_info") {
            const details = await getTelegramTradeDetails(linkedUserId);
            await editTelegramMessage(chatId, messageId, buildTelegramTradeWalletInfoText(details), {
              replyMarkup: buildTelegramTradeWalletInfoKeyboard(details),
            });
          } else if (dataKey === "trade:wallet_key") {
            const details = await getTelegramTradeDetails(linkedUserId);
            const wallet = await getTelegramTradeWalletSecret(linkedUserId, details?.selectedWallet?.id || "");
            await editTelegramMessage(chatId, messageId, buildTelegramTradeWalletKeyText(wallet), {
              replyMarkup: buildTelegramFieldPromptKeyboard("menu:trade"),
            });
          } else if (dataKey === "trade:wallets") {
            const details = await getTelegramTradeDetails(linkedUserId);
            await editTelegramMessage(chatId, messageId, buildTelegramTradeWalletPickerText(details), {
              replyMarkup: buildTelegramTradeWalletPickerKeyboard(details),
            });
          } else if (dataKey.startsWith("trade:wallet:")) {
            const walletId = String(dataKey.split(":")[2] || "").trim();
            const details = await selectTelegramTradeWallet(linkedUserId, walletId);
            await editTelegramMessage(chatId, messageId, buildTelegramTradeDetailsText(details), {
              replyMarkup: buildTelegramTradeActionKeyboard(details),
            });
          } else if (dataKey === "trade:pick_attached") {
            const overview = await getTelegramBotOverview(linkedUserId);
            await editTelegramMessage(chatId, messageId, buildTelegramTradeTokenPickerText(overview), {
              replyMarkup: buildTelegramTradeTokenPickerKeyboard(overview),
            });
          } else if (dataKey === "trade:token_links") {
            const details = await getTelegramTradeDetails(linkedUserId);
            await editTelegramMessage(chatId, messageId, buildTelegramTradeTokenLinksText(details), {
              replyMarkup: buildTelegramTradeTokenLinksKeyboard(details),
            });
          } else if (dataKey.startsWith("trade:token:")) {
            const tokenId = String(dataKey.split(":")[2] || "").trim();
            const token = await getTelegramBotDetails(linkedUserId, tokenId);
            const details = await setTelegramTradeMint(linkedUserId, token.mint);
            await editTelegramMessage(chatId, messageId, buildTelegramTradeDetailsText(details), {
              replyMarkup: buildTelegramTradeActionKeyboard(details),
            });
          } else if (dataKey === "trade:set_mint") {
            await upsertTelegramFlowState(linkedUserId, "trade", "field:mint", {});
            await editTelegramMessage(chatId, messageId, buildTelegramTradeFieldPrompt("mint"), {
              replyMarkup: buildTelegramFieldPromptKeyboard("menu:trade"),
            });
          } else if (dataKey === "trade:buy_custom") {
            await upsertTelegramFlowState(linkedUserId, "trade", "field:buy_amount", {});
            await editTelegramMessage(chatId, messageId, buildTelegramTradeFieldPrompt("buy_amount"), {
              replyMarkup: buildTelegramFieldPromptKeyboard("menu:trade"),
            });
          } else if (dataKey === "trade:sell_custom") {
            await upsertTelegramFlowState(linkedUserId, "trade", "field:sell_percent", {});
            await editTelegramMessage(chatId, messageId, buildTelegramTradeFieldPrompt("sell_percent"), {
              replyMarkup: buildTelegramFieldPromptKeyboard("menu:trade"),
            });
          } else if (dataKey === "trade:withdraw") {
            await upsertTelegramFlowState(linkedUserId, "trade", "field:withdraw_destination", {});
            await editTelegramMessage(chatId, messageId, buildTelegramTradeFieldPrompt("withdraw_destination"), {
              replyMarkup: buildTelegramFieldPromptKeyboard("menu:trade"),
            });
          } else if (dataKey.startsWith("trade:buy:")) {
            const amount = Number(dataKey.split(":")[2] || 0);
            const details = await executeTelegramTradeBuy(linkedUserId, amount);
            await editTelegramMessage(chatId, messageId, buildTelegramTradeDetailsText(details), {
              replyMarkup: buildTelegramTradeActionKeyboard(details),
            });
          } else if (dataKey.startsWith("trade:sell:")) {
            const percent = Number(dataKey.split(":")[2] || 0);
            const details = await executeTelegramTradeSellPct(linkedUserId, percent);
            await editTelegramMessage(chatId, messageId, buildTelegramTradeDetailsText(details), {
              replyMarkup: buildTelegramTradeActionKeyboard(details),
            });
          } else if (dataKey.startsWith("bot_settings:")) {
            const tokenId = String(dataKey.split(":")[1] || "").trim();
            const token = await getTelegramBotDetails(linkedUserId, tokenId);
            await editTelegramMessage(chatId, messageId, buildTelegramBotSettingsText(token), {
              replyMarkup: buildTelegramBotSettingsKeyboard(token),
            });
          } else if (dataKey.startsWith("bot:")) {
            const parts = dataKey.split(":");
            const action = String(parts[1] || "").trim();
            const tokenId = String(parts[2] || parts[1] || "").trim();
            if (action === "live") {
              const token = await getTelegramBotDetails(linkedUserId, tokenId);
              const snapshot = await getTelegramBotLiveSnapshot(linkedUserId, tokenId);
              await editTelegramMessage(chatId, messageId, buildTelegramBotLiveText(token, snapshot), {
                replyMarkup: buildTelegramBotLiveKeyboard(tokenId),
              });
            } else if (action === "wallets") {
              const token = await getTelegramBotDetails(linkedUserId, tokenId);
              const snapshot = await getTelegramBotLiveSnapshot(linkedUserId, tokenId);
              await editTelegramMessage(chatId, messageId, buildTelegramBotWalletsText(token, snapshot), {
                replyMarkup: buildTelegramBotWalletsKeyboard(token, snapshot),
              });
            } else if (action === "keys") {
              const payload = await getTelegramBotWalletSecrets(linkedUserId, tokenId);
              await editTelegramMessage(chatId, messageId, buildTelegramBotWalletKeysText(payload), {
                replyMarkup: buildTelegramBotWalletKeysKeyboard(tokenId),
              });
            } else if (action === "withdraw") {
              const token = await getTelegramBotDetails(linkedUserId, tokenId);
              if (!token?.permissions?.canManageFunds) {
                throw new Error("Managers cannot withdraw bot funds.");
              }
              await upsertTelegramFlowState(linkedUserId, "bot", "field:withdraw_destination", {
                tokenId,
              });
              await editTelegramMessage(
                chatId,
                messageId,
                `${formatTelegramBotDetails(token)}\n\n${buildTelegramBotFieldPrompt("withdraw_destination", token)}`,
                { replyMarkup: buildTelegramFieldPromptKeyboard(`bot:${tokenId}`) }
              );
            } else if (action === "sweep") {
              const token = await getTelegramBotDetails(linkedUserId, tokenId);
              if (!token?.permissions?.canManageFunds) {
                throw new Error("Managers cannot sweep bot wallets.");
              }
              const result = await sweepVolumeWallets(linkedUserId, tokenId);
              const refreshed = await getTelegramBotDetails(linkedUserId, tokenId);
              await editTelegramMessage(chatId, messageId, formatTelegramBotDetails(refreshed), {
                replyMarkup: buildTelegramBotKeyboard(refreshed),
              });
              await sendTelegramDirectMessage(chatId, buildTelegramBotActionResultText(refreshed, "sweep", result), {
                replyMarkup: buildTelegramBotKeyboard(refreshed),
              });
            } else {
              const token = await getTelegramBotDetails(linkedUserId, tokenId);
              await editTelegramMessage(chatId, messageId, formatTelegramBotDetails(token), {
                replyMarkup: buildTelegramBotKeyboard(token),
              });
            }
          } else if (dataKey.startsWith("cfg:bot_setting:")) {
            const parts = dataKey.split(":");
            const tokenId = String(parts[2] || "").trim();
            const field = String(parts[3] || "").trim().toLowerCase();
            const action = String(parts[4] || "").trim().toLowerCase();
            const current = await getTelegramBotDetails(linkedUserId, tokenId);
            const selectedBot = normalizeModuleType(current.selectedBot, MODULE_TYPES.burn);
            let payload = {};
            if (field === "claim") {
              payload.claimSec = stepTelegramPresetValue(current.claimSec, [60, 120, 300, 600, 900, 1800], action);
            } else if (field === "burn" && selectedBot === MODULE_TYPES.burn) {
              payload.burnSec = stepTelegramPresetValue(current.burnSec, [60, 120, 300, 600, 900, 1800], action);
            } else if (field === "splits" && selectedBot === MODULE_TYPES.burn) {
              payload.splits = stepTelegramPresetValue(current.splits, [1, 2, 3, 5, 10], "inc");
            } else if (field === "aggression" && isTradeBotModuleType(selectedBot)) {
              const delta = action === "dec" ? -10 : 10;
              payload.aggression = Math.max(0, Math.min(100, Number(current.moduleConfig?.aggression || 35) + delta));
            } else if (field === "min_trade" && isTradeBotModuleType(selectedBot)) {
              payload.minTradeSol = stepTelegramPresetValue(
                current.moduleConfig?.minTradeSol,
                [0.001, 0.005, 0.01, 0.02, 0.05, 0.1],
                action
              );
            } else if (field === "wallets" && isTradeBotModuleType(selectedBot)) {
              const currentWallets = Math.max(1, Math.min(5, Number(current.moduleConfig?.tradeWalletCount || 1)));
              payload.tradeWalletCount = currentWallets >= 5 ? 1 : currentWallets + 1;
            } else if (field === "inventory" && selectedBot === MODULE_TYPES.marketMaker) {
              payload.targetInventoryPct = stepTelegramPresetValue(
                current.moduleConfig?.targetInventoryPct,
                [20, 30, 40, 50, 60, 70, 80],
                "inc"
              );
            }
            const updated = await updateToken(linkedUserId, tokenId, payload);
            const refreshed = await getTelegramBotDetails(linkedUserId, updated.id);
            await editTelegramMessage(chatId, messageId, buildTelegramBotSettingsText(refreshed), {
              replyMarkup: buildTelegramBotSettingsKeyboard(refreshed),
            });
          } else if (dataKey.startsWith("cfg:bot_mode:")) {
            const tokenId = String(dataKey.split(":")[2] || "").trim();
            const current = await getTelegramBotDetails(linkedUserId, tokenId);
            const updated = await updateToken(linkedUserId, tokenId, {
              selectedBot: cycleTelegramBotMode(current.selectedBot),
            });
            const refreshed = await getTelegramBotDetails(linkedUserId, updated.id);
            await editTelegramMessage(chatId, messageId, formatTelegramBotDetails(refreshed), {
              replyMarkup: buildTelegramBotKeyboard(refreshed),
            });
          } else if (dataKey.startsWith("tool:funding:")) {
            const toolId = String(dataKey.split(":")[2] || "").trim();
            const details = await getToolInstanceDetails(linkedUserId, toolId);
            await editTelegramMessage(chatId, messageId, buildTelegramToolFundingText(details), {
              replyMarkup: buildTelegramToolFundingKeyboard(details),
            });
          } else if (dataKey.startsWith("tool:wallets:")) {
            const toolId = String(dataKey.split(":")[2] || "").trim();
            const details = await getToolInstanceDetails(linkedUserId, toolId);
            await editTelegramMessage(chatId, messageId, buildTelegramToolWalletsText(details), {
              replyMarkup: buildTelegramToolWalletsKeyboard(details),
            });
          } else if (dataKey.startsWith("tool:wallet_keys:")) {
            const toolId = String(dataKey.split(":")[2] || "").trim();
            const payload = await getToolManagedWalletSecretsForOwner(linkedUserId, toolId);
            await editTelegramMessage(chatId, messageId, buildTelegramToolWalletKeysText(payload), {
              replyMarkup: buildTelegramFieldPromptKeyboard(`tool:wallets:${toolId}`),
            });
          } else if (dataKey.startsWith("tool:events:")) {
            const toolId = String(dataKey.split(":")[2] || "").trim();
            const details = await getToolInstanceDetails(linkedUserId, toolId);
            await editTelegramMessage(chatId, messageId, buildTelegramToolEventsText(details), {
              replyMarkup: buildTelegramToolEventsKeyboard(details),
            });
          } else if (dataKey.startsWith("tool:key:")) {
            const toolId = String(dataKey.split(":")[2] || "").trim();
            const fundingWallet = await getToolFundingSecretForOwner(linkedUserId, toolId);
            await editTelegramMessage(chatId, messageId, buildTelegramToolFundingKeyText(fundingWallet), {
              replyMarkup: buildTelegramFieldPromptKeyboard(`tool:${toolId}`),
            });
          } else if (dataKey.startsWith("tool:")) {
            const toolId = String(dataKey.slice(5) || "").trim();
            const details = await getToolInstanceDetails(linkedUserId, toolId);
            await editTelegramMessage(chatId, messageId, formatTelegramToolDetails(details), {
              replyMarkup: buildTelegramToolKeyboard(details),
            });
          } else if (dataKey.startsWith("tool_settings:")) {
            const toolId = String(dataKey.split(":")[1] || "").trim();
            const details = await getToolInstanceDetails(linkedUserId, toolId);
            await editTelegramMessage(chatId, messageId, buildTelegramToolSettingsText(details), {
              replyMarkup: buildTelegramToolSettingsKeyboard(details),
            });
          } else if (dataKey.startsWith("cfg:tool_funding:")) {
            const parts = dataKey.split(":");
            const toolId = String(parts[2] || "").trim();
            const current = await getToolInstanceDetails(linkedUserId, toolId);
            const currentMode = String(current?.tool?.config?.fundingMode || "direct").trim().toLowerCase();
            const nextMode = currentMode === TOOL_FUNDING_MODES.ember ? TOOL_FUNDING_MODES.direct : TOOL_FUNDING_MODES.ember;
            const updated = await updateToolInstance(linkedUserId, toolId, {
              simpleMode: Boolean(current?.tool?.simpleMode),
              title: current?.tool?.title || current?.tool?.label || "",
              config: {
                ...(current?.tool?.config || {}),
                fundingMode: nextMode,
              },
            });
            await editTelegramMessage(chatId, messageId, formatTelegramToolDetails(updated), {
              replyMarkup: buildTelegramToolKeyboard(updated),
            });
          } else if (dataKey.startsWith("cfg:tool_setting:")) {
            const parts = dataKey.split(":");
            const toolId = String(parts[2] || "").trim();
            const field = String(parts[3] || "").trim().toLowerCase();
            const action = String(parts[4] || "").trim().toLowerCase();
            const current = await getToolInstanceDetails(linkedUserId, toolId);
            const tool = current?.tool || {};
            const configJson = tool?.config && typeof tool.config === "object" ? { ...tool.config } : {};

            if (field === "imports" && action === "prompt" && tool.toolType === TOOL_TYPES.bundleManager) {
              await upsertTelegramFlowState(linkedUserId, "tool_settings", `bundle_imports:${toolId}`, {
                title: tool.title || tool.label || "",
                simpleMode: Boolean(tool.simpleMode),
                config: configJson,
              });
              await editTelegramMessage(chatId, messageId, buildTelegramBundleImportPrompt(), {
                replyMarkup: buildTelegramFieldPromptKeyboard(`tool_settings:${toolId}`),
              });
              await answerTelegramCallbackQuery(callbackId);
              continue;
            }

            if (tool.toolType === TOOL_TYPES.holderPooler) {
              if (field === "wallets") {
                configJson.walletCount = Math.max(1, Math.min(50, Number(configJson.walletCount || 10) + (action === "dec" ? -1 : 1)));
              } else if (field === "tokens") {
                configJson.tokenAmountPerWallet = stepTelegramDecimal(configJson.tokenAmountPerWallet || 1, 1, 1, 1000000000, action, 2);
              }
            } else if (tool.toolType === TOOL_TYPES.reactionManager) {
              if (field === "reaction") {
                configJson.reactionType = cycleTelegramValue(String(configJson.reactionType || "rocket"), ["rocket", "fire", "poop", "flag"]);
              } else if (field === "target") {
                configJson.targetCount = stepTelegramPresetValue(configJson.targetCount || 1000, [1000, 2500, 5000, 10000, 25000, 50000, 100000], action);
              }
            } else if (tool.toolType === TOOL_TYPES.smartSell) {
              if (field === "funding") {
                configJson.fundingMode = normalizeToolFundingMode(
                  configJson.fundingMode === TOOL_FUNDING_MODES.ember ? TOOL_FUNDING_MODES.direct : TOOL_FUNDING_MODES.ember,
                  TOOL_FUNDING_MODES.direct
                );
              } else if (field === "trigger") {
                configJson.triggerMode = cycleTelegramValue(String(configJson.triggerMode || "every_buy"), ["every_buy", "threshold"]);
              } else if (field === "sellpct") {
                configJson.sellPct = Math.max(1, Math.min(100, Number(configJson.sellPct || 25) + (action === "dec" ? -5 : 5)));
              } else if (field === "threshold") {
                configJson.thresholdSol = stepTelegramDecimal(configJson.thresholdSol || 0, 0.1, 0, 1000, action, 3);
              } else if (field === "timing") {
                configJson.timingMode = cycleTelegramValue(String(configJson.timingMode || "randomized"), ["instant", "randomized", "split"]);
              } else if (field === "wallets") {
                configJson.walletCount = Math.max(1, Math.min(50, Number(configJson.walletCount || 6) + (action === "dec" ? -1 : 1)));
              }
            } else if (tool.toolType === TOOL_TYPES.bundleManager) {
              if (field === "funding") {
                configJson.fundingMode = normalizeToolFundingMode(
                  configJson.fundingMode === TOOL_FUNDING_MODES.ember ? TOOL_FUNDING_MODES.direct : TOOL_FUNDING_MODES.ember,
                  TOOL_FUNDING_MODES.direct
                );
              } else if (field === "walletmode") {
                configJson.walletMode = cycleTelegramValue(String(configJson.walletMode || "managed"), ["managed", "imported"]);
              } else if (field === "wallets") {
                configJson.walletCount = Math.max(1, Math.min(50, Number(configJson.walletCount || 10) + (action === "dec" ? -1 : 1)));
              } else if (field === "buysol") {
                configJson.buySolPerWallet = stepTelegramDecimal(configJson.buySolPerWallet || 0.01, 0.01, 0, 100, action, 3);
              } else if (field === "sellpct") {
                configJson.sellPctPerWallet = Math.max(1, Math.min(100, Number(configJson.sellPctPerWallet || 25) + (action === "dec" ? -5 : 5)));
              } else if (field === "bias") {
                configJson.sideBias = Math.max(0, Math.min(100, Number(configJson.sideBias || 50) + (action === "dec" ? -10 : 10)));
              } else if (field === "reserve") {
                configJson.walletReserveSol = stepTelegramDecimal(configJson.walletReserveSol || 0.0015, 0.0005, 0.0005, 0.02, action, 4);
              }
            }

            const updated = await updateToolInstance(linkedUserId, toolId, {
              simpleMode: Boolean(tool.simpleMode),
              title: tool.title || tool.label || "",
              config: configJson,
            });
            await editTelegramMessage(chatId, messageId, buildTelegramToolSettingsText(updated), {
              replyMarkup: buildTelegramToolSettingsKeyboard(updated),
            });
          } else if (dataKey.startsWith("act:")) {
            const parts = dataKey.split(":");
            const action = String(parts[1] || "");
            const toolId = String(parts[2] || "");
            let details = null;
            if (action === "bot_toggle") {
              const desired = String(parts[3] || "").trim().toLowerCase();
              const token = await updateToken(linkedUserId, toolId, { active: desired === "start" });
              const refreshed = await getTelegramBotDetails(linkedUserId, token.id);
              await editTelegramMessage(chatId, messageId, formatTelegramBotDetails(refreshed), {
                replyMarkup: buildTelegramBotKeyboard(refreshed),
              });
            } else if (action === "holder_run") {
              details = await runHolderPoolerDistribution(linkedUserId, toolId);
            } else if (action === "reaction_run") {
              details = await runReactionManagerCampaign(linkedUserId, toolId);
            } else if (action === "reaction_status") {
              details = await refreshReactionManagerStatus(linkedUserId, toolId);
            } else if (action === "tool_funding_refresh") {
              details = await refreshToolFunding(linkedUserId, toolId);
            } else if (action === "smart_arm") {
              details = await armSmartSell(linkedUserId, toolId);
            } else if (action === "smart_pause") {
              details = await pauseSmartSell(linkedUserId, toolId);
            } else if (action === "smart_reclaim") {
              details = await reclaimSmartSell(linkedUserId, toolId);
            } else if (action === "bundle_run") {
              details = await runBundleManagerCampaign(linkedUserId, toolId);
            } else if (action === "bundle_reclaim") {
              details = await reclaimBundleManager(linkedUserId, toolId);
            }
            if (details) {
              await editTelegramMessage(chatId, messageId, formatTelegramToolDetails(details), {
                replyMarkup: buildTelegramToolKeyboard(details),
              });
            }
          }
          await answerTelegramCallbackQuery(callbackId);
        } catch (error) {
          await answerTelegramCallbackQuery(callbackId, String(error?.message || error || "Action failed").slice(0, 180));
        }
        continue;
      }
      const message = update?.message;
      const text = String(message?.text || "").trim();
      const chatType = String(message?.chat?.type || "").trim().toLowerCase();
      if (chatType !== "private") continue;
      const chatId = String(message?.chat?.id || "").trim();
      const linkedUserId = chatId ? await getTelegramLinkedUserId(chatId) : 0;
      const flow = linkedUserId ? await getTelegramFlowState(linkedUserId) : null;
      const isCommand = text.startsWith("/");
      if (linkedUserId && flow && !isCommand) {
        try {
          if (flow.flowType === "tool_create" && flow.step === "await_target") {
            const toolType = normalizeToolType(flow.state?.toolType, TOOL_TYPES.holderPooler);
            const targetRaw = text;
            const tool = await createToolInstance(linkedUserId, {
              toolType,
              title: toolTypeLabel(toolType),
              targetMint: toolType === TOOL_TYPES.reactionManager ? "" : targetRaw,
              targetUrl: toolType === TOOL_TYPES.reactionManager ? targetRaw : "",
              simpleMode: true,
            });
            const details = await getToolInstanceDetails(linkedUserId, tool.id);
            await clearTelegramFlowState(linkedUserId);
            await sendTelegramDirectMessage(
              chatId,
              [
                `\u2705 ${tool.label} created.`,
                `Funding wallet: ${tool.fundingWalletPubkey}`,
                `Required: ${tool.requiredSol.toFixed(6)} SOL`,
              ].join("\n"),
              { replyMarkup: buildTelegramToolKeyboard(details) }
            );
            if (details?.permissions?.canManageFunds) {
              const fundingWallet = await getToolFundingSecretForOwner(linkedUserId, tool.id);
              await sendTelegramDirectMessage(chatId, buildTelegramToolFundingKeyText(fundingWallet), {
                replyMarkup: buildTelegramFieldPromptKeyboard(`tool:${tool.id}`),
              });
            }
            continue;
          }
          if (flow.flowType === "attach" && String(flow.step || "").startsWith("field:")) {
            const field = String(flow.step.split(":")[1] || "").trim().toLowerCase();
            const nextFlow = { ...defaultTelegramAttachState(), ...(flow.state || {}) };
            if (field === "mint") {
              const metadata = await resolveMintMetadata(text);
              nextFlow.mint = String(metadata?.mint || text || "").trim();
              nextFlow.name = String(metadata?.name || "").trim();
              nextFlow.symbol = String(metadata?.symbol || "").trim().toUpperCase();
              nextFlow.pictureUrl = normalizeMediaUrl(metadata?.pictureUrl || "").slice(0, 255);
              nextFlow.pendingDepositId = "";
              nextFlow.deposit = "";
            }
            await upsertTelegramFlowState(linkedUserId, "attach", "config", nextFlow);
            await sendTelegramDirectMessage(chatId, buildTelegramAttachText(nextFlow), {
              replyMarkup: buildTelegramAttachKeyboard(nextFlow),
            });
            continue;
          }
          if (flow.flowType === "trade" && flow.step === "field:buy_amount") {
            const amount = Number(text);
            if (!Number.isFinite(amount) || amount <= 0) {
              throw new Error("Enter a valid SOL amount.");
            }
            const details = await executeTelegramTradeBuy(linkedUserId, amount);
            await clearTelegramFlowState(linkedUserId);
            await sendTelegramDirectMessage(chatId, buildTelegramTradeDetailsText(details), {
              replyMarkup: buildTelegramTradeActionKeyboard(details),
            });
            continue;
          }
          if (flow.flowType === "trade" && flow.step === "field:sell_percent") {
            const percent = Number(text);
            if (!Number.isFinite(percent) || percent <= 0 || percent > 100) {
              throw new Error("Enter a valid percent from 1 to 100.");
            }
            const details = await executeTelegramTradeSellPct(linkedUserId, percent);
            await clearTelegramFlowState(linkedUserId);
            await sendTelegramDirectMessage(chatId, buildTelegramTradeDetailsText(details), {
              replyMarkup: buildTelegramTradeActionKeyboard(details),
            });
            continue;
          }
          if (flow.flowType === "trade" && flow.step === "field:withdraw_destination") {
            const details = await executeTelegramTradeWithdraw(linkedUserId, text);
            await clearTelegramFlowState(linkedUserId);
            await sendTelegramDirectMessage(chatId, buildTelegramTradeDetailsText(details), {
              replyMarkup: buildTelegramTradeActionKeyboard(details),
            });
            continue;
          }
          if (flow.flowType === "trade" && flow.step === "field:import_wallet") {
            const details = await importTelegramTradeWalletSlot(linkedUserId, text);
            await clearTelegramFlowState(linkedUserId);
            const wallet = await getTelegramTradeWalletSecret(linkedUserId, details?.selectedWallet?.id || "");
            await sendTelegramDirectMessage(chatId, buildTelegramTradeDetailsText(details), {
              replyMarkup: buildTelegramTradeActionKeyboard(details),
            });
            await sendTelegramDirectMessage(chatId, buildTelegramTradeWalletKeyText(wallet), {
              replyMarkup: buildTelegramFieldPromptKeyboard("menu:trade"),
            });
            continue;
          }
          if (flow.flowType === "deploy" && String(flow.step || "").startsWith("field:")) {
            const field = String(flow.step.split(":")[1] || "").trim().toLowerCase();
            const nextFlow = { ...defaultTelegramDeployState(), ...(flow.state || {}) };
            if (field === "image" || field === "banner") {
              const photo = Array.isArray(message?.photo) && message.photo.length ? message.photo[message.photo.length - 1] : null;
              const document = message?.document;
              const fileId = String(
                photo?.file_id ||
                  (((String(document?.mime_type || "").trim().toLowerCase().startsWith("image/")) && document?.file_id) || "")
              ).trim();
              if (!fileId) {
                throw new Error(field === "banner" ? "Send a photo or image document for the token banner." : "Send a photo or image document for the token image.");
              }
              const uploaded = await fetchTelegramFileDataUri(
                fileId,
                field === "banner" ? (nextFlow.bannerFileName || "banner") : (nextFlow.symbol || nextFlow.name || "token")
              );
              if (field === "banner") {
                nextFlow.bannerDataUri = uploaded.dataUri;
                nextFlow.bannerFileName = uploaded.fileName;
              } else {
                nextFlow.imageDataUri = uploaded.dataUri;
                nextFlow.imageFileName = uploaded.fileName;
              }
            } else {
              const normalizedText = String(text || "").trim();
              if (!normalizedText) {
                throw new Error("Reply with a value or tap cancel.");
              }
              if (["twitter", "telegram", "website", "banner"].includes(field) && normalizedText.toLowerCase() === "skip") {
                if (field === "banner") {
                  nextFlow.bannerDataUri = "";
                  nextFlow.bannerFileName = "banner";
                } else {
                  nextFlow[field] = "";
                }
              } else if (field === "symbol") {
                nextFlow.symbol = normalizedText.toUpperCase().slice(0, 12);
              } else if (field === "name") {
                nextFlow.name = normalizedText.slice(0, 40);
              } else if (field === "description") {
                nextFlow.description = normalizedText.slice(0, 300);
              } else if (["twitter", "telegram", "website"].includes(field)) {
                nextFlow[field] = normalizedText;
              }
            }
            await upsertTelegramFlowState(linkedUserId, "deploy", "config", nextFlow);
            await sendTelegramDirectMessage(chatId, buildTelegramDeployText(nextFlow), {
              replyMarkup: buildTelegramDeployKeyboard(nextFlow),
            });
            continue;
          }
          if (flow.flowType === "tool_settings" && String(flow.step || "").startsWith("bundle_imports:")) {
            const toolId = String(flow.step.split(":")[1] || "").trim();
            const importedWallets = String(text || "")
              .split(/[\n,\r]+/)
              .map((value) => value.trim())
              .filter(Boolean);
            if (!importedWallets.length) {
              throw new Error("Paste one or more base58 private keys, one per line.");
            }
            const updated = await updateToolInstance(linkedUserId, toolId, {
              simpleMode: Boolean(flow.state?.simpleMode),
              title: String(flow.state?.title || "").trim(),
              config: {
                ...(flow.state?.config || {}),
                walletMode: "imported",
                importedWallets,
              },
            });
            await clearTelegramFlowState(linkedUserId);
            await sendTelegramDirectMessage(chatId, buildTelegramToolSettingsText(updated), {
              replyMarkup: buildTelegramToolSettingsKeyboard(updated),
            });
            continue;
          }
          if (flow.flowType === "trade" && flow.step === "field:mint") {
            const details = await setTelegramTradeMint(linkedUserId, text);
            await clearTelegramFlowState(linkedUserId);
            await sendTelegramDirectMessage(chatId, buildTelegramTradeDetailsText(details), {
              replyMarkup: buildTelegramTradeActionKeyboard(details),
            });
            continue;
          }
          if (flow.flowType === "bot" && flow.step === "field:withdraw_destination") {
            const tokenId = String(flow.state?.tokenId || "").trim();
            if (!tokenId) {
              throw new Error("Bot token is missing from this action.");
            }
            const token = await getTelegramBotDetails(linkedUserId, tokenId);
            if (!token?.permissions?.canManageFunds) {
              throw new Error("Managers cannot withdraw bot funds.");
            }
            const selectedBot = normalizeModuleType(token.selectedBot, MODULE_TYPES.burn);
            const result = selectedBot === MODULE_TYPES.burn
              ? await withdrawBurnFunds(linkedUserId, tokenId, { destination: text })
              : await withdrawVolumeFunds(linkedUserId, tokenId, { destination: text });
            await clearTelegramFlowState(linkedUserId);
            const refreshed = await getTelegramBotDetails(linkedUserId, tokenId);
            await sendTelegramDirectMessage(chatId, buildTelegramBotActionResultText(refreshed, "withdraw", result), {
              replyMarkup: buildTelegramBotKeyboard(refreshed),
            });
            continue;
          }
        } catch (error) {
          await sendTelegramDirectMessage(chatId, `EMBER: ${error?.message || error}`);
          continue;
        }
      }
      if (text.startsWith("/tools")) {
        if (!linkedUserId) {
          await sendTelegramDirectMessage(chatId, "EMBER: connect your dashboard account first, then open Tools again.");
          continue;
        }
        try {
          const overview = await getTelegramToolOverview(linkedUserId);
          await sendTelegramDirectMessage(chatId, buildTelegramToolsOverviewText(overview), {
            replyMarkup: buildTelegramToolsKeyboard(overview),
          });
        } catch (error) {
          await sendTelegramDirectMessage(chatId, `EMBER: unable to load tools right now (${error?.message || error}).`);
        }
        continue;
      }
      if (text.startsWith("/bots")) {
        if (!linkedUserId) {
          await sendTelegramDirectMessage(chatId, "EMBER: connect your dashboard account first, then open Bots again.");
          continue;
        }
        try {
          const overview = await getTelegramBotOverview(linkedUserId);
          await sendTelegramDirectMessage(chatId, buildTelegramBotOverviewText(overview), {
            replyMarkup: buildTelegramBotsKeyboard(overview),
          });
        } catch (error) {
          await sendTelegramDirectMessage(chatId, `EMBER: unable to load bots right now (${error?.message || error}).`);
        }
        continue;
      }
      if (text.startsWith("/deploy")) {
        if (!linkedUserId) {
          await sendTelegramDirectMessage(chatId, "EMBER: connect your dashboard account first, then open Deploy again.");
          continue;
        }
        const flow = defaultTelegramDeployState();
        await upsertTelegramFlowState(linkedUserId, "deploy", "config", flow);
        await sendTelegramDirectMessage(chatId, buildTelegramDeployText(flow), {
          replyMarkup: buildTelegramDeployKeyboard(flow),
        });
        continue;
      }
      if (text.startsWith("/trade")) {
        if (!linkedUserId) {
          await sendTelegramDirectMessage(chatId, "EMBER: connect your dashboard account first, then open Trade again.");
          continue;
        }
        const details = await getTelegramTradeDetails(linkedUserId);
        await sendTelegramDirectMessage(chatId, buildTelegramTradeDetailsText(details), {
          replyMarkup: buildTelegramTradeActionKeyboard(details),
        });
        continue;
      }
      if (text.startsWith("/tool_new")) {
        if (!linkedUserId) {
          await sendTelegramDirectMessage(chatId, "EMBER: connect your dashboard account first, then open Tools and create a tool there.");
          continue;
        }
        const parts = text.split(/\s+/).filter(Boolean);
        const toolTypeRaw = String(parts[1] || "").trim().toLowerCase();
        const targetRaw = parts.slice(2).join(" ").trim();
        if (!toolTypeRaw || !targetRaw) {
          await sendTelegramDirectMessage(chatId, "EMBER: open Tools from the menu and create a new tool there.");
          continue;
        }
        try {
          const tool = await createToolInstance(linkedUserId, {
            toolType: toolTypeRaw,
            title: toolTypeLabel(toolTypeRaw),
            targetMint: toolTypeRaw === TOOL_TYPES.reactionManager ? "" : targetRaw,
            targetUrl: toolTypeRaw === TOOL_TYPES.reactionManager ? targetRaw : "",
            simpleMode: true,
          });
          const details = await getToolInstanceDetails(linkedUserId, tool.id);
          await sendTelegramDirectMessage(
            chatId,
            [
              `${tool.label} created.`,
              `Tool id: ${tool.id}`,
              `Funding wallet: ${tool.fundingWalletPubkey}`,
              `Required: ${tool.requiredSol.toFixed(6)} SOL`,
            ].join("\n"),
            { replyMarkup: buildTelegramToolKeyboard(details) }
          );
          if (details?.permissions?.canManageFunds) {
            const fundingWallet = await getToolFundingSecretForOwner(linkedUserId, tool.id);
            await sendTelegramDirectMessage(chatId, buildTelegramToolFundingKeyText(fundingWallet), {
              replyMarkup: buildTelegramFieldPromptKeyboard(`tool:${tool.id}`),
            });
          }
        } catch (error) {
          await sendTelegramDirectMessage(chatId, `EMBER: ${error?.message || error}`);
        }
        continue;
      }
      if (
        text.startsWith("/holder_run") ||
        text.startsWith("/reaction_run") ||
        text.startsWith("/reaction_status") ||
        text.startsWith("/smart_arm") ||
        text.startsWith("/smart_pause") ||
        text.startsWith("/smart_reclaim") ||
        text.startsWith("/bundle_run") ||
        text.startsWith("/bundle_reclaim")
      ) {
        if (!linkedUserId) {
          await sendTelegramDirectMessage(chatId, "EMBER: connect your dashboard account first.");
          continue;
        }
        const [, toolIdRaw] = text.split(/\s+/, 2);
        const toolId = String(toolIdRaw || "").trim();
        if (!toolId) {
          await sendTelegramDirectMessage(chatId, "EMBER: open Tools from the menu and pick the instance you want.");
          continue;
        }
        try {
          let details = null;
          if (text.startsWith("/holder_run")) {
            details = await runHolderPoolerDistribution(linkedUserId, toolId);
          } else if (text.startsWith("/reaction_run")) {
            details = await runReactionManagerCampaign(linkedUserId, toolId);
          } else if (text.startsWith("/reaction_status")) {
            details = await refreshReactionManagerStatus(linkedUserId, toolId);
          } else if (text.startsWith("/smart_arm")) {
            details = await armSmartSell(linkedUserId, toolId);
          } else if (text.startsWith("/smart_pause")) {
            details = await pauseSmartSell(linkedUserId, toolId);
          } else if (text.startsWith("/smart_reclaim")) {
            details = await reclaimSmartSell(linkedUserId, toolId);
          } else if (text.startsWith("/bundle_run")) {
            details = await runBundleManagerCampaign(linkedUserId, toolId);
          } else if (text.startsWith("/bundle_reclaim")) {
            details = await reclaimBundleManager(linkedUserId, toolId);
          }
          await sendTelegramDirectMessage(chatId, formatTelegramToolDetails(details), {
            replyMarkup: buildTelegramToolKeyboard(details),
          });
        } catch (error) {
          await sendTelegramDirectMessage(chatId, `EMBER: ${error?.message || error}`);
        }
        continue;
      }
      if (text.startsWith("/tool")) {
        if (!linkedUserId) {
          await sendTelegramDirectMessage(chatId, "EMBER: connect your dashboard account first.");
          continue;
        }
        const [, toolIdRaw] = text.split(/\s+/, 2);
        const toolId = String(toolIdRaw || "").trim();
        if (!toolId) {
          await sendTelegramDirectMessage(chatId, "EMBER: open Tools from the menu and pick the instance you want.");
          continue;
        }
        try {
          const details = await getToolInstanceDetails(linkedUserId, toolId);
          await sendTelegramDirectMessage(chatId, formatTelegramToolDetails(details), {
            replyMarkup: buildTelegramToolKeyboard(details),
          });
        } catch (error) {
          await sendTelegramDirectMessage(chatId, `EMBER: ${error?.message || error}`);
        }
        continue;
      }
      if (text.startsWith("/menu")) {
        if (!linkedUserId) {
          await sendTelegramDirectMessage(chatId, "EMBER: connect your dashboard account first.");
          continue;
        }
        const stats = await getTelegramMainMenuStats(linkedUserId);
        await sendTelegramDirectMessage(chatId, buildTelegramMainMenuText(stats), {
          replyMarkup: buildTelegramMainKeyboard(stats),
        });
        continue;
      }
      if (!isCommand) {
        if (linkedUserId) {
          const stats = await getTelegramMainMenuStats(linkedUserId);
          await sendTelegramDirectMessage(chatId, buildTelegramMainMenuText(stats), {
            replyMarkup: buildTelegramMainKeyboard(stats),
          });
        } else if (text) {
          await sendTelegramDirectMessage(
            chatId,
            "EMBER: open your dashboard and use the Telegram connect link there first."
          );
        }
        continue;
      }
      if (!text.startsWith("/start")) continue;
      const connectToken = text.split(/\s+/, 2)[1]?.trim();
      if (!connectToken) {
        const chatId = String(message?.chat?.id || "").trim();
        const linkedUserId = await getTelegramLinkedUserId(chatId);
        if (linkedUserId) {
          const stats = await getTelegramMainMenuStats(linkedUserId);
          await sendTelegramDirectMessage(chatId, buildTelegramMainMenuText(stats), {
            replyMarkup: buildTelegramMainKeyboard(stats),
          });
        } else {
          await sendTelegramDirectMessage(
            chatId,
            "EMBER: open your dashboard and use the Telegram connect link there first."
          );
        }
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
        const refreshedLinkedUserId = await getTelegramLinkedUserId(String(message?.chat?.id || ""));
        const stats = refreshedLinkedUserId ? await getTelegramMainMenuStats(refreshedLinkedUserId) : null;
        await sendTelegramDirectMessage(
          String(message?.chat?.id || ""),
          [
            "EMBER alerts connected.",
            "You will now receive direct alerts for the dashboard account linked from the site.",
            "Adjust alert settings inside the EMBER dashboard anytime.",
          ].join("\n"),
          { replyMarkup: buildTelegramMainKeyboard(stats) }
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
  const workerProtocolSettings = await getProtocolSettings(pool).catch(() => DEFAULT_PROTOCOL_SETTINGS);
  if (Boolean(workerProtocolSettings.maintenanceEnabled) && String(workerProtocolSettings.maintenanceMode) === "hard") {
    await processTelegramConnectUpdates();
    await deliverPendingTelegramAlerts();
    return {
      dueTokens: 0,
      eventsCreated: 0,
      enqueuedJobs: 0,
      executedJobs: 0,
      maintenanceMode: "hard",
    };
  }
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

  try {
    eventsCreated += await processActiveSmartSellTools();
  } catch (error) {
    console.warn(`[smart-sell] runtime failed: ${error?.message || error}`);
  }

  const now = Date.now();
  if (now - lastPersonalBurnRunAt >= personalBurnIntervalMs) {
    const client = await pool.connect();
    try {
      const protocolSettings = workerProtocolSettings;
      let result = { txCreated: 0 };
      if (protocolSettings.personalBotEnabled !== false) {
        if (protocolSettings.personalBotMode === MODULE_TYPES.volume) {
          result = await runPersonalVolumeExecutor(client, protocolSettings);
        } else if (protocolSettings.personalBotMode === MODULE_TYPES.marketMaker) {
          result = await runPersonalMarketMakerExecutor(client, protocolSettings);
        } else if (protocolSettings.personalBotMode === MODULE_TYPES.dca) {
          result = await runPersonalDcaExecutor(client, protocolSettings);
        } else if (protocolSettings.personalBotMode === MODULE_TYPES.rekindle) {
          result = await runPersonalRekindleExecutor(client, protocolSettings);
        } else {
          result = await runPersonalBurnExecutor(client, protocolSettings);
        }
      }
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


