import { API_BASE } from "../config/site";

/** @typedef {{ id: string, symbol: string, name: string, mint: string, pictureUrl: string, deposit: string, claimSec: number, burnSec: number, splits: number, selectedBot: string, active: boolean, disconnected: boolean, burned: number, pending: number, txCount: number, marketCap: number, moduleType: string, moduleEnabled: boolean, moduleConfig: Record<string, any>, moduleState: Record<string, any>, moduleLastError: string }} Token */
/** @typedef {{ id: number|string, tokenId: string|null, token: string, moduleType: string, type: string, amount: number, msg: string, tx: string|null, age: number, createdAt: string }} FeedEvent */
/** @typedef {{ key: string, d: string, v: number }} ChartPoint */
/** @typedef {{ tokens: Token[], feed: FeedEvent[], logs: FeedEvent[], chartData: ChartPoint[] }} DashboardResponse */
/** @typedef {{ lifetimeIncinerated: number, totalBotTransactions: number, activeTokens: number, totalHolders: number, emberIncinerated: number, totalRewardsProcessedSol: number, totalFeesTakenSol: number }} PublicMetrics */

function apiUrl(path) {
  return `${API_BASE}${path}`;
}

function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function str(value, fallback = "") {
  return typeof value === "string" ? value : fallback;
}

function num(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function bool(value, fallback = false) {
  return typeof value === "boolean" ? value : fallback;
}

class ApiError extends Error {
  constructor(message, status = 0, payload = null) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.payload = payload;
  }
}

export function isApiError(error) {
  return error instanceof ApiError;
}

export function isUpgradeRequiredError(error) {
  return isApiError(error) && error.status === 426;
}

async function requestJson(path, options = {}) {
  let res;
  try {
    res = await fetch(apiUrl(path), {
      credentials: "include",
      ...options,
      headers: {
        "Content-Type": "application/json",
        ...(options.headers || {}),
      },
    });
  } catch {
    throw new ApiError("Unable to reach server. Make sure the API is running.", 0);
  }

  const raw = await res.text().catch(() => "");
  let data = {};
  try {
    data = raw ? JSON.parse(raw) : {};
  } catch {
    data = {};
  }

  if (!res.ok) {
    const serverMsg = str(data.error, raw || "Request failed.").trim();
    const fallback = `Request failed (${res.status}${res.statusText ? ` ${res.statusText}` : ""}).`;
    throw new ApiError(serverMsg || fallback, res.status, data);
  }

  if (!isRecord(data)) return {};
  return data;
}

function toToken(value) {
  if (!isRecord(value)) {
    throw new ApiError("Invalid token payload from API.", 500, value);
  }
  return {
    id: str(value.id),
    symbol: str(value.symbol),
    name: str(value.name),
    mint: str(value.mint),
    pictureUrl: str(value.pictureUrl),
    deposit: str(value.deposit),
    claimSec: num(value.claimSec),
    burnSec: num(value.burnSec),
    splits: num(value.splits, 1),
    selectedBot: str(value.selectedBot, "burn"),
    active: bool(value.active),
    disconnected: bool(value.disconnected),
    burned: num(value.burned),
    pending: num(value.pending),
    txCount: num(value.txCount),
    marketCap: num(value.marketCap),
    moduleType: str(value.moduleType, str(value.selectedBot, "burn")),
    moduleEnabled: bool(value.moduleEnabled, bool(value.active)),
    moduleConfig: isRecord(value.moduleConfig) ? value.moduleConfig : {},
    moduleState: isRecord(value.moduleState) ? value.moduleState : {},
    moduleLastError: str(value.moduleLastError),
  };
}

function toEvent(value) {
  if (!isRecord(value)) {
    throw new ApiError("Invalid event payload from API.", 500, value);
  }
  return {
    id: str(value.id, String(value.id ?? "")),
    tokenId: str(value.tokenId) || null,
    token: str(value.token),
    moduleType: str(value.moduleType),
    type: str(value.type),
    amount: num(value.amount),
    msg: str(value.msg),
    tx: str(value.tx) || null,
    age: num(value.age),
    createdAt: str(value.createdAt),
  };
}

function toChartPoint(value) {
  if (!isRecord(value)) {
    throw new ApiError("Invalid chart point payload from API.", 500, value);
  }
  return {
    key: str(value.key),
    d: str(value.d),
    v: num(value.v),
  };
}

function toUserPayload(data) {
  if (!isRecord(data) || !isRecord(data.user)) {
    return { user: null };
  }
  const username = str(data.user.username);
  if (!username) return { user: null };
  return { user: { username } };
}

export async function apiAuthLogin(username, password) {
  const data = await requestJson("/api/auth/login", {
    method: "POST",
    body: JSON.stringify({ username, password }),
  });
  return toUserPayload(data);
}

export async function apiAuthRegister(username, password) {
  const data = await requestJson("/api/auth/register", {
    method: "POST",
    body: JSON.stringify({ username, password }),
  });
  return toUserPayload(data);
}

export async function apiAuthMe() {
  const data = await requestJson("/api/auth/me", { method: "GET" });
  return toUserPayload(data);
}

export async function apiAuthLogout() {
  await requestJson("/api/auth/logout", { method: "POST", headers: {} });
}

export async function apiDashboard() {
  const data = await requestJson("/api/dashboard", { method: "GET" });
  const tokens = Array.isArray(data.tokens) ? data.tokens.map(toToken) : [];
  const feed = Array.isArray(data.feed) ? data.feed.map(toEvent) : [];
  const logs = Array.isArray(data.logs) ? data.logs.map(toEvent) : [];
  const chartData = Array.isArray(data.chartData) ? data.chartData.map(toChartPoint) : [];
  return { tokens, feed, logs, chartData };
}

export async function apiPublicMetrics() {
  const data = await requestJson("/api/public-metrics", { method: "GET" });
  return {
    lifetimeIncinerated: num(data.lifetimeIncinerated),
    totalBotTransactions: num(data.totalBotTransactions ?? data.transactions ?? data.burnBuybackTransactions),
    activeTokens: num(data.activeTokens),
    totalHolders: num(data.totalHolders),
    emberIncinerated: num(data.emberIncinerated),
    totalRewardsProcessedSol: num(data.totalRewardsProcessedSol ?? data.totalRewardsProcessed),
    totalFeesTakenSol: num(data.totalFeesTakenSol ?? data.totalFeesTaken),
  };
}

export async function apiCreateToken(payload) {
  const data = await requestJson("/api/tokens", {
    method: "POST",
    body: JSON.stringify(payload),
  });
  return { token: toToken(data.token) };
}

export async function apiUpdateToken(id, payload) {
  const data = await requestJson(`/api/tokens/${id}`, {
    method: "PATCH",
    body: JSON.stringify(payload),
  });
  return { token: toToken(data.token) };
}

export async function apiTokenLiveDetails(id) {
  const data = await requestJson(`/api/tokens/${id}/details`, { method: "GET" });
  const token = isRecord(data.token) ? data.token : {};
  const addresses = Array.isArray(data.addresses) ? data.addresses : [];
  const creatorRewards = isRecord(data.creatorRewards) ? data.creatorRewards : {};
  return {
    token: {
      id: str(token.id),
      symbol: str(token.symbol),
      name: str(token.name),
      mint: str(token.mint),
      pictureUrl: str(token.pictureUrl),
      selectedBot: str(token.selectedBot, "burn"),
      active: bool(token.active),
      disconnected: bool(token.disconnected),
    },
    addresses: addresses.map((a) => ({
      type: str(a.type),
      label: str(a.label),
      pubkey: str(a.pubkey),
      solBalance: num(a.solBalance),
      tokenBalance: num(a.tokenBalance),
    })),
    totals: isRecord(data.totals)
      ? { sol: num(data.totals.sol), token: num(data.totals.token) }
      : { sol: 0, token: 0 },
    creatorRewards: {
      profileUrl: str(creatorRewards.profileUrl),
      directSol: num(creatorRewards.directSol),
      shareableSol: num(creatorRewards.shareableSol),
      distributableSol: num(creatorRewards.distributableSol),
      totalSol: num(creatorRewards.totalSol),
      shareableEnabled: bool(creatorRewards.shareableEnabled),
      isShareholder: bool(creatorRewards.isShareholder),
      shareBps: num(creatorRewards.shareBps),
      canDistribute: bool(creatorRewards.canDistribute),
      isGraduated: bool(creatorRewards.isGraduated),
    },
  };
}

export async function apiVolumeWithdrawOptions(id) {
  const data = await requestJson(`/api/tokens/${id}/volume/withdraw-options`, { method: "GET" });
  const sources = Array.isArray(data.sources) ? data.sources : [];
  return {
    deposit: str(data.deposit),
    active: bool(data.active),
    reserveSol: num(data.reserveSol),
    withdrawableSol: num(data.withdrawableSol),
    sources: sources.map((s) => ({
      wallet: str(s.wallet),
      totalSol: num(s.totalSol),
    })),
  };
}

export async function apiVolumeSweep(id) {
  const data = await requestJson(`/api/tokens/${id}/volume/sweep`, {
    method: "POST",
    body: JSON.stringify({}),
  });
  return {
    ok: bool(data.ok, true),
    walletsTotal: num(data.walletsTotal),
    walletsSwept: num(data.walletsSwept),
    soldTokens: num(data.soldTokens),
    sweptSol: num(data.sweptSol),
    txCreated: num(data.txCreated),
  };
}

export async function apiVolumeWithdraw(id, payload) {
  const data = await requestJson(`/api/tokens/${id}/volume/withdraw`, {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
  return {
    ok: bool(data.ok, true),
    signature: str(data.signature),
    sentSol: num(data.sentSol),
    remainingSol: num(data.remainingSol),
  };
}

export async function apiBurnWithdraw(id, payload) {
  const data = await requestJson(`/api/tokens/${id}/burn/withdraw`, {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
  return {
    ok: bool(data.ok, true),
    signature: str(data.signature),
    sentSol: num(data.sentSol),
    remainingSol: num(data.remainingSol),
  };
}

export async function apiDeleteToken(id) {
  const data = await requestJson(`/api/tokens/${id}`, { method: "DELETE" });
  return { ok: bool(data.ok, true) };
}

export async function apiGenerateDepositAddress() {
  const data = await requestJson("/api/tokens/generate-deposit", {
    method: "POST",
    body: JSON.stringify({}),
  });
  if (Array.isArray(data.deposits) && data.deposits.length > 0) {
    const first = isRecord(data.deposits[0]) ? data.deposits[0] : {};
    return {
      pendingDepositId: str(first.pendingDepositId),
      deposit: str(first.deposit),
    };
  }
  return {
    pendingDepositId: str(data.pendingDepositId),
    deposit: str(data.deposit),
  };
}

export async function apiResolveMint(mint) {
  const data = await requestJson("/api/tokens/resolve-mint", {
    method: "POST",
    body: JSON.stringify({ mint }),
  });
  const token = isRecord(data.token) ? data.token : {};
  return {
    mint: str(token.mint),
    symbol: str(token.symbol),
    name: str(token.name),
    pictureUrl: str(token.pictureUrl),
    marketCap: num(token.marketCap),
  };
}

export async function apiRecordDeploy(payload) {
  const data = await requestJson("/api/deploy/record", {
    method: "POST",
    body: JSON.stringify(payload),
  });
  return data;
}
