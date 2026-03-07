import { API_BASE } from "../config/site";

/** @typedef {{ id: string, symbol: string, name: string, mint: string, pictureUrl: string, deposit: string, claimSec: number, burnSec: number, splits: number, selectedBot: string, active: boolean, disconnected: boolean, burned: number, pending: number, txCount: number, marketCap: number, moduleType: string, moduleEnabled: boolean, moduleConfig: Record<string, any>, moduleState: Record<string, any>, moduleLastError: string, deployedViaEmber: boolean, deployWalletPubkey: string }} Token */
/** @typedef {{ id: number|string, tokenId: string|null, token: string, moduleType: string, type: string, amount: number, msg: string, tx: string|null, age: number, createdAt: string }} FeedEvent */
/** @typedef {{ key: string, d: string, v: number }} ChartPoint */
/** @typedef {{ tokens: Token[], feed: FeedEvent[], logs: FeedEvent[], chartData: ChartPoint[] }} DashboardResponse */
/** @typedef {{ lifetimeIncinerated: number, totalBotTransactions: number, activeTokens: number, totalHolders: number, emberIncinerated: number, totalRewardsProcessedSol: number, totalFeesTakenSol: number }} PublicMetrics */
/** @typedef {{ symbol: string, amount: number }} BurnBreakdownItem */

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
    deployedViaEmber: bool(value.deployedViaEmber),
    deployWalletPubkey: str(value.deployWalletPubkey),
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
  return {
    user: {
      id: num(data.user.id),
      username,
      ownerUserId: num(data.user.ownerUserId),
      ownerUsername: str(data.user.ownerUsername),
      role: str(data.user.role, "owner"),
      isOperator: bool(data.user.isOperator),
      isAdmin: bool(data.user.isAdmin),
      isOg: bool(data.user.isOg),
      referralCode: str(data.user.referralCode),
      feeBpsOverride: num(data.user.feeBpsOverride, 0),
      canManageFunds: bool(data.user.canManageFunds, true),
      canDelete: bool(data.user.canDelete, true),
      canManageAccess: bool(data.user.canManageAccess, true),
    },
  };
}

export async function apiAuthLogin(username, password) {
  const data = await requestJson("/api/auth/login", {
    method: "POST",
    body: JSON.stringify({ username, password }),
  });
  return toUserPayload(data);
}

export async function apiAuthRegister(username, password, referralCode = "") {
  const data = await requestJson("/api/auth/register", {
    method: "POST",
    body: JSON.stringify({ username, password, referralCode }),
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

export async function apiManagerAccess() {
  const data = await requestJson("/api/auth/manager-access", { method: "GET" });
  return {
    enabled: bool(data.enabled),
    username: str(data.username),
    userId: num(data.userId),
  };
}

export async function apiUpsertManagerAccess(payload) {
  const data = await requestJson("/api/auth/manager-access", {
    method: "PUT",
    body: JSON.stringify(payload || {}),
  });
  return {
    enabled: bool(data.enabled, true),
    username: str(data.username),
    userId: num(data.userId),
    user: toUserPayload(data).user,
  };
}

export async function apiDeleteManagerAccess() {
  const data = await requestJson("/api/auth/manager-access", { method: "DELETE" });
  return {
    ok: bool(data.ok, true),
    removed: bool(data.removed),
    user: toUserPayload(data).user,
  };
}

export async function apiTelegramAlerts() {
  const data = await requestJson("/api/alerts/telegram", { method: "GET" });
  return {
    connected: bool(data.connected),
    chatIdMasked: str(data.chatIdMasked),
    telegramUsername: str(data.telegramUsername),
    firstName: str(data.firstName),
    lastName: str(data.lastName),
    connectedAt: str(data.connectedAt),
    updatedAt: str(data.updatedAt),
    botUsername: str(data.botUsername),
    connectToken: str(data.connectToken),
    connectUrl: str(data.connectUrl),
    prefs: isRecord(data.prefs) ? data.prefs : {},
  };
}

export async function apiUpdateTelegramAlerts(payload) {
  const data = await requestJson("/api/alerts/telegram", {
    method: "PATCH",
    body: JSON.stringify(payload || {}),
  });
  return {
    prefs: isRecord(data.prefs) ? data.prefs : {},
  };
}

export async function apiTelegramTestAlert() {
  const data = await requestJson("/api/alerts/telegram/test", {
    method: "POST",
    body: JSON.stringify({}),
  });
  return {
    ok: bool(data.ok, true),
  };
}

export async function apiDisconnectTelegramAlerts() {
  const data = await requestJson("/api/alerts/telegram", {
    method: "DELETE",
  });
  return {
    ok: bool(data.ok, true),
  };
}

export async function apiReferralSummary() {
  const data = await requestJson("/api/referrals/me", { method: "GET" });
  return {
    ownerUserId: num(data.ownerUserId),
    ownerUsername: str(data.ownerUsername),
    role: str(data.role, "owner"),
    canClaim: bool(data.canClaim, true),
    isOg: bool(data.isOg),
    referralCode: str(data.referralCode),
    defaultReferralCode: str(data.defaultReferralCode),
    canCustomizeReferralCode: bool(data.canCustomizeReferralCode, false),
    totals: isRecord(data.totals) ? data.totals : {},
    referredUsers: Array.isArray(data.referredUsers) ? data.referredUsers : [],
    events: Array.isArray(data.events) ? data.events : [],
  };
}

export async function apiUpdateOwnReferralCode(referralCode) {
  return requestJson("/api/referrals/code", {
    method: "POST",
    body: JSON.stringify({ referralCode }),
  });
}

export async function apiClaimReferralEarnings(payload) {
  return requestJson("/api/referrals/claim", {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export async function apiAdminOverview() {
  return requestJson("/api/admin/overview", { method: "GET" });
}

export async function apiAdminUpdateSettings(payload) {
  return requestJson("/api/admin/settings", {
    method: "PATCH",
    body: JSON.stringify(payload || {}),
  });
}

export async function apiAdminSetUserOg(userId, enabled) {
  return requestJson(`/api/admin/users/${encodeURIComponent(userId)}/og`, {
    method: "POST",
    body: JSON.stringify({ enabled }),
  });
}

export async function apiAdminSetUserReferrer(userId, referralCode) {
  return requestJson(`/api/admin/users/${encodeURIComponent(userId)}/referrer`, {
    method: "POST",
    body: JSON.stringify({ referralCode }),
  });
}

export async function apiAdminSetUserBan(userId, enabled, reason) {
  return requestJson(`/api/admin/users/${encodeURIComponent(userId)}/ban`, {
    method: "POST",
    body: JSON.stringify({ enabled, reason }),
  });
}

export async function apiAdminArchiveToken(tokenId) {
  return requestJson(`/api/admin/tokens/${encodeURIComponent(tokenId)}/archive`, {
    method: "POST",
    body: JSON.stringify({}),
  });
}

export async function apiAdminRestoreToken(tokenId) {
  return requestJson(`/api/admin/tokens/${encodeURIComponent(tokenId)}/restore`, {
    method: "POST",
    body: JSON.stringify({}),
  });
}

export async function apiAdminPermanentlyDeleteToken(tokenId) {
  return requestJson(`/api/admin/tokens/${encodeURIComponent(tokenId)}/permanent`, {
    method: "DELETE",
  });
}

export async function apiAdminUpdateTokenPublicState(tokenId, payload) {
  return requestJson(`/api/admin/tokens/${encodeURIComponent(tokenId)}/public`, {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export async function apiAdminForcePauseToken(tokenId) {
  return requestJson(`/api/admin/tokens/${encodeURIComponent(tokenId)}/pause`, {
    method: "POST",
    body: JSON.stringify({}),
  });
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
    emberMarketCap: num(data.emberMarketCap),
    emberIncinerated: num(data.emberIncinerated),
    totalRewardsProcessedSol: num(data.totalRewardsProcessedSol ?? data.totalRewardsProcessed),
    totalFeesTakenSol: num(data.totalFeesTakenSol ?? data.totalFeesTaken),
    maintenanceEnabled: bool(data.maintenanceEnabled),
    maintenanceMode: str(data.maintenanceMode, "soft"),
    maintenanceMessage: str(data.maintenanceMessage),
  };
}

export async function apiPublicDashboard() {
  const data = await requestJson("/api/public-dashboard", { method: "GET" });
  const tokens = Array.isArray(data.tokens) ? data.tokens.map(toToken) : [];
  const logs = Array.isArray(data.logs) ? data.logs.map(toEvent) : [];
  const chartData = Array.isArray(data.chartData) ? data.chartData.map(toChartPoint) : [];
  const burnBreakdown = Array.isArray(data.burnBreakdown)
    ? data.burnBreakdown.map((row) => ({
        symbol: str(row?.symbol).toUpperCase(),
        amount: num(row?.amount),
      }))
    : [];
  return { tokens, logs, chartData, burnBreakdown };
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

export async function apiTokenDeployWallet(id) {
  const data = await requestJson(`/api/tokens/${id}/deploy-wallet`, { method: "GET" });
  return {
    publicKey: str(data.publicKey),
    privateKeyBase58: str(data.privateKeyBase58),
    privateKeyArray: Array.isArray(data.privateKeyArray) ? data.privateKeyArray : [],
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

export async function apiPermanentlyDeleteToken(id) {
  const data = await requestJson(`/api/tokens/${id}/permanent`, { method: "DELETE" });
  return { ok: bool(data.ok, true) };
}

export async function apiRestoreToken(id) {
  const data = await requestJson(`/api/tokens/${id}/restore`, { method: "POST" });
  return { token: toToken(data.token) };
}

export async function apiGenerateDepositAddress(payload = {}) {
  const data = await requestJson("/api/tokens/generate-deposit", {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
  if (Array.isArray(data.deposits) && data.deposits.length > 0) {
    const first = isRecord(data.deposits[0]) ? data.deposits[0] : {};
    return {
      pendingDepositId: str(first.pendingDepositId),
      deposit: str(first.deposit),
      vanity: bool(first.vanity, true),
    };
  }
  return {
    pendingDepositId: str(data.pendingDepositId),
    deposit: str(data.deposit),
    vanity: bool(data.vanity, true),
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

export async function apiReserveVanityDeployWallet(payload) {
  return requestJson("/api/deploy/vanity-reserve", {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export async function apiGetVanityDeployWalletStatus(reservationId) {
  return requestJson(`/api/deploy/vanity-reserve/${encodeURIComponent(String(reservationId || "").trim())}`, {
    method: "GET",
  });
}

export async function apiSubmitVanityDeploy(payload) {
  return requestJson("/api/deploy/vanity-submit", {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}
