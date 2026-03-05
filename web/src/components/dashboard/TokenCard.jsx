import { useCallback, useEffect, useMemo, useState } from "react";
import { EVT_META } from "../../config/appConstants";
import { fmt, fmtAge, fmtFull, fmtSec, solscanAddr, solscanTx } from "../../lib/format";

function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function fmtSol(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n)) return "0";
  return n.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 6 });
}

function fmtToken(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n)) return "0";
  return n.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 4 });
}

function normalizeImageUrl(url) {
  const raw = String(url || "").trim();
  if (!raw) return "";
  if (raw.startsWith("ipfs://")) {
    return `https://ipfs.io/ipfs/${raw.slice("ipfs://".length).replace(/^ipfs\//i, "")}`;
  }
  if (raw.startsWith("ar://")) {
    return `https://arweave.net/${raw.slice("ar://".length)}`;
  }
  return raw;
}

function maskAddress(addr) {
  const value = String(addr || "");
  if (value.length <= 10) return "Hidden";
  return `${value.slice(0, 4)}...${value.slice(-4)}`;
}

function botModeName(bot) {
  if (bot === "volume") return "Volume Bot";
  if (bot === "market_maker") return "Market Maker Bot";
  return "Burn Bot";
}

function botModeUpper(bot) {
  if (bot === "volume") return "VOLUME";
  if (bot === "market_maker") return "MM";
  return "BURN";
}

function runningLabel(bot, active, disconnected) {
  if (disconnected) return `${botModeUpper(bot)} DISCONNECTED`;
  if (!active) return `${botModeUpper(bot)} PAUSED`;
  if (bot === "volume") return "VOLUME RUNNING";
  if (bot === "market_maker") return "MM RUNNING";
  return "BURNING";
}

function normalizeTokenState(token) {
  const moduleConfig =
    token?.moduleConfig && typeof token.moduleConfig === "object" ? token.moduleConfig : {};
  return {
    ...token,
    selectedBot: String(token?.selectedBot || token?.moduleType || "burn"),
    disconnected: Boolean(token?.disconnected),
    claimSec: Math.max(60, Math.floor(toNum(token?.claimSec, 120))),
    burnSec: Math.max(60, Math.floor(toNum(token?.burnSec, 300))),
    splits: Math.max(1, Math.floor(toNum(token?.splits, 1))),
    moduleConfig: {
      claimEnabled: moduleConfig.claimEnabled !== false,
      tradeWalletCount: Math.max(
        1,
        Math.min(5, Math.floor(toNum(moduleConfig.tradeWalletCount, 1)))
      ),
      speed: Math.max(0, Math.min(100, toNum(moduleConfig.speed, 35))),
      aggression: Math.max(0, Math.min(100, toNum(moduleConfig.aggression, 35))),
      minTradeSol: Math.max(0.001, toNum(moduleConfig.minTradeSol, 0.01)),
      maxTradeSol: Math.max(0.001, toNum(moduleConfig.maxTradeSol, 0.05)),
    },
  };
}

export function TxLink({ tx, short = true }) {
  if (!tx) return null;
  const display = short ? `${tx.slice(0, 6)}...${tx.slice(-4)}` : tx;
  return (
    <a
      href={solscanTx(tx)}
      target="_blank"
      rel="noopener noreferrer"
      className="tx-link mono"
      style={{ fontSize: 10 }}
      onClick={(e) => e.stopPropagation()}
    >
      {display} {"\u2197"}
    </a>
  );
}

function AddrLink({ addr, label }) {
  if (!addr) return null;
  const display = label || `${addr.slice(0, 6)}...${addr.slice(-4)}`;
  return (
    <a
      href={solscanAddr(addr)}
      target="_blank"
      rel="noopener noreferrer"
      className="tx-link mono"
      style={{ fontSize: 11 }}
      onClick={(e) => e.stopPropagation()}
    >
      {display} {"\u2197"}
    </a>
  );
}

function QR({ seed = "" }) {
  const [expanded, setExpanded] = useState(false);
  const src = `https://api.qrserver.com/v1/create-qr-code/?size=200x200&data=${encodeURIComponent(seed || "")}`;
  return (
    <div
      onClick={() => setExpanded((v) => !v)}
      title="Click to resize"
      style={{ background: "#fff", padding: 5, borderRadius: 8, display: "inline-flex", flexShrink: 0, cursor: "pointer", transition: "transform .15s ease", transform: expanded ? "scale(1.12)" : "scale(1)" }}
    >
      <img src={src} alt="Deposit QR" width={72} height={72} style={{ display: "block", borderRadius: 4 }} />
    </div>
  );
}

function Ring({ total }) {
  const [rem, setRem] = useState(total);
  useEffect(() => {
    setRem(total);
    const id = setInterval(() => setRem((r) => (r > 0 ? r - 1 : total)), 1000);
    return () => clearInterval(id);
  }, [total]);
  const r = 17;
  const circ = 2 * Math.PI * r;
  const prog = ((total - rem) / total) * circ;
  return (
    <div style={{ position: "relative", width: 46, height: 46, flexShrink: 0 }}>
      <svg width={46} height={46} style={{ transform: "rotate(-90deg)" }}>
        <circle cx={23} cy={23} r={r} fill="none" stroke="rgba(255,106,0,.1)" strokeWidth={2.5} />
        <circle
          cx={23}
          cy={23}
          r={r}
          fill="none"
          stroke="#ff6a00"
          strokeWidth={2.5}
          strokeLinecap="round"
          strokeDasharray={circ}
          strokeDashoffset={circ - prog}
          style={{ transition: "stroke-dashoffset .9s linear", filter: "drop-shadow(0 0 4px #ff6a00)" }}
        />
      </svg>
      <div
        style={{
          position: "absolute",
          inset: 0,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          fontSize: 9,
          fontWeight: 700,
          color: "#ff8c42",
          fontFamily: "'JetBrains Mono',monospace",
        }}
      >
        {rem}
      </div>
    </div>
  );
}

function WalletBalancesList({ addresses, hideDeposit }) {
  if (!addresses.length) {
    return <div style={{ fontSize: 12, color: "rgba(255,255,255,.32)" }}>No wallet balances yet.</div>;
  }
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
      {addresses.map((item, index) => {
        const isDeposit = String(item.type) === "deposit";
        const isHidden = isDeposit && hideDeposit;
        const addr = String(item.pubkey || "");
        const label = String(item.label || `${item.type || "wallet"} ${index + 1}`);
        return (
          <div
            key={`${label}-${addr}-${index}`}
            style={{
              display: "grid",
              gridTemplateColumns: "minmax(0,1fr) auto auto",
              gap: 10,
              alignItems: "center",
              background: "rgba(255,255,255,.03)",
              border: "1px solid rgba(255,255,255,.06)",
              borderRadius: 8,
              padding: "8px 10px",
            }}
          >
            <div style={{ minWidth: 0 }}>
              <div style={{ fontSize: 10, color: "rgba(255,255,255,.35)", textTransform: "uppercase", letterSpacing: 0.6, marginBottom: 3, fontWeight: 700 }}>
                {label}
              </div>
              <div className="mono" style={{ fontSize: 11, color: "#fff", wordBreak: "break-all" }}>
                {isHidden ? maskAddress(addr) : <AddrLink addr={addr} label={addr} />}
              </div>
            </div>
            <div className="mono" style={{ fontSize: 11, color: "#9ce6ff", fontWeight: 700 }}>
              {fmtSol(item.solBalance)} SOL
            </div>
            <div className="mono" style={{ fontSize: 11, color: "#ffd6a1", fontWeight: 700 }}>
              {fmtToken(item.tokenBalance)} TOK
            </div>
          </div>
        );
      })}
    </div>
  );
}

function TokenLogs({ tokenId, logs }) {
  const tokenLogs = logs.filter((l) => l.tokenId === tokenId);
  if (!tokenLogs.length) {
    return <div style={{ fontSize: 12, color: "rgba(255,255,255,.25)", padding: "10px 0" }}>No activity yet.</div>;
  }
  return (
    <div className="log-scroll">
      {tokenLogs.map((e, i) => {
        const meta = EVT_META[e.type] || EVT_META.claim;
        return (
          <div
            key={e.id}
            className="log-row"
            style={{ display: "flex", gap: 10, alignItems: "flex-start", padding: "9px 0", animation: i === 0 ? "slideUp .25s ease" : "none" }}
          >
            <div
              style={{
                width: 26,
                height: 26,
                borderRadius: 7,
                background: `${meta.color}18`,
                border: `1px solid ${meta.color}30`,
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                fontSize: 12,
                flexShrink: 0,
              }}
            >
              {meta.icon}
            </div>
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ fontSize: 12, color: "rgba(255,255,255,.75)", lineHeight: 1.5, marginBottom: e.tx ? 3 : 0 }}>{e.msg}</div>
              {e.tx && <TxLink tx={e.tx} />}
            </div>
            <div style={{ fontSize: 10, color: "rgba(255,255,255,.2)", whiteSpace: "nowrap", flexShrink: 0, marginTop: 2 }}>{fmtAge(e.age)}</div>
          </div>
        );
      })}
    </div>
  );
}

function LiveLogsPanel({ token, logs, details, detailsLoading, detailsError, onRefreshDetails, onClose }) {
  const [hideDeposit, setHideDeposit] = useState(true);
  const [showImage, setShowImage] = useState(Boolean(normalizeImageUrl(token.pictureUrl)));
  useEffect(() => {
    setShowImage(Boolean(normalizeImageUrl(token.pictureUrl)));
  }, [token.pictureUrl]);

  const tokenLogs = useMemo(() => logs.filter((l) => l.tokenId === token.id).slice(0, 120), [logs, token.id]);
  const addresses = Array.isArray(details?.addresses) ? details.addresses : [];
  const totals = details?.totals || { sol: 0, token: 0 };
  const imageUrl = normalizeImageUrl(token.pictureUrl);
  const bot = String(token.selectedBot || token.moduleType || "burn");
  const depositAddr = String(addresses.find((a) => String(a.type) === "deposit")?.pubkey || token.deposit || "");
  const burnCycleCount = tokenLogs.filter((l) => String(l.type) === "burn").length;
  const cycleCount = bot === "volume"
    ? tokenLogs.filter((l) => ["buy", "sell", "claim"].includes(String(l.type))).length
    : burnCycleCount;

  return (
    <div style={{ position: "fixed", inset: 0, zIndex: 1300, display: "flex", alignItems: "center", justifyContent: "center" }} onClick={onClose}>
      <div style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,.76)", backdropFilter: "blur(8px)" }} />
      <div className="glass" style={{ position: "relative", zIndex: 1, width: "min(980px,95vw)", maxHeight: "88vh", overflowY: "auto", padding: "18px 20px", border: "1px solid rgba(255,106,0,.25)" }} onClick={(e) => e.stopPropagation()}>
        <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center", marginBottom: 14 }}>
          <div style={{ display: "flex", gap: 12, alignItems: "center", minWidth: 0 }}>
            {showImage && imageUrl ? (
              <img src={imageUrl} alt={`${token.symbol} token`} style={{ width: 46, height: 46, borderRadius: 12, objectFit: "cover", border: "1px solid rgba(255,255,255,.2)", flexShrink: 0 }} onError={() => setShowImage(false)} />
            ) : (
              <div style={{ width: 46, height: 46, borderRadius: 12, background: "linear-gradient(135deg,#ff6a00,#cc2200)", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 18, fontWeight: 800, flexShrink: 0 }}>
                {String(token.symbol || "T").slice(0, 1)}
              </div>
            )}
            <div style={{ minWidth: 0 }}>
              <div style={{ fontSize: 16, fontWeight: 800, color: "#fff" }}>Live Logs - ${token.symbol}</div>
              <div style={{ fontSize: 12, color: "rgba(255,255,255,.35)" }}>
                {botModeName(bot)} - {token.disconnected ? "Disconnected" : token.active ? "Active" : "Paused"} - TX {fmtFull(token.txCount)}
              </div>
            </div>
          </div>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap", justifyContent: "flex-end" }}>
            <button className="btn-ghost" onClick={() => setHideDeposit((v) => !v)} style={{ padding: "6px 12px", fontSize: 12 }}>
              {hideDeposit ? "Show Deposit" : "Hide Deposit"}
            </button>
            <button className="btn-ghost" onClick={() => onRefreshDetails?.()} style={{ padding: "6px 12px", fontSize: 12 }}>Refresh</button>
            <button className="btn-ghost" onClick={onClose} style={{ padding: "6px 12px", fontSize: 12 }}>Close</button>
          </div>
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(210px,1fr))", gap: 10, marginBottom: 12 }}>
          <div style={{ background: "rgba(255,255,255,.03)", borderRadius: 8, padding: "9px 10px" }}>
            <div style={{ fontSize: 10, color: "rgba(255,255,255,.35)", letterSpacing: 0.5, marginBottom: 4 }}>MINT</div>
            <div className="mono" style={{ fontSize: 11, color: "#fff", wordBreak: "break-all" }}>
              <AddrLink addr={token.mint} label={token.mint} />
            </div>
          </div>
          <div style={{ background: "rgba(255,255,255,.03)", borderRadius: 8, padding: "9px 10px" }}>
            <div style={{ fontSize: 10, color: "rgba(255,255,255,.35)", letterSpacing: 0.5, marginBottom: 4 }}>DEPOSIT</div>
            <div className="mono" style={{ fontSize: 11, color: "#fff", wordBreak: "break-all" }}>
              {hideDeposit ? maskAddress(depositAddr) : <AddrLink addr={depositAddr} label={depositAddr} />}
            </div>
          </div>
          <div style={{ background: "rgba(255,255,255,.03)", borderRadius: 8, padding: "9px 10px" }}>
            <div style={{ fontSize: 10, color: "rgba(255,255,255,.35)", letterSpacing: 0.5, marginBottom: 4 }}>BOT BALANCE</div>
            <div className="mono" style={{ fontSize: 11, color: "#9ce6ff", fontWeight: 700 }}>{fmtSol(totals.sol)} SOL</div>
            <div className="mono" style={{ fontSize: 11, color: "#ffd6a1", fontWeight: 700, marginTop: 2 }}>{fmtToken(totals.token)} TOK</div>
          </div>
          <div style={{ background: "rgba(255,255,255,.03)", borderRadius: 8, padding: "9px 10px" }}>
            <div style={{ fontSize: 10, color: "rgba(255,255,255,.35)", letterSpacing: 0.5, marginBottom: 4 }}>{bot === "volume" ? "TOTAL TX" : "TOTAL TOKENS BURNED"}</div>
            <div style={{ fontSize: 13, fontWeight: 700, color: "#fff" }}>{bot === "volume" ? fmtFull(token.txCount) : fmt(token.burned)}</div>
            <div style={{ fontSize: 10, color: "rgba(255,255,255,.3)", marginTop: 2 }}>Total cycles: {fmtFull(cycleCount)}</div>
          </div>
        </div>

        <div style={{ marginBottom: 14 }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
            <div style={{ fontSize: 12, color: "rgba(255,255,255,.5)", fontWeight: 700 }}>Wallet Balances</div>
            {detailsLoading && <div style={{ fontSize: 11, color: "rgba(255,255,255,.4)" }}>Refreshing...</div>}
          </div>
          {detailsError ? <div style={{ fontSize: 12, color: "#ff8f8f" }}>{detailsError}</div> : <WalletBalancesList addresses={addresses} hideDeposit={hideDeposit} />}
        </div>

        <div style={{ fontSize: 11, color: "rgba(255,255,255,.32)", marginBottom: 8 }}>
          Showing last {tokenLogs.length} events for this token.
        </div>
        <TokenLogs tokenId={token.id} logs={tokenLogs} />
      </div>
    </div>
  );
}

function DeleteConfirmModal({ onClose, onConfirm, deleting, disableConfirm, reason }) {
  return (
    <div style={{ position: "fixed", inset: 0, zIndex: 1320, display: "flex", alignItems: "center", justifyContent: "center" }} onClick={onClose}>
      <div style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,.7)", backdropFilter: "blur(6px)" }} />
      <div className="glass" style={{ position: "relative", zIndex: 1, width: "min(520px,92vw)", padding: "20px 22px" }} onClick={(e) => e.stopPropagation()}>
        <div style={{ fontSize: 18, fontWeight: 800, color: "#fff", marginBottom: 10 }}>Delete Bot</div>
        <div style={{ fontSize: 13, color: "rgba(255,255,255,.68)", lineHeight: 1.55, marginBottom: 10 }}>
          This deletes the bot from active execution and keeps the token in your ticker history as disconnected. Sweep and withdraw first.
        </div>
        <div style={{ fontSize: 12, color: disableConfirm ? "#ff8f8f" : "rgba(255,255,255,.45)", marginBottom: 14 }}>{reason || "This action cannot be undone."}</div>
        <div style={{ display: "flex", gap: 10, justifyContent: "flex-end" }}>
          <button className="btn-ghost" onClick={onClose} disabled={deleting} style={{ padding: "8px 14px", fontSize: 12 }}>Cancel</button>
          <button className="btn-ghost" onClick={onConfirm} disabled={deleting || disableConfirm} style={{ padding: "8px 14px", fontSize: 12, borderColor: "rgba(255,90,90,.35)", color: disableConfirm ? "rgba(255,255,255,.28)" : "#ff9f9f" }}>
            {deleting ? "Deleting..." : "Delete Bot"}
          </button>
        </div>
      </div>
    </div>
  );
}

export default function TokenCard({
  token,
  onUpdate,
  allLogs,
  onDelete,
  onFetchDetails,
  onFetchVolumeWithdrawOptions,
  onVolumeSweep,
  onVolumeWithdraw,
}) {
  const [open, setOpen] = useState(false);
  const [tab, setTab] = useState("overview");
  const [editing, setEditing] = useState(false);
  const [local, setLocal] = useState(normalizeTokenState(token));
  const [copied, setCopied] = useState(false);
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState("");
  const [showLiveLogs, setShowLiveLogs] = useState(false);
  const [details, setDetails] = useState(null);
  const [detailsLoading, setDetailsLoading] = useState(false);
  const [detailsErr, setDetailsErr] = useState("");
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [showCardImage, setShowCardImage] = useState(Boolean(normalizeImageUrl(token.pictureUrl)));
  const [sweeping, setSweeping] = useState(false);
  const [withdrawLoading, setWithdrawLoading] = useState(false);
  const [withdrawing, setWithdrawing] = useState(false);
  const [showWithdrawModal, setShowWithdrawModal] = useState(false);
  const [withdrawOptions, setWithdrawOptions] = useState(null);
  const [actionMsg, setActionMsg] = useState("");
  const [optimisticActive, setOptimisticActive] = useState(null);
  const clamp01 = (v) => Math.max(0.001, Number(v) || 0.001);
  const deriveMaxTradeSol = (minTrade, aggression) => {
    const min = clamp01(minTrade);
    const agg = Math.max(0, Math.min(100, Number(aggression) || 0));
    const cap = Math.max(0.2, min * 10);
    return Number((min + (cap - min) * (agg / 100)).toFixed(3));
  };
  const speedEverySec = (speed) =>
    Math.max(3, 25 - Math.round((Math.max(0, Math.min(100, Number(speed) || 0)) / 100) * 20));

  useEffect(() => {
    setLocal(normalizeTokenState(token));
    setOptimisticActive(null);
  }, [token]);

  useEffect(() => {
    setShowCardImage(Boolean(normalizeImageUrl(token.pictureUrl)));
  }, [token.pictureUrl]);

  useEffect(() => {
    setDetails(null);
    setDetailsErr("");
  }, [token.id]);

  useEffect(() => {
    if (String(local.selectedBot || "burn") !== "volume") return;
    const derived = deriveMaxTradeSol(local.moduleConfig.minTradeSol, local.moduleConfig.aggression);
    if (Math.abs(Number(local.moduleConfig.maxTradeSol || 0) - derived) < 0.0005) return;
    setLocal((prev) => ({
      ...prev,
      moduleConfig: { ...prev.moduleConfig, maxTradeSol: derived },
    }));
  }, [local.selectedBot, local.moduleConfig.minTradeSol, local.moduleConfig.aggression, local.moduleConfig.maxTradeSol]);

  const fetchDetails = useCallback(async (quiet = false) => {
    if (typeof onFetchDetails !== "function") return null;
    if (!quiet) setDetailsLoading(true);
    setDetailsErr("");
    try {
      const data = await onFetchDetails(token.id);
      setDetails(data);
      return data;
    } catch (error) {
      setDetailsErr(error?.message || "Unable to load live balances.");
      return null;
    } finally {
      setDetailsLoading(false);
    }
  }, [onFetchDetails, token.id]);

  useEffect(() => {
    if (!open && !showLiveLogs) return;
    let alive = true;
    const run = async (quiet) => {
      if (!alive) return;
      await fetchDetails(quiet);
    };
    void run(false);
    const id = setInterval(() => {
      void run(true);
    }, 15000);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, [open, showLiveLogs, fetchDetails]);

  const isVolumeMode = String(local.selectedBot || "burn") === "volume";
  const cycleSeconds = isVolumeMode
    ? Math.max(3, 25 - Math.round((toNum(local.moduleConfig.speed, 35) / 100) * 20))
    : local.burnSec;
  const tokenLogs = Array.isArray(allLogs) ? allLogs : [];
  const tokenLogCount = useMemo(() => tokenLogs.filter((l) => l.tokenId === token.id).length, [tokenLogs, token.id]);

  const copy = () => {
    navigator.clipboard.writeText(token.deposit).catch(() => {});
    setCopied(true);
    setTimeout(() => setCopied(false), 1500);
  };

  const save = async () => {
    setErr("");
    setSaving(true);
    try {
      const nextModuleConfig = {
        ...local.moduleConfig,
        tradeWalletCount: Math.max(1, Math.min(5, Math.floor(toNum(local.moduleConfig.tradeWalletCount, 1)))),
        speed: Math.max(0, Math.min(100, toNum(local.moduleConfig.speed, 35))),
        aggression: Math.max(0, Math.min(100, toNum(local.moduleConfig.aggression, 35))),
        minTradeSol: Math.max(0.001, toNum(local.moduleConfig.minTradeSol, 0.01)),
        maxTradeSol: Math.max(0.001, toNum(local.moduleConfig.maxTradeSol, 0.05)),
      };
      if (nextModuleConfig.maxTradeSol < nextModuleConfig.minTradeSol) {
        nextModuleConfig.maxTradeSol = nextModuleConfig.minTradeSol;
      }
      await onUpdate({
        ...local,
        claimSec: Math.max(60, Math.floor(toNum(local.claimSec, 120))),
        burnSec: Math.max(60, Math.floor(toNum(local.burnSec, 300))),
        splits: Math.max(1, Math.floor(toNum(local.splits, 1))),
        moduleConfig: nextModuleConfig,
      });
      setEditing(false);
      void fetchDetails(true);
    } catch (error) {
      setErr(error?.message || "Unable to update token settings.");
    } finally {
      setSaving(false);
    }
  };

  const toggle = async (e) => {
    e.stopPropagation();
    if (saving) return;
    if (token.disconnected) return;
    setErr("");
    const currentActive =
      optimisticActive === null ? Boolean(local.active) : Boolean(optimisticActive);
    const nextActive = !currentActive;
    setOptimisticActive(nextActive);
    setLocal((prev) => ({ ...prev, active: nextActive }));
    setSaving(true);
    try {
      await onUpdate({
        ...token,
        selectedBot: token.selectedBot || token.moduleType || "burn",
        moduleConfig: token.moduleConfig || {},
        active: nextActive,
      });
      setOptimisticActive(null);
    } catch (error) {
      setOptimisticActive(null);
      setLocal((prev) => ({ ...prev, active: currentActive }));
      setErr(error?.message || "Unable to update token status.");
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async () => {
    if (typeof onDelete !== "function") return;
    setErr("");
    setDeleting(true);
    try {
      await onDelete(token.id);
      setShowDeleteConfirm(false);
    } catch (error) {
      setErr(error?.message || "Unable to delete bot.");
    } finally {
      setDeleting(false);
    }
  };

  const isDisconnected = Boolean(token.disconnected);
  const activeBase =
    optimisticActive === null ? Boolean(local.active) : Boolean(optimisticActive);
  const isActive = activeBase && !isDisconnected;
  const bot = String(token.selectedBot || token.moduleType || "burn");
  const statusLabel = runningLabel(bot, isActive, isDisconnected);
  const imageUrl = normalizeImageUrl(token.pictureUrl);
  const detailAddresses = Array.isArray(details?.addresses) ? details.addresses : [];
  const detailTotals = details?.totals || { sol: 0, token: 0 };
  const hasKnownFunds = detailAddresses.some((item) => Number(item?.solBalance || 0) > 0.00005 || Number(item?.tokenBalance || 0) > 0.000001);
  const deleteReason = hasKnownFunds
    ? "Cannot delete while funds remain on deposit or trade wallets."
    : "Deleting keeps this token in your ticker and marks it as disconnected.";

  return (
    <>
      <div
        className="glass"
        style={{
          cursor: "pointer",
          transition: "all .25s",
          border: isActive ? "1px solid rgba(255,106,0,.2)" : "1px solid rgba(255,255,255,.07)",
          ...(isActive ? { animation: "borderPulse 3s infinite" } : {}),
        }}
        onClick={() => setOpen((o) => !o)}
      >
        {isActive && <div style={{ position: "absolute", top: 0, left: 0, right: 0, height: 1, background: "linear-gradient(90deg,transparent,#ff6a00,#ff4500,transparent)", zIndex: 1 }} />}

        <div style={{ padding: "18px 20px", display: "flex", gap: 14, alignItems: "center" }}>
          {showCardImage && imageUrl ? (
            <img src={imageUrl} alt={`${token.symbol} token`} style={{ width: 44, height: 44, borderRadius: 12, objectFit: "cover", border: "1px solid rgba(255,255,255,.2)", flexShrink: 0, boxShadow: isActive ? "0 0 18px rgba(255,106,0,.3)" : "none" }} onError={() => setShowCardImage(false)} />
          ) : (
            <div style={{ width: 44, height: 44, borderRadius: 12, background: "linear-gradient(135deg,#ff6a00,#cc2200)", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 18, fontWeight: 800, flexShrink: 0, boxShadow: isActive ? "0 0 18px rgba(255,106,0,.4)" : "none", transition: "box-shadow .3s" }}>
              {String(token.symbol || "T").slice(0, 1)}
            </div>
          )}

          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4, flexWrap: "wrap" }}>
              <span style={{ fontWeight: 800, fontSize: 16, color: "#fff" }}>${token.symbol}</span>
              <span style={{ fontSize: 12, color: "rgba(255,255,255,.3)" }}>{token.name}</span>
              <span className={isActive ? "tag-on" : "tag-off"} style={isDisconnected ? { color: "#ff9f9f", borderColor: "rgba(255,140,140,.35)", background: "rgba(255,120,120,.08)" } : {}}>
                {isActive && <span style={{ width: 5, height: 5, borderRadius: "50%", background: "#ff6a00", animation: "pulse-dot 2s infinite", display: "inline-block" }} />}
                {statusLabel}
              </span>
            </div>
            <div style={{ display: "flex", gap: 16, flexWrap: "wrap" }}>
              {[
                ...(bot === "volume" ? [] : [["BURNED", fmt(token.burned), "#ff8c42"]]),
                ["TXS", fmtFull(token.txCount), "rgba(255,255,255,.6)"],
                ["BOT", botModeUpper(bot), "rgba(255,255,255,.65)"],
              ].map(([label, value, color]) => (
                <div key={label}>
                  <span style={{ fontSize: 10, color: "rgba(255,255,255,.25)", fontWeight: 600, letterSpacing: 0.4 }}>{label} </span>
                  <span style={{ fontSize: 13, fontWeight: 700, color }}>{value}</span>
                </div>
              ))}
            </div>
          </div>

          <div style={{ display: "flex", alignItems: "center", gap: 10, flexShrink: 0 }}>
            {isActive && <Ring total={Math.max(3, cycleSeconds)} />}
            <button className="btn-ghost" onClick={toggle} disabled={saving || isDisconnected} style={{ padding: "6px 14px", fontSize: 12, opacity: isDisconnected ? 0.55 : 1, cursor: isDisconnected ? "not-allowed" : "pointer" }}>
              {saving ? "Saving..." : isDisconnected ? "Disconnected" : isActive ? "Pause" : "Start"}
            </button>
            <span style={{ color: "rgba(255,255,255,.2)", fontSize: 18, transform: open ? "rotate(180deg)" : "none", transition: "transform .2s", userSelect: "none" }}>{"\u2304"}</span>
          </div>
        </div>
        {err && <div style={{ padding: "0 20px 12px", fontSize: 12, color: "#ff8f8f" }}>{err}</div>}

        {open && (
          <div style={{ borderTop: "1px solid rgba(255,255,255,.05)" }} onClick={(e) => e.stopPropagation()}>
            <div style={{ display: "flex", gap: 0, padding: "0 20px", borderBottom: "1px solid rgba(255,255,255,.05)" }}>
              {[["overview", "Overview"], ["logs", "Logs"], ["settings", "Settings"]].map(([value, label]) => (
                <button
                  key={value}
                  onClick={() => setTab(value)}
                  style={{
                    background: "none",
                    border: "none",
                    borderBottom: `2px solid ${tab === value ? "#ff6a00" : "transparent"}`,
                    color: tab === value ? "#ff8c42" : "rgba(255,255,255,.35)",
                    padding: "10px 16px",
                    cursor: "pointer",
                    fontSize: 12,
                    fontWeight: 700,
                    transition: "all .15s",
                    letterSpacing: 0.3,
                  }}
                >
                  {label}
                </button>
              ))}
            </div>

            <div style={{ padding: "18px 20px", animation: "slideUp .2s ease" }}>
              {tab === "overview" && (
                <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
                  <div style={{ background: "rgba(255,106,0,.05)", border: "1px solid rgba(255,106,0,.1)", borderRadius: 10, padding: "14px 16px" }}>
                    <div style={{ fontSize: 10, color: "rgba(255,255,255,.3)", fontWeight: 600, letterSpacing: 1, marginBottom: 10 }}>DEPOSIT ADDRESS</div>
                    <div style={{ display: "flex", gap: 14, alignItems: "center" }}>
                      <QR seed={token.deposit} />
                      <div style={{ flex: 1, minWidth: 0 }}>
                        <div style={{ marginBottom: 8 }}><AddrLink addr={token.deposit} label={token.deposit} /></div>
                        <div className="mono" style={{ fontSize: 10, color: "#ff8c42", wordBreak: "break-all", lineHeight: 1.6, marginBottom: 8, background: "rgba(0,0,0,.3)", padding: "8px 10px", borderRadius: 6, border: "1px solid rgba(255,106,0,.1)" }}>{token.deposit}</div>
                        <button className="btn-ghost" onClick={copy} style={{ padding: "5px 12px", fontSize: 11 }}>{copied ? "Copied!" : "Copy"}</button>
                      </div>
                    </div>
                  </div>

                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
                    {[["Current SOL", `${fmtSol(detailTotals.sol)} SOL`], ["Current Token", `${fmtToken(detailTotals.token)} TOK`]].map(([key, value]) => (
                      <div key={key} style={{ background: "rgba(255,255,255,.03)", borderRadius: 8, padding: "10px 12px" }}>
                        <div style={{ fontSize: 10, color: "rgba(255,255,255,.3)", marginBottom: 4, fontWeight: 600, letterSpacing: 0.5 }}>{key.toUpperCase()}</div>
                        <div className="mono" style={{ fontSize: 13, color: "#fff", fontWeight: 700 }}>{detailsLoading ? "Refreshing..." : value}</div>
                      </div>
                    ))}
                  </div>

                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
                    {[["Claim Every", fmtSec(token.claimSec)], ["Cycle Every", fmtSec(cycleSeconds)], ["Mode", botModeName(bot)], ["Mint", <AddrLink addr={token.mint} label={token.mint} />]].map(([key, value]) => (
                      <div key={key} style={{ background: "rgba(255,255,255,.03)", borderRadius: 8, padding: "10px 12px" }}>
                        <div style={{ fontSize: 10, color: "rgba(255,255,255,.3)", marginBottom: 4, fontWeight: 600, letterSpacing: 0.5 }}>{String(key).toUpperCase()}</div>
                        <div style={{ fontSize: 13, color: "#fff", fontWeight: 700 }}>{value}</div>
                      </div>
                    ))}
                  </div>

                  <div style={{ background: "rgba(255,255,255,.03)", borderRadius: 10, border: "1px solid rgba(255,255,255,.06)", padding: "12px 12px" }}>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
                      <div style={{ fontSize: 12, fontWeight: 700, color: "#fff" }}>Wallet Balances</div>
                      <button className="btn-ghost" onClick={() => fetchDetails(false)} style={{ padding: "5px 10px", fontSize: 11 }}>Refresh</button>
                    </div>
                    {detailsErr ? <div style={{ fontSize: 12, color: "#ff8f8f" }}>{detailsErr}</div> : <WalletBalancesList addresses={detailAddresses} hideDeposit={false} />}
                  </div>
                </div>
              )}

              {tab === "logs" && (
                <div>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10 }}>
                    <div style={{ fontSize: 11, color: "rgba(255,255,255,.25)" }}>Showing last {tokenLogCount} events</div>
                    <button className="btn-ghost" onClick={() => setShowLiveLogs(true)} style={{ padding: "6px 10px", fontSize: 11 }}>
                      Live Logs Panel
                    </button>
                  </div>
                  <TokenLogs tokenId={token.id} logs={tokenLogs} />
                </div>
              )}

              {tab === "settings" && (
                <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
                  {editing ? (
                    <>
                      <div>
                        <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", letterSpacing: 1, marginBottom: 7, fontWeight: 600 }}>BOT MODE</label>
                        <select className="input-f" value={local.selectedBot} onChange={(e) => setLocal((prev) => ({ ...prev, selectedBot: String(e.target.value || "burn") }))}>
                          <option value="burn">Burn Bot</option>
                          <option value="volume">Volume Bot</option>
                        </select>
                      </div>

                      {isVolumeMode ? (
                        <>
                          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14 }}>
                            <div>
                              <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", letterSpacing: 1, marginBottom: 7, fontWeight: 600 }}>CLAIM INTERVAL (s)</label>
                              <input type="number" min={60} className="input-f" style={{ padding: "9px 12px", fontSize: 13 }} value={local.claimSec} onChange={(e) => setLocal((prev) => ({ ...prev, claimSec: Math.max(60, Math.floor(toNum(e.target.value, prev.claimSec))) }))} />
                            </div>
                            <div>
                              <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", letterSpacing: 1, marginBottom: 7, fontWeight: 600 }}>TRADE WALLETS (1-5)</label>
                              <input type="number" min={1} max={5} className="input-f" style={{ padding: "9px 12px", fontSize: 13 }} value={local.moduleConfig.tradeWalletCount} onChange={(e) => setLocal((prev) => ({ ...prev, moduleConfig: { ...prev.moduleConfig, tradeWalletCount: Math.max(1, Math.min(5, Math.floor(toNum(e.target.value, prev.moduleConfig.tradeWalletCount)))) } }))} />
                            </div>
                          </div>

                          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14 }}>
                            <div>
                              <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", letterSpacing: 1, marginBottom: 7, fontWeight: 600 }}>MIN TRADE SOL</label>
                              <input type="number" min={0.001} step="0.001" className="input-f" style={{ padding: "9px 12px", fontSize: 13 }} value={local.moduleConfig.minTradeSol} onChange={(e) => setLocal((prev) => ({ ...prev, moduleConfig: { ...prev.moduleConfig, minTradeSol: Math.max(0.001, toNum(e.target.value, prev.moduleConfig.minTradeSol)), maxTradeSol: deriveMaxTradeSol(e.target.value, prev.moduleConfig.aggression) } }))} />
                            </div>
                            <div>
                              <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", letterSpacing: 1, marginBottom: 7, fontWeight: 600 }}>MAX TRADE SOL (AUTO)</label>
                              <input type="number" readOnly className="input-f" style={{ padding: "9px 12px", fontSize: 13 }} value={Number(local.moduleConfig.maxTradeSol || 0).toFixed(3)} />
                            </div>
                          </div>

                          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14 }}>
                            <div>
                              <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", letterSpacing: 1, marginBottom: 7, fontWeight: 600 }}>SPEED ({Math.round(toNum(local.moduleConfig.speed, 35))})</label>
                              <input type="range" min={0} max={100} className="input-f ember-range" value={local.moduleConfig.speed} onChange={(e) => setLocal((prev) => ({ ...prev, moduleConfig: { ...prev.moduleConfig, speed: Math.max(0, Math.min(100, toNum(e.target.value, prev.moduleConfig.speed))) } }))} />
                              <div style={{ fontSize: 11, color: "rgba(255,255,255,.35)", marginTop: 6 }}>Executes around every <span className="mono" style={{ color: "#ff9f5a" }}>{speedEverySec(local.moduleConfig.speed)}s</span>.</div>
                            </div>
                            <div>
                              <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", letterSpacing: 1, marginBottom: 7, fontWeight: 600 }}>AGGRESSION ({Math.round(toNum(local.moduleConfig.aggression, 35))})</label>
                              <input type="range" min={0} max={100} className="input-f ember-range" value={local.moduleConfig.aggression} onChange={(e) => setLocal((prev) => ({ ...prev, moduleConfig: { ...prev.moduleConfig, aggression: Math.max(0, Math.min(100, toNum(e.target.value, prev.moduleConfig.aggression))), maxTradeSol: deriveMaxTradeSol(prev.moduleConfig.minTradeSol, e.target.value) } }))} />
                              <div style={{ fontSize: 11, color: "rgba(255,255,255,.35)", marginTop: 6 }}>Max trade auto-tunes to <span className="mono" style={{ color: "#ff9f5a" }}>{Number(local.moduleConfig.maxTradeSol || 0).toFixed(3)} SOL</span>.</div>
                            </div>
                          </div>

                          <label style={{ display: "inline-flex", alignItems: "center", gap: 10, fontSize: 12, color: "rgba(255,255,255,.72)" }}>
                            <span className="ember-toggle">
                              <input type="checkbox" checked={local.moduleConfig.claimEnabled !== false} onChange={(e) => setLocal((prev) => ({ ...prev, moduleConfig: { ...prev.moduleConfig, claimEnabled: e.target.checked } }))} />
                              <span className="ember-toggle-track" />
                            </span>
                            Enable creator reward claiming
                          </label>
                        </>
                      ) : (
                        <>
                          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14 }}>
                            {[{ label: "Claim Interval (s)", key: "claimSec", min: 60 }, { label: "Burn Interval (s)", key: "burnSec", min: 60 }].map((fd) => (
                              <div key={fd.key}>
                                <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", letterSpacing: 1, marginBottom: 7, fontWeight: 600 }}>{fd.label.toUpperCase()}</label>
                                <input type="number" min={fd.min} className="input-f" style={{ padding: "9px 12px", fontSize: 13 }} value={local[fd.key]} onChange={(e) => setLocal((prev) => ({ ...prev, [fd.key]: Math.max(fd.min, Math.floor(toNum(e.target.value, prev[fd.key]))) }))} />
                              </div>
                            ))}
                          </div>
                          <div>
                            <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", letterSpacing: 1, marginBottom: 8, fontWeight: 600 }}>SPLIT BUYS PER CYCLE</label>
                            <div style={{ maxWidth: 220 }}>
                              <input type="number" min={1} step={1} className="input-f" style={{ padding: "9px 12px", fontSize: 13 }} value={local.splits} onChange={(e) => setLocal((prev) => ({ ...prev, splits: Math.max(1, Math.floor(toNum(e.target.value, prev.splits))) }))} />
                            </div>
                          </div>
                        </>
                      )}

                      <div style={{ display: "flex", gap: 8 }}>
                        <button className="btn-fire" onClick={save} disabled={saving} style={{ padding: "9px 20px", fontSize: 13 }}>{saving ? "Saving..." : "Save"}</button>
                        <button className="btn-ghost" onClick={() => { setEditing(false); setLocal(normalizeTokenState(token)); }} disabled={saving} style={{ padding: "9px 16px", fontSize: 13 }}>Cancel</button>
                      </div>
                    </>
                  ) : (
                    <>
                      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
                        {[["Mode", botModeName(bot)], ["Claim Every", fmtSec(token.claimSec)], ["Cycle Every", fmtSec(cycleSeconds)], ["Status", isDisconnected ? "Disconnected" : isActive ? "Active" : "Paused"]].map(([key, value]) => (
                          <div key={key} style={{ background: "rgba(255,255,255,.03)", borderRadius: 8, padding: "10px 12px" }}>
                            <div style={{ fontSize: 10, color: "rgba(255,255,255,.3)", marginBottom: 4, fontWeight: 600, letterSpacing: 0.5 }}>{String(key).toUpperCase()}</div>
                            <div style={{ fontSize: 13, color: "#fff", fontWeight: 700 }}>{value}</div>
                          </div>
                        ))}
                      </div>
                      <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                        <button className="btn-ghost" onClick={() => setEditing(true)} disabled={isDisconnected} style={{ padding: "8px 18px", fontSize: 12, width: "fit-content", opacity: isDisconnected ? 0.55 : 1, cursor: isDisconnected ? "not-allowed" : "pointer" }}>
                          {isDisconnected ? "Reattach To Edit" : "Edit Settings"}
                        </button>
                        <button className="btn-ghost" onClick={() => setShowLiveLogs(true)} style={{ padding: "8px 18px", fontSize: 12, width: "fit-content" }}>Live Logs</button>
                        <button
                          className="btn-ghost"
                          onClick={() => { void fetchDetails(false); setShowDeleteConfirm(true); }}
                          disabled={isDisconnected}
                          style={{
                            padding: "8px 18px",
                            fontSize: 12,
                            width: "fit-content",
                            borderColor: "rgba(255,90,90,.3)",
                            color: "#ff9f9f",
                            opacity: isDisconnected ? 0.55 : 1,
                            cursor: isDisconnected ? "not-allowed" : "pointer",
                          }}
                        >
                          Delete Bot
                        </button>
                      </div>
                    </>
                  )}
                </div>
              )}
            </div>
          </div>
        )}
      </div>

      {showLiveLogs && (
        <LiveLogsPanel
          token={token}
          logs={tokenLogs}
          details={details}
          detailsLoading={detailsLoading}
          detailsError={detailsErr}
          onRefreshDetails={() => fetchDetails(false)}
          onClose={() => setShowLiveLogs(false)}
        />
      )}

      {showDeleteConfirm && (
        <DeleteConfirmModal
          onClose={() => {
            if (deleting) return;
            setShowDeleteConfirm(false);
          }}
          onConfirm={handleDelete}
          deleting={deleting}
          disableConfirm={hasKnownFunds}
          reason={deleteReason}
        />
      )}
    </>
  );
}
