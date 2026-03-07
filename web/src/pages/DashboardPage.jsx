import { useEffect, useMemo, useState } from "react";
import BurnChart from "../components/BurnChart";
import LiveFeed from "../components/dashboard/LiveFeed";
import TokenCard from "../components/dashboard/TokenCard";
import { useI18n } from "../i18n/I18nProvider";
import { fmt, fmtFull } from "../lib/format";

function ActionModal({ title, onClose, children, topOffset = 112 }) {
  return (
    <div
      style={{
        position: "fixed",
        inset: 0,
        zIndex: 1400,
        display: "flex",
        alignItems: "flex-start",
        justifyContent: "center",
        paddingTop: topOffset,
        paddingBottom: 24,
      }}
      onClick={onClose}
    >
      <div style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,.74)", backdropFilter: "blur(8px)" }} />
      <div
        className="glass"
        style={{
          position: "relative",
          zIndex: 1,
          width: "min(620px,92vw)",
          maxHeight: "86vh",
          overflowY: "auto",
          padding: "22px 24px",
          border: "1px solid rgba(255,106,0,.24)",
        }}
        onClick={(e) => e.stopPropagation()}
      >
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12, marginBottom: 16 }}>
          <div style={{ fontSize: 18, fontWeight: 800, color: "#fff" }}>{title}</div>
          <button className="btn-ghost" onClick={onClose} style={{ padding: "7px 12px", fontSize: 12 }}>
            Close
          </button>
        </div>
        {children}
      </div>
    </div>
  );
}

export default function DashboardPage({
  user,
  tokens,
  allLogs,
  chartData,
  feed,
  onShowAttach,
  onUpdateToken,
  onDeleteToken,
  onPermanentDeleteToken,
  onRestoreToken,
  onFetchTokenDetails,
  onFetchDeployWallet,
  onFetchVolumeWithdrawOptions,
  onVolumeSweep,
  onVolumeWithdraw,
  onBurnWithdraw,
  onLoadManagerAccess,
  onSaveManagerAccess,
  onDeleteManagerAccess,
  onLoadTelegramAlerts,
  onSaveTelegramAlerts,
  onDisconnectTelegramAlerts,
  onSendTelegramTestAlert,
  onLoadReferralSummary,
  onClaimReferralEarnings,
  onUpdateReferralCode,
  onLoadAdminOverview,
  onSaveAdminSettings,
  onAdminSetUserOg,
  onAdminSetUserReferrer,
  onAdminSetUserBan,
  onAdminArchiveToken,
  onAdminRestoreToken,
  onAdminPermanentDeleteToken,
  onAdminUpdateTokenPublicState,
  onAdminForcePauseToken,
}) {
  const { t } = useI18n();
  const username = typeof user === "string" ? user : String(user?.username || "");
  const isManager = Boolean(user?.role === "manager" || user?.isOperator);
  const canManageAccess = Boolean(user?.canManageAccess);
  const isAdmin = Boolean(user?.isAdmin);
  const isOg = Boolean(user?.isOg);
  const visibleTokens = tokens.filter((t) => !t?.disconnected);
  const archivedTokens = tokens.filter((t) => t?.disconnected);
  const totalBurned = tokens.reduce((sum, t) => sum + (Number(t.burned) || 0), 0);
  const totalTransactions = tokens.reduce((sum, t) => sum + (Number(t.txCount) || 0), 0);
  const activeCount = visibleTokens.filter((t) => t.active).length;
  const getBotType = (token) => String(token?.selectedBot || token?.moduleType || "burn");
  const burnBots = visibleTokens.filter((t) => getBotType(t) === "burn");
  const volumeBots = visibleTokens.filter((t) => getBotType(t) === "volume");
  const otherBots = visibleTokens.filter((t) => !["burn", "volume"].includes(getBotType(t)));
  const activeBurnBots = burnBots.filter((t) => t.active).length;
  const activeVolumeBots = volumeBots.filter((t) => t.active).length;
  const activeOtherBots = otherBots.filter((t) => t.active).length;

  const pieColors = ["#22d3ee", "#a78bfa", "#34d399", "#facc15", "#f472b6", "#60a5fa"];
  const tokenBreakdown = tokens
    .map((t) => ({ symbol: t.symbol, amount: Number(t.burned) || 0 }))
    .filter((t) => t.amount > 0)
    .sort((a, b) => b.amount - a.amount);
  const pieHead = tokenBreakdown.slice(0, 5);
  const pieOther = tokenBreakdown.slice(5).reduce((sum, t) => sum + t.amount, 0);
  const pieSlices = pieOther > 0 ? [...pieHead, { symbol: "OTHER", amount: pieOther }] : pieHead;
  const pieTotal = pieSlices.reduce((sum, s) => sum + s.amount, 0);
  const [managerAccess, setManagerAccess] = useState({ enabled: false, username: "" });
  const [managerForm, setManagerForm] = useState({ username: "", password: "" });
  const [managerLoading, setManagerLoading] = useState(false);
  const [managerError, setManagerError] = useState("");
  const [managerMessage, setManagerMessage] = useState("");
  const [telegramState, setTelegramState] = useState({
    connected: false,
    chatIdMasked: "",
    telegramUsername: "",
    botUsername: "",
    connectUrl: "",
    prefs: {
      enabled: false,
      deliveryMode: "smart",
      digestIntervalMin: 15,
      alertDeposit: true,
      alertClaim: true,
      alertBurn: true,
      alertTrade: false,
      alertError: true,
      alertStatus: true,
    },
  });
  const [telegramLoading, setTelegramLoading] = useState(false);
  const [telegramError, setTelegramError] = useState("");
  const [telegramMessage, setTelegramMessage] = useState("");
  const [showManagerModal, setShowManagerModal] = useState(false);
  const [showTelegramModal, setShowTelegramModal] = useState(false);
  const [showReferralModal, setShowReferralModal] = useState(false);
  const [showAdminModal, setShowAdminModal] = useState(false);
  const [referralState, setReferralState] = useState({
    referralCode: "",
    defaultReferralCode: "",
    canCustomizeReferralCode: false,
    totals: { totalEarnedSol: 0, pendingSol: 0, claimedSol: 0 },
    referredUsers: [],
    events: [],
    canClaim: true,
  });
  const [referralClaimDestination, setReferralClaimDestination] = useState("");
  const [referralCodeDraft, setReferralCodeDraft] = useState("");
  const [referralLoading, setReferralLoading] = useState(false);
  const [referralError, setReferralError] = useState("");
  const [referralMessage, setReferralMessage] = useState("");
  const [adminState, setAdminState] = useState({
    settings: null,
    health: null,
    users: [],
    tokens: [],
    referralLeaders: [],
    auditTrail: [],
  });
  const [adminLoading, setAdminLoading] = useState(false);
  const [adminError, setAdminError] = useState("");
  const [adminMessage, setAdminMessage] = useState("");
  const [adminUserSearch, setAdminUserSearch] = useState("");
  const [adminTokenSearch, setAdminTokenSearch] = useState("");

  useEffect(() => {
    if (!canManageAccess || typeof onLoadManagerAccess !== "function") return;
    let alive = true;
    (async () => {
      try {
        const data = await onLoadManagerAccess();
        if (!alive) return;
        setManagerAccess({
          enabled: Boolean(data?.enabled),
          username: String(data?.username || ""),
        });
        setManagerForm((prev) => ({ ...prev, username: String(data?.username || "") }));
      } catch (error) {
        if (!alive) return;
        setManagerError(error?.message || "Unable to load manager access.");
      }
    })();
    return () => {
      alive = false;
    };
  }, [canManageAccess, onLoadManagerAccess]);

  useEffect(() => {
    if (typeof onLoadTelegramAlerts !== "function") return;
    let alive = true;
    (async () => {
      try {
        const data = await onLoadTelegramAlerts();
        if (!alive) return;
        setTelegramState((prev) => ({
          ...prev,
          ...data,
          prefs: {
            ...prev.prefs,
            ...(data?.prefs || {}),
          },
        }));
      } catch (error) {
        if (!alive) return;
        setTelegramError(error?.message || "Unable to load Telegram alerts.");
      }
    })();
    return () => {
      alive = false;
    };
  }, [onLoadTelegramAlerts]);

  useEffect(() => {
    if (!showReferralModal || typeof onLoadReferralSummary !== "function") return;
    let alive = true;
    setReferralLoading(true);
    setReferralError("");
    (async () => {
      try {
        const data = await onLoadReferralSummary();
        if (!alive) return;
        setReferralState((prev) => ({
          ...prev,
          ...data,
          totals: { ...prev.totals, ...(data?.totals || {}) },
          referredUsers: Array.isArray(data?.referredUsers) ? data.referredUsers : [],
          events: Array.isArray(data?.events) ? data.events : [],
        }));
        setReferralCodeDraft((prev) => prev || String(data?.referralCode || ""));
      } catch (error) {
        if (!alive) return;
        setReferralError(error?.message || "Unable to load referrals.");
      } finally {
        if (alive) setReferralLoading(false);
      }
    })();
    return () => {
      alive = false;
    };
  }, [showReferralModal, onLoadReferralSummary]);

  useEffect(() => {
    if (!showAdminModal || !isAdmin || typeof onLoadAdminOverview !== "function") return;
    let alive = true;
    setAdminLoading(true);
    setAdminError("");
    (async () => {
      try {
        const data = await onLoadAdminOverview();
        if (!alive) return;
        setAdminState({
          settings: data?.settings || null,
          health: data?.health || null,
          users: Array.isArray(data?.users) ? data.users : [],
          tokens: Array.isArray(data?.tokens) ? data.tokens : [],
          referralLeaders: Array.isArray(data?.referralLeaders) ? data.referralLeaders : [],
          auditTrail: Array.isArray(data?.auditTrail) ? data.auditTrail : [],
        });
      } catch (error) {
        if (!alive) return;
        setAdminError(error?.message || "Unable to load admin panel.");
      } finally {
        if (alive) setAdminLoading(false);
      }
    })();
    return () => {
      alive = false;
    };
  }, [showAdminModal, isAdmin, onLoadAdminOverview]);

  const saveManager = async () => {
    if (typeof onSaveManagerAccess !== "function") return;
    setManagerError("");
    setManagerMessage("");
    setManagerLoading(true);
    try {
      const data = await onSaveManagerAccess(managerForm);
      setManagerAccess({
        enabled: Boolean(data?.enabled),
        username: String(data?.username || managerForm.username || ""),
      });
      setManagerForm((prev) => ({ ...prev, password: "" }));
      setManagerMessage("Manager access saved.");
    } catch (error) {
      setManagerError(error?.message || "Unable to save manager access.");
    } finally {
      setManagerLoading(false);
    }
  };

  const removeManager = async () => {
    if (typeof onDeleteManagerAccess !== "function") return;
    setManagerError("");
    setManagerMessage("");
    setManagerLoading(true);
    try {
      await onDeleteManagerAccess();
      setManagerAccess({ enabled: false, username: "" });
      setManagerForm({ username: "", password: "" });
      setManagerMessage("Manager access removed.");
    } catch (error) {
      setManagerError(error?.message || "Unable to remove manager access.");
    } finally {
      setManagerLoading(false);
    }
  };

  const saveTelegram = async () => {
    if (typeof onSaveTelegramAlerts !== "function") return;
    setTelegramError("");
    setTelegramMessage("");
    setTelegramLoading(true);
    try {
      const data = await onSaveTelegramAlerts(telegramState.prefs);
      setTelegramState((prev) => ({
        ...prev,
        prefs: {
          ...prev.prefs,
          ...(data?.prefs || {}),
        },
      }));
      setTelegramMessage("Telegram alerts saved.");
    } catch (error) {
      setTelegramError(error?.message || "Unable to save Telegram alerts.");
    } finally {
      setTelegramLoading(false);
    }
  };

  const refreshTelegram = async () => {
    if (typeof onLoadTelegramAlerts !== "function") return;
    setTelegramError("");
    setTelegramLoading(true);
    try {
      const data = await onLoadTelegramAlerts();
      setTelegramState((prev) => ({
        ...prev,
        ...data,
        prefs: {
          ...prev.prefs,
          ...(data?.prefs || {}),
        },
      }));
    } catch (error) {
      setTelegramError(error?.message || "Unable to refresh Telegram alerts.");
    } finally {
      setTelegramLoading(false);
    }
  };

  const disconnectTelegram = async () => {
    if (typeof onDisconnectTelegramAlerts !== "function") return;
    setTelegramError("");
    setTelegramMessage("");
    setTelegramLoading(true);
    try {
      await onDisconnectTelegramAlerts();
      const data = await onLoadTelegramAlerts();
      setTelegramState((prev) => ({
        ...prev,
        ...data,
        prefs: {
          ...prev.prefs,
          ...(data?.prefs || {}),
        },
      }));
      setTelegramMessage("Telegram alerts disconnected.");
    } catch (error) {
      setTelegramError(error?.message || "Unable to disconnect Telegram alerts.");
    } finally {
      setTelegramLoading(false);
    }
  };

  const sendTelegramTest = async () => {
    if (typeof onSendTelegramTestAlert !== "function") return;
    setTelegramError("");
    setTelegramMessage("");
    setTelegramLoading(true);
    try {
      await onSendTelegramTestAlert();
      setTelegramMessage("Test alert sent.");
    } catch (error) {
      setTelegramError(error?.message || "Unable to send Telegram test alert.");
    } finally {
      setTelegramLoading(false);
    }
  };

  const claimReferral = async () => {
    if (typeof onClaimReferralEarnings !== "function") return;
    setReferralError("");
    setReferralMessage("");
    setReferralLoading(true);
    try {
      const result = await onClaimReferralEarnings({ destinationWallet: referralClaimDestination });
      const data = await onLoadReferralSummary();
      setReferralState((prev) => ({
        ...prev,
        ...data,
        totals: { ...prev.totals, ...(data?.totals || {}) },
        referredUsers: Array.isArray(data?.referredUsers) ? data.referredUsers : [],
        events: Array.isArray(data?.events) ? data.events : [],
      }));
      setReferralMessage(`Claimed ${Number(result?.claimedSol || 0).toFixed(6)} SOL to ${result?.destinationWallet || "wallet"}.`);
    } catch (error) {
      setReferralError(error?.message || "Unable to claim referral earnings.");
    } finally {
      setReferralLoading(false);
    }
  };

  const saveReferralCode = async () => {
    if (typeof onUpdateReferralCode !== "function") return;
    setReferralError("");
    setReferralMessage("");
    setReferralLoading(true);
    try {
      const result = await onUpdateReferralCode(referralCodeDraft);
      const data = await onLoadReferralSummary();
      setReferralState((prev) => ({
        ...prev,
        ...data,
        totals: { ...prev.totals, ...(data?.totals || {}) },
        referredUsers: Array.isArray(data?.referredUsers) ? data.referredUsers : [],
        events: Array.isArray(data?.events) ? data.events : [],
      }));
      setReferralCodeDraft(String(result?.referralCode || referralCodeDraft || ""));
      setReferralMessage(`Referral code updated to ${String(result?.referralCode || referralCodeDraft || "").trim()}.`);
    } catch (error) {
      setReferralError(error?.message || "Unable to update referral code.");
    } finally {
      setReferralLoading(false);
    }
  };

  const refreshAdmin = async () => {
    if (!isAdmin || typeof onLoadAdminOverview !== "function") return;
    const data = await onLoadAdminOverview();
    setAdminState({
      settings: data?.settings || null,
      health: data?.health || null,
      users: Array.isArray(data?.users) ? data.users : [],
      tokens: Array.isArray(data?.tokens) ? data.tokens : [],
      referralLeaders: Array.isArray(data?.referralLeaders) ? data.referralLeaders : [],
      auditTrail: Array.isArray(data?.auditTrail) ? data.auditTrail : [],
    });
  };

  const filteredAdminUsers = useMemo(() => {
    const query = String(adminUserSearch || "").trim().toLowerCase();
    if (!query) return adminState.users;
    return adminState.users.filter((row) =>
      [
        row.username,
        row.referralCode,
        row.referrerCode,
        row.referrerUsername,
        row.bannedReason,
      ]
        .map((value) => String(value || "").toLowerCase())
        .some((value) => value.includes(query))
    );
  }, [adminState.users, adminUserSearch]);

  const filteredAdminTokens = useMemo(() => {
    const query = String(adminTokenSearch || "").trim().toLowerCase();
    if (!query) return adminState.tokens;
    return adminState.tokens.filter((row) =>
      [
        row.symbol,
        row.mint,
        row.username,
        row.selectedBot,
        row.deposit,
      ]
        .map((value) => String(value || "").toLowerCase())
        .some((value) => value.includes(query))
    );
  }, [adminState.tokens, adminTokenSearch]);

  const personalBotIntensity = Math.max(0, Math.min(100, Number(adminState.settings?.personalBotIntensity ?? 45)));
  const personalBotSafety = Math.max(0, Math.min(100, Number(adminState.settings?.personalBotSafety ?? 65)));
  const personalBotModeLabel = {
    burn: "Burn",
    volume: "Volume",
    market_maker: "Market Maker",
    dca: "DCA",
    rekindle: "Rekindle",
  }[String(adminState.settings?.personalBotMode || "burn")] || "Burn";
  const intensityLabel =
    personalBotIntensity >= 75 ? "high" : personalBotIntensity >= 40 ? "medium" : "low";
  const safetyLabel =
    personalBotSafety >= 75 ? "very conservative" : personalBotSafety >= 45 ? "balanced" : "aggressive";

  return (
    <div style={{ position: "relative", zIndex: 2, maxWidth: 1300, margin: "0 auto", padding: "32px 24px" }}>
      <div style={{ marginBottom: 24, animation: "slideUp .4s ease" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap", marginBottom: 4 }}>
          <h1 style={{ fontSize: 26, fontWeight: 800, color: "#fff", margin: 0 }}>{t("dashboard.hey", { user: username })} {"\u{1F525}"}</h1>
          {isOg && (
            <span style={{ padding: "5px 10px", borderRadius: 999, fontSize: 11, fontWeight: 800, letterSpacing: 0.8, color: "#fff2bd", background: "linear-gradient(135deg, rgba(255,197,61,.92), rgba(255,119,0,.88))", boxShadow: "0 0 18px rgba(255,170,0,.28)" }}>
              OG
            </span>
          )}
          {isAdmin && (
            <span style={{ padding: "5px 10px", borderRadius: 999, fontSize: 11, fontWeight: 800, letterSpacing: 0.8, color: "#d8ecff", background: "linear-gradient(135deg, rgba(43,112,255,.92), rgba(0,189,255,.88))" }}>
              ADMIN
            </span>
          )}
          {isManager && (
            <span style={{ padding: "5px 10px", borderRadius: 999, fontSize: 11, fontWeight: 800, letterSpacing: 0.8, color: "#ffd9b8", background: "rgba(255,106,0,.18)", border: "1px solid rgba(255,106,0,.28)" }}>
              MANAGER
            </span>
          )}
        </div>
        <p style={{ fontSize: 13, color: "rgba(255,255,255,.3)" }}>{t("dashboard.synced")}</p>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(3,1fr)", gap: 14, marginBottom: 28 }}>
        {[
          { l: t("dashboard.tokensConnected"), v: visibleTokens.length, s: t("dashboard.integrations"), i: "\u{1FA99}", c: "#ff8c42" },
          { l: t("dashboard.totalIncinerated"), v: fmt(totalBurned), s: t("dashboard.tokensGone"), i: "\u{1F525}", c: "#ff4500" },
          { l: t("dashboard.activeBots"), v: activeCount, s: t("dashboard.runningNow"), i: "\u26A1", c: "#ffd700" },
        ].map((k, i) => (
          <div
            key={k.l}
            className="glass"
            style={{
              padding: "20px 20px",
              animation: `slideUp .4s ease ${i * 0.07}s both`,
              display: "flex",
              flexDirection: "column",
              alignItems: "center",
              textAlign: "center",
            }}
          >
            <div style={{ fontSize: 28, marginBottom: 10, lineHeight: 1 }}>{k.i}</div>
            <div style={{ fontSize: 10, color: "rgba(255,255,255,.28)", fontWeight: 700, letterSpacing: 1, marginBottom: 8 }}>{k.l}</div>
            <div style={{ fontSize: 32, fontWeight: 800, color: k.c, lineHeight: 1, marginBottom: 4 }}>{k.v}</div>
            <div style={{ fontSize: 11, color: "rgba(255,255,255,.2)", fontWeight: 600 }}>{k.s}</div>
          </div>
        ))}
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 350px", gap: 20, alignItems: "start" }}>
        <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12, flexWrap: "wrap" }}>
            <div>
              <div style={{ fontSize: 17, fontWeight: 800, color: "#fff" }}>{t("dashboard.yourTokens")}</div>
              <div style={{ fontSize: 12, color: "rgba(255,255,255,.28)" }}>{t("dashboard.tokensHint")}</div>
            </div>
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap", justifyContent: "flex-end" }}>
              <button className="btn-ghost" onClick={() => setShowReferralModal(true)} style={{ padding: "9px 14px", fontSize: 12 }}>
                Referrals
              </button>
              <button className="btn-ghost" onClick={() => setShowTelegramModal(true)} style={{ padding: "9px 14px", fontSize: 12 }}>
                Telegram Alerts
              </button>
              <button className="btn-ghost" onClick={() => setShowManagerModal(true)} style={{ padding: "9px 14px", fontSize: 12 }}>
                {canManageAccess ? "Team Access" : "Access Level"}
              </button>
              {isAdmin && (
                <button className="btn-ghost" onClick={() => setShowAdminModal(true)} style={{ padding: "9px 14px", fontSize: 12 }}>
                  Admin
                </button>
              )}
              <button className="btn-fire" onClick={onShowAttach} style={{ padding: "9px 18px", fontSize: 13 }}>
                + {t("dashboard.attachToken")}
              </button>
            </div>
          </div>

          {visibleTokens.map((t) => (
            <TokenCard
              key={t.id}
              token={t}
              allLogs={allLogs}
              onUpdate={onUpdateToken}
              onDelete={onDeleteToken}
              onPermanentDelete={onPermanentDeleteToken}
              onRestore={onRestoreToken}
              onFetchDetails={onFetchTokenDetails}
              onFetchDeployWallet={onFetchDeployWallet}
              onFetchVolumeWithdrawOptions={onFetchVolumeWithdrawOptions}
              onVolumeSweep={onVolumeSweep}
              onVolumeWithdraw={onVolumeWithdraw}
              onBurnWithdraw={onBurnWithdraw}
              canManageFunds={Boolean(user?.canManageFunds)}
              canDelete={Boolean(user?.canDelete)}
            />
          ))}
          {!visibleTokens.length && (
            <div className="glass" style={{ padding: "18px 20px", fontSize: 13, color: "rgba(255,255,255,.5)", textAlign: "center" }}>
              No active bots on dashboard. Use Attach to connect a token.
            </div>
          )}

          {archivedTokens.length > 0 && (
            <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
              <div>
                <div style={{ fontSize: 15, fontWeight: 800, color: "#fff" }}>Archived EMBR Tokens</div>
                <div style={{ fontSize: 12, color: "rgba(255,255,255,.28)" }}>
                  Archived tokens stay on your account and can be restored in paused mode.
                </div>
              </div>
              {archivedTokens.map((t) => (
                <TokenCard
                  key={t.id}
                  token={t}
                  allLogs={allLogs}
                  onUpdate={onUpdateToken}
                  onDelete={onDeleteToken}
                  onPermanentDelete={onPermanentDeleteToken}
                  onRestore={onRestoreToken}
                  onFetchDetails={onFetchTokenDetails}
                  onFetchDeployWallet={onFetchDeployWallet}
                  onFetchVolumeWithdrawOptions={onFetchVolumeWithdrawOptions}
                  onVolumeSweep={onVolumeSweep}
                  onVolumeWithdraw={onVolumeWithdraw}
                  onBurnWithdraw={onBurnWithdraw}
                  canManageFunds={Boolean(user?.canManageFunds)}
                  canDelete={Boolean(user?.canDelete)}
                />
              ))}
            </div>
          )}

          <div className="glass" style={{ padding: "22px 24px" }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 18 }}>
              <div>
                <div style={{ fontSize: 15, fontWeight: 800, color: "#fff" }}>{t("dashboard.burnActivity")}</div>
                <div style={{ fontSize: 12, color: "rgba(255,255,255,.28)" }}>{t("dashboard.last7Days")}</div>
              </div>
              <div style={{ fontSize: 12, color: "#ff8c42", fontWeight: 700 }}>
                {fmt(chartData.reduce((sum, d) => sum + (Number(d?.v) || 0), 0))} {t("dashboard.total")}
              </div>
            </div>

            <div style={{ display: "grid", gridTemplateColumns: "minmax(0,1fr) 220px", gap: 14, alignItems: "start" }}>
              <BurnChart data={chartData} />

              <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 10 }}>
                {pieTotal > 0 ? (
                  <>
                    <svg width="150" height="150" viewBox="0 0 162 162" aria-label="Burn distribution pie chart">
                      <circle cx="81" cy="81" r="50" fill="none" stroke="rgba(255,255,255,.08)" strokeWidth="18" />
                      {(() => {
                        const circumference = 2 * Math.PI * 50;
                        let acc = 0;
                        return pieSlices.map((s, i) => {
                          const frac = s.amount / pieTotal;
                          const dash = frac * circumference;
                          const el = (
                            <circle
                              key={`${s.symbol}-${i}`}
                              cx="81"
                              cy="81"
                              r="50"
                              fill="none"
                              stroke={pieColors[i % pieColors.length]}
                              strokeWidth="18"
                              strokeDasharray={`${dash} ${circumference - dash}`}
                              strokeDashoffset={-acc * circumference}
                              transform="rotate(-90 81 81)"
                              strokeLinecap="butt"
                            />
                          );
                          acc += frac;
                          return el;
                        });
                      })()}
                    </svg>

                    <div style={{ fontSize: 11, color: "rgba(255,255,255,.42)", fontWeight: 700, letterSpacing: 0.8, textTransform: "uppercase" }}>
                      {t("dashboard.burnShareByToken")}
                    </div>

                    <div style={{ width: "100%", maxHeight: 118, overflowY: "auto", paddingRight: 4 }}>
                      <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                        {pieSlices.map((s, i) => {
                          const pct = pieTotal > 0 ? ((s.amount / pieTotal) * 100).toFixed(1) : "0.0";
                          return (
                            <div key={`dash-legend-${s.symbol}-${i}`} style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8 }}>
                              <span style={{ display: "inline-flex", alignItems: "center", gap: 6, fontSize: 11, color: "rgba(255,255,255,.72)", fontWeight: 700 }}>
                                <span style={{ width: 8, height: 8, borderRadius: 999, background: pieColors[i % pieColors.length], display: "inline-block" }} />
                                {s.symbol}
                              </span>
                              <span style={{ fontSize: 11, color: "rgba(255,255,255,.48)", fontFamily: "'JetBrains Mono', monospace" }}>{pct}%</span>
                            </div>
                          );
                        })}
                      </div>
                    </div>
                  </>
                ) : (
                  <div style={{ height: 180, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 12, color: "rgba(255,255,255,.32)" }}>
                    {t("dashboard.noTokenData")}
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
          <div className="glass" style={{ padding: "20px 22px" }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
              <div style={{ fontSize: 14, fontWeight: 800, color: "#fff" }}>{t("dashboard.liveFeed")}</div>
              <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
                <div style={{ width: 5, height: 5, borderRadius: "50%", background: "#ff6a00", animation: "pulse-dot 1.5s infinite" }} />
                <span style={{ fontSize: 10, color: "#ff8c42", fontWeight: 700 }}>{t("dashboard.realtime")}</span>
              </div>
            </div>
            <LiveFeed events={feed} />
          </div>

          <div className="glass" style={{ padding: "20px 22px" }}>
            <div style={{ fontSize: 14, fontWeight: 800, color: "#fff", marginBottom: 14, textAlign: "center" }}>{t("dashboard.protocolStats")}</div>
            {[
              [t("dashboard.allTimeBurned"), `${fmt(totalBurned)} ${t("dashboard.tokens")}`],
              [t("dashboard.totalTransactions"), fmtFull(totalTransactions)],
              [t("dashboard.activeSchedulers"), `${activeCount} / ${visibleTokens.length}`],
              [t("dashboard.burnBots"), `${activeBurnBots} / ${burnBots.length}`],
              [t("dashboard.volumeBots"), `${activeVolumeBots} / ${volumeBots.length}`],
              [t("dashboard.otherBots"), `${activeOtherBots} / ${otherBots.length}`],
            ].map(([k, v]) => (
              <div key={k} style={{ display: "flex", justifyContent: "space-between", padding: "9px 0", borderBottom: "1px solid rgba(255,255,255,.04)", fontSize: 13 }}>
                <span style={{ color: "rgba(255,255,255,.32)", fontWeight: 600 }}>{k}</span>
                <span style={{ color: "#fff", fontWeight: 700 }}>{v}</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      {showReferralModal && (
        <ActionModal title="Referrals" onClose={() => setShowReferralModal(false)} topOffset={112}>
          <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
            <div style={{ fontSize: 12, color: "rgba(255,255,255,.46)", lineHeight: 1.6 }}>
              Standard accounts pay 10% protocol fees. Referred accounts keep the same total fee, but part of it accrues to the referrer. OG accounts pay 0%.
            </div>
            <div className="glass" style={{ padding: "14px 16px", display: "grid", gridTemplateColumns: "repeat(3, minmax(0,1fr))", gap: 10 }}>
              {[
                ["Pending", `${Number(referralState.totals?.pendingSol || 0).toFixed(6)} SOL`],
                ["Total Earned", `${Number(referralState.totals?.totalEarnedSol || 0).toFixed(6)} SOL`],
                ["Claimed", `${Number(referralState.totals?.claimedSol || 0).toFixed(6)} SOL`],
              ].map(([label, value]) => (
                <div key={label} style={{ textAlign: "center" }}>
                  <div style={{ fontSize: 10, color: "rgba(255,255,255,.35)", letterSpacing: 1, fontWeight: 700 }}>{label.toUpperCase()}</div>
                  <div style={{ marginTop: 6, fontSize: 16, fontWeight: 800, color: "#fff" }}>{value}</div>
                </div>
              ))}
            </div>
            <div>
              <div style={{ fontSize: 10, color: "rgba(255,255,255,.35)", letterSpacing: 1, marginBottom: 6, fontWeight: 700 }}>YOUR REFERRAL CODE</div>
              <div className="input-f" style={{ display: "flex", alignItems: "center", minHeight: 42 }}>{referralState.referralCode || "Loading..."}</div>
            </div>
            <div className="glass" style={{ padding: "12px 14px", border: "1px solid rgba(255,255,255,.06)" }}>
              <div style={{ fontSize: 12, fontWeight: 800, color: "#fff", marginBottom: 8 }}>Customize Referral Code</div>
              <div style={{ fontSize: 11, color: "rgba(255,255,255,.48)", lineHeight: 1.55, marginBottom: 10 }}>
                You can replace the default referral code once. After that, it is locked. Pick carefully.
              </div>
              <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                <input
                  className="input-f"
                  value={referralCodeDraft}
                  onChange={(e) => setReferralCodeDraft(String(e.target.value || "").toUpperCase())}
                  placeholder="Enter a custom referral code"
                  spellCheck={false}
                  disabled={!referralState.canCustomizeReferralCode || !referralState.canClaim}
                  style={{ flex: 1, minWidth: 220 }}
                />
                <button
                  className="btn-ghost"
                  onClick={saveReferralCode}
                  disabled={
                    referralLoading ||
                    !referralState.canCustomizeReferralCode ||
                    !referralState.canClaim ||
                    !String(referralCodeDraft || "").trim()
                  }
                  style={{ padding: "9px 16px", fontSize: 12 }}
                >
                  {referralLoading ? "Saving..." : "Set Code"}
                </button>
              </div>
              <div style={{ fontSize: 11, color: "rgba(255,255,255,.38)", marginTop: 8 }}>
                Allowed: A-Z, 0-9, underscore. Length 4-40.
              </div>
              {!referralState.canClaim && (
                <div style={{ fontSize: 11, color: "rgba(255,178,107,.86)", marginTop: 8 }}>
                  Manager logins cannot change the owner referral code.
                </div>
              )}
              {referralState.canClaim && !referralState.canCustomizeReferralCode && (
                <div style={{ fontSize: 11, color: "rgba(255,178,107,.86)", marginTop: 8 }}>
                  This referral code has already been customized and is now locked.
                </div>
              )}
            </div>
            <div>
              <div style={{ fontSize: 10, color: "rgba(255,255,255,.35)", letterSpacing: 1, marginBottom: 6, fontWeight: 700 }}>CLAIM DESTINATION</div>
              <input
                className="input-f"
                value={referralClaimDestination}
                onChange={(e) => setReferralClaimDestination(e.target.value)}
                placeholder="Enter a Solana wallet address"
                spellCheck={false}
              />
              <div style={{ fontSize: 11, color: "rgba(255,255,255,.42)", marginTop: 6 }}>
                Referral payouts can be sent to any valid Solana wallet address you control.
              </div>
            </div>
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
              <button
                className="btn-fire"
                onClick={claimReferral}
                disabled={
                  referralLoading ||
                  !referralState.canClaim ||
                  !referralClaimDestination ||
                  Number(referralState.totals?.pendingSol || 0) <= 0
                }
                style={{ padding: "9px 16px", fontSize: 12 }}
              >
                {referralLoading ? "Claiming..." : "Claim Earnings"}
              </button>
              {!referralState.canClaim && (
                <div style={{ fontSize: 12, color: "rgba(255,178,107,.86)", alignSelf: "center" }}>
                  Manager logins can view referrals but cannot claim.
                </div>
              )}
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
              <div className="glass" style={{ padding: "14px 16px" }}>
                <div style={{ fontSize: 13, fontWeight: 800, color: "#fff", marginBottom: 10 }}>Referred Users</div>
                <div style={{ display: "flex", flexDirection: "column", gap: 8, maxHeight: 220, overflowY: "auto" }}>
                  {referralState.referredUsers.length ? referralState.referredUsers.map((row) => (
                    <div key={row.userId} style={{ display: "flex", justifyContent: "space-between", gap: 8, fontSize: 12, alignItems: "flex-start" }}>
                      <div>
                        <div style={{ color: "#fff", fontWeight: 700 }}>{row.username}</div>
                        <div style={{ color: "rgba(255,255,255,.45)", fontSize: 11, marginTop: 4 }}>{row.referralStatus || "Eligible"}</div>
                        {(row.hasWalletOverlap || row.hasIpOverlap) && (
                          <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginTop: 6 }}>
                            {row.hasWalletOverlap && <span style={{ fontSize: 10, fontWeight: 800, color: "#ffd3a7", background: "rgba(255,127,62,.18)", borderRadius: 999, padding: "3px 8px" }}>WALLET OVERLAP</span>}
                            {row.hasIpOverlap && <span style={{ fontSize: 10, fontWeight: 800, color: "#ffd3a7", background: "rgba(255,127,62,.18)", borderRadius: 999, padding: "3px 8px" }}>IP OVERLAP</span>}
                          </div>
                        )}
                      </div>
                      <span style={{ color: "rgba(255,255,255,.52)" }}>{Number(row.earnedSol || 0).toFixed(6)} SOL</span>
                    </div>
                  )) : (
                    <div style={{ fontSize: 12, color: "rgba(255,255,255,.38)" }}>No referrals yet.</div>
                  )}
                </div>
              </div>
              <div className="glass" style={{ padding: "14px 16px" }}>
                <div style={{ fontSize: 13, fontWeight: 800, color: "#fff", marginBottom: 10 }}>Referral Earnings</div>
                <div style={{ display: "flex", flexDirection: "column", gap: 8, maxHeight: 220, overflowY: "auto" }}>
                  {referralState.events.length ? referralState.events.map((row) => (
                    <div key={row.id} style={{ borderBottom: "1px solid rgba(255,255,255,.05)", paddingBottom: 8 }}>
                      <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                        <span style={{ color: "#fff", fontSize: 12, fontWeight: 700 }}>{row.username}</span>
                        <span style={{ color: "#7ae7ab", fontSize: 12, fontWeight: 700 }}>{Number(row.referralSol || 0).toFixed(6)} SOL</span>
                      </div>
                      <div style={{ fontSize: 11, color: "rgba(255,255,255,.38)", marginTop: 4 }}>
                        {row.tokenSymbol} · {row.moduleType || "bot"}
                      </div>
                    </div>
                  )) : (
                    <div style={{ fontSize: 12, color: "rgba(255,255,255,.38)" }}>No earnings logged yet.</div>
                  )}
                </div>
              </div>
            </div>
            {referralError && <div style={{ fontSize: 12, color: "#ff8f8f" }}>{referralError}</div>}
            {referralMessage && <div style={{ fontSize: 12, color: "#7ae7ab" }}>{referralMessage}</div>}
          </div>
        </ActionModal>
      )}

      {showAdminModal && isAdmin && (
        <ActionModal title="Admin Panel" onClose={() => setShowAdminModal(false)} topOffset={148}>
          <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
            <div style={{ fontSize: 12, color: "rgba(255,255,255,.46)", lineHeight: 1.6 }}>
              Admin controls are bound to the primary `satoshEH_` account. Manager access does not inherit admin rights.
            </div>

            {adminState.health && (
              <div className="glass" style={{ padding: "14px 16px" }}>
                <div style={{ fontSize: 13, fontWeight: 800, color: "#fff", marginBottom: 10 }}>Protocol Health</div>
                <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0,1fr))", gap: 10 }}>
                  {[
                    ["Users", adminState.health.userCount],
                    ["Banned", adminState.health.bannedUserCount],
                    ["Tokens", adminState.health.tokenCount],
                    ["Hidden", adminState.health.hiddenTokenCount],
                    ["Archived", adminState.health.archivedTokenCount],
                    ["Active", adminState.health.activeTokenCount],
                    ["Live Bots", adminState.health.enabledModuleCount],
                    ["Errors 24h", adminState.health.errorCount24h],
                  ].map(([label, value]) => (
                    <div key={label} style={{ padding: "10px 12px", borderRadius: 12, border: "1px solid rgba(255,255,255,.06)", background: "rgba(255,255,255,.02)" }}>
                      <div style={{ fontSize: 10, fontWeight: 800, color: "rgba(255,255,255,.35)", letterSpacing: 0.8 }}>{label}</div>
                      <div style={{ fontSize: 16, fontWeight: 800, color: "#fff", marginTop: 6 }}>{fmtFull(value)}</div>
                    </div>
                  ))}
                </div>
                <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0,1fr))", gap: 10, marginTop: 10 }}>
                  <div style={{ padding: "10px 12px", borderRadius: 12, border: "1px solid rgba(255,255,255,.06)", background: "rgba(255,255,255,.02)" }}>
                    <div style={{ fontSize: 10, fontWeight: 800, color: "rgba(255,255,255,.35)", letterSpacing: 0.8 }}>DEPOSIT POOL</div>
                    <div style={{ fontSize: 12, color: "#fff", marginTop: 6 }}>{adminState.health.depositPoolSummary || "No pool data."}</div>
                  </div>
                  <div style={{ padding: "10px 12px", borderRadius: 12, border: "1px solid rgba(255,255,255,.06)", background: "rgba(255,255,255,.02)" }}>
                    <div style={{ fontSize: 10, fontWeight: 800, color: "rgba(255,255,255,.35)", letterSpacing: 0.8 }}>TREASURY SOL</div>
                    <div style={{ fontSize: 16, fontWeight: 800, color: "#fff", marginTop: 6 }}>{Number(adminState.health.treasurySol || 0).toFixed(4)}</div>
                  </div>
                  <div style={{ padding: "10px 12px", borderRadius: 12, border: "1px solid rgba(255,255,255,.06)", background: "rgba(255,255,255,.02)" }}>
                    <div style={{ fontSize: 10, fontWeight: 800, color: "rgba(255,255,255,.35)", letterSpacing: 0.8 }}>DEV WALLET SOL</div>
                    <div style={{ fontSize: 16, fontWeight: 800, color: "#fff", marginTop: 6 }}>{Number(adminState.health.devWalletSol || 0).toFixed(4)}</div>
                  </div>
                </div>
              </div>
            )}
            <div className="glass" style={{ padding: "14px 16px" }}>
              <div style={{ fontSize: 13, fontWeight: 800, color: "#fff", marginBottom: 10 }}>Protocol Bot</div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
                <div>
                  <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", marginBottom: 6, fontWeight: 700 }}>BOT MODE</label>
                  <select className="input-f" value={adminState.settings?.personalBotMode || "burn"} onChange={(e) => setAdminState((prev) => ({ ...prev, settings: { ...(prev.settings || {}), personalBotMode: e.target.value } }))}>
                    <option value="burn">Burn</option>
                    <option value="volume">Volume</option>
                    <option value="market_maker">Market Maker</option>
                    <option value="dca">DCA</option>
                    <option value="rekindle">Rekindle</option>
                  </select>
                </div>
                <div style={{ display: "flex", alignItems: "flex-end" }}>
                  <label style={{ display: "inline-flex", alignItems: "center", gap: 8, fontSize: 12, color: "#fff", fontWeight: 700 }}>
                    <input
                      type="checkbox"
                      checked={Boolean(adminState.settings?.personalBotEnabled)}
                      onChange={(e) => setAdminState((prev) => ({ ...prev, settings: { ...(prev.settings || {}), personalBotEnabled: e.target.checked } }))}
                    />
                    Protocol bot enabled
                  </label>
                </div>
              </div>
              <div style={{ marginTop: 12 }}>
                <div style={{ display: "flex", justifyContent: "space-between", gap: 8, fontSize: 11, color: "rgba(255,255,255,.62)", marginBottom: 6 }}>
                  <span>Intensity</span>
                  <span>{personalBotIntensity}%</span>
                </div>
                <input type="range" min="0" max="100" value={personalBotIntensity} onChange={(e) => setAdminState((prev) => ({ ...prev, settings: { ...(prev.settings || {}), personalBotIntensity: Math.max(0, Number(e.target.value) || 0) } }))} style={{ width: "100%" }} />
              </div>
              <div style={{ marginTop: 10 }}>
                <div style={{ display: "flex", justifyContent: "space-between", gap: 8, fontSize: 11, color: "rgba(255,255,255,.62)", marginBottom: 6 }}>
                  <span>Funding Safety</span>
                  <span>{personalBotSafety}%</span>
                </div>
                <input type="range" min="0" max="100" value={personalBotSafety} onChange={(e) => setAdminState((prev) => ({ ...prev, settings: { ...(prev.settings || {}), personalBotSafety: Math.max(0, Number(e.target.value) || 0) } }))} style={{ width: "100%" }} />
              </div>
              <div style={{ fontSize: 12, color: "rgba(255,255,255,.52)", lineHeight: 1.6, marginTop: 10 }}>
                {personalBotModeLabel} is set to <span style={{ color: "#fff", fontWeight: 700 }}>{intensityLabel}</span> intensity with a <span style={{ color: "#fff", fontWeight: 700 }}>{safetyLabel}</span> reserve posture.
              </div>
            </div>
            <div className="glass" style={{ padding: "14px 16px" }}>
              <div style={{ fontSize: 13, fontWeight: 800, color: "#fff", marginBottom: 10 }}>Protocol Fees</div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0,1fr))", gap: 10 }}>
                <div>
                  <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", marginBottom: 6, fontWeight: 700 }}>TOTAL FEE BPS</label>
                  <input className="input-f" type="number" value={adminState.settings?.defaultFeeBps || 1000} onChange={(e) => setAdminState((prev) => ({ ...prev, settings: { ...(prev.settings || {}), defaultFeeBps: Math.max(0, Number(e.target.value) || 0) } }))} />
                </div>
                <div>
                  <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", marginBottom: 6, fontWeight: 700 }}>TREASURY BPS</label>
                  <input className="input-f" type="number" value={adminState.settings?.defaultTreasuryBps || 500} onChange={(e) => setAdminState((prev) => ({ ...prev, settings: { ...(prev.settings || {}), defaultTreasuryBps: Math.max(0, Number(e.target.value) || 0) } }))} />
                </div>
                <div>
                  <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", marginBottom: 6, fontWeight: 700 }}>BURN BPS</label>
                  <input className="input-f" type="number" value={adminState.settings?.defaultBurnBps || 500} onChange={(e) => setAdminState((prev) => ({ ...prev, settings: { ...(prev.settings || {}), defaultBurnBps: Math.max(0, Number(e.target.value) || 0) } }))} />
                </div>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0,1fr))", gap: 10, marginTop: 10 }}>
                <div>
                  <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", marginBottom: 6, fontWeight: 700 }}>REFERRED TREASURY</label>
                  <input className="input-f" type="number" value={adminState.settings?.referredTreasuryBps || 250} onChange={(e) => setAdminState((prev) => ({ ...prev, settings: { ...(prev.settings || {}), referredTreasuryBps: Math.max(0, Number(e.target.value) || 0) } }))} />
                </div>
                <div>
                  <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", marginBottom: 6, fontWeight: 700 }}>REFERRED BURN</label>
                  <input className="input-f" type="number" value={adminState.settings?.referredBurnBps || 250} onChange={(e) => setAdminState((prev) => ({ ...prev, settings: { ...(prev.settings || {}), referredBurnBps: Math.max(0, Number(e.target.value) || 0) } }))} />
                </div>
                <div>
                  <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", marginBottom: 6, fontWeight: 700 }}>REFERRED REFERRAL</label>
                  <input className="input-f" type="number" value={adminState.settings?.referredReferralBps || 500} onChange={(e) => setAdminState((prev) => ({ ...prev, settings: { ...(prev.settings || {}), referredReferralBps: Math.max(0, Number(e.target.value) || 0) } }))} />
                </div>
              </div>
              <div style={{ fontSize: 12, fontWeight: 800, color: "#fff", marginTop: 14, marginBottom: 10 }}>Tool Pricing (SOL)</div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0,1fr))", gap: 10 }}>
                <div>
                  <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", marginBottom: 6, fontWeight: 700 }}>HOLDER POOLER</label>
                  <input
                    className="input-f"
                    type="number"
                    step="0.001"
                    value={(Number(adminState.settings?.toolFeeHolderPoolerLamports || 0) / 1e9).toFixed(3)}
                    onChange={(e) =>
                      setAdminState((prev) => ({
                        ...prev,
                        settings: {
                          ...(prev.settings || {}),
                          toolFeeHolderPoolerLamports: Math.max(0, Math.floor((Number(e.target.value) || 0) * 1e9)),
                        },
                      }))
                    }
                  />
                </div>
                <div>
                  <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", marginBottom: 6, fontWeight: 700 }}>REACTION MANAGER</label>
                  <input
                    className="input-f"
                    type="number"
                    step="0.001"
                    value={(Number(adminState.settings?.toolFeeReactionManagerLamports || 0) / 1e9).toFixed(3)}
                    onChange={(e) =>
                      setAdminState((prev) => ({
                        ...prev,
                        settings: {
                          ...(prev.settings || {}),
                          toolFeeReactionManagerLamports: Math.max(0, Math.floor((Number(e.target.value) || 0) * 1e9)),
                        },
                      }))
                    }
                  />
                </div>
                <div>
                  <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", marginBottom: 6, fontWeight: 700 }}>BUNDLE MANAGER</label>
                  <input
                    className="input-f"
                    type="number"
                    step="0.001"
                    value={(Number(adminState.settings?.toolFeeBundleManagerLamports || 0) / 1e9).toFixed(3)}
                    onChange={(e) =>
                      setAdminState((prev) => ({
                        ...prev,
                        settings: {
                          ...(prev.settings || {}),
                          toolFeeBundleManagerLamports: Math.max(0, Math.floor((Number(e.target.value) || 0) * 1e9)),
                        },
                      }))
                    }
                  />
                </div>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0,1fr))", gap: 10, marginTop: 10 }}>
                <div>
                  <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", marginBottom: 6, fontWeight: 700 }}>SMART SELL SETUP</label>
                  <input
                    className="input-f"
                    type="number"
                    step="0.001"
                    value={(Number(adminState.settings?.toolFeeSmartSellLamports || 0) / 1e9).toFixed(3)}
                    onChange={(e) =>
                      setAdminState((prev) => ({
                        ...prev,
                        settings: {
                          ...(prev.settings || {}),
                          toolFeeSmartSellLamports: Math.max(0, Math.floor((Number(e.target.value) || 0) * 1e9)),
                        },
                      }))
                    }
                  />
                </div>
                <div>
                  <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", marginBottom: 6, fontWeight: 700 }}>SMART SELL RUNTIME</label>
                  <input
                    className="input-f"
                    type="number"
                    step="0.001"
                    value={(Number(adminState.settings?.toolFeeSmartSellRuntimeLamports || 0) / 1e9).toFixed(3)}
                    onChange={(e) =>
                      setAdminState((prev) => ({
                        ...prev,
                        settings: {
                          ...(prev.settings || {}),
                          toolFeeSmartSellRuntimeLamports: Math.max(0, Math.floor((Number(e.target.value) || 0) * 1e9)),
                        },
                      }))
                    }
                  />
                </div>
                <div>
                  <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", marginBottom: 6, fontWeight: 700 }}>SMART SELL WINDOW (H)</label>
                  <input
                    className="input-f"
                    type="number"
                    min="0"
                    step="1"
                    value={adminState.settings?.toolFeeSmartSellRuntimeWindowHours || 24}
                    onChange={(e) =>
                      setAdminState((prev) => ({
                        ...prev,
                        settings: {
                          ...(prev.settings || {}),
                          toolFeeSmartSellRuntimeWindowHours: Math.max(0, Math.floor(Number(e.target.value) || 0)),
                        },
                      }))
                    }
                  />
                </div>
              </div>
              <div style={{ fontSize: 12, fontWeight: 800, color: "#fff", marginTop: 14, marginBottom: 10 }}>Telegram Trade Fees (SOL)</div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0,1fr))", gap: 10 }}>
                <div>
                  <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", marginBottom: 6, fontWeight: 700 }}>BUY FEE</label>
                  <input
                    className="input-f"
                    type="number"
                    step="0.0001"
                    value={(Number(adminState.settings?.telegramTradeBuyFeeLamports || 0) / 1e9).toFixed(4)}
                    onChange={(e) =>
                      setAdminState((prev) => ({
                        ...prev,
                        settings: {
                          ...(prev.settings || {}),
                          telegramTradeBuyFeeLamports: Math.max(0, Math.floor((Number(e.target.value) || 0) * 1e9)),
                        },
                      }))
                    }
                  />
                </div>
                <div>
                  <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", marginBottom: 6, fontWeight: 700 }}>SELL FEE</label>
                  <input
                    className="input-f"
                    type="number"
                    step="0.0001"
                    value={(Number(adminState.settings?.telegramTradeSellFeeLamports || 0) / 1e9).toFixed(4)}
                    onChange={(e) =>
                      setAdminState((prev) => ({
                        ...prev,
                        settings: {
                          ...(prev.settings || {}),
                          telegramTradeSellFeeLamports: Math.max(0, Math.floor((Number(e.target.value) || 0) * 1e9)),
                        },
                      }))
                    }
                  />
                </div>
              </div>
              <div style={{ fontSize: 12, color: "rgba(255,255,255,.52)", lineHeight: 1.6, marginTop: 10 }}>
                Telegram trading uses these fixed SOL fees per executed buy and sell. Keep them low if you want the in-chat trading surface to stay competitive.
              </div>
              <div style={{ display: "flex", gap: 8, marginTop: 12 }}>
                <button
                  className="btn-fire"
                  disabled={adminLoading || !adminState.settings}
                  onClick={async () => {
                    if (!onSaveAdminSettings || !adminState.settings) return;
                    setAdminLoading(true);
                    setAdminError("");
                    setAdminMessage("");
                    try {
                      const saved = await onSaveAdminSettings(adminState.settings);
                      setAdminState((prev) => ({ ...prev, settings: saved }));
                      setAdminMessage("Admin settings saved.");
                    } catch (error) {
                      setAdminError(error?.message || "Unable to save admin settings.");
                    } finally {
                      setAdminLoading(false);
                    }
                  }}
                  style={{ padding: "9px 16px", fontSize: 12 }}>
                  {adminLoading ? "Saving..." : "Save Settings"}
                </button>
                <button className="btn-ghost" disabled={adminLoading} onClick={refreshAdmin} style={{ padding: "9px 16px", fontSize: 12 }}>
                  Refresh
                </button>
              </div>
            </div>
            <div className="glass" style={{ padding: "14px 16px" }}>
              <div style={{ fontSize: 13, fontWeight: 800, color: "#fff", marginBottom: 10 }}>Platform Control</div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
                <div style={{ display: "flex", alignItems: "flex-end" }}>
                  <label style={{ display: "inline-flex", alignItems: "center", gap: 8, fontSize: 12, color: "#fff", fontWeight: 700 }}>
                    <input
                      type="checkbox"
                      checked={Boolean(adminState.settings?.maintenanceEnabled)}
                      onChange={(e) =>
                        setAdminState((prev) => ({
                          ...prev,
                          settings: { ...(prev.settings || {}), maintenanceEnabled: e.target.checked },
                        }))
                      }
                    />
                    Maintenance mode enabled
                  </label>
                </div>
                <div>
                  <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", marginBottom: 6, fontWeight: 700 }}>MODE</label>
                  <select
                    className="input-f"
                    value={adminState.settings?.maintenanceMode || "soft"}
                    onChange={(e) =>
                      setAdminState((prev) => ({
                        ...prev,
                        settings: { ...(prev.settings || {}), maintenanceMode: e.target.value },
                      }))
                    }
                  >
                    <option value="soft">Soft</option>
                    <option value="hard">Hard</option>
                  </select>
                </div>
              </div>
              <div style={{ marginTop: 10 }}>
                <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", marginBottom: 6, fontWeight: 700 }}>BANNER MESSAGE</label>
                <input
                  className="input-f"
                  value={adminState.settings?.maintenanceMessage || ""}
                  placeholder="Optional public maintenance message"
                  onChange={(e) =>
                    setAdminState((prev) => ({
                      ...prev,
                      settings: { ...(prev.settings || {}), maintenanceMessage: e.target.value },
                    }))
                  }
                />
              </div>
              <div style={{ fontSize: 12, color: "rgba(255,255,255,.52)", lineHeight: 1.6, marginTop: 10 }}>
                Soft mode blocks new user write actions but keeps reads live. Hard mode also stops worker execution until maintenance is turned off.
              </div>
            </div>
            <div className="glass" style={{ padding: "14px 16px" }}>
              <div style={{ display: "flex", justifyContent: "space-between", gap: 10, alignItems: "center", marginBottom: 10 }}>
                <div style={{ fontSize: 13, fontWeight: 800, color: "#fff" }}>Users</div>
                <input className="input-f" value={adminUserSearch} onChange={(e) => setAdminUserSearch(e.target.value)} placeholder="Search users" style={{ width: 220 }} />
              </div>
              <div style={{ display: "flex", flexDirection: "column", gap: 10, maxHeight: 260, overflowY: "auto" }}>
                {filteredAdminUsers.map((row) => (
                  <div key={row.id} style={{ borderBottom: "1px solid rgba(255,255,255,.05)", paddingBottom: 10 }}>
                    <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "center" }}>
                      <div>
                        <div style={{ color: "#fff", fontWeight: 700, fontSize: 13, display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
                          <span>{row.username}</span>
                          {row.isAdmin && <span style={{ fontSize: 10, fontWeight: 800, color: "#d8ecff", background: "rgba(55,132,255,.25)", borderRadius: 999, padding: "3px 8px" }}>ADMIN</span>}
                          {row.isOg && <span style={{ fontSize: 10, fontWeight: 800, color: "#fff2bd", background: "rgba(255,177,0,.22)", borderRadius: 999, padding: "3px 8px" }}>OG</span>}
                          {row.isBanned && <span style={{ fontSize: 10, fontWeight: 800, color: "#ffb3b3", background: "rgba(255,64,64,.2)", borderRadius: 999, padding: "3px 8px" }}>SUSPENDED</span>}
                        </div>
                        <div style={{ color: "rgba(255,255,255,.4)", fontSize: 11 }}>
                          {row.referralCode} / {row.tokenCount} token(s) / {Number(row.pendingReferralSol || 0).toFixed(6)} SOL pending
                        </div>
                        {!!row.referralStatus && (
                          <div style={{ color: "rgba(255,255,255,.45)", fontSize: 11, marginTop: 4 }}>
                            Referral status: {row.referralStatus}
                          </div>
                        )}
                        {(row.hasWalletOverlap || row.hasIpOverlap) && (
                          <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginTop: 6 }}>
                            {row.hasWalletOverlap && <span style={{ fontSize: 10, fontWeight: 800, color: "#ffd3a7", background: "rgba(255,127,62,.18)", borderRadius: 999, padding: "3px 8px" }}>WALLET OVERLAP</span>}
                            {row.hasIpOverlap && <span style={{ fontSize: 10, fontWeight: 800, color: "#ffd3a7", background: "rgba(255,127,62,.18)", borderRadius: 999, padding: "3px 8px" }}>IP OVERLAP</span>}
                          </div>
                        )}
                        {!!row.bannedReason && <div style={{ color: "rgba(255,160,160,.82)", fontSize: 11, marginTop: 4 }}>Reason: {row.bannedReason}</div>}
                      </div>
                      <div style={{ display: "flex", gap: 8, alignItems: "flex-end", flexWrap: "wrap", justifyContent: "flex-end" }}>
                        <label style={{ display: "inline-flex", alignItems: "center", gap: 6, fontSize: 12, color: "rgba(255,255,255,.72)" }}>
                          <input
                            type="checkbox"
                            checked={Boolean(row.isOg)}
                            onChange={async (e) => {
                              if (!onAdminSetUserOg) return;
                              setAdminLoading(true);
                              setAdminError("");
                              try {
                                await onAdminSetUserOg(row.id, e.target.checked);
                                await refreshAdmin();
                              } catch (error) {
                                setAdminError(error?.message || "Unable to update OG account.");
                              } finally {
                                setAdminLoading(false);
                              }
                            }}
                          />
                          OG
                        </label>
                        <div>
                          <div style={{ fontSize: 10, color: "rgba(255,255,255,.35)", marginBottom: 4, fontWeight: 700, letterSpacing: 0.5 }}>SUSPEND REASON</div>
                          <input
                            className="input-f"
                            style={{ width: 180 }}
                            value={row.bannedReason || ""}
                            placeholder="Reason"
                            onChange={(e) =>
                              setAdminState((prev) => ({
                                ...prev,
                                users: prev.users.map((item) => (item.id === row.id ? { ...item, bannedReason: e.target.value } : item)),
                              }))
                            }
                          />
                        </div>
                        <div>
                          <div style={{ fontSize: 10, color: "rgba(255,255,255,.35)", marginBottom: 4, fontWeight: 700, letterSpacing: 0.5 }}>REFERRED BY CODE</div>
                          <input
                            className="input-f"
                            style={{ width: 140 }}
                            value={row.referrerCode || ""}
                            placeholder="Referral code"
                            onChange={(e) =>
                              setAdminState((prev) => ({
                                ...prev,
                                users: prev.users.map((item) => (item.id === row.id ? { ...item, referrerCode: e.target.value } : item)),
                              }))
                            }
                            onBlur={async (e) => {
                              if (!onAdminSetUserReferrer) return;
                              const value = String(e.target.value || "").trim();
                              try {
                                setAdminLoading(true);
                                await onAdminSetUserReferrer(row.id, value);
                                await refreshAdmin();
                              } catch (error) {
                                setAdminError(error?.message || "Unable to update referrer.");
                              } finally {
                                setAdminLoading(false);
                              }
                            }}
                          />
                        </div>
                        {!row.isAdmin && (
                          <button
                            className="btn-ghost"
                            style={{ padding: "8px 12px", fontSize: 12, borderColor: row.isBanned ? "rgba(122,231,171,.24)" : "rgba(255,90,90,.24)", color: row.isBanned ? "#a7f3c4" : "#ffb0b0" }}
                            onClick={async () => {
                              if (!onAdminSetUserBan) return;
                              const reason = String(adminState.users.find((item) => item.id === row.id)?.bannedReason || row.bannedReason || "").trim();
                              if (!row.isBanned && !reason) {
                                setAdminError("A suspension reason is required.");
                                return;
                              }
                              setAdminLoading(true);
                              setAdminError("");
                              try {
                                await onAdminSetUserBan(row.id, !row.isBanned, reason);
                                await refreshAdmin();
                              } catch (error) {
                                setAdminError(error?.message || "Unable to update suspension.");
                              } finally {
                                setAdminLoading(false);
                              }
                            }}>
                            {row.isBanned ? "Unsuspend" : "Suspend"}
                          </button>
                        )}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
            <div className="glass" style={{ padding: "14px 16px" }}>
              <div style={{ display: "flex", justifyContent: "space-between", gap: 10, alignItems: "center", marginBottom: 10 }}>
                <div style={{ fontSize: 13, fontWeight: 800, color: "#fff" }}>Tokens</div>
                <input className="input-f" value={adminTokenSearch} onChange={(e) => setAdminTokenSearch(e.target.value)} placeholder="Search tokens" style={{ width: 220 }} />
              </div>
              <div style={{ display: "flex", flexDirection: "column", gap: 10, maxHeight: 240, overflowY: "auto" }}>
                {filteredAdminTokens.map((row) => (
                  <div key={row.id} style={{ display: "flex", justifyContent: "space-between", gap: 10, alignItems: "center", borderBottom: "1px solid rgba(255,255,255,.05)", paddingBottom: 10 }}>
                    <div>
                      <div style={{ color: "#fff", fontWeight: 700, fontSize: 13, display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
                        <span>{row.symbol}</span>
                        <span style={{ color: "rgba(255,255,255,.45)", fontWeight: 500 }}>/ {row.username}</span>
                        {row.hiddenFromPublic && <span style={{ fontSize: 10, fontWeight: 800, color: "#ffd4a7", background: "rgba(255,127,62,.18)", borderRadius: 999, padding: "3px 8px" }}>HIDDEN</span>}
                        {Number(row.pinnedRank || 0) > 0 && <span style={{ fontSize: 10, fontWeight: 800, color: "#d8ecff", background: "rgba(55,132,255,.2)", borderRadius: 999, padding: "3px 8px" }}>PIN {row.pinnedRank}</span>}
                      </div>
                      <div style={{ color: "rgba(255,255,255,.4)", fontSize: 11 }}>{row.selectedBot} / {row.disconnected ? "archived" : row.active ? "active" : "paused"}</div>
                      <div style={{ color: "rgba(255,255,255,.28)", fontSize: 10, marginTop: 4 }}>{row.mint}</div>
                    </div>
                    <div style={{ display: "flex", gap: 8, alignItems: "flex-end", flexWrap: "wrap", justifyContent: "flex-end" }}>
                      <label style={{ display: "inline-flex", alignItems: "center", gap: 6, fontSize: 12, color: "rgba(255,255,255,.72)" }}>
                        <input
                          type="checkbox"
                          checked={Boolean(row.hiddenFromPublic)}
                          onChange={async (e) => {
                            if (!onAdminUpdateTokenPublicState) return;
                            setAdminLoading(true);
                            setAdminError("");
                            try {
                              await onAdminUpdateTokenPublicState(row.id, {
                                hiddenFromPublic: e.target.checked,
                                pinnedRank: Number(row.pinnedRank || 0),
                              });
                              await refreshAdmin();
                            } catch (error) {
                              setAdminError(error?.message || "Unable to update token visibility.");
                            } finally {
                              setAdminLoading(false);
                            }
                          }}
                        />
                        Hidden
                      </label>
                      <div>
                        <div style={{ fontSize: 10, color: "rgba(255,255,255,.35)", marginBottom: 4, fontWeight: 700, letterSpacing: 0.5 }}>PIN RANK</div>
                        <input
                          className="input-f"
                          type="number"
                          min="0"
                          max="999"
                          style={{ width: 90 }}
                          value={row.pinnedRank || 0}
                          onChange={(e) =>
                            setAdminState((prev) => ({
                              ...prev,
                              tokens: prev.tokens.map((item) => (item.id === row.id ? { ...item, pinnedRank: Math.max(0, Number(e.target.value) || 0) } : item)),
                            }))
                          }
                          onBlur={async (e) => {
                            if (!onAdminUpdateTokenPublicState) return;
                            setAdminLoading(true);
                            setAdminError("");
                            try {
                              await onAdminUpdateTokenPublicState(row.id, {
                                hiddenFromPublic: Boolean(adminState.tokens.find((item) => item.id === row.id)?.hiddenFromPublic ?? row.hiddenFromPublic),
                                pinnedRank: Math.max(0, Number(e.target.value) || 0),
                              });
                              await refreshAdmin();
                            } catch (error) {
                              setAdminError(error?.message || "Unable to update pin rank.");
                            } finally {
                              setAdminLoading(false);
                            }
                          }}
                        />
                      </div>
                      {row.disconnected ? (
                        <>
                          <button
                            className="btn-ghost"
                            style={{ padding: "8px 12px", fontSize: 12 }}
                            onClick={async () => {
                              if (!onAdminRestoreToken) return;
                              setAdminLoading(true);
                              try {
                                await onAdminRestoreToken(row.id);
                                await refreshAdmin();
                              } catch (error) {
                                setAdminError(error?.message || "Unable to restore token.");
                              } finally {
                                setAdminLoading(false);
                              }
                            }}>
                            Restore
                          </button>
                          <button
                            className="btn-ghost"
                            style={{ padding: "8px 12px", fontSize: 12, borderColor: "rgba(255,90,90,.3)", color: "#ff9f9f" }}
                            onClick={async () => {
                              if (!onAdminPermanentDeleteToken) return;
                              const warnings = [
                                "This will permanently remove the archived token from the site and database.",
                                "This cannot be undone and the token will disappear from ticker/dashboard history.",
                                "This should only be used after confirming all token wallets are empty.",
                              ];
                              for (const warning of warnings) {
                                if (!window.confirm(warning)) return;
                              }
                              setAdminLoading(true);
                              setAdminError("");
                              try {
                                await onAdminPermanentDeleteToken(row.id);
                                await refreshAdmin();
                              } catch (error) {
                                setAdminError(error?.message || "Unable to permanently delete token.");
                              } finally {
                                setAdminLoading(false);
                              }
                            }}>
                            Permanent Delete
                          </button>
                        </>
                      ) : (
                        <>
                          {row.active && (
                            <button
                              className="btn-ghost"
                              style={{ padding: "8px 12px", fontSize: 12, borderColor: "rgba(255,177,107,.24)", color: "#ffd3a7" }}
                              onClick={async () => {
                                if (!onAdminForcePauseToken) return;
                                setAdminLoading(true);
                                setAdminError("");
                                try {
                                  await onAdminForcePauseToken(row.id);
                                  await refreshAdmin();
                                } catch (error) {
                                  setAdminError(error?.message || "Unable to force pause token.");
                                } finally {
                                  setAdminLoading(false);
                                }
                              }}>
                              Force Pause
                            </button>
                          )}
                          <button
                            className="btn-ghost"
                            style={{ padding: "8px 12px", fontSize: 12 }}
                            onClick={async () => {
                              if (!onAdminArchiveToken) return;
                              setAdminLoading(true);
                              try {
                                await onAdminArchiveToken(row.id);
                                await refreshAdmin();
                              } catch (error) {
                                setAdminError(error?.message || "Unable to archive token.");
                              } finally {
                                setAdminLoading(false);
                              }
                            }}>
                            Archive
                          </button>
                        </>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>
            {!!adminState.referralLeaders.length && (
              <div className="glass" style={{ padding: "14px 16px" }}>
                <div style={{ fontSize: 13, fontWeight: 800, color: "#fff", marginBottom: 10 }}>Referral Leaders</div>
                <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                  {adminState.referralLeaders.slice(0, 10).map((row) => (
                    <div key={row.userId} style={{ display: "flex", justifyContent: "space-between", gap: 8, fontSize: 12 }}>
                      <span style={{ color: "#fff", fontWeight: 700 }}>{row.username}</span>
                      <span style={{ color: "rgba(255,255,255,.52)" }}>{Number(row.earnedSol || 0).toFixed(6)} SOL</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
            {!!adminState.auditTrail?.length && (
              <div className="glass" style={{ padding: "14px 16px" }}>
                <div style={{ fontSize: 13, fontWeight: 800, color: "#fff", marginBottom: 10 }}>Audit Trail</div>
                <div style={{ display: "flex", flexDirection: "column", gap: 8, maxHeight: 220, overflowY: "auto" }}>
                  {adminState.auditTrail.slice(0, 25).map((row) => (
                    <div key={row.id} style={{ borderBottom: "1px solid rgba(255,255,255,.05)", paddingBottom: 8 }}>
                      <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                        <span style={{ color: "#fff", fontSize: 12, fontWeight: 700 }}>{String(row.action || "").replaceAll("_", " ")}</span>
                        <span style={{ color: "rgba(255,255,255,.38)", fontSize: 11 }}>{new Date(row.createdAt).toLocaleString()}</span>
                      </div>
                      <div style={{ fontSize: 11, color: "rgba(255,255,255,.52)", marginTop: 4 }}>
                        {row.actorUsername || "admin"}
                        {row.targetUsername ? ` -> ${row.targetUsername}` : ""}
                        {row.targetTokenId ? ` / token ${row.targetTokenId}` : ""}
                      </div>
                      {!!row.details?.reason && (
                        <div style={{ fontSize: 11, color: "rgba(255,176,176,.82)", marginTop: 4 }}>
                          Reason: {String(row.details.reason)}
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            )}
            {adminError && <div style={{ fontSize: 12, color: "#ff8f8f" }}>{adminError}</div>}
            {adminMessage && <div style={{ fontSize: 12, color: "#7ae7ab" }}>{adminMessage}</div>}
          </div>
        </ActionModal>
      )}

      {showManagerModal && (
        <ActionModal title={canManageAccess ? "Team Access" : "Access Level"} onClose={() => setShowManagerModal(false)}>
          {canManageAccess ? (
            <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
              <div style={{ fontSize: 12, color: "rgba(255,255,255,.45)", lineHeight: 1.6 }}>
                Secondary manager login can run bots and edit configs, but cannot withdraw, sweep, restore wallet keys, or delete.
              </div>
              <div>
                <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", letterSpacing: 1, marginBottom: 6, fontWeight: 700 }}>MANAGER USERNAME</label>
                <input className="input-f" value={managerForm.username} onChange={(e) => setManagerForm((prev) => ({ ...prev, username: e.target.value }))} />
              </div>
              <div>
                <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", letterSpacing: 1, marginBottom: 6, fontWeight: 700 }}>MANAGER PASSWORD</label>
                <input className="input-f" type="password" value={managerForm.password} onChange={(e) => setManagerForm((prev) => ({ ...prev, password: e.target.value }))} placeholder={managerAccess.enabled ? "Set a new password" : "Create a password"} />
              </div>
              {managerAccess.enabled && (
                <div style={{ fontSize: 11, color: "rgba(255,255,255,.42)" }}>
                  Active manager: <span style={{ color: "#fff", fontWeight: 700 }}>{managerAccess.username}</span>
                </div>
              )}
              {managerError && <div style={{ fontSize: 12, color: "#ff8f8f" }}>{managerError}</div>}
              {managerMessage && <div style={{ fontSize: 12, color: "#7ae7ab" }}>{managerMessage}</div>}
              <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                <button className="btn-fire" onClick={saveManager} disabled={managerLoading} style={{ padding: "9px 16px", fontSize: 12 }}>
                  {managerLoading ? "Saving..." : managerAccess.enabled ? "Update Manager" : "Create Manager"}
                </button>
                {managerAccess.enabled && (
                  <button className="btn-ghost" onClick={removeManager} disabled={managerLoading} style={{ padding: "9px 16px", fontSize: 12 }}>
                    Remove Manager
                  </button>
                )}
              </div>
            </div>
          ) : (
            <div style={{ fontSize: 12, color: "rgba(255,178,107,.86)", lineHeight: 1.6 }}>
              Signed in as manager. Withdraw, sweep, delete, restore, and access management are disabled on this login.
            </div>
          )}
        </ActionModal>
      )}

      {showTelegramModal && (
        <ActionModal title="Telegram Alerts" onClose={() => setShowTelegramModal(false)}>
          <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
            <div style={{ fontSize: 12, color: "rgba(255,255,255,.45)", lineHeight: 1.55 }}>
              Connect the existing EMBER Telegram bot to receive direct alerts without channel spam. Errors can stay instant while routine trade activity can roll into digests.
            </div>
            <div style={{ fontSize: 11, color: telegramState.connected ? "#7ae7ab" : "rgba(255,178,107,.86)" }}>
              {telegramState.connected
                ? `Connected${telegramState.telegramUsername ? ` as @${telegramState.telegramUsername}` : ""}${telegramState.chatIdMasked ? ` (${telegramState.chatIdMasked})` : ""}`
                : "Not connected yet. Open the bot link below, press Start, then refresh this panel."}
            </div>
            {telegramState.connectUrl && (
              <a
                href={telegramState.connectUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="btn-ghost"
                style={{ padding: "9px 16px", fontSize: 12, textAlign: "center", textDecoration: "none" }}
              >
                Open Telegram Bot
              </a>
            )}

            <label style={{ display: "inline-flex", alignItems: "center", gap: 10, fontSize: 12, color: "rgba(255,255,255,.72)" }}>
              <span className="ember-toggle">
                <input
                  type="checkbox"
                  checked={Boolean(telegramState.prefs.enabled)}
                  onChange={(e) =>
                    setTelegramState((prev) => ({
                      ...prev,
                      prefs: { ...prev.prefs, enabled: e.target.checked },
                    }))
                  }
                />
                <span className="ember-toggle-track" />
              </span>
              Enable Telegram alerts
            </label>

            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
              <div>
                <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", letterSpacing: 1, marginBottom: 6, fontWeight: 700 }}>DELIVERY MODE</label>
                <select
                  className="input-f"
                  value={telegramState.prefs.deliveryMode}
                  onChange={(e) =>
                    setTelegramState((prev) => ({
                      ...prev,
                      prefs: { ...prev.prefs, deliveryMode: e.target.value },
                    }))
                  }
                >
                  <option value="smart">Smart</option>
                  <option value="instant">Instant</option>
                  <option value="digest">Digest</option>
                </select>
              </div>
              <div>
                <label style={{ display: "block", fontSize: 10, color: "rgba(255,255,255,.35)", letterSpacing: 1, marginBottom: 6, fontWeight: 700 }}>DIGEST MINUTES</label>
                <input
                  type="number"
                  min={5}
                  max={120}
                  className="input-f"
                  value={telegramState.prefs.digestIntervalMin}
                  onChange={(e) =>
                    setTelegramState((prev) => ({
                      ...prev,
                      prefs: {
                        ...prev.prefs,
                        digestIntervalMin: Math.max(5, Math.min(120, Math.floor(Number(e.target.value) || 15))),
                      },
                    }))
                  }
                />
              </div>
            </div>

            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
              {[
                ["alertDeposit", "Deposits"],
                ["alertClaim", "Claims"],
                ["alertBurn", "Burns"],
                ["alertTrade", "Trades"],
                ["alertError", "Errors"],
                ["alertStatus", "Status"],
              ].map(([key, label]) => (
                <label key={key} style={{ display: "inline-flex", alignItems: "center", gap: 8, fontSize: 12, color: "rgba(255,255,255,.72)" }}>
                  <input
                    type="checkbox"
                    checked={Boolean(telegramState.prefs[key])}
                    onChange={(e) =>
                      setTelegramState((prev) => ({
                        ...prev,
                        prefs: { ...prev.prefs, [key]: e.target.checked },
                      }))
                    }
                  />
                  {label}
                </label>
              ))}
            </div>

            {telegramError && <div style={{ fontSize: 12, color: "#ff8f8f" }}>{telegramError}</div>}
            {telegramMessage && <div style={{ fontSize: 12, color: "#7ae7ab" }}>{telegramMessage}</div>}

            <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
              <button className="btn-fire" onClick={saveTelegram} disabled={telegramLoading} style={{ padding: "9px 16px", fontSize: 12 }}>
                {telegramLoading ? "Saving..." : "Save Alerts"}
              </button>
              <button className="btn-ghost" onClick={refreshTelegram} disabled={telegramLoading} style={{ padding: "9px 16px", fontSize: 12 }}>
                Refresh
              </button>
              <button className="btn-ghost" onClick={sendTelegramTest} disabled={telegramLoading || !telegramState.connected} style={{ padding: "9px 16px", fontSize: 12 }}>
                Send Test
              </button>
              {telegramState.connected && (
                <button className="btn-ghost" onClick={disconnectTelegram} disabled={telegramLoading} style={{ padding: "9px 16px", fontSize: 12 }}>
                  Disconnect
                </button>
              )}
            </div>
          </div>
        </ActionModal>
      )}
    </div>
  );
}
