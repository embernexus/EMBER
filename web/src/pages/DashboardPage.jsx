import { useEffect, useState } from "react";
import BurnChart from "../components/BurnChart";
import LiveFeed from "../components/dashboard/LiveFeed";
import TokenCard from "../components/dashboard/TokenCard";
import { useI18n } from "../i18n/I18nProvider";
import { fmt, fmtFull } from "../lib/format";

function ActionModal({ title, onClose, children }) {
  return (
    <div
      style={{
        position: "fixed",
        inset: 0,
        zIndex: 1400,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
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
}) {
  const { t } = useI18n();
  const username = typeof user === "string" ? user : String(user?.username || "");
  const isManager = Boolean(user?.role === "manager" || user?.isOperator);
  const canManageAccess = Boolean(user?.canManageAccess);
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

  return (
    <div style={{ position: "relative", zIndex: 2, maxWidth: 1300, margin: "0 auto", padding: "32px 24px" }}>
      <div style={{ marginBottom: 24, animation: "slideUp .4s ease" }}>
        <h1 style={{ fontSize: 26, fontWeight: 800, color: "#fff", marginBottom: 4 }}>{t("dashboard.hey", { user: username })} {"\u{1F525}"}</h1>
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
              <button className="btn-ghost" onClick={() => setShowTelegramModal(true)} style={{ padding: "9px 14px", fontSize: 12 }}>
                Telegram Alerts
              </button>
              <button className="btn-ghost" onClick={() => setShowManagerModal(true)} style={{ padding: "9px 14px", fontSize: 12 }}>
                {canManageAccess ? "Manager Access" : "Access Level"}
              </button>
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

      {showManagerModal && (
        <ActionModal title={canManageAccess ? "Manager Access" : "Access Level"} onClose={() => setShowManagerModal(false)}>
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
