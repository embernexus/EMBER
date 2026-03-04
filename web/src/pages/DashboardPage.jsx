import BurnChart from "../components/BurnChart";
import LiveFeed from "../components/dashboard/LiveFeed";
import TokenCard from "../components/dashboard/TokenCard";
import { useI18n } from "../i18n/I18nProvider";
import { fmt, fmtFull } from "../lib/format";

export default function DashboardPage({ user, tokens, allLogs, chartData, feed, onShowAttach, onUpdateToken, onDeleteToken, onFetchTokenDetails }) {
  const { t } = useI18n();
  const totalBurned = tokens.reduce((sum, t) => sum + (Number(t.burned) || 0), 0);
  const activeCount = tokens.filter((t) => t.active).length;

  const pieColors = ["#22d3ee", "#a78bfa", "#34d399", "#facc15", "#f472b6", "#60a5fa"];
  const tokenBreakdown = tokens
    .map((t) => ({ symbol: t.symbol, amount: Number(t.burned) || 0 }))
    .filter((t) => t.amount > 0)
    .sort((a, b) => b.amount - a.amount);
  const pieHead = tokenBreakdown.slice(0, 5);
  const pieOther = tokenBreakdown.slice(5).reduce((sum, t) => sum + t.amount, 0);
  const pieSlices = pieOther > 0 ? [...pieHead, { symbol: "OTHER", amount: pieOther }] : pieHead;
  const pieTotal = pieSlices.reduce((sum, s) => sum + s.amount, 0);

  return (
    <div style={{ position: "relative", zIndex: 2, maxWidth: 1300, margin: "0 auto", padding: "32px 24px" }}>
      <div style={{ marginBottom: 24, animation: "slideUp .4s ease" }}>
        <h1 style={{ fontSize: 26, fontWeight: 800, color: "#fff", marginBottom: 4 }}>{t("dashboard.hey", { user })} {"\u{1F525}"}</h1>
        <p style={{ fontSize: 13, color: "rgba(255,255,255,.3)" }}>{t("dashboard.synced")}</p>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(3,1fr)", gap: 14, marginBottom: 28 }}>
        {[
          { l: t("dashboard.tokensConnected"), v: tokens.length, s: t("dashboard.integrations"), i: "\u{1FA99}", c: "#ff8c42" },
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
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <div>
              <div style={{ fontSize: 17, fontWeight: 800, color: "#fff" }}>{t("dashboard.yourTokens")}</div>
              <div style={{ fontSize: 12, color: "rgba(255,255,255,.28)" }}>{t("dashboard.tokensHint")}</div>
            </div>
            <button className="btn-fire" onClick={onShowAttach} style={{ padding: "9px 18px", fontSize: 13 }}>
              + {t("dashboard.attachToken")}
            </button>
          </div>

          {tokens.map((t) => (
            <TokenCard
              key={t.id}
              token={t}
              allLogs={allLogs}
              onUpdate={onUpdateToken}
              onDelete={onDeleteToken}
              onFetchDetails={onFetchTokenDetails}
            />
          ))}

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
              [t("dashboard.totalTransactions"), fmtFull(tokens.reduce((sum, t) => sum + (Number(t.txCount) || 0), 0))],
              [t("dashboard.activeSchedulers"), `${activeCount} / ${tokens.length}`],
            ].map(([k, v]) => (
              <div key={k} style={{ display: "flex", justifyContent: "space-between", padding: "9px 0", borderBottom: "1px solid rgba(255,255,255,.04)", fontSize: 13 }}>
                <span style={{ color: "rgba(255,255,255,.32)", fontWeight: 600 }}>{k}</span>
                <span style={{ color: "#fff", fontWeight: 700 }}>{v}</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
