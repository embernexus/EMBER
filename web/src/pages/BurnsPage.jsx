import { useEffect, useState } from "react";
import { useI18n } from "../i18n/I18nProvider";
import { fmtAge, fmtFull, solscanTx } from "../lib/format";

export default function BurnsPage({ tokens, allLogs, publicMetrics, chartData }) {
  const { t } = useI18n();
  const [query, setQuery] = useState("");
  const [historyPage, setHistoryPage] = useState(1);
  const [hoverPoint, setHoverPoint] = useState(null);
  const rowsPerPage = 25;
  const burnRows = allLogs
    .filter(row => row.type === "burn" || row.type === "buyback")
    .slice(0, 250);
  const q = query.trim().toLowerCase();
  const filteredRows = q
    ? burnRows.filter(row =>
        String(row.tokenId ?? "").toLowerCase().includes(q) ||
        String(row.msg ?? "").toLowerCase().includes(q) ||
        String(row.tx ?? "").toLowerCase().includes(q)
      )
    : burnRows;

  const tokenBreakdown = tokens
    .map(t => ({ symbol: t.symbol, amount: Number(t.burned) || 0 }))
    .sort((a, b) => b.amount - a.amount);
  const totalTokenBurned = tokenBreakdown.reduce((sum, t) => sum + t.amount, 0);
  const hasHistory = filteredRows.length > 0;
  const hasBreakdown = tokenBreakdown.some(t => t.amount > 0);
  const burnSeries = Array.isArray(chartData) && chartData.length ? chartData : [];
  const hasBurnSeries = burnSeries.some(d => Number(d.v) > 0);
  const pieColors = ["#22d3ee", "#a78bfa", "#34d399", "#facc15", "#f472b6", "#60a5fa"];
  const rawPie = tokenBreakdown.filter(t => t.amount > 0);
  const pieHead = rawPie.slice(0, 5);
  const pieOther = rawPie.slice(5).reduce((sum, t) => sum + t.amount, 0);
  const pieSlices = pieOther > 0 ? [...pieHead, { symbol: "OTHER", amount: pieOther }] : pieHead;
  const pieTotal = pieSlices.reduce((sum, s) => sum + s.amount, 0);
  const totalPages = Math.max(1, Math.ceil(filteredRows.length / rowsPerPage));
  const safePage = Math.min(historyPage, totalPages);
  const pageStart = (safePage - 1) * rowsPerPage;
  const pagedRows = filteredRows.slice(pageStart, pageStart + rowsPerPage);

  useEffect(() => { setHistoryPage(1); }, [query]);
  useEffect(() => {
    if (historyPage > totalPages) setHistoryPage(totalPages);
  }, [historyPage, totalPages]);

  return (
    <section style={{ position: "relative", zIndex: 2, maxWidth: 1240, margin: "0 auto", padding: "84px 24px 80px" }}>
      <div style={{ textAlign: "center", marginBottom: 34 }}>
        <div style={{ display: "inline-flex", alignItems: "center", gap: 8, borderRadius: 999, border: "1px solid rgba(255,106,0,.25)", background: "rgba(255,106,0,.08)", color: "#ff9f5a", padding: "6px 12px", fontSize: 11, fontWeight: 700, letterSpacing: .7, textTransform: "uppercase", marginBottom: 14 }}>
          <span>{"\u{1F525}"}</span>
          <span>{t("burns.liveBurnTracker")}</span>
        </div>
        <p style={{ fontSize: 15, color: "rgba(255,255,255,.45)", maxWidth: 720, margin: "0 auto", lineHeight: 1.7 }}>
          {t("burns.subtitle")}
        </p>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(220px,1fr))", gap: 14, marginBottom: 18 }}>
        {[
          { icon: "\u{1F525}", value: fmtFull(publicMetrics.lifetimeIncinerated), label: t("burns.totalIncinerated") },
          { icon: "\u{1F9FE}", value: fmtFull(publicMetrics.emberIncinerated), label: t("burns.emberBurned") },
          { icon: "\u{1F4CA}", value: fmtFull(publicMetrics.totalBotTransactions), label: t("burns.totalBotTransactions") },
          { icon: "\u{1F300}", value: fmtFull(publicMetrics.activeTokens), label: t("burns.tokensBurning") },
        ].map(card => (
          <div key={card.label} className="glass" style={{ padding: "16px 18px", border: "1px solid rgba(255,106,0,.18)", textAlign: "center", display: "flex", flexDirection: "column", alignItems: "center" }}>
            <div style={{ fontSize: 16, marginBottom: 12, color: "#ff8c42" }}>{card.icon}</div>
            <div style={{ fontSize: 28, color: "#fff", fontWeight: 800, lineHeight: 1.1, marginBottom: 8 }}>{card.value}</div>
            <div style={{ fontSize: 11, fontWeight: 700, letterSpacing: 1, color: "rgba(255,255,255,.42)", textTransform: "uppercase" }}>{card.label}</div>
          </div>
        ))}
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(320px,1fr))", gap: 14, marginBottom: 16 }}>
        <div className="glass" style={{ padding: "16px 16px 8px", minHeight: 256 }}>
          <div style={{ fontSize: 13, fontWeight: 700, color: "#fff", marginBottom: 18, textAlign: "center" }}>{t("burns.dailyBurnActivity")}</div>
          {hasBurnSeries || pieTotal > 0 ? (
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(220px,1fr))", gap: 14, alignItems: "start", marginTop: 4 }}>
              <div>
                {hasBurnSeries ? (
                  <>
                    <div style={{ display: "flex", alignItems: "flex-end", gap: 7, height: 112, paddingTop: 12, paddingBottom: 22, position: "relative" }}>
                      {[.25, .5, .75, 1].map(f => (
                        <div key={f} style={{ position: "absolute", left: 0, right: 0, bottom: `${f * 76 + 22}px`, height: 1, background: "rgba(255,255,255,.04)" }} />
                      ))}
                      {burnSeries.map((d, i) => {
                        const max = Math.max(...burnSeries.map(x => Number(x.v) || 0), 1);
                        const h = Math.max(4, (d.v / max) * 76);
                        const color = pieColors[i % pieColors.length];
                        return (
                          <div
                            key={i}
                            style={{ flex: 1, display: "flex", flexDirection: "column", alignItems: "center", gap: 5 }}
                            onMouseEnter={() => setHoverPoint({ d: d.d, v: d.v })}
                            onMouseLeave={() => setHoverPoint(null)}
                          >
                            <div title={`${d.d}: ${fmtFull(d.v)} burned`} style={{ width: "100%", height: h, borderRadius: "4px 4px 0 0", background: `linear-gradient(180deg, ${color} 0%, rgba(255,255,255,.16) 100%)`, boxShadow: `0 0 14px ${color}55`, cursor: "pointer" }} />
                            <span style={{ fontSize: 10, color: "rgba(255,255,255,.34)", fontWeight: 600 }}>{d.d}</span>
                          </div>
                        );
                      })}
                    </div>
                    <div style={{ marginTop: 8, minHeight: 16, fontSize: 11, color: "rgba(255,255,255,.58)", fontFamily: "'JetBrains Mono', monospace" }}>
                      {hoverPoint ? `${hoverPoint.d}: ${fmtFull(hoverPoint.v)} ${t("burns.burned")}` : t("burns.hoverDay")}
                    </div>
                  </>
                ) : (
                  <div style={{ minHeight: 190, display: "flex", alignItems: "center", justifyContent: "center", color: "rgba(255,255,255,.3)", fontSize: 13 }}>
                    {t("burns.noBurnData")}
                  </div>
                )}
              </div>
              <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 10, paddingTop: 8 }}>
                {pieTotal > 0 ? (
                  <>
                    <svg width="162" height="162" viewBox="0 0 162 162" aria-label="Burn distribution pie chart">
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
                    <div style={{ fontSize: 11, color: "rgba(255,255,255,.42)", fontWeight: 700, letterSpacing: .8, textTransform: "uppercase" }}>{t("burns.burnShareByToken")}</div>
                    <div style={{ width: "100%", maxWidth: 230, maxHeight: 132, overflowY: "auto", paddingRight: 4 }}>
                      <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                        {pieSlices.map((s, i) => {
                          const pct = pieTotal > 0 ? ((s.amount / pieTotal) * 100).toFixed(1) : "0.0";
                          return (
                            <div key={`legend-${s.symbol}-${i}`} style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8 }}>
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
                  <div style={{ minHeight: 190, display: "flex", alignItems: "center", justifyContent: "center", color: "rgba(255,255,255,.3)", fontSize: 13 }}>
                    {t("burns.noTokenData")}
                  </div>
                )}
              </div>
            </div>
          ) : (
            <div style={{ minHeight: 190, display: "flex", alignItems: "center", justifyContent: "center", color: "rgba(255,255,255,.3)", fontSize: 13 }}>
              {t("burns.noBurnData")}
            </div>
          )}
        </div>

        <div className="glass" style={{ padding: "16px", minHeight: 256 }}>
          <div style={{ fontSize: 13, fontWeight: 700, color: "#fff", marginBottom: 12, textAlign: "center" }}>{t("burns.tokenBurnBreakdown")}</div>
          {hasBreakdown ? (
            <div style={{ height: 220, overflowY: "auto", paddingRight: 4 }}>
              <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
              {tokenBreakdown.map((t, idx) => {
                const share = totalTokenBurned > 0 ? (t.amount / totalTokenBurned) * 100 : 0;
                const color = pieColors[idx % pieColors.length];
                return (
                  <div key={t.symbol}>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 5 }}>
                      <span style={{ fontSize: 12, color: "rgba(255,255,255,.75)", fontWeight: 700 }}>{t.symbol}</span>
                      <span style={{ fontSize: 11, color: "rgba(255,255,255,.45)", fontFamily: "'JetBrains Mono', monospace" }}>{fmtFull(t.amount)}</span>
                    </div>
                    <div style={{ height: 8, borderRadius: 999, background: "rgba(255,255,255,.08)", overflow: "hidden" }}>
                      <div style={{ width: `${Math.max(share, 3)}%`, height: "100%", background: `linear-gradient(90deg, ${color}, rgba(255,255,255,.18))` }} />
                    </div>
                  </div>
                );
              })}
              </div>
            </div>
          ) : (
            <div style={{ minHeight: 190, display: "flex", alignItems: "center", justifyContent: "center", color: "rgba(255,255,255,.3)", fontSize: 13 }}>
              {t("burns.noTokenData")}
            </div>
          )}
        </div>
      </div>

      <div className="glass" style={{ padding: "16px", border: "1px solid rgba(255,106,0,.18)" }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12, flexWrap: "wrap", marginBottom: 12 }}>
          <div style={{ fontSize: 14, fontWeight: 800, color: "#fff" }}>{t("burns.burnHistory")}</div>
          <input
            value={query}
            onChange={e => { setQuery(e.target.value); setHistoryPage(1); }}
            placeholder={t("burns.searchPlaceholder")}
            className="input-f"
            style={{ width: 260, height: 34, fontSize: 12 }}
          />
        </div>
        {hasHistory ? (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 760 }}>
              <thead>
                <tr>
                  {[t("burns.type"), t("burns.details"), t("burns.txSignature"), t("burns.age")].map(h => (
                    <th key={h} style={{ textAlign: "left", padding: "10px 8px", fontSize: 11, letterSpacing: .8, color: "rgba(255,255,255,.42)", textTransform: "uppercase", borderBottom: "1px solid rgba(255,255,255,.08)" }}>
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {pagedRows.map((row, idx) => (
                  <tr key={`${row.id ?? idx}-${row.tx ?? "none"}`}>
                    <td style={{ padding: "11px 8px", borderBottom: "1px solid rgba(255,255,255,.05)" }}>
                      <span style={{ display: "inline-flex", alignItems: "center", padding: "4px 8px", borderRadius: 999, fontSize: 10, fontWeight: 700, letterSpacing: .7, textTransform: "uppercase", background: row.type === "burn" ? "rgba(255,106,0,.12)" : "rgba(255,149,0,.12)", color: row.type === "burn" ? "#ff8c42" : "#ffb266", border: "1px solid rgba(255,106,0,.24)" }}>
                        {row.type}
                      </span>
                    </td>
                    <td style={{ padding: "11px 8px", borderBottom: "1px solid rgba(255,255,255,.05)", fontSize: 13, color: "rgba(255,255,255,.72)" }}>
                      {row.msg}
                    </td>
                    <td style={{ padding: "11px 8px", borderBottom: "1px solid rgba(255,255,255,.05)", fontFamily: "'JetBrains Mono', monospace", fontSize: 12 }}>
                      {row.tx ? (
                        <a href={solscanTx(row.tx)} target="_blank" rel="noopener noreferrer" className="tx-link">
                          {row.tx.slice(0, 8)}...{row.tx.slice(-6)} {"\u2197"}
                        </a>
                      ) : (
                        <span style={{ color: "rgba(255,255,255,.3)" }}>{t("burns.na")}</span>
                      )}
                    </td>
                    <td style={{ padding: "11px 8px", borderBottom: "1px solid rgba(255,255,255,.05)", fontSize: 12, color: "rgba(255,255,255,.42)" }}>
                      {fmtAge(row.age ?? 0)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: 12, gap: 10, flexWrap: "wrap" }}>
              <div style={{ fontSize: 11, color: "rgba(255,255,255,.4)", fontWeight: 600 }}>
                {t("burns.showingResults", { shown: pagedRows.length, total: filteredRows.length })}
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <button
                  type="button"
                  className="btn-ghost"
                  disabled={safePage <= 1}
                  onClick={() => setHistoryPage(p => Math.max(1, p - 1))}
                  style={{ height: 30, padding: "0 10px", fontSize: 12, borderRadius: 8 }}
                >
                  {t("burns.prev")}
                </button>
                <span style={{ fontSize: 12, color: "rgba(255,255,255,.58)", minWidth: 84, textAlign: "center" }}>
                  {t("burns.page")} {safePage} / {totalPages}
                </span>
                <button
                  type="button"
                  className="btn-ghost"
                  disabled={safePage >= totalPages}
                  onClick={() => setHistoryPage(p => Math.min(totalPages, p + 1))}
                  style={{ height: 30, padding: "0 10px", fontSize: 12, borderRadius: 8 }}
                >
                  {t("burns.next")}
                </button>
              </div>
            </div>
          </div>
        ) : (
          <div style={{ minHeight: 120, display: "flex", alignItems: "center", justifyContent: "center", color: "rgba(255,255,255,.34)", fontSize: 13 }}>
            {t("burns.noBurnTx")}
          </div>
        )}
      </div>
    </section>
  );
}
