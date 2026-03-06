import HowItWorks from "../components/HowItWorks";
import { BUY_EMBER_URL } from "../config/site";
import { useI18n } from "../i18n/I18nProvider";
import { fmtFull, fmtSol } from "../lib/format";

export default function HomePage({ heroWord, publicMetrics, onShowLogin }) {
  const { t } = useI18n();

  return (
    <>
      <div
        style={{
          position: "relative",
          zIndex: 2,
          minHeight: "88vh",
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          justifyContent: "center",
          textAlign: "center",
          padding: "80px 24px",
        }}
      >
        <div
          style={{
            position: "absolute",
            width: 640,
            height: 640,
            borderRadius: "50%",
            border: "1px solid rgba(255,106,0,.05)",
            left: "50%",
            top: "42%",
            transform: "translate(-50%,-50%)",
            animation: "ringExpand 5s linear infinite",
            pointerEvents: "none",
          }}
        />
        <div
          style={{
            position: "absolute",
            width: 640,
            height: 640,
            borderRadius: "50%",
            border: "1px solid rgba(255,106,0,.04)",
            left: "50%",
            top: "42%",
            transform: "translate(-50%,-50%)",
            animation: "ringExpand 5s linear 2.5s infinite",
            pointerEvents: "none",
          }}
        />

        <div style={{ fontSize: 72, marginBottom: 20, animation: "flicker 3s infinite", filter: "drop-shadow(0 0 40px rgba(255,106,0,.55))" }}>
          {"\u{1F525}"}
        </div>

        <h1
          style={{
            fontSize: "clamp(44px,8vw,90px)",
            fontWeight: 800,
            color: "#fff",
            lineHeight: 1.05,
            marginBottom: 16,
            letterSpacing: 0.6,
            wordSpacing: ".38em",
            background: "linear-gradient(135deg,#ffffff 0%,#ffd0a0 35%,#ff6a00 80%)",
            WebkitBackgroundClip: "text",
            WebkitTextFillColor: "transparent",
            backgroundSize: "200% 200%",
            animation: "gradShift 5s ease infinite",
          }}
        >
          <span style={{ display: "inline-block", minWidth: "5ch", textAlign: "right" }}>{heroWord}</span>
          <span style={{ display: "inline-block", marginLeft: ".38em" }}>EVERYTHING.</span>
        </h1>

        <p
          style={{
            fontSize: "clamp(15px,2.2vw,20px)",
            color: "rgba(255,255,255,.38)",
            maxWidth: 520,
            lineHeight: 1.65,
            marginBottom: 38,
            fontWeight: 500,
          }}
        >
          {t("home.subtitle")}
        </p>

        <div style={{ display: "flex", gap: 10, flexWrap: "wrap", justifyContent: "center", maxWidth: 760, marginBottom: 26 }}>
          {[
            "Burn Bot",
            "Volume Bot",
            "Market Maker",
            "DCA Bot",
            "Rekindle Bot",
            "Telegram Alerts",
            "Creator Rewards / External Funding / Hybrid",
            "Manager Access",
          ].map((item) => (
            <div
              key={item}
              style={{
                padding: "8px 12px",
                borderRadius: 999,
                background: "rgba(255,255,255,.04)",
                border: "1px solid rgba(255,106,0,.16)",
                color: "rgba(255,232,214,.84)",
                fontSize: 11,
                fontWeight: 700,
                letterSpacing: 0.6,
                textTransform: "uppercase",
              }}
            >
              {item}
            </div>
          ))}
        </div>

        <div style={{ display: "flex", gap: 12, flexWrap: "wrap", justifyContent: "center" }}>
          <button className="btn-fire" onClick={onShowLogin} style={{ padding: "15px 38px", fontSize: 15 }}>
            {t("home.getStarted")}
          </button>
          <button
            className="btn-ghost"
            onClick={() => document.getElementById("how-it-works")?.scrollIntoView({ behavior: "smooth" })}
            style={{ padding: "15px 30px", fontSize: 15 }}
          >
            {t("home.howItWorks")}
          </button>
        </div>

        <div style={{ marginTop: 12 }}>
          <button
            className="btn-fire"
            onClick={() => window.open(BUY_EMBER_URL, "_blank", "noopener,noreferrer")}
            style={{ padding: "12px 22px", fontSize: 14, display: "inline-flex", alignItems: "center", gap: 10 }}
          >
            <img
              src="https://pump.fun/logo.png"
              alt="Pump.fun"
              width="18"
              height="18"
              style={{ display: "block", objectFit: "contain", filter: "drop-shadow(0 0 6px rgba(0,0,0,.25))" }}
            />
            <span>{t("home.buyEmber")}</span>
          </button>
        </div>

        <div style={{ marginTop: 72, display: "flex", flexWrap: "wrap", justifyContent: "center", gap: 34, maxWidth: 980, width: "100%" }}>
          {[
            [fmtFull(publicMetrics.totalBotTransactions), t("metrics.totalBotTransactionsUpper")],
            [fmtFull(publicMetrics.activeTokens), t("metrics.activeTokensAttached")],
            [fmtFull(publicMetrics.totalHolders), t("metrics.totalEmberHolders")],
            [fmtFull(publicMetrics.emberIncinerated), t("metrics.totalEmberIncinerated")],
            [`${fmtSol(publicMetrics.totalRewardsProcessedSol)} SOL`, t("metrics.totalRewardsProcessed")],
            [`${fmtSol(publicMetrics.totalFeesTakenSol)} SOL`, t("metrics.totalFeesTaken")],
            [fmtFull(publicMetrics.lifetimeIncinerated), t("metrics.totalIncineratedUpper")],
          ].map(([v, l]) => (
            <div key={l} style={{ textAlign: "center", minWidth: 220 }}>
              <div style={{ fontSize: 28, fontWeight: 800, color: "#ff8c42", animation: "textGlow 4s infinite", lineHeight: 1.1 }}>{v}</div>
              <div style={{ fontSize: 11, color: "rgba(255,255,255,.4)", fontWeight: 700, letterSpacing: 1.1, marginTop: 6 }}>{l.toUpperCase()}</div>
            </div>
          ))}
        </div>
      </div>

      <HowItWorks />
    </>
  );
}
