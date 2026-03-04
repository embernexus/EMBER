import { SOLANA_WEBSITE_URL } from "../../config/site";
import { useI18n } from "../../i18n/I18nProvider";

export default function Footer() {
  const { t } = useI18n();

  return (
    <footer
      style={{
        position: "relative",
        zIndex: 2,
        padding: "28px 24px",
        borderTop: "1px solid rgba(255,255,255,.04)",
        marginTop: 40,
        textAlign: "center",
      }}
    >
      <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 6 }}>
        <div style={{ display: "inline-flex", alignItems: "center", gap: 9, textShadow: "0 0 10px rgba(255,255,255,.08)" }}>
          <span style={{ fontSize: 14 }}>{"\u{1F525}"}</span>
          <div style={{ display: "flex", alignItems: "baseline", gap: 8 }}>
            <span style={{ fontSize: 12, fontWeight: 800, letterSpacing: 1, color: "#fff", textTransform: "uppercase" }}>EMBER</span>
            <span style={{ fontSize: 10, color: "rgba(255,236,214,.88)", fontWeight: 700, letterSpacing: 2, textTransform: "uppercase" }}>NEXUS</span>
          </div>
        </div>

        <a
          href={SOLANA_WEBSITE_URL}
          target="_blank"
          rel="noopener noreferrer"
          style={{
            display: "inline-flex",
            alignItems: "center",
            gap: 8,
            padding: "6px 10px",
            borderRadius: 999,
            border: "1px solid rgba(255,255,255,.22)",
            background: "rgba(255,255,255,.07)",
            textDecoration: "none",
            cursor: "pointer",
          }}
        >
          <svg width="20" height="15" viewBox="0 0 128 96" fill="none" aria-hidden="true">
            <defs>
              <linearGradient id="solana-g" x1="0" y1="0" x2="128" y2="96" gradientUnits="userSpaceOnUse">
                <stop offset="0" stopColor="#00FFA3" />
                <stop offset="1" stopColor="#DC1FFF" />
              </linearGradient>
            </defs>
            <path d="M20 4H124L104 24H0L20 4Z" fill="url(#solana-g)" />
            <path d="M24 36H128L108 56H4L24 36Z" fill="url(#solana-g)" />
            <path d="M20 68H124L104 88H0L20 68Z" fill="url(#solana-g)" />
          </svg>
          <span style={{ fontSize: 12, color: "rgba(255,255,255,.86)", fontWeight: 600, letterSpacing: 0.2 }}>{t("footer.poweredBy")}</span>
          <span style={{ fontSize: 12, color: "#fff", fontWeight: 700, letterSpacing: 0.2 }}>Solana</span>
        </a>

        <div style={{ fontSize: 11, color: "rgba(255,255,255,.66)", textShadow: "0 0 8px rgba(255,255,255,.06)" }}>
          {t("footer.copyright", { year: new Date().getFullYear() })}
        </div>
      </div>
    </footer>
  );
}
