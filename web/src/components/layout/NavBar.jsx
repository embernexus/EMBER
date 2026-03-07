import { useState } from "react";
import { CREATOR_X_URL, NAV_ITEMS, NAV_SOCIALS } from "../../config/site";
import { useI18n } from "../../i18n/I18nProvider";
import { fmtFull, fmtSol } from "../../lib/format";
import NavSocialIcon from "../NavSocialIcon";

function isActiveNavItem(itemKey, page) {
  if (itemKey === "roadmap") return page === "roadmap";
  if (itemKey === "burns") return page === "burns";
  if (itemKey === "docs") return page === "docs";
  if (itemKey === "whitepaper") return page === "whitepaper";
  if (itemKey === "updates") return page === "updates";
  if (itemKey === "dashboard") return page === "dashboard";
  if (itemKey === "how") return page === "home";
  return false;
}

export default function NavBar({
  page,
  user,
  menuOpen,
  onToggleMenu,
  onSignOut,
  onShowLogin,
  onNavItemClick,
  publicMetrics,
  onBrandClick,
}) {
  const { t } = useI18n();
  const username = typeof user === "string" ? user : String(user?.username || "");
  const isManager = Boolean(user?.role === "manager" || user?.isOperator);
  const isOg = Boolean(user?.isOg);
  const isAdmin = Boolean(user?.isAdmin);
  const [showStatsHover, setShowStatsHover] = useState(false);
  const [statsHoverPos, setStatsHoverPos] = useState(null);
  const [showComingSoonHover, setShowComingSoonHover] = useState(false);
  const [comingSoonPos, setComingSoonPos] = useState(null);

  const navLabelByKey = {
    dashboard: t("nav.dashboard"),
    how: t("nav.how"),
    burns: t("nav.burns"),
    roadmap: t("nav.roadmap"),
    whitepaper: t("nav.whitepaper"),
    updates: t("nav.updates"),
    docs: t("nav.docs"),
    stats: t("nav.stats"),
    trading: t("nav.trading"),
    deploy: t("nav.deploy"),
  };

  return (
    <nav className="nav-blur nav-grid" style={{ position: "fixed", top: 0, left: 0, right: 0, zIndex: 120 }}>
      <div style={{ display: "flex", flexDirection: "column", alignItems: "center" }}>
        <button className="brand-btn" onClick={onBrandClick} aria-label="Go to home">
          <span style={{ fontSize: 26, animation: "flicker 5s infinite" }}>{"\u{1F525}"}</span>
          <div>
            <span
              style={{
                fontSize: 20,
                fontWeight: 800,
                letterSpacing: 2,
                color: "#fff",
                animation: "textGlow 4s infinite",
              }}
            >
              EMBER
            </span>
            <span
              style={{
                fontSize: 10,
                color: "rgba(255,236,214,.88)",
                fontWeight: 700,
                letterSpacing: 2.2,
                marginLeft: 10,
                textShadow: "0 0 10px rgba(255,106,0,.28)",
              }}
            >
              NEXUS
            </span>
          </div>
        </button>
        <a
          href={CREATOR_X_URL}
          target="_blank"
          rel="noopener noreferrer"
          style={{
            marginTop: -4,
            transform: "translateX(18px)",
            fontSize: 10,
            color: "rgba(255,255,255,.62)",
            textDecoration: "none",
            letterSpacing: 0.2,
          }}
          onClick={(e) => e.stopPropagation()}
        >
          {t("nav.createdBy")}
        </a>
      </div>

      <div className="nav-links">
        {NAV_ITEMS.map((item) => {
          const active = isActiveNavItem(item.key, page);

          if (item.key === "stats") {
            return (
              <div
                key={item.key}
                style={{ position: "relative" }}
                onMouseEnter={(e) => {
                  const rect = e.currentTarget.getBoundingClientRect();
                  setStatsHoverPos({ left: rect.left + rect.width / 2, top: rect.bottom + 10 });
                  setShowStatsHover(true);
                }}
                onMouseLeave={() => setShowStatsHover(false)}
              >
                <button type="button" className="nav-link-btn" onClick={(e) => e.preventDefault()}>
                  {navLabelByKey[item.key] || item.label}
                </button>
                {showStatsHover && statsHoverPos && (
                  <div
                    style={{
                      position: "fixed",
                      top: statsHoverPos.top,
                      left: statsHoverPos.left,
                      transform: "translateX(-50%)",
                      width: 250,
                      background: "rgba(10,4,18,.96)",
                      border: "1px solid rgba(255,255,255,.12)",
                      borderRadius: 12,
                      padding: "10px 12px",
                      boxShadow: "0 14px 36px rgba(0,0,0,.45)",
                      zIndex: 300,
                      pointerEvents: "none",
                    }}
                  >
                    <div
                      style={{
                        fontSize: 10,
                        fontWeight: 800,
                        letterSpacing: 0.8,
                        color: "#ff9f5a",
                        textTransform: "uppercase",
                        marginBottom: 8,
                        textAlign: "center",
                      }}
                    >
                      {t("nav.nexusStatus")}
                    </div>
                    {[
                      [t("metrics.totalIncinerated"), fmtFull(publicMetrics.lifetimeIncinerated)],
                      [t("metrics.totalBotTransactions"), fmtFull(publicMetrics.totalBotTransactions)],
                      [t("metrics.activeTokens"), fmtFull(publicMetrics.activeTokens)],
                      [t("metrics.totalHolders"), fmtFull(publicMetrics.totalHolders)],
                      [t("metrics.emberIncinerated"), fmtFull(publicMetrics.emberIncinerated)],
                      [t("metrics.rewardsProcessed"), `${fmtSol(publicMetrics.totalRewardsProcessedSol)} SOL`],
                      [t("metrics.totalFees"), `${fmtSol(publicMetrics.totalFeesTakenSol)} SOL`],
                    ].map(([k, v]) => (
                      <div
                        key={k}
                        style={{
                          display: "flex",
                          justifyContent: "space-between",
                          gap: 8,
                          fontSize: 11,
                          padding: "4px 0",
                          borderBottom: "1px solid rgba(255,255,255,.05)",
                        }}
                      >
                        <span style={{ color: "rgba(255,255,255,.62)" }}>{k}</span>
                        <span style={{ color: "#fff", fontWeight: 700, textAlign: "right" }}>{v}</span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            );
          }

          if (item.key === "trading") {
            return (
              <div
                key={item.key}
                style={{ position: "relative" }}
                onMouseEnter={(e) => {
                  const rect = e.currentTarget.getBoundingClientRect();
                  setComingSoonPos({ left: rect.left + rect.width / 2, top: rect.bottom + 10 });
                  setShowComingSoonHover(true);
                }}
                onMouseLeave={() => setShowComingSoonHover(false)}
              >
                <button
                  type="button"
                  className={`nav-link-btn ${item.enabled ? "" : "disabled"} ${active ? "active" : ""}`}
                  onClick={(e) => e.preventDefault()}
                  aria-label="AI Trading coming soon"
                >
                  {navLabelByKey[item.key] || item.label}
                </button>
                {showComingSoonHover && comingSoonPos && (
                  <div
                    style={{
                      position: "fixed",
                      top: comingSoonPos.top,
                      left: comingSoonPos.left,
                      transform: "translateX(-50%)",
                      background: "rgba(10,4,18,.96)",
                      border: "1px solid rgba(255,255,255,.12)",
                      borderRadius: 10,
                      padding: "6px 10px",
                      boxShadow: "0 14px 36px rgba(0,0,0,.45)",
                      zIndex: 300,
                      pointerEvents: "none",
                      fontSize: 11,
                      fontWeight: 700,
                      letterSpacing: 0.4,
                      color: "#ff9f5a",
                      textTransform: "uppercase",
                    }}
                  >
                    {t("nav.comingSoon")}
                  </div>
                )}
              </div>
            );
          }

          return (
            <button
              key={item.key}
              type="button"
              onClick={() => onNavItemClick(item)}
              className={`nav-link-btn ${item.enabled ? "" : "disabled"} ${active ? "active" : ""}`}
            >
              {navLabelByKey[item.key] || item.label}
            </button>
          );
        })}

        <div className="nav-socials">
          {NAV_SOCIALS.map((link) => (
            <a
              key={link.key}
              href={link.href}
              target="_blank"
              rel="noopener noreferrer"
              className="nav-social-btn"
              aria-label={link.label}
              title={link.label}
            >
              <NavSocialIcon type={link.key} />
            </a>
          ))}
        </div>
      </div>

      <div style={{ display: "flex", alignItems: "center", gap: 10, justifyContent: "flex-end" }}>
        {username ? (
          <div style={{ position: "relative" }}>
            <button
              onClick={onToggleMenu}
              style={{
                background: "rgba(255,106,0,.1)",
                border: "1px solid rgba(255,106,0,.18)",
                borderRadius: 10,
                padding: "7px 14px",
                color: "#fff",
                cursor: "pointer",
                fontSize: 13,
                fontWeight: 600,
                display: "flex",
                alignItems: "center",
                gap: 8,
              }}
            >
              <div
                style={{
                  width: 22,
                  height: 22,
                  borderRadius: "50%",
                  background: "linear-gradient(135deg,#ff6a00,#cc2200)",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  fontSize: 11,
                  fontWeight: 800,
                }}
              >
                {String(username).slice(0, 1).toUpperCase()}
              </div>
              <span>{username}</span>
              {isOg && (
                <span style={{ fontSize: 10, color: "#fff2bd", fontWeight: 800, letterSpacing: 0.6 }}>
                  OG
                </span>
              )}
              {isAdmin && (
                <span style={{ fontSize: 10, color: "#9fdbff", fontWeight: 800, letterSpacing: 0.6 }}>
                  ADMIN
                </span>
              )}
              {isManager && (
                <span style={{ fontSize: 10, color: "#ffb26b", fontWeight: 800, letterSpacing: 0.6 }}>
                  MANAGER
                </span>
              )}
            </button>
            {menuOpen && (
              <div
                style={{
                  position: "absolute",
                  top: "calc(100% + 8px)",
                  right: 0,
                  background: "#0f0407",
                  border: "1px solid rgba(255,255,255,.08)",
                  borderRadius: 10,
                  overflow: "hidden",
                  minWidth: 160,
                  boxShadow: "0 16px 40px rgba(0,0,0,.5)",
                  animation: "slideUp .15s ease",
                }}
              >
                <div
                  style={{
                    padding: "10px 16px",
                    fontSize: 11,
                    color: "rgba(255,255,255,.25)",
                    borderBottom: "1px solid rgba(255,255,255,.06)",
                  }}
                >
                  {t("nav.signedInAs", { user: username })}
                </div>
                {isManager && (
                  <div
                    style={{
                      padding: "10px 16px",
                      fontSize: 11,
                      color: "rgba(255,178,107,.86)",
                      borderBottom: "1px solid rgba(255,255,255,.06)",
                    }}
                  >
                    Manager access: withdraw, sweep, and delete are disabled.
                  </div>
                )}
                <button
                  onClick={onSignOut}
                  style={{
                    width: "100%",
                    background: "none",
                    border: "none",
                    color: "rgba(255,255,255,.65)",
                    padding: "11px 16px",
                    textAlign: "left",
                    cursor: "pointer",
                    fontSize: 13,
                    fontWeight: 600,
                  }}
                >
                  {t("nav.signOut")}
                </button>
              </div>
            )}
          </div>
        ) : (
          <button
            className="btn-fire"
            onClick={onShowLogin}
            style={{
              height: 36,
              padding: "0 18px",
              fontSize: 13,
              fontWeight: 700,
              lineHeight: 1,
              display: "inline-flex",
              alignItems: "center",
              justifyContent: "center",
              borderRadius: 10,
            }}
          >
            {t("login.signIn")}
          </button>
        )}
      </div>
    </nav>
  );
}
