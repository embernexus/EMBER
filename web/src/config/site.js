export const envOr = (value, fallback) => {
  const v = String(value || "").trim();
  const lowered = v.toLowerCase();
  if (!v || lowered === "xxxxx" || lowered === "xxx" || lowered === "tbd") {
    return fallback;
  }
  return v;
};

export const API_BASE = String(import.meta.env.VITE_API_BASE_URL || "").trim().replace(/\/+$/, "");

export const SOLANA_INCINERATOR = "1nc1nerator11111111111111111111111111111111";
export const EMBER_DEV_WALLET = envOr(
  import.meta.env.VITE_EMBER_DEV_WALLET,
  "EMBERth7keHqbLNR4JHCfduPm5YT1LGhJv7X8uTefaoR"
);
export const EMBER_TREASURY_WALLET = envOr(
  import.meta.env.VITE_EMBER_TREASURY_WALLET,
  "EMBERDnVaS8rc3mVVRjaiciCdFVCJdu2h22bYWeAm953"
);
export const EMBER_TOKEN_CONTRACT = envOr(
  import.meta.env.VITE_EMBER_TOKEN_CONTRACT,
  "EMBEReyhmvbc6hfdFsvkfLf4eb4jdKSoybnbJ8oUzL66"
);

export const BUY_EMBER_URL = envOr(import.meta.env.VITE_BUY_EMBER_URL, "https://pump.fun");
export const SOLANA_WEBSITE_URL = envOr(import.meta.env.VITE_SOLANA_URL, "https://solana.com");
export const CREATOR_X_URL = envOr(import.meta.env.VITE_CREATOR_X_URL, "https://x.com/satoshEH_");

export const SOCIAL_LINKS = {
  x: envOr(
    import.meta.env.VITE_COMMUNITY_X_URL
      || import.meta.env.VITE_CREATOR_X_URL
      || import.meta.env.VITE_X_URL,
    "https://x.com/i/communities/2029665598809198626"
  ),
  telegram: envOr(import.meta.env.VITE_TELEGRAM_URL, "https://t.me/ember_nexus"),
  github: envOr(import.meta.env.VITE_GITHUB_URL, "https://github.com/embernexus"),
};

export const NAV_SOCIALS = [
  { key: "x", href: SOCIAL_LINKS.x, label: "X" },
  { key: "telegram", href: SOCIAL_LINKS.telegram, label: "Telegram" },
  { key: "github", href: SOCIAL_LINKS.github, label: "GitHub" },
];

export const NAV_ITEMS = [
  { key: "dashboard", label: "Nexus", enabled: true },
  { key: "how", label: "How It Works", enabled: true },
  { key: "burns", label: "Burns", enabled: true },
  { key: "roadmap", label: "Roadmap", enabled: true },
  { key: "whitepaper", label: "Whitepaper", enabled: true },
  { key: "updates", label: "Dev Logs", enabled: true },
  { key: "docs", label: "Docu", enabled: true },
  { key: "stats", label: "Stats", enabled: false },
  { key: "trading", label: "AI Trading", enabled: false },
  { key: "deploy", label: "Deploy", enabled: true },
];
