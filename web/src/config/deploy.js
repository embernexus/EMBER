export const BOT_ATTACH_OPTIONS = [
  { value: "", label: "No Auto-Attach" },
  { value: "burn", label: "Burn Bot" },
  { value: "volume", label: "Volume Bot" },
  { value: "market_maker", label: "Market Maker Bot (Coming Soon)", disabled: true },
  { value: "more_automation", label: "More automation coming soon", disabled: true },
];

export const DEPLOY_SLIPPAGE = 10;
export const DEPLOY_PRIORITY_FEE = 0.0005;
export const DEPLOY_RPC_URL = import.meta.env.VITE_SOLANA_RPC_URL || "https://api.mainnet-beta.solana.com";

export const DEPLOY_IMAGE_MAX_BYTES = 15 * 1024 * 1024;
export const DEPLOY_VIDEO_MAX_BYTES = 30 * 1024 * 1024;
export const DEPLOY_BANNER_MAX_BYTES = 4.3 * 1024 * 1024;

export const DEPLOY_ALLOWED_IMAGE_TYPES = new Set(["image/jpeg", "image/png", "image/gif"]);
export const DEPLOY_ALLOWED_VIDEO_TYPES = new Set(["video/mp4"]);
export const DEPLOY_ALLOWED_BANNER_TYPES = new Set(["image/jpeg", "image/png", "image/gif"]);

export const LAMPORTS_PER_SOL_BI = 1_000_000_000n;
export const PUMP_TOKEN_DECIMALS = 6;
export const PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
export const PUMP_GLOBAL_SEED = "global";
export const PUMP_GLOBAL_DEFAULT_RESERVES = {
  initialVirtualTokenReserves: 1_073_000_000_000_000n,
  initialVirtualSolReserves: 30_000_000_000n,
  initialRealTokenReserves: 793_100_000_000_000n,
};
export const PUMP_RPC_FALLBACKS = [
  "https://api.mainnet-beta.solana.com",
  "https://rpc.ankr.com/solana",
];
