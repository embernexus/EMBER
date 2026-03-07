import dotenv from "dotenv";
import bs58 from "bs58";
import { Keypair } from "@solana/web3.js";

dotenv.config();

function cleanEnvString(input) {
  return String(input || "")
    .trim()
    .replace(/^['"]+|['"]+$/g, "")
    .replace(/\\n/g, "")
    .trim();
}

function parsePrivateKey(input, label) {
  const name = String(label || "PRIVATE_KEY");
  const raw = String(input || "").trim();
  if (!raw) return null;

  let secretKey;

  if (raw.startsWith("[")) {
    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch {
      throw new Error(
        `${name} JSON is invalid. Expected a JSON array of 64 numbers.`
      );
    }
    if (!Array.isArray(parsed)) {
      throw new Error(`${name} JSON must be an array.`);
    }
    const invalid = parsed.some((n) => !Number.isInteger(n) || n < 0 || n > 255);
    if (invalid) {
      throw new Error(`${name} JSON must contain only byte values (0-255).`);
    }
    secretKey = Uint8Array.from(parsed);
  } else {
    try {
      secretKey = bs58.decode(raw);
    } catch {
      throw new Error(
        `${name} base58 is invalid. Provide valid base58 or JSON array format.`
      );
    }
  }

  if (secretKey.length !== 64) {
    throw new Error(
      `${name} must decode to 64 bytes. Received ${secretKey.length} bytes.`
    );
  }

  const keypair = Keypair.fromSecretKey(secretKey);
  return {
    secretKey,
    publicKey: keypair.publicKey.toBase58(),
  };
}

const devWalletPrivateKeyInput =
  process.env.DEV_WALLET_PRIVATE_KEY || process.env.DEV_WALLET_SECRET_KEY || "";
const treasuryWalletPrivateKeyInput =
  process.env.TREASURY_WALLET_PRIVATE_KEY || process.env.TREASURY_PRIVATE_KEY || "";
const parsedDevWallet = parsePrivateKey(devWalletPrivateKeyInput, "DEV_WALLET_PRIVATE_KEY");
const parsedTreasuryWallet = parsePrivateKey(
  treasuryWalletPrivateKeyInput,
  "TREASURY_WALLET_PRIVATE_KEY"
);

export const config = {
  port: Number(process.env.PORT || 3001),
  databaseUrl: cleanEnvString(process.env.DATABASE_URL || ""),
  sessionCookieName: process.env.SESSION_COOKIE_NAME || "ember_session",
  sessionTtlDays: Number(process.env.SESSION_TTL_DAYS || 14),
  maxTokensPerAccount: Number(process.env.MAX_TOKENS_PER_ACCOUNT || 5),
  workerTickMs: Number(process.env.WORKER_TICK_MS || 1000),
  executorTickMs: Number(process.env.EXECUTOR_TICK_MS || 250),
  schedulerBatchLimit: Number(process.env.SCHEDULER_BATCH_LIMIT || 500),
  executorBatchLimit: Number(process.env.EXECUTOR_BATCH_LIMIT || 32),
  rpcUrl: cleanEnvString(
    process.env.SOLANA_RPC_URL ||
      process.env.HELIUS_RPC_URL ||
      process.env.VITE_SOLANA_RPC_URL ||
      ""
  ),
  treasuryWallet: String(process.env.TREASURY_WALLET || "EMBERDnVaS8rc3mVVRjaiciCdFVCJdu2h22bYWeAm953").trim(),
  emberTokenMint: String(
    process.env.EMBER_TOKEN_MINT ||
    process.env.VITE_EMBER_TOKEN_CONTRACT ||
    ""
  ).trim(),
  personalCreatorMints: String(process.env.PERSONAL_CREATOR_MINTS || "")
    .split(",")
    .map((v) => String(v || "").trim())
    .filter(Boolean),
  basePriorityFeeSol: Number(process.env.BASE_PRIORITY_FEE_SOL || 0.0005),
  personalClaimMinSol: Number(process.env.PERSONAL_CLAIM_MIN_SOL || 0),
  claimGasTopupSol: Number(process.env.CLAIM_GAS_TOPUP_SOL || 0.003),
  botSolReserve: Number(process.env.BOT_SOL_RESERVE || 0.005),
  devWalletSolReserve: Number(process.env.DEV_WALLET_SOL_RESERVE || 0.01),
  volumeDefaultMinTradeSol: Number(process.env.VOLUME_DEFAULT_MIN_TRADE_SOL || 0.01),
  volumeDefaultMaxTradeSol: Number(process.env.VOLUME_DEFAULT_MAX_TRADE_SOL || 0.05),
  pumpPortalApiKey: process.env.PUMPPORTAL_API_KEY || "",
  reactionManagerApiKey: cleanEnvString(
    process.env.REACTION_MANAGER_API_KEY ||
    process.env.DEXMOJI_API_KEY ||
    ""
  ),
  reactionManagerApiUrl: cleanEnvString(
    process.env.REACTION_MANAGER_API_URL ||
    "https://api.dexemoji.fun/api/v2"
  ),
  emberFundingApiKey: cleanEnvString(
    process.env.EMBER_FUNDING_API_KEY ||
    process.env.SPLITNOW_API_KEY ||
    ""
  ),
  emberFundingApiUrl: cleanEnvString(
    process.env.EMBER_FUNDING_API_URL ||
    process.env.SPLITNOW_API_URL ||
    "https://splitnow.io/api"
  ).replace(/\/+$/g, ""),
  telegramBotToken: String(process.env.TELEGRAM_BOT_TOKEN || "").trim(),
  telegramChatId: String(process.env.TELEGRAM_CHAT_ID || "").trim(),
  telegramTopicId: String(process.env.TELEGRAM_TOPIC_ID || "").trim(),
  solanaKeygenBin: process.env.SOLANA_KEYGEN_BIN || "solana-keygen",
  depositVanityPrefix: String(process.env.DEPOSIT_VANITY_PREFIX || "EMBR").trim().toUpperCase(),
  depositVanityPrimaryPrefix: String(
    process.env.DEPOSIT_VANITY_PRIMARY_PREFIX || process.env.DEPOSIT_VANITY_PREFIX || "EMBR"
  )
    .trim()
    .toUpperCase(),
  depositVanityFallbackPrefix: String(
    process.env.DEPOSIT_VANITY_FALLBACK_PREFIX ||
      (process.env.DEPOSIT_VANITY_PRIMARY_PREFIX ? process.env.DEPOSIT_VANITY_PREFIX || "EMBR" : "")
  )
    .trim()
    .toUpperCase(),
  depositVanityThreads: Number(process.env.DEPOSIT_VANITY_THREADS || 4),
  depositVanityTimeoutMs: Number(process.env.DEPOSIT_VANITY_TIMEOUT_MS || 300000),
  depositVanityAllowJsFallback: String(process.env.DEPOSIT_VANITY_ALLOW_JS_FALLBACK || "true").toLowerCase() !== "false",
  depositPoolTarget: Number(process.env.DEPOSIT_POOL_TARGET || 20),
  depositPoolTargetEmber: Number(process.env.DEPOSIT_POOL_TARGET_EMBER || 0),
  depositPoolTargetEmbr: Number(process.env.DEPOSIT_POOL_TARGET_EMBR || process.env.DEPOSIT_POOL_TARGET || 20),
  depositPoolRefillIntervalMs: Number(process.env.DEPOSIT_POOL_REFILL_INTERVAL_MS || 15000),
  depositPoolEtaPerAddressSec: Number(process.env.DEPOSIT_POOL_ETA_PER_ADDRESS_SEC || 45),
  deployVanityBufferSol: Number(process.env.DEPLOY_VANITY_BUFFER_SOL || 0.03),
  deployVanityReservationMinutes: Number(process.env.DEPLOY_VANITY_RESERVATION_MINUTES || 30),
  depositKeyEncryptionKey: String(process.env.DEPOSIT_KEY_ENCRYPTION_KEY || "").trim(),
  devWalletPrivateKey: parsedDevWallet?.secretKey || null,
  devWalletPublicKey: parsedDevWallet?.publicKey || "",
  treasuryWalletPrivateKey: parsedTreasuryWallet?.secretKey || null,
  treasuryWalletPublicKey: parsedTreasuryWallet?.publicKey || "",
};

if (!config.databaseUrl) {
  console.warn("[config] DATABASE_URL is not set. API/worker startup will fail until provided.");
}

if (!config.rpcUrl) {
  console.warn("[config] SOLANA_RPC_URL is not set. On-chain executors will fail until provided.");
}

if (config.devWalletPublicKey) {
  console.log(`[config] dev wallet loaded (${config.devWalletPublicKey})`);
}

if (config.treasuryWalletPublicKey) {
  console.log(`[config] treasury funding wallet loaded (${config.treasuryWalletPublicKey})`);
  if (
    config.treasuryWallet &&
    config.treasuryWalletPublicKey &&
    config.treasuryWallet !== config.treasuryWalletPublicKey
  ) {
    console.warn(
      `[config] TREASURY_WALLET (${config.treasuryWallet}) does not match TREASURY_WALLET_PRIVATE_KEY pubkey (${config.treasuryWalletPublicKey}).`
    );
  }
}
