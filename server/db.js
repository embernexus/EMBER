import crypto from "node:crypto";
import dns from "node:dns";
import { Pool } from "pg";
import { config } from "./config.js";

if (!config.databaseUrl) {
  throw new Error("DATABASE_URL is required.");
}
if (!/^postgres(ql)?:\/\//i.test(config.databaseUrl)) {
  throw new Error("DATABASE_URL must be a valid postgres:// or postgresql:// URL.");
}
if (/[<>]/.test(config.databaseUrl)) {
  throw new Error("DATABASE_URL still contains placeholder markers (< >). Use your real DB URL.");
}

try {
  // Prefer IPv4 first to avoid ENETUNREACH when host IPv6 egress is unavailable.
  const order =
    String(process.env.DNS_RESULT_ORDER || "").trim().toLowerCase() === "verbatim"
      ? "verbatim"
      : "ipv4first";
  dns.setDefaultResultOrder(order);
} catch {
  // best effort
}

let resolvedDatabaseUrl = config.databaseUrl;
try {
  const forceIpv4 = String(process.env.DB_FORCE_IPV4 || "true").toLowerCase() !== "false";
  if (forceIpv4 && resolvedDatabaseUrl) {
    const parsed = new URL(resolvedDatabaseUrl);
    const host = String(parsed.hostname || "").trim();
    const isIpv4 = /^\d{1,3}(\.\d{1,3}){3}$/.test(host);
    const looksLikeSupabaseDirectHost =
      host.startsWith("db.") && host.endsWith(".supabase.co");
    const looksLikeSupabasePoolerHost = host.endsWith(".pooler.supabase.com");
    const fallbackPoolerHost = String(
      process.env.SUPABASE_POOLER_HOST || "aws-0-us-west-2.pooler.supabase.com"
    ).trim();
    const fallbackPoolerPort = String(process.env.SUPABASE_POOLER_PORT || "6543").trim();
    if (looksLikeSupabaseDirectHost) {
      if (fallbackPoolerHost) {
        parsed.hostname = fallbackPoolerHost;
        if (!parsed.port || parsed.port === "5432") parsed.port = fallbackPoolerPort;
        resolvedDatabaseUrl = parsed.toString();
        console.warn(
          `[db] swapped Supabase direct host (${host}) for pooler (${parsed.hostname}:${parsed.port})`
        );
      }
    }
    if (looksLikeSupabasePoolerHost && (!parsed.port || parsed.port === "5432")) {
      parsed.port = fallbackPoolerPort || "6543";
      resolvedDatabaseUrl = parsed.toString();
      console.warn(`[db] adjusted Supabase pooler port to ${parsed.port} for ${host}`);
    }
    if (looksLikeSupabasePoolerHost) {
      const user = decodeURIComponent(String(parsed.username || ""));
      if (user && user === "postgres") {
        console.warn(
          "[db] Supabase pooler usually requires username format 'postgres.<project-ref>' (not plain 'postgres')."
        );
      }
    }
    if (host && !isIpv4) {
      const lookupHost = String(new URL(resolvedDatabaseUrl).hostname || "").trim();
      const resolved = await dns.promises.lookup(lookupHost, { family: 4 });
      if (resolved?.address) {
        const resolvedParsed = new URL(resolvedDatabaseUrl);
        resolvedParsed.hostname = resolved.address;
        resolvedDatabaseUrl = resolvedParsed.toString();
        console.log(`[db] resolved ${lookupHost} -> ${resolved.address} (ipv4)`);
      }
    }
  }
} catch (error) {
  console.warn(`[db] ipv4 resolution skipped: ${error?.message || error}`);
}

let poolConnectionString = resolvedDatabaseUrl;
try {
  const parsed = new URL(poolConnectionString);
  const hadSslMode = parsed.searchParams.has("sslmode");
  const hadLibpqCompat = parsed.searchParams.has("uselibpqcompat");
  if (hadSslMode) parsed.searchParams.delete("sslmode");
  if (hadLibpqCompat) parsed.searchParams.delete("uselibpqcompat");
  poolConnectionString = parsed.toString();
  if (hadSslMode || hadLibpqCompat) {
    console.warn("[db] stripped sslmode/uselibpqcompat from DATABASE_URL; using pg ssl config instead");
  }
} catch {
  // best effort
}
try {
  const parsed = new URL(poolConnectionString);
  const safeUser = decodeURIComponent(String(parsed.username || ""));
  const safeHost = String(parsed.hostname || "");
  const safePort = String(parsed.port || "5432");
  console.log(`[db] target ${safeUser || "<no-user>"}@${safeHost}:${safePort}`);
} catch {
  // best effort
}

const poolMax = Math.max(1, Number(process.env.PG_POOL_MAX || 4));
const poolMin = Math.max(0, Math.min(poolMax - 1, Number(process.env.PG_POOL_MIN || 0)));
const connectTimeoutMs = Math.max(3000, Number(process.env.PG_CONNECT_TIMEOUT_MS || 10000));
const idleTimeoutMs = Math.max(5000, Number(process.env.PG_IDLE_TIMEOUT_MS || 30000));
const keepAliveInitialDelayMs = Math.max(
  1000,
  Number(process.env.PG_KEEPALIVE_INITIAL_DELAY_MS || 10000)
);

export const pool = new Pool({
  connectionString: poolConnectionString,
  ssl: process.env.PGSSLMODE === "disable" ? false : { rejectUnauthorized: false },
  max: poolMax,
  min: poolMin,
  connectionTimeoutMillis: connectTimeoutMs,
  idleTimeoutMillis: idleTimeoutMs,
  keepAlive: true,
  keepAliveInitialDelayMillis: keepAliveInitialDelayMs,
  statement_timeout: 0,
  query_timeout: 0,
  idle_in_transaction_session_timeout: 0,
});

export async function withTx(fn) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await fn(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export function makeId(prefix = "id") {
  return `${prefix}_${crypto.randomBytes(8).toString("hex")}`;
}

export function makeSessionToken() {
  return crypto.randomBytes(32).toString("hex");
}

const requiredInitTables = [
  "users",
  "tokens",
  "bot_modules",
  "user_access_grants",
  "user_telegram_alert_queue",
  "deploy_wallet_reservations",
];

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function areRequiredTablesPresent() {
  const names = requiredInitTables.map((table) => `public.${table}`);
  const res = await pool.query("SELECT to_regclass(name) AS regclass FROM unnest($1::text[]) AS name", [names]);
  return res.rows.every((row) => Boolean(row.regclass));
}

async function waitForRequiredTables(timeoutMs = 45000, pollMs = 1000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      if (await areRequiredTablesPresent()) return true;
    } catch {
      // best effort while another process is still initializing
    }
    await wait(pollMs);
  }
  return false;
}

export async function initDb() {
  const lockClient = await pool.connect();
  const initLockKey = "884420002777";
  let acquired = false;
  try {
    const lockRes = await lockClient.query("SELECT pg_try_advisory_lock($1::bigint) AS ok", [initLockKey]);
    acquired = Boolean(lockRes.rows[0]?.ok);
    if (!acquired) {
      console.log("[db] init already running in another process; waiting for schema");
      const ready = await waitForRequiredTables();
      if (!ready) {
        throw new Error("database init wait timed out before required tables were ready");
      }
      return;
    }

  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id BIGSERIAL PRIMARY KEY,
      username TEXT NOT NULL UNIQUE,
      password_hash TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS sessions (
      token TEXT PRIMARY KEY,
      user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      expires_at TIMESTAMPTZ NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS user_access_grants (
      id BIGSERIAL PRIMARY KEY,
      owner_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      grantee_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      role TEXT NOT NULL DEFAULT 'manager',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (owner_user_id),
      UNIQUE (grantee_user_id),
      CHECK (owner_user_id <> grantee_user_id)
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_user_access_grants_owner
    ON user_access_grants(owner_user_id);
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS tokens (
      id TEXT PRIMARY KEY,
      user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      symbol TEXT NOT NULL,
      name TEXT NOT NULL,
      mint TEXT NOT NULL,
      picture_url TEXT,
      deposit TEXT NOT NULL,
      claim_sec INTEGER NOT NULL,
      burn_sec INTEGER NOT NULL,
      splits INTEGER NOT NULL,
      selected_bot TEXT NOT NULL DEFAULT 'burn',
      active BOOLEAN NOT NULL DEFAULT FALSE,
      disconnected BOOLEAN NOT NULL DEFAULT FALSE,
      burned BIGINT NOT NULL DEFAULT 0,
      pending BIGINT NOT NULL DEFAULT 0,
      tx_count BIGINT NOT NULL DEFAULT 0,
      next_claim_at TIMESTAMPTZ,
      next_burn_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (user_id, mint)
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_tokens_user_id ON tokens(user_id);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_tokens_active_next ON tokens(active, next_claim_at, next_burn_at);
  `);

  await pool.query(`
    ALTER TABLE tokens
    ADD COLUMN IF NOT EXISTS selected_bot TEXT NOT NULL DEFAULT 'burn';
  `);

  await pool.query(`
    ALTER TABLE tokens
    ADD COLUMN IF NOT EXISTS disconnected BOOLEAN NOT NULL DEFAULT FALSE;
  `);

  await pool.query(`
    ALTER TABLE tokens
    ADD COLUMN IF NOT EXISTS deployed_via_ember BOOLEAN NOT NULL DEFAULT FALSE;
  `);

  await pool.query(`
    ALTER TABLE tokens
    ADD COLUMN IF NOT EXISTS deploy_wallet_pubkey TEXT;
  `);

  await pool.query(`
    ALTER TABLE tokens
    ADD COLUMN IF NOT EXISTS deploy_wallet_secret_key_base58 TEXT;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS token_deposit_keys (
      token_id TEXT PRIMARY KEY REFERENCES tokens(id) ON DELETE CASCADE,
      user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      deposit_pubkey TEXT NOT NULL UNIQUE,
      secret_key_base58 TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_token_deposit_keys_user_id ON token_deposit_keys(user_id);
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS token_deposit_pool (
      id BIGSERIAL PRIMARY KEY,
      deposit_pubkey TEXT NOT NULL UNIQUE,
      secret_key_base58 TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'available',
      reservation_id TEXT UNIQUE,
      reserved_user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
      reserved_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_token_deposit_pool_status ON token_deposit_pool(status);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_token_deposit_pool_reserved_user_id ON token_deposit_pool(reserved_user_id);
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS deploy_wallet_reservations (
      id TEXT PRIMARY KEY,
      user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
      deposit_pubkey TEXT NOT NULL UNIQUE,
      secret_key_base58 TEXT NOT NULL,
      required_lamports BIGINT NOT NULL,
      balance_lamports BIGINT NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'reserved',
      last_error TEXT,
      expires_at TIMESTAMPTZ NOT NULL,
      deployed_mint TEXT,
      deploy_signature TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_deploy_wallet_reservations_status
    ON deploy_wallet_reservations(status, created_at DESC);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_deploy_wallet_reservations_user
    ON deploy_wallet_reservations(user_id, created_at DESC);
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS token_events (
      id BIGSERIAL PRIMARY KEY,
      user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      token_id TEXT REFERENCES tokens(id) ON DELETE SET NULL,
      token_symbol TEXT,
      module_type TEXT,
      event_type TEXT NOT NULL,
      amount NUMERIC NOT NULL DEFAULT 0,
      message TEXT NOT NULL,
      tx TEXT,
      idempotency_key TEXT,
      metadata JSONB,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    ALTER TABLE token_events
    ADD COLUMN IF NOT EXISTS amount NUMERIC NOT NULL DEFAULT 0;
  `);

  await pool.query(`
    ALTER TABLE token_events
    ADD COLUMN IF NOT EXISTS module_type TEXT;
  `);

  await pool.query(`
    ALTER TABLE token_events
    ADD COLUMN IF NOT EXISTS idempotency_key TEXT;
  `);

  await pool.query(`
    ALTER TABLE token_events
    ADD COLUMN IF NOT EXISTS metadata JSONB;
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_token_events_user_created ON token_events(user_id, created_at DESC);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_token_events_token_created ON token_events(token_id, created_at DESC);
  `);

  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS ux_token_events_idempotency
    ON token_events(idempotency_key)
    WHERE idempotency_key IS NOT NULL;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS protocol_metrics (
      id SMALLINT PRIMARY KEY,
      total_bot_transactions BIGINT NOT NULL DEFAULT 0,
      lifetime_incinerated NUMERIC NOT NULL DEFAULT 0,
      ember_incinerated NUMERIC NOT NULL DEFAULT 0,
      rewards_processed_sol NUMERIC NOT NULL DEFAULT 0,
      fees_taken_sol NUMERIC NOT NULL DEFAULT 0,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    INSERT INTO protocol_metrics (
      id,
      total_bot_transactions,
      lifetime_incinerated,
      ember_incinerated,
      rewards_processed_sol,
      fees_taken_sol
    )
    VALUES (1, 0, 0, 0, 0, 0)
    ON CONFLICT (id) DO NOTHING;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS user_telegram_links (
      user_id BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
      chat_id BIGINT NOT NULL UNIQUE,
      telegram_username TEXT,
      first_name TEXT,
      last_name TEXT,
      is_connected BOOLEAN NOT NULL DEFAULT TRUE,
      connected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS user_telegram_connect_tokens (
      token TEXT PRIMARY KEY,
      user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      expires_at TIMESTAMPTZ NOT NULL,
      consumed_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_user_telegram_connect_tokens_user
    ON user_telegram_connect_tokens(user_id, expires_at DESC);
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS user_telegram_alert_prefs (
      user_id BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
      enabled BOOLEAN NOT NULL DEFAULT FALSE,
      delivery_mode TEXT NOT NULL DEFAULT 'smart',
      digest_interval_min INTEGER NOT NULL DEFAULT 15,
      alert_deposit BOOLEAN NOT NULL DEFAULT TRUE,
      alert_claim BOOLEAN NOT NULL DEFAULT TRUE,
      alert_burn BOOLEAN NOT NULL DEFAULT TRUE,
      alert_trade BOOLEAN NOT NULL DEFAULT FALSE,
      alert_error BOOLEAN NOT NULL DEFAULT TRUE,
      alert_status BOOLEAN NOT NULL DEFAULT TRUE,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS user_telegram_alert_queue (
      id BIGSERIAL PRIMARY KEY,
      user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      owner_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      token_id TEXT,
      token_symbol TEXT,
      module_type TEXT,
      event_type TEXT NOT NULL,
      amount NUMERIC NOT NULL DEFAULT 0,
      message TEXT NOT NULL,
      tx TEXT,
      delivery_kind TEXT NOT NULL,
      digest_key TEXT,
      status TEXT NOT NULL DEFAULT 'pending',
      attempts INTEGER NOT NULL DEFAULT 0,
      scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      sent_at TIMESTAMPTZ,
      error TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_user_telegram_alert_queue_due
    ON user_telegram_alert_queue(status, scheduled_at, delivery_kind);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_user_telegram_alert_queue_digest
    ON user_telegram_alert_queue(user_id, digest_key, status);
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS bot_modules (
      id TEXT PRIMARY KEY,
      user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      token_id TEXT NOT NULL REFERENCES tokens(id) ON DELETE CASCADE,
      module_type TEXT NOT NULL,
      enabled BOOLEAN NOT NULL DEFAULT FALSE,
      config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      state_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      next_run_at TIMESTAMPTZ,
      last_run_at TIMESTAMPTZ,
      last_error TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (token_id, module_type)
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_bot_modules_due
    ON bot_modules(enabled, next_run_at);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_bot_modules_user
    ON bot_modules(user_id, module_type);
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS bot_jobs (
      id TEXT PRIMARY KEY,
      module_id TEXT NOT NULL REFERENCES bot_modules(id) ON DELETE CASCADE,
      user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      token_id TEXT NOT NULL REFERENCES tokens(id) ON DELETE CASCADE,
      module_type TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'queued',
      run_after TIMESTAMPTZ NOT NULL,
      lease_until TIMESTAMPTZ,
      attempts INTEGER NOT NULL DEFAULT 0,
      max_attempts INTEGER NOT NULL DEFAULT 5,
      priority INTEGER NOT NULL DEFAULT 100,
      idempotency_key TEXT NOT NULL,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      result_json JSONB,
      error TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (idempotency_key)
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_bot_jobs_queue
    ON bot_jobs(status, run_after, priority, created_at);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_bot_jobs_module
    ON bot_jobs(module_id, status);
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS volume_trade_wallets (
      id TEXT PRIMARY KEY,
      module_id TEXT NOT NULL REFERENCES bot_modules(id) ON DELETE CASCADE,
      user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      token_id TEXT NOT NULL REFERENCES tokens(id) ON DELETE CASCADE,
      label TEXT NOT NULL,
      wallet_pubkey TEXT NOT NULL UNIQUE,
      secret_key_base58 TEXT NOT NULL,
      funded_from_deposit_lamports BIGINT NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_volume_trade_wallets_module
    ON volume_trade_wallets(module_id);
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS token_funding_sources (
      id TEXT PRIMARY KEY,
      user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      token_id TEXT NOT NULL REFERENCES tokens(id) ON DELETE CASCADE,
      source_wallet TEXT NOT NULL,
      total_lamports BIGINT NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (token_id, source_wallet)
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_token_funding_sources_token
    ON token_funding_sources(token_id);
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS protocol_fee_credits (
      id BIGSERIAL PRIMARY KEY,
      source_token_id TEXT REFERENCES tokens(id) ON DELETE SET NULL,
      source_user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
      source_token_symbol TEXT NOT NULL,
      total_lamports BIGINT NOT NULL DEFAULT 0,
      pending_lamports BIGINT NOT NULL DEFAULT 0,
      spent_lamports BIGINT NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS ux_protocol_fee_credits_source_token
    ON protocol_fee_credits(source_token_id)
    WHERE source_token_id IS NOT NULL;
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_protocol_fee_credits_pending
    ON protocol_fee_credits(pending_lamports DESC, updated_at ASC);
  `);

  await pool.query(`
    CREATE OR REPLACE FUNCTION set_updated_at()
    RETURNS TRIGGER AS $$
    BEGIN
      NEW.updated_at = NOW();
      RETURN NEW;
    END;
    $$ language 'plpgsql';
  `);

  await pool.query(`
    CREATE OR REPLACE TRIGGER trg_tokens_updated_at
    BEFORE UPDATE ON tokens
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
  `);

  await pool.query(`
    CREATE OR REPLACE TRIGGER trg_bot_modules_updated_at
    BEFORE UPDATE ON bot_modules
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
  `);

  await pool.query(`
    CREATE OR REPLACE TRIGGER trg_bot_jobs_updated_at
    BEFORE UPDATE ON bot_jobs
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
  `);

  await pool.query(`
    CREATE OR REPLACE TRIGGER trg_volume_trade_wallets_updated_at
    BEFORE UPDATE ON volume_trade_wallets
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
  `);

  await pool.query(`
    CREATE OR REPLACE TRIGGER trg_token_funding_sources_updated_at
    BEFORE UPDATE ON token_funding_sources
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
  `);

  await pool.query(`
    CREATE OR REPLACE TRIGGER trg_protocol_fee_credits_updated_at
    BEFORE UPDATE ON protocol_fee_credits
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
  `);

  await pool.query(`
    CREATE OR REPLACE TRIGGER trg_deploy_wallet_reservations_updated_at
    BEFORE UPDATE ON deploy_wallet_reservations
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
  `);
  } finally {
    if (acquired) {
      await lockClient.query("SELECT pg_advisory_unlock($1::bigint)", [initLockKey]).catch(() => {});
    }
    lockClient.release();
  }
}
