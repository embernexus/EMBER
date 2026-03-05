import crypto from "node:crypto";
import dns from "node:dns";
import { Pool } from "pg";
import { config } from "./config.js";

if (!config.databaseUrl) {
  throw new Error("DATABASE_URL is required.");
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


export const pool = new Pool({
  connectionString: config.databaseUrl,
  ssl: process.env.PGSSLMODE === "disable" ? false : { rejectUnauthorized: false },
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

export async function initDb() {
  const lockClient = await pool.connect();
  const initLockKey = "884420002777";
  try {
    await lockClient.query("SELECT pg_advisory_lock($1::bigint)", [initLockKey]);

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
  } finally {
    await lockClient.query("SELECT pg_advisory_unlock($1::bigint)", [initLockKey]).catch(() => {});
    lockClient.release();
  }
}
