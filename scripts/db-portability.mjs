import fs from "node:fs/promises";
import path from "node:path";
import dotenv from "dotenv";
import { initDb, pool } from "../server/db.js";

dotenv.config();

const TABLES = [
  { name: "users", orderBy: "id" },
  { name: "sessions", orderBy: "created_at" },
  { name: "tokens", orderBy: "created_at" },
  { name: "token_deposit_keys", orderBy: "created_at" },
  { name: "token_deposit_pool", orderBy: "id" },
  { name: "token_events", orderBy: "id" },
];

function argValue(flag) {
  const idx = process.argv.findIndex((v) => v === flag);
  if (idx === -1) return "";
  return String(process.argv[idx + 1] || "").trim();
}

function nowStamp() {
  const d = new Date();
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const mi = String(d.getUTCMinutes()).padStart(2, "0");
  const ss = String(d.getUTCSeconds()).padStart(2, "0");
  return `${yyyy}${mm}${dd}-${hh}${mi}${ss}Z`;
}

async function exportSnapshot(filePathArg) {
  await initDb();
  const filePath = filePathArg || path.resolve(process.cwd(), `db-snapshot-${nowStamp()}.json`);

  const data = {
    exportedAt: new Date().toISOString(),
    schemaVersion: 1,
    tables: {},
  };

  for (const table of TABLES) {
    const res = await pool.query(`SELECT * FROM ${table.name} ORDER BY ${table.orderBy}`);
    data.tables[table.name] = res.rows;
  }

  await fs.writeFile(filePath, JSON.stringify(data, null, 2), "utf8");
  console.log(`[db-portability] snapshot exported: ${filePath}`);
}

async function importSnapshot(filePathArg) {
  await initDb();
  const filePath = filePathArg || path.resolve(process.cwd(), "db-snapshot.json");
  const raw = await fs.readFile(filePath, "utf8");
  const parsed = JSON.parse(raw);
  const tables = parsed?.tables || {};

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    await client.query("TRUNCATE TABLE token_events RESTART IDENTITY CASCADE");
    await client.query("TRUNCATE TABLE token_deposit_pool RESTART IDENTITY CASCADE");
    await client.query("TRUNCATE TABLE token_deposit_keys RESTART IDENTITY CASCADE");
    await client.query("TRUNCATE TABLE tokens RESTART IDENTITY CASCADE");
    await client.query("TRUNCATE TABLE sessions RESTART IDENTITY CASCADE");
    await client.query("TRUNCATE TABLE users RESTART IDENTITY CASCADE");

    for (const row of tables.users || []) {
      await client.query(
        `
          INSERT INTO users (id, username, password_hash, created_at)
          VALUES ($1, $2, $3, $4)
        `,
        [row.id, row.username, row.password_hash, row.created_at]
      );
    }

    for (const row of tables.sessions || []) {
      await client.query(
        `
          INSERT INTO sessions (token, user_id, expires_at, created_at)
          VALUES ($1, $2, $3, $4)
        `,
        [row.token, row.user_id, row.expires_at, row.created_at]
      );
    }

    for (const row of tables.tokens || []) {
      await client.query(
        `
          INSERT INTO tokens (
            id, user_id, symbol, name, mint, picture_url, deposit,
            claim_sec, burn_sec, splits, active, burned, pending, tx_count,
            next_claim_at, next_burn_at, created_at, updated_at
          )
          VALUES (
            $1, $2, $3, $4, $5, $6, $7,
            $8, $9, $10, $11, $12, $13, $14,
            $15, $16, $17, $18
          )
        `,
        [
          row.id,
          row.user_id,
          row.symbol,
          row.name,
          row.mint,
          row.picture_url || null,
          row.deposit,
          row.claim_sec,
          row.burn_sec,
          row.splits,
          row.active,
          row.burned,
          row.pending,
          row.tx_count,
          row.next_claim_at,
          row.next_burn_at,
          row.created_at,
          row.updated_at,
        ]
      );
    }

    for (const row of tables.token_deposit_keys || []) {
      await client.query(
        `
          INSERT INTO token_deposit_keys (token_id, user_id, deposit_pubkey, secret_key_base58, created_at)
          VALUES ($1, $2, $3, $4, $5)
        `,
        [row.token_id, row.user_id, row.deposit_pubkey, row.secret_key_base58, row.created_at]
      );
    }

    for (const row of tables.token_deposit_pool || []) {
      await client.query(
        `
          INSERT INTO token_deposit_pool (
            id, deposit_pubkey, secret_key_base58, status,
            reservation_id, reserved_user_id, reserved_at, created_at
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        `,
        [
          row.id,
          row.deposit_pubkey,
          row.secret_key_base58,
          row.status,
          row.reservation_id || null,
          row.reserved_user_id || null,
          row.reserved_at || null,
          row.created_at,
        ]
      );
    }

    for (const row of tables.token_events || []) {
      await client.query(
        `
          INSERT INTO token_events (id, user_id, token_id, token_symbol, event_type, amount, message, tx, created_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `,
        [
          row.id,
          row.user_id,
          row.token_id,
          row.token_symbol,
          row.event_type,
          row.amount ?? 0,
          row.message,
          row.tx || null,
          row.created_at,
        ]
      );
    }

    await client.query(`
      SELECT setval(
        pg_get_serial_sequence('users', 'id'),
        COALESCE((SELECT MAX(id)::bigint FROM users), 1),
        (SELECT COUNT(*) > 0 FROM users)
      )
    `);

    await client.query(`
      SELECT setval(
        pg_get_serial_sequence('token_deposit_pool', 'id'),
        COALESCE((SELECT MAX(id)::bigint FROM token_deposit_pool), 1),
        (SELECT COUNT(*) > 0 FROM token_deposit_pool)
      )
    `);

    await client.query(`
      SELECT setval(
        pg_get_serial_sequence('token_events', 'id'),
        COALESCE((SELECT MAX(id)::bigint FROM token_events), 1),
        (SELECT COUNT(*) > 0 FROM token_events)
      )
    `);

    await client.query("COMMIT");
    console.log(`[db-portability] snapshot imported: ${filePath}`);
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function status() {
  await initDb();
  for (const table of TABLES) {
    const res = await pool.query(`SELECT COUNT(*)::bigint AS c FROM ${table.name}`);
    console.log(`${table.name}: ${res.rows[0].c}`);
  }
}

async function main() {
  const cmd = String(process.argv[2] || "").trim().toLowerCase();
  const file = argValue("--file");

  try {
    if (cmd === "export") {
      await exportSnapshot(file);
    } else if (cmd === "import") {
      await importSnapshot(file);
    } else if (cmd === "status") {
      await status();
    } else {
      console.log("Usage:");
      console.log("  node scripts/db-portability.mjs export --file <snapshot.json>");
      console.log("  node scripts/db-portability.mjs import --file <snapshot.json>");
      console.log("  node scripts/db-portability.mjs status");
      process.exitCode = 1;
    }
  } finally {
    await pool.end().catch(() => {});
  }
}

main().catch((error) => {
  console.error("[db-portability] failed:", error?.message || error);
  process.exit(1);
});
