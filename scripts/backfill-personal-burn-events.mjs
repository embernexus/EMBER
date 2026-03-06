import bcrypt from "bcryptjs";
import { Connection, PublicKey } from "@solana/web3.js";
import { config } from "../server/config.js";
import { pool } from "../server/db.js";

const INCINERATOR_OWNER = "1nc1nerator11111111111111111111111111111111";
const SYSTEM_USERNAME = "__ember_protocol__";

function toNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function sumOwnerMintUi(entries, owner, mint) {
  if (!Array.isArray(entries)) return 0;
  let total = 0;
  for (const item of entries) {
    if (String(item?.owner || "") !== owner) continue;
    if (String(item?.mint || "") !== mint) continue;
    total += toNumber(item?.uiTokenAmount?.uiAmountString ?? item?.uiTokenAmount?.uiAmount);
  }
  return Math.max(0, total);
}

function fmtAmount(value) {
  return Number(value || 0).toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  });
}

async function getOrCreateSystemUser(client) {
  const existing = await client.query(
    "SELECT id FROM users WHERE username = $1 LIMIT 1",
    [SYSTEM_USERNAME]
  );
  if (existing.rowCount) return Number(existing.rows[0].id);

  const seed = config.treasuryWalletPublicKey || config.devWalletPublicKey || "protocol";
  const hash = await bcrypt.hash(`${SYSTEM_USERNAME}:${seed}`, 10);
  const inserted = await client.query(
    `
      INSERT INTO users (username, password_hash)
      VALUES ($1, $2)
      ON CONFLICT (username) DO NOTHING
      RETURNING id
    `,
    [SYSTEM_USERNAME, hash]
  );
  if (inserted.rowCount) return Number(inserted.rows[0].id);

  const fallback = await client.query(
    "SELECT id FROM users WHERE username = $1 LIMIT 1",
    [SYSTEM_USERNAME]
  );
  if (!fallback.rowCount) {
    throw new Error("Unable to create protocol system user.");
  }
  return Number(fallback.rows[0].id);
}

async function collectSignatures(connection, address, limit) {
  const out = [];
  let before;
  while (out.length < limit) {
    const remaining = Math.max(1, Math.min(1000, limit - out.length));
    const batch = await connection.getSignaturesForAddress(address, {
      limit: Math.min(100, remaining),
      before,
    });
    if (!batch.length) break;
    out.push(...batch);
    before = batch[batch.length - 1]?.signature;
    if (batch.length < 100) break;
  }
  return out;
}

async function backfill() {
  if (!config.rpcUrl) throw new Error("SOLANA_RPC_URL is required.");
  if (!config.devWalletPublicKey) throw new Error("DEV_WALLET_PRIVATE_KEY is required.");
  if (!config.emberTokenMint) throw new Error("EMBER_TOKEN_MINT is required.");

  const limit = Math.max(100, Number(process.env.PERSONAL_BURN_BACKFILL_LIMIT || 1500));
  const connection = new Connection(config.rpcUrl, "confirmed");
  const devWallet = new PublicKey(config.devWalletPublicKey);

  const signatures = await collectSignatures(connection, devWallet, limit);
  if (!signatures.length) {
    console.log("[backfill] no signatures found for dev wallet.");
    return;
  }

  const client = await pool.connect();
  let inserted = 0;
  let scanned = 0;
  try {
    const systemUserId = await getOrCreateSystemUser(client);

    for (const sigInfo of signatures) {
      const signature = String(sigInfo.signature || "").trim();
      if (!signature) continue;
      scanned += 1;

      const tx = await connection.getTransaction(signature, {
        commitment: "confirmed",
        maxSupportedTransactionVersion: 0,
      });
      if (!tx?.meta) continue;

      const preInc = sumOwnerMintUi(tx.meta.preTokenBalances, INCINERATOR_OWNER, config.emberTokenMint);
      const postInc = sumOwnerMintUi(tx.meta.postTokenBalances, INCINERATOR_OWNER, config.emberTokenMint);
      const burned = Math.max(0, postInc - preInc);
      if (burned <= 0.0000001) continue;

      const blockTime = Number(sigInfo.blockTime || 0);
      const values = [
        systemUserId,
        "EMBER",
        "personal_burn",
        "burn",
        burned,
        `Protocol incinerated ${fmtAmount(burned)} EMBER`,
        signature,
        `backfill:personal:burn:${signature}`,
        JSON.stringify({
          backfilled: true,
          source: "chain",
          mint: config.emberTokenMint,
          blockTime: blockTime || null,
        }),
        blockTime,
      ];

      const sql = `
        INSERT INTO token_events (
          user_id, token_id, token_symbol, module_type, event_type, amount, message, tx, idempotency_key, metadata, created_at
        )
        VALUES ($1, NULL, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, COALESCE(to_timestamp(NULLIF($10::bigint, 0)), NOW()))
        ON CONFLICT DO NOTHING
      `;
      const res = await client.query(sql, values);
      inserted += Number(res.rowCount || 0);
    }
  } finally {
    client.release();
  }

  console.log(`[backfill] scanned=${scanned} inserted=${inserted}`);
}

backfill()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error("[backfill] failed:", error?.message || error);
    process.exit(1);
  });
