# EMBER

Production-ready starter for:
- Web app (React/Vite)
- API (Express + PostgreSQL)
- Worker (scheduler loop)

## What is implemented

- Open signup/login with username + password
- Per-account token isolation
- Max burners per account enforced at API/DB layer (`MAX_TOKENS_PER_ACCOUNT`, default `5`)
- Token attach/update flow from website
- Worker process that scans active burners and writes claim/split/burn events
- Dashboard data served from API (no browser-only fake event loop)

## Stack

- Frontend: React + Vite
- Backend: Express
- DB: PostgreSQL (Railway Postgres recommended)
- Worker: Node process

## Local setup

1. Install Node 20.19+ (or 22.12+). Current Vite warns on lower versions.
2. Create `.env` from `.env.example` and set `DATABASE_URL`.
   - `DEV_WALLET_PRIVATE_KEY` accepts either:
     - JSON byte array (example: `[12,34,...]`)
     - base58 encoded 64-byte secret key
3. Install deps:

```bash
npm install
```

4. Run app + API + worker:

```bash
npm run dev
```

- Web: `http://localhost:5173`
- API: `http://localhost:3001`

## Railway deploy

Use 2 Railway services from this same repo:

1. `ember-api`
- Build command: `npm ci && npm run build`
- Start command: `npm run start`
- Variables: copy from `.env.example`, set real values

2. `ember-worker`
- Build command: `npm ci`
- Start command: `npm run start:worker`
- Variables: same DB/config values as API

Attach both to the same Railway Postgres database.

## Provider migration safety (Render -> Railway -> VM)

Use the built-in DB portability scripts before switching providers:

```bash
# On old provider DB
DATABASE_URL=... npm run db:export -- --file ./db-snapshot.json

# On new provider DB
DATABASE_URL=... npm run db:import -- --file ./db-snapshot.json

# Verify row counts
DATABASE_URL=... npm run db:status
```

What this preserves:

- users
- sessions
- tokens
- token deposit signing keys
- token events/history

This avoids provider-specific tooling requirements (`pg_dump`/`pg_restore`) and keeps schema/data consistent across Railway, Render, Supabase, or Oracle VM-hosted Postgres.

### Vanity deposit runtime (EMBR addresses)

Attach flow now generates real Solana vanity deposit addresses (`EMBR...`) and stores the deposit private key server-side.

Environment:

- `SOLANA_KEYGEN_BIN=solana-keygen`
- `DEPOSIT_VANITY_PREFIX=EMBR`
- `DEPOSIT_VANITY_THREADS=4`
- `DEPOSIT_VANITY_TIMEOUT_MS=300000`
- `DEPOSIT_VANITY_ALLOW_JS_FALLBACK=true`
- `DEPOSIT_POOL_TARGET=20`
- `DEPOSIT_POOL_REFILL_INTERVAL_MS=15000`
- `DEPOSIT_POOL_ETA_PER_ADDRESS_SEC=45`
- `DEPOSIT_KEY_ENCRYPTION_KEY=` (recommended for production; 32-byte key in hex-64 or base64)

Notes:

- Primary path uses `solana-keygen grind`.
- If CLI is missing/fails and `DEPOSIT_VANITY_ALLOW_JS_FALLBACK=true`, API falls back to JS keypair grinding.
- Private keys in `token_deposit_keys.secret_key_base58` are encrypted at rest when `DEPOSIT_KEY_ENCRYPTION_KEY` is set.
- Pool preloads and maintains up to `DEPOSIT_POOL_TARGET` addresses (hard-capped at 20 in code).
- When pool is temporarily empty, API returns a warm-up message with estimated time remaining.

## Real autoburner integration point

Current worker logic is in `server/core.js` (`runWorkerTick`) and is simulation-safe.
When you send your existing autoburner files, replace the mocked claim/buy/burn operations there with your real Solana transaction flow.

## API routes

- `POST /api/auth/register`
- `POST /api/auth/login`
- `POST /api/auth/logout`
- `GET /api/auth/me`
- `GET /api/dashboard`
- `POST /api/tokens`
- `PATCH /api/tokens/:id`

## Notes

- Sessions are HTTP-only cookies.
- DB schema is auto-created on API/worker startup (`server/db.js`).
- Telegram features are intentionally not included.
