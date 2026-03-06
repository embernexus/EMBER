import { config } from "../server/config.js";
import { initDb } from "../server/db.js";
import { runWorkerTick } from "../server/core.js";

let busy = false;
let authBackoffMs = 0;
let nextTickAt = 0;

function isDbAuthFailureMessage(input) {
  const msg = String(input || "").toLowerCase();
  return (
    msg.includes("authentication") ||
    msg.includes("circuit breaker open") ||
    msg.includes("too many authentication errors") ||
    msg.includes("password authentication failed")
  );
}

async function tick() {
  if (Date.now() < nextTickAt) return;
  if (busy) return;
  busy = true;
  try {
    const summary = await runWorkerTick();
    authBackoffMs = 0;
    nextTickAt = 0;
    if (summary.eventsCreated > 0) {
      console.log(
        `[worker] due=${summary.dueTokens} events=${summary.eventsCreated} at ${new Date().toISOString()}`
      );
    }
  } catch (error) {
    const message = String(error?.message || error);
    if (isDbAuthFailureMessage(message)) {
      authBackoffMs = authBackoffMs > 0 ? Math.min(60_000, Math.floor(authBackoffMs * 1.8)) : 5_000;
      nextTickAt = Date.now() + authBackoffMs;
      console.error(`[worker] tick failed ${message}; backing off ${authBackoffMs}ms`);
    } else {
      console.error("[worker] tick failed", message);
    }
  } finally {
    busy = false;
  }
}

async function initDbWithRetry() {
  let retryMs = Math.max(3000, Number(process.env.DB_INIT_RETRY_MS || 5000));
  const maxRetryMs = Math.max(retryMs, Number(process.env.DB_INIT_MAX_RETRY_MS || 60000));
  while (true) {
    try {
      await initDb();
      console.log("[worker] database initialized");
      return;
    } catch (error) {
      const message = String(error?.message || error);
      console.warn(`[worker] initDb failed, retrying in ${retryMs}ms: ${message}`);
      await new Promise((resolve) => setTimeout(resolve, retryMs));
      retryMs = Math.min(maxRetryMs, Math.floor(retryMs * 1.5));
    }
  }
}

async function start() {
  console.log(`[worker] running every ${config.workerTickMs}ms`);
  await initDbWithRetry();
  await tick();
  setInterval(tick, config.workerTickMs);
}

start().catch((error) => {
  console.error("[worker] failed to start", error);
  process.exit(1);
});
