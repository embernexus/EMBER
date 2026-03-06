import { config } from "../server/config.js";
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

async function start() {
  console.log(`[worker] running every ${config.workerTickMs}ms`);
  await tick();
  setInterval(tick, config.workerTickMs);
}

start().catch((error) => {
  console.error("[worker] failed to start", error);
  process.exit(1);
});