import { config } from "../server/config.js";
import { runWorkerTick } from "../server/core.js";
import { initDb } from "../server/db.js";

let busy = false;

async function tick() {
  if (busy) return;
  busy = true;
  try {
    const summary = await runWorkerTick();
    if (summary.eventsCreated > 0) {
      console.log(
        `[worker] due=${summary.dueTokens} events=${summary.eventsCreated} at ${new Date().toISOString()}`
      );
    }
  } catch (error) {
    console.error("[worker] tick failed", error.message);
  } finally {
    busy = false;
  }
}

async function start() {
  const retryMs = Math.max(3000, Number(process.env.DB_INIT_RETRY_MS || 5000));
  while (true) {
    try {
      await initDb();
      break;
    } catch (error) {
      console.warn(`[worker] initDb failed, retrying in ${retryMs}ms: ${error?.message || error}`);
      await new Promise((resolve) => setTimeout(resolve, retryMs));
    }
  }
  console.log(`[worker] running every ${config.workerTickMs}ms`);
  await tick();
  setInterval(tick, config.workerTickMs);
}

start().catch((error) => {
  console.error("[worker] failed to start", error);
  process.exit(1);
});
