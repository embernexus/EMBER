import { config } from "../server/config.js";
import { runWorkerTick } from "../server/core.js";

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
  console.log(`[worker] running every ${config.workerTickMs}ms`);
  await tick();
  setInterval(tick, config.workerTickMs);
}

start().catch((error) => {
  console.error("[worker] failed to start", error);
  process.exit(1);
});
