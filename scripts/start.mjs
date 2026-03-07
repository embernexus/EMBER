import { spawn } from "node:child_process";
import process from "node:process";

const isWindows = process.platform === "win32";
const npmBin = isWindows ? "npm.cmd" : "npm";
const apiPort = Math.max(1, Number(process.env.PORT || 3002));
const apiReadyUrl = `http://127.0.0.1:${apiPort}/api/ready`;

let shuttingDown = false;
let apiChild = null;
let workerChild = null;
let workerRestartTimer = null;
let workerStarted = false;

function pipeWithPrefix(stream, tag) {
  if (!stream) return;
  stream.on("data", (chunk) => {
    const text = String(chunk || "");
    if (!text) return;
    const lines = text.split(/\r?\n/);
    for (const line of lines) {
      if (!line) continue;
      process.stdout.write(`[${tag}] ${line}\n`);
    }
  });
}

function spawnNpm(script, tag) {
  const child = spawn(npmBin, ["run", script], {
    stdio: ["inherit", "pipe", "pipe"],
    env: process.env,
  });
  pipeWithPrefix(child.stdout, tag);
  pipeWithPrefix(child.stderr, tag);
  return child;
}

function stopChild(child) {
  if (!child) return;
  try {
    child.kill("SIGTERM");
  } catch {
    // ignore
  }
}

function shutdown(exitCode = 0) {
  if (shuttingDown) return;
  shuttingDown = true;
  if (workerRestartTimer) {
    clearTimeout(workerRestartTimer);
    workerRestartTimer = null;
  }
  stopChild(workerChild);
  stopChild(apiChild);
  setTimeout(() => process.exit(exitCode), 800);
}

async function waitForApiReady(timeoutMs = 180_000) {
  const startedAt = Date.now();
  while (!shuttingDown && Date.now() - startedAt < timeoutMs) {
    try {
      const res = await fetch(apiReadyUrl, { method: "GET" });
      if (res.ok) return true;
    } catch {
      // keep polling
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  return false;
}

function startWorker() {
  if (shuttingDown) return;
  workerStarted = true;
  workerChild = spawnNpm("start:worker", "worker");
  workerChild.on("exit", (code, signal) => {
    if (shuttingDown) return;
    const reason = signal ? `signal ${signal}` : `code ${code}`;
    console.error(`[start] worker exited (${reason}). Restarting in 5s...`);
    workerRestartTimer = setTimeout(() => {
      workerRestartTimer = null;
      startWorker();
    }, 5000);
  });
}

process.on("SIGINT", () => shutdown(0));
process.on("SIGTERM", () => shutdown(0));

apiChild = spawnNpm("start:api", "api");
apiChild.on("exit", (code, signal) => {
  if (shuttingDown) return;
  const reason = signal ? `signal ${signal}` : `code ${code}`;
  console.error(`[start] api exited (${reason}). Shutting down.`);
  shutdown(code === 0 ? 1 : code || 1);
});

void (async () => {
  const ready = await waitForApiReady();
  if (shuttingDown) return;
  if (!ready) {
    console.error(`[start] api was not ready at ${apiReadyUrl} within 180000ms. Shutting down.`);
    shutdown(1);
    return;
  }
  console.log(`[start] api ready at ${apiReadyUrl}. Starting worker...`);
  if (!workerStarted) startWorker();
})();
