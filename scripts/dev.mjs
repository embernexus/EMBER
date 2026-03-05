import { spawn } from "node:child_process";
import process from "node:process";

const API_PORT = Number(process.env.PORT || 3002);
const API_HEALTH_URL = `http://127.0.0.1:${API_PORT}/api/health`;
const API_BOOT_TIMEOUT_MS = 45_000;
const API_POLL_MS = 400;

const children = [];

function prefixAndPipe(stream, tag) {
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

function spawnNpmScript(script, tag) {
  const isWindows = process.platform === "win32";
  const child = spawn(isWindows ? "npm.cmd" : "npm", ["run", script], {
    stdio: ["inherit", "pipe", "pipe"],
    env: process.env,
  });
  children.push(child);

  prefixAndPipe(child.stdout, tag);
  prefixAndPipe(child.stderr, tag);

  child.on("exit", (code, signal) => {
    if (shuttingDown) return;
    const reason = signal ? `signal ${signal}` : `code ${code}`;
    console.error(`[dev] ${tag} exited (${reason}). Shutting down all processes.`);
    shutdown(code === 0 ? 1 : code || 1);
  });

  return child;
}

async function waitForApiReady(timeoutMs) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const res = await fetch(API_HEALTH_URL, { method: "GET" });
      if (res.ok) return true;
    } catch {
      // API not ready yet.
    }
    await new Promise((resolve) => setTimeout(resolve, API_POLL_MS));
  }
  return false;
}

let shuttingDown = false;
function shutdown(exitCode = 0) {
  if (shuttingDown) return;
  shuttingDown = true;
  for (const child of children) {
    try {
      child.kill("SIGTERM");
    } catch {
      // ignore child shutdown errors
    }
  }
  setTimeout(() => {
    for (const child of children) {
      try {
        child.kill("SIGKILL");
      } catch {
        // ignore child force-kill errors
      }
    }
    process.exit(exitCode);
  }, 800);
}

process.on("SIGINT", () => shutdown(0));
process.on("SIGTERM", () => shutdown(0));

spawnNpmScript("dev:api", "api");
spawnNpmScript("dev:worker", "worker");

const ready = await waitForApiReady(API_BOOT_TIMEOUT_MS);
if (!ready) {
  console.error(`[dev] API was not ready at ${API_HEALTH_URL} within ${API_BOOT_TIMEOUT_MS}ms.`);
  shutdown(1);
} else {
  console.log(`[dev] API ready at ${API_HEALTH_URL}. Starting web...`);
  spawnNpmScript("dev:web", "web");
}
