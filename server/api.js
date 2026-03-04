import cookieParser from "cookie-parser";
import cors from "cors";
import express from "express";
import path from "node:path";
import {
  attachToken,
  clearSession,
  cookieMaxAgeMs,
  deleteToken,
  getDashboard,
  deployToken,
  ensureDepositPool,
  generatePendingDepositAddress,
  getTokenLiveDetails,
  getVolumeWithdrawOptions,
  recordDeployFromChain,
  getPublicMetrics,
  getUserBySession,
  loginUser,
  resolveMintMetadata,
  registerUser,
  sweepVolumeWallets,
  updateToken,
  withdrawVolumeFunds,
} from "./core.js";
import { config } from "./config.js";
import { initDb } from "./db.js";

const app = express();

app.use(express.json({ limit: "12mb" }));
app.use(cookieParser());
app.use(
  cors({
    origin: true,
    credentials: true,
  })
);

const secureCookie = process.env.COOKIE_SECURE === "true" || process.env.NODE_ENV === "production";
const cookieSameSite = process.env.COOKIE_SAME_SITE || "lax";

function readToken(req) {
  return req.cookies[config.sessionCookieName] || null;
}

function setSessionCookie(res, token) {
  res.cookie(config.sessionCookieName, token, {
    httpOnly: true,
    secure: secureCookie,
    sameSite: cookieSameSite,
    path: "/",
    maxAge: cookieMaxAgeMs(),
  });
}

function clearSessionCookie(res) {
  res.clearCookie(config.sessionCookieName, {
    httpOnly: true,
    secure: secureCookie,
    sameSite: cookieSameSite,
    path: "/",
  });
}

async function authOptional(req, _res, next) {
  try {
    const token = readToken(req);
    req.user = token ? await getUserBySession(token) : null;
    next();
  } catch (error) {
    next(error);
  }
}

async function authRequired(req, res, next) {
  try {
    const token = readToken(req);
    const user = token ? await getUserBySession(token) : null;
    if (!user) {
      return res.status(401).json({ error: "Unauthorized" });
    }
    req.user = user;
    return next();
  } catch (error) {
    return next(error);
  }
}

app.get("/api/health", (_req, res) => {
  res.json({ ok: true });
});

app.get("/api/public-metrics", async (_req, res, next) => {
  try {
    const data = await getPublicMetrics();
    res.json(data);
  } catch (error) {
    next(error);
  }
});

app.get("/api/auth/me", authOptional, (req, res) => {
  res.json({ user: req.user || null });
});

app.post("/api/auth/register", async (req, res, next) => {
  try {
    const { username, password } = req.body || {};
    const result = await registerUser(username, password);
    setSessionCookie(res, result.sessionToken);
    res.json({ user: result.user });
  } catch (error) {
    next(error);
  }
});

app.post("/api/auth/login", async (req, res, next) => {
  try {
    const { username, password } = req.body || {};
    const result = await loginUser(username, password);
    setSessionCookie(res, result.sessionToken);
    res.json({ user: result.user });
  } catch (error) {
    next(error);
  }
});

app.post("/api/auth/logout", authOptional, async (req, res, next) => {
  try {
    const token = readToken(req);
    await clearSession(token);
    clearSessionCookie(res);
    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

app.get("/api/dashboard", authRequired, async (req, res, next) => {
  try {
    const data = await getDashboard(req.user.id);
    res.json(data);
  } catch (error) {
    next(error);
  }
});

app.post("/api/tokens", authRequired, async (req, res, next) => {
  try {
    const token = await attachToken(req.user.id, req.body || {});
    res.json({ token });
  } catch (error) {
    next(error);
  }
});

app.post("/api/tokens/generate-deposit", authRequired, async (req, res, next) => {
  try {
    const count = Number(req.body?.count || 1);
    const data = await generatePendingDepositAddress(req.user.id, count);
    res.json(data);
  } catch (error) {
    next(error);
  }
});

app.post("/api/tokens/resolve-mint", authRequired, async (req, res, next) => {
  try {
    const mint = String(req.body?.mint || "").trim();
    const token = await resolveMintMetadata(mint);
    res.json({ token });
  } catch (error) {
    next(error);
  }
});

app.patch("/api/tokens/:id", authRequired, async (req, res, next) => {
  try {
    const token = await updateToken(req.user.id, req.params.id, req.body || {});
    res.json({ token });
  } catch (error) {
    next(error);
  }
});

app.get("/api/tokens/:id/details", authRequired, async (req, res, next) => {
  try {
    const data = await getTokenLiveDetails(req.user.id, req.params.id);
    res.json(data);
  } catch (error) {
    next(error);
  }
});

app.get("/api/tokens/:id/volume/withdraw-options", authRequired, async (req, res, next) => {
  try {
    const data = await getVolumeWithdrawOptions(req.user.id, req.params.id);
    res.json(data);
  } catch (error) {
    next(error);
  }
});

app.post("/api/tokens/:id/volume/sweep", authRequired, async (req, res, next) => {
  try {
    const data = await sweepVolumeWallets(req.user.id, req.params.id);
    res.json(data);
  } catch (error) {
    next(error);
  }
});

app.post("/api/tokens/:id/volume/withdraw", authRequired, async (req, res, next) => {
  try {
    const data = await withdrawVolumeFunds(req.user.id, req.params.id, req.body || {});
    res.json(data);
  } catch (error) {
    next(error);
  }
});

app.delete("/api/tokens/:id", authRequired, async (req, res, next) => {
  try {
    const data = await deleteToken(req.user.id, req.params.id);
    res.json(data);
  } catch (error) {
    next(error);
  }
});

app.post("/api/deploy", authOptional, async (req, res, next) => {
  try {
    const result = await deployToken(req.user?.id || null, req.body || {});
    res.json(result);
  } catch (error) {
    next(error);
  }
});

app.post("/api/deploy/record", authOptional, async (req, res, next) => {
  try {
    const result = await recordDeployFromChain(req.user?.id || null, req.body || {});
    res.json(result);
  } catch (error) {
    next(error);
  }
});

const webDist = path.resolve(process.cwd(), "dist/web");
app.use(express.static(webDist));
app.get(/^\/(?!api).*/, (_req, res) => {
  res.sendFile(path.join(webDist, "index.html"));
});

app.use((error, _req, res, _next) => {
  const message = error?.message || "Internal server error";
  const isClient =
    message.includes("required") ||
    message.includes("exists") ||
    message.includes("Unauthorized") ||
    message.includes("Invalid") ||
    message.includes("Max");

  const status = Number.isInteger(error?.status) ? Number(error.status) : isClient ? 400 : 500;
  res.status(status).json({ error: message });
});

async function start() {
  await initDb();
  void ensureDepositPool().catch((error) => {
    console.warn("[api] deposit pool warmup failed:", error?.message || error);
  });
  const refillMs = Math.max(5000, Number(config.depositPoolRefillIntervalMs || 15000));
  setInterval(() => {
    void ensureDepositPool().catch((error) => {
      console.warn("[api] deposit pool refill failed:", error?.message || error);
    });
  }, refillMs);
  app.listen(config.port, () => {
    console.log(`[api] listening on :${config.port}`);
  });
}

start().catch((error) => {
  console.error("[api] failed to start", error);
  process.exit(1);
});
