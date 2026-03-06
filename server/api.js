import cookieParser from "cookie-parser";
import cors from "cors";
import express from "express";
import path from "node:path";
import {
  attachToken,
  buildDeployLocalTx,
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
  submitSignedDeployTx,
  updateToken,
  withdrawBurnFunds,
  withdrawVolumeFunds,
} from "./core.js";
import { config } from "./config.js";
import { initDb } from "./db.js";

const app = express();
app.disable("x-powered-by");

const trustProxy = String(process.env.TRUST_PROXY || "true").toLowerCase() !== "false";
if (trustProxy) app.set("trust proxy", 1);

function normalizeOrigin(input) {
  const raw = String(input || "").trim();
  if (!raw) return "";
  try {
    const parsed = new URL(raw);
    return parsed.origin.toLowerCase();
  } catch {
    return "";
  }
}

function parseAllowedOrigins() {
  const configured = String(process.env.CORS_ALLOWED_ORIGINS || "")
    .split(",")
    .map((v) => normalizeOrigin(v))
    .filter(Boolean);

  if (configured.length > 0) return new Set(configured);

  const defaults = [];
  const renderExternal = normalizeOrigin(process.env.RENDER_EXTERNAL_URL || "");
  if (renderExternal) defaults.push(renderExternal);

  if (process.env.NODE_ENV !== "production") {
    defaults.push(
      "http://localhost:5173",
      "http://127.0.0.1:5173",
      "http://localhost:3000",
      "http://127.0.0.1:3000"
    );
  }

  return new Set(defaults);
}

const allowedOrigins = parseAllowedOrigins();

function requestIp(req) {
  const fwd = String(req.headers["x-forwarded-for"] || "").split(",")[0]?.trim();
  return fwd || req.ip || "unknown";
}

function createRateLimiter({ windowMs, max, keyFn, message }) {
  const buckets = new Map();
  const safeWindow = Math.max(1000, Number(windowMs) || 1000);
  const safeMax = Math.max(1, Number(max) || 1);

  return (req, res, next) => {
    const now = Date.now();
    const key = String((typeof keyFn === "function" ? keyFn(req) : requestIp(req)) || "unknown");
    const hit = buckets.get(key);

    if (!hit || now > hit.resetAt) {
      buckets.set(key, { count: 1, resetAt: now + safeWindow });
      if (buckets.size > 50_000) {
        for (const [k, v] of buckets.entries()) {
          if (now > Number(v?.resetAt || 0)) buckets.delete(k);
          if (buckets.size <= 25_000) break;
        }
      }
      return next();
    }

    hit.count += 1;
    if (hit.count > safeMax) {
      const retryAfter = Math.max(1, Math.ceil((hit.resetAt - now) / 1000));
      res.setHeader("Retry-After", String(retryAfter));
      return res.status(429).json({ error: message || "Too many requests. Please try again shortly." });
    }
    return next();
  };
}

const authLimiter = createRateLimiter({
  windowMs: 15 * 60 * 1000,
  max: 25,
  keyFn: (req) => {
    const username = String(req.body?.username || "").trim().toLowerCase();
    return `${requestIp(req)}:auth:${username || "-"}`;
  },
  message: "Too many auth attempts. Please wait before retrying.",
});

const writeLimiter = createRateLimiter({
  windowMs: 60 * 1000,
  max: 120,
  keyFn: (req) => `${requestIp(req)}:write`,
  message: "Too many write requests. Please slow down.",
});

const deployLimiter = createRateLimiter({
  windowMs: 60 * 1000,
  max: 30,
  keyFn: (req) => `${requestIp(req)}:deploy`,
  message: "Too many deploy requests. Please slow down.",
});

app.use(express.json({ limit: "50mb" }));
app.use(cookieParser());
app.use(
  cors({
    origin(origin, cb) {
      if (!origin) return cb(null, true);
      const normalized = normalizeOrigin(origin);
      if (!normalized) return cb(new Error("CORS origin is invalid."));
      if (allowedOrigins.size === 0 || allowedOrigins.has(normalized)) return cb(null, true);
      return cb(new Error("CORS origin denied."));
    },
    credentials: true,
  })
);
app.use((req, res, next) => {
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "DENY");
  res.setHeader("Referrer-Policy", "strict-origin-when-cross-origin");
  res.setHeader("Permissions-Policy", "camera=(), microphone=(), geolocation=()");
  res.setHeader("Cross-Origin-Opener-Policy", "same-origin");
  if (secureCookie) {
    res.setHeader("Strict-Transport-Security", "max-age=31536000; includeSubDomains");
  }
  next();
});

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

function isTrustedOrigin(req) {
  const origin = normalizeOrigin(req.headers.origin || "");
  const referer = String(req.headers.referer || "").trim();
  const refererOrigin = normalizeOrigin(referer);
  if (!origin && !refererOrigin) return true; // non-browser clients

  const proto = String(req.headers["x-forwarded-proto"] || req.protocol || "https")
    .split(",")[0]
    .trim()
    .toLowerCase();
  const host = String(req.headers["x-forwarded-host"] || req.headers.host || "")
    .split(",")[0]
    .trim()
    .toLowerCase();
  const hostOrigin = host ? `${proto}://${host}` : "";

  const matchesAllowed = (value) =>
    Boolean(value) && (allowedOrigins.size === 0 || allowedOrigins.has(value) || value === hostOrigin);

  return matchesAllowed(origin) || matchesAllowed(refererOrigin);
}

app.use("/api", (req, res, next) => {
  if (req.method === "GET" || req.method === "HEAD" || req.method === "OPTIONS") return next();
  if (isTrustedOrigin(req)) return next();
  return res.status(403).json({ error: "Request origin not allowed." });
});

app.use("/api", (req, res, next) => {
  if (req.method === "GET" || req.method === "HEAD" || req.method === "OPTIONS") return next();
  return writeLimiter(req, res, next);
});

function parseDataUri(input, label = "File") {
  const raw = String(input || "").trim();
  const match = raw.match(/^data:([^;]+);base64,(.+)$/);
  if (!match) {
    throw new Error(`${label} data is invalid.`);
  }
  const mime = String(match[1] || "application/octet-stream").trim() || "application/octet-stream";
  const base64 = String(match[2] || "").trim();
  if (!base64) {
    throw new Error(`${label} payload is empty.`);
  }
  return { mime, buffer: Buffer.from(base64, "base64") };
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

app.post("/api/auth/register", authLimiter, async (req, res, next) => {
  try {
    const { username, password } = req.body || {};
    const result = await registerUser(username, password);
    setSessionCookie(res, result.sessionToken);
    res.json({ user: result.user });
  } catch (error) {
    next(error);
  }
});

app.post("/api/auth/login", authLimiter, async (req, res, next) => {
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

app.post("/api/tokens/:id/burn/withdraw", authRequired, async (req, res, next) => {
  try {
    const data = await withdrawBurnFunds(req.user.id, req.params.id, req.body || {});
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

app.post("/api/deploy", deployLimiter, authOptional, async (req, res, next) => {
  try {
    const result = await deployToken(req.user?.id || null, req.body || {});
    res.json(result);
  } catch (error) {
    next(error);
  }
});

app.post("/api/deploy/pump-ipfs", deployLimiter, authOptional, async (req, res, next) => {
  try {
    const body = req.body || {};
    const image = parseDataUri(body.imageDataUri, "Token media");
    const imageName = String(body.imageFileName || "token-media").trim().slice(0, 120) || "token-media";

    const form = new FormData();
    form.append("file", new Blob([image.buffer], { type: image.mime }), imageName);

    const bannerDataUri = String(body.bannerDataUri || "").trim();
    if (bannerDataUri) {
      const banner = parseDataUri(bannerDataUri, "Banner");
      const bannerName = String(body.bannerFileName || "token-banner").trim().slice(0, 120) || "token-banner";
      form.append("banner", new Blob([banner.buffer], { type: banner.mime }), bannerName);
    }

    form.append("name", String(body.name || "").trim());
    form.append("symbol", String(body.symbol || "").trim());
    form.append("description", String(body.description || "").trim());
    form.append("twitter", String(body.twitter || "").trim());
    form.append("telegram", String(body.telegram || "").trim());
    form.append("website", String(body.website || "").trim());
    form.append("showName", "true");

    const upstream = await fetch("https://pump.fun/api/ipfs", {
      method: "POST",
      body: form,
    });

    const text = await upstream.text().catch(() => "");
    let data = {};
    try {
      data = text ? JSON.parse(text) : {};
    } catch {
      data = {};
    }

    if (!upstream.ok) {
      const message = String(data?.error || text || "Pump metadata upload failed.").trim();
      return res.status(upstream.status || 502).json({ error: message || "Pump metadata upload failed." });
    }

    return res.json(data);
  } catch (error) {
    return next(error);
  }
});

app.post("/api/deploy/pump-trade-local", deployLimiter, authOptional, async (req, res, next) => {
  try {
    const body = req.body || {};
    const upstream = await fetch("https://pumpportal.fun/api/trade-local", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    if (!upstream.ok) {
      const text = String(await upstream.text().catch(() => "")).trim();
      const statusText = String(upstream.statusText || "").trim();
      const isCreate = String(body.action || "").trim().toLowerCase() === "create";

      if (isCreate) {
        try {
          const localTxBytes = await buildDeployLocalTx(body);
          const localRaw = Buffer.from(localTxBytes);
          return res.json({ txBase64: localRaw.toString("base64") });
        } catch (fallbackError) {
          const fallbackMessage = String(fallbackError?.message || "").trim();
          const upstreamMessage =
            (!text || /^bad request$/i.test(text) ? statusText : text) ||
            text ||
            statusText ||
            "Pump trade-local request failed.";
          const combined = [upstreamMessage, fallbackMessage].filter(Boolean).join(" | local fallback: ");
          return res.status(upstream.status || 502).json({ error: combined || "Pump trade-local request failed." });
        }
      }

      const message =
        (!text || /^bad request$/i.test(text) ? statusText : text) ||
        text ||
        statusText ||
        "Pump trade-local request failed.";
      return res.status(upstream.status || 502).json({ error: message || "Pump trade-local request failed." });
    }

    const raw = Buffer.from(await upstream.arrayBuffer());
    return res.json({ txBase64: raw.toString("base64") });
  } catch (error) {
    return next(error);
  }
});

app.post("/api/deploy/submit-signed", deployLimiter, authOptional, async (req, res, next) => {
  try {
    const result = await submitSignedDeployTx(req.body || {});
    return res.json(result);
  } catch (error) {
    return next(error);
  }
});

app.post("/api/deploy/record", deployLimiter, authOptional, async (req, res, next) => {
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
  const message = String(error?.message || "Internal server error");
  const isClient =
    message.includes("required") ||
    message.includes("exists") ||
    message.includes("Unauthorized") ||
    message.includes("Invalid") ||
    message.includes("Max") ||
    message.includes("denied") ||
    message.includes("not allowed");

  const status = Number.isInteger(error?.status) ? Number(error.status) : isClient ? 400 : 500;
  const safeMessage = status >= 500 && process.env.NODE_ENV === "production"
    ? "Internal server error"
    : message;
  if (status >= 500) {
    console.error("[api] request failed:", message);
  }
  res.status(status).json({ error: safeMessage });
});

let dbReady = false;

function isDbInitTimeout(error) {
  const code = String(error?.code || "").trim();
  const msg = String(error?.message || "").toLowerCase();
  return code === "57014" || msg.includes("statement timeout");
}

async function initDbWithRetry() {
  const retryMs = Math.max(3000, Number(process.env.DB_INIT_RETRY_MS || 5000));
  const maxRetries = Math.max(1, Number(process.env.DB_INIT_MAX_RETRIES || 5));
  let attempts = 0;
  while (!dbReady) {
    try {
      await initDb();
      dbReady = true;
      console.log("[api] database initialized");
      void ensureDepositPool().catch((error) => {
        console.warn("[api] deposit pool warmup failed:", error?.message || error);
      });
      const refillMs = Math.max(5000, Number(config.depositPoolRefillIntervalMs || 15000));
      setInterval(() => {
        void ensureDepositPool().catch((error) => {
          console.warn("[api] deposit pool refill failed:", error?.message || error);
        });
      }, refillMs);
      return;
    } catch (error) {
      attempts += 1;
      if (isDbInitTimeout(error) || attempts >= maxRetries) {
        dbReady = true;
        console.warn(
          `[api] database init skipped after ${attempts} attempt(s): ${error?.message || error}`
        );
        return;
      }
      console.warn(`[api] database init failed, retrying in ${retryMs}ms:`, error?.message || error);
      await new Promise((resolve) => setTimeout(resolve, retryMs));
    }
  }
}

async function start() {
  const host = String(process.env.HOST || "0.0.0.0").trim() || "0.0.0.0";
  app.listen(config.port, host, () => {
    console.log(`[api] listening on ${host}:${config.port}`);
  });
  void initDbWithRetry();
}

start().catch((error) => {
  console.error("[api] failed to start", error);
  process.exit(1);
});
