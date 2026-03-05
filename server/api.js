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

app.use(express.json({ limit: "50mb" }));
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

app.post("/api/deploy", authOptional, async (req, res, next) => {
  try {
    const result = await deployToken(req.user?.id || null, req.body || {});
    res.json(result);
  } catch (error) {
    next(error);
  }
});

app.post("/api/deploy/pump-ipfs", authOptional, async (req, res, next) => {
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

app.post("/api/deploy/pump-trade-local", authOptional, async (req, res, next) => {
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

app.post("/api/deploy/submit-signed", authOptional, async (req, res, next) => {
  try {
    const result = await submitSignedDeployTx(req.body || {});
    return res.json(result);
  } catch (error) {
    return next(error);
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
