#!/usr/bin/env node
/**
 * Hollo Stream Proxy
 *
 * 機能:
 * 1. WebSocket Streaming (/api/v1/streaming)
 * 2. WebPush Subscription API (/api/v1/push/subscription)
 *
 * 認証方式:
 * - WebSocket: ?access_token=, Authorization: Bearer, Sec-WebSocket-Protocol
 * - Push: Authorization: Bearer
 * - トークン検証: Hollo API (/api/v1/accounts/verify_credentials) で検証、結果を30秒間キャッシュ
 * - データ取得: 各クライアントのトークンで Hollo API をポーリング
 */

import { readFile, writeFile, mkdir, rename } from "node:fs/promises";
import { createServer } from "node:http";
import { WebSocketServer } from "ws";
import webpush from "web-push";

// ─ Config ────────────────────────────────────────────────────────────────
const PORT = parseInt(process.env.PORT || "3001", 10);
const HOLLO_URL = process.env.HOLLO_URL;
const HOLLO_INTERNAL_URL = process.env.HOLLO_INTERNAL_URL || HOLLO_URL;
if (!HOLLO_URL) {
  console.error("FATAL: HOLLO_URL environment variable is required");
  process.exit(1);
}
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL || "5000", 10);
const DATA_DIR = process.env.DATA_DIR || "/data";
const SUBS_FILE = `${DATA_DIR}/push_subscriptions.json`;

const VAPID_PUBLIC_KEY = process.env.VAPID_PUBLIC_KEY || "";
const VAPID_PRIVATE_KEY = process.env.VAPID_PRIVATE_KEY || "";
const VAPID_SUBJECT = process.env.VAPID_SUBJECT || "https://example.com";

// ── Logging ───────────────────────────────────────────────────────────────
function ts() {
  return new Date().toISOString().replace("T", " ").slice(0, 19);
}
function log(level, msg, extra) {
  const line = [`[${ts()}]`, `[${level}]`, msg];
  if (extra) line.push(JSON.stringify(extra));
  console.log(line.join(" "));
}
const logger = {
  info: (m, e) => log("info", m, e),
  warn: (m, e) => log("warn", m, e),
  error: (m, e) => log("error", m, e),
  stream: (m, e) => log("stream", m, e),
  push: (m, e) => log("push", m, e),
};

// ─ Init web-push ─────────────────────────────────────────────────────────
if (VAPID_PRIVATE_KEY) {
  webpush.setVapidDetails(VAPID_SUBJECT, VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);
  logger.info("web-push VAPID initialized");
} else {
  logger.warn("VAPID_PRIVATE_KEY not set — push delivery disabled");
}

// ── Data dir ──────────────────────────────────────────────────────────────
await mkdir(DATA_DIR, { recursive: true });

// ── Push subscriptions (PVC JSON file) ───────────────────────────────────
// ── Token cache (DBクエリ削減) ──────────────────────────────────────────
const tokenCache = new Map(); // token → { accountOwnerId, scopes, valid, expiresAt }
const TOKEN_CACHE_TTL_MS = 30_000;
const TOKEN_CACHE_NEG_TTL_MS = 30_000;

// ── Token validation via Hollo API ──────────────────────────────────────
async function verifyToken(token) {
  if (!token) return null;

  const cached = tokenCache.get(token);
  if (cached) {
    if (cached.valid && cached.expiresAt > Date.now()) return cached;
    if (!cached.valid && cached.expiresAt > Date.now()) return null;
  }

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15_000);
    const res = await fetch(`${HOLLO_INTERNAL_URL}/api/v1/accounts/verify_credentials`, {
      headers: { Authorization: `Bearer ${token}` },
      signal: controller.signal,
    });
    clearTimeout(timeout);
    if (!res.ok) {
      tokenCache.set(token, { valid: false, expiresAt: Date.now() + TOKEN_CACHE_NEG_TTL_MS });
      return null;
    }
    const account = await res.json();
    const scopesStr = res.headers.get("X-OAuth-Scopes") || res.headers.get("x-oauth-scopes") || "";
    const scopes = scopesStr.split(/\s+/).filter(Boolean);
    const info = {
      accountOwnerId: account.id,
      scopes,
      valid: true,
      expiresAt: Date.now() + TOKEN_CACHE_TTL_MS,
      account: {
        id: account.id,
        acct: account.acct || account.username,
        display_name: account.display_name || account.username,
        avatar: account.avatar || "",
      },
    };
    tokenCache.set(token, info);
    return info;
  } catch (err) {
    logger.error("token verification failed", { error: err.message });
    return null;
  }
}

// ── Push subscriptions ──────────────────────────────────────────────────
async function loadSubsFile() {
  try {
    return JSON.parse(await readFile(SUBS_FILE, "utf8"));
  } catch {
    return { subscriptions: {}, push_since: {} };
  }
}

async function saveSubsFile(data) {
  await mkdir(DATA_DIR, { recursive: true });
  const tmp = `${SUBS_FILE}.tmp`;
  await writeFile(tmp, JSON.stringify(data, null, 2));
  await rename(tmp, SUBS_FILE);
}

async function saveSubscription(accountId, sub, alertsData, accessToken) {
  const data = await loadSubsFile();
  if (!data.subscriptions[accountId]) data.subscriptions[accountId] = [];

  const idx = data.subscriptions[accountId].findIndex(
    (s) => s.endpoint === sub.endpoint,
  );

  const entry = {
    endpoint: sub.endpoint,
    keys: { p256dh: sub.keys.p256dh, auth: sub.keys.auth },
    access_token: accessToken,
    alerts: {
      mention: alertsData.mention !== false,
      status: alertsData.status !== false,
      reblog: alertsData.reblog !== false,
      favourite: alertsData.favourite !== false,
      follow: alertsData.follow !== false,
      follow_request: alertsData.follow_request !== false,
      poll: alertsData.poll !== false,
      update: alertsData.update !== false,
      admin_sign_up: alertsData.admin_sign_up !== false,
      admin_report: alertsData.admin_report !== false,
      severed_relationships: alertsData.severed_relationships !== false,
      reaction: alertsData.reaction !== false,
    },
    created_at: new Date().toISOString(),
  };

  if (idx >= 0) {
    data.subscriptions[accountId][idx] = entry;
  } else {
    data.subscriptions[accountId].push(entry);
  }

  await saveSubsFile(data);
}

async function loadSubscription(accountId, accessToken) {
  const data = await loadSubsFile();
  const subs = data.subscriptions[accountId];
  if (!subs || subs.length === 0) return null;
  return subs.find(s => s.access_token === accessToken) || null;
}

async function loadSubscriptions(accountId) {
  const data = await loadSubsFile();
  return data.subscriptions[accountId] || [];
}

async function deleteSubscription(accountId, accessToken) {
  const data = await loadSubsFile();
  const subs = data.subscriptions[accountId];
  if (!subs) return;
  data.subscriptions[accountId] = subs.filter(s => s.access_token !== accessToken);
  if (data.subscriptions[accountId].length === 0) {
    delete data.subscriptions[accountId];
  }
  await saveSubsFile(data);
}

async function updateAlerts(accountId, alertsData, accessToken) {
  const data = await loadSubsFile();
  const subs = data.subscriptions[accountId];
  if (!subs) return;
  const sub = subs.find(s => s.access_token === accessToken);
  if (!sub) return;
  sub.alerts = {
    mention: alertsData.mention !== false,
    status: alertsData.status !== false,
    reblog: alertsData.reblog !== false,
    favourite: alertsData.favourite !== false,
    follow: alertsData.follow !== false,
    follow_request: alertsData.follow_request !== false,
    poll: alertsData.poll !== false,
    update: alertsData.update !== false,
    admin_sign_up: alertsData.admin_sign_up !== false,
    admin_report: alertsData.admin_report !== false,
    severed_relationships: alertsData.severed_relationships !== false,
    reaction: alertsData.reaction !== false,
  };
  await saveSubsFile(data);
}

async function removePushSubscription(endpoint) {
  const data = await loadSubsFile();
  for (const accountId of Object.keys(data.subscriptions)) {
    data.subscriptions[accountId] = data.subscriptions[accountId].filter(
      (s) => s.endpoint !== endpoint,
    );
    if (data.subscriptions[accountId].length === 0) {
      delete data.subscriptions[accountId];
    }
  }
  await saveSubsFile(data);
  logger.push("subscription removed", { endpoint });
}

// ── Mastodon API client ─────────────────────────────────────────────────
async function fetchHomeTimelineAPI(accessToken, sinceId = null) {
  const params = new URLSearchParams({ limit: "40" });
  if (sinceId) params.set("since_id", sinceId);

  const res = await fetch(
    `${HOLLO_URL}/api/v1/timelines/home?${params}`,
    { headers: { Authorization: `Bearer ${accessToken}` } },
  );

  if (!res.ok) {
    logger.error("timeline API error", { status: res.status });
    return { statuses: [], latestId: null };
  }

  const statuses = await res.json();

  // 新着投稿の中で最も新しいIDを次回のsince_idとして保存
  const latestId = statuses.length > 0 ? statuses[0].id : null;

  return { statuses, latestId };
}

async function fetchListTimelineAPI(accessToken, listId, sinceId = null) {
  const params = new URLSearchParams({ limit: "40" });
  if (sinceId) params.set("since_id", sinceId);

  const res = await fetch(
    `${HOLLO_URL}/api/v1/timelines/list/${listId}?${params}`,
    { headers: { Authorization: `Bearer ${accessToken}` } },
  );

  if (!res.ok) {
    logger.error("list timeline API error", { status: res.status, listId });
    return { statuses: [], latestId: null };
  }

  const statuses = await res.json();
  const latestId = statuses.length > 0 ? statuses[0].id : null;

  return { statuses, latestId };
}

async function fetchInstanceAPI(req, apiVersion = "v1") {
  const res = await fetch(`${HOLLO_INTERNAL_URL}/api/${apiVersion}/instance`, {
    headers: { Accept: "application/json" },
  });
  if (!res.ok) {
    throw new Error(`instance API error: ${res.status}`);
  }
  const data = await res.json();

  // 公開ドメインをリクエストホストに戻す（内部 URL 経由で取得すると Hollo が内部ホスト名を返す）
  const publicHost = req.headers.host || "localhost";
  const publicDomain = publicHost.split(":")[0];
  const streamingUrl = `wss://${publicHost}`;

  if (apiVersion === "v2") {
    data.domain = publicDomain;
    if (!data.configuration) data.configuration = {};
    if (!data.configuration.urls) data.configuration.urls = {};
    data.configuration.urls.streaming = streamingUrl;
  } else {
    data.uri = publicDomain;
    if (!data.urls) data.urls = {};
    data.urls.streaming_api = streamingUrl;
  }

  // Hollo は内部 URL 経由で取得すると内部ホスト名を含むフィールドを返すので、公開ドメインに差し替える
  data.title = publicDomain;
  if (data.short_description !== undefined) {
    data.short_description = `A Hollo instance at ${publicDomain}`;
  }
  data.description = `A Hollo instance at ${publicDomain}`;
  if (data.thumbnail && typeof data.thumbnail === "string") {
    data.thumbnail = data.thumbnail.replace(/^https?:\/\/[^/]+/, `https://${publicHost}`);
  } else if (data.thumbnail && typeof data.thumbnail === "object" && typeof data.thumbnail.url === "string") {
    data.thumbnail.url = data.thumbnail.url.replace(/^https?:\/\/[^/]+/, `https://${publicHost}`);
  }
  if (Array.isArray(data.icon)) {
    for (const icon of data.icon) {
      if (icon && typeof icon.src === "string") {
        icon.src = icon.src.replace(/^https?:\/\/[^/]+/, `https://${publicHost}`);
      }
    }
  }

  return data;
}

async function fetchPublicTimelineAPI(accessToken, { local = false, remote = false, sinceId = null } = {}) {
  const params = new URLSearchParams({ limit: "40" });
  if (local) params.set("local", "true");
  if (remote) params.set("remote", "true");
  if (sinceId) params.set("since_id", sinceId);

  const res = await fetch(
    `${HOLLO_URL}/api/v1/timelines/public?${params}`,
    { headers: { Authorization: `Bearer ${accessToken}` } },
  );

  if (!res.ok) {
    logger.error("public timeline API error", { status: res.status, local, remote });
    return { statuses: [], latestId: null };
  }

  const statuses = await res.json();
  const latestId = statuses.length > 0 ? statuses[0].id : null;
  return { statuses, latestId };
}

async function fetchHashtagTimelineAPI(accessToken, tag, { local = false, sinceId = null } = {}) {
  const params = new URLSearchParams({ limit: "40" });
  if (local) params.set("local", "true");
  if (sinceId) params.set("since_id", sinceId);

  const res = await fetch(
    `${HOLLO_URL}/api/v1/timelines/tag/${encodeURIComponent(tag)}?${params}`,
    { headers: { Authorization: `Bearer ${accessToken}` } },
  );

  if (!res.ok) {
    logger.error("hashtag timeline API error", { status: res.status, tag, local });
    return { statuses: [], latestId: null };
  }

  const statuses = await res.json();
  const latestId = statuses.length > 0 ? statuses[0].id : null;
  return { statuses, latestId };
}

async function fetchNotificationsAPI(accessToken, sinceId = null) {
  const params = new URLSearchParams({ limit: "30" });
  if (sinceId) params.set("since_id", sinceId);

  const res = await fetch(
    `${HOLLO_URL}/api/v1/notifications?${params}`,
    { headers: { Authorization: `Bearer ${accessToken}` } },
  );

  if (!res.ok) {
    logger.error("notifications API error", { status: res.status });
    return { notifications: [], latestId: null };
  }

  const notifications = await res.json();

  let latestId = null;
  if (notifications.length > 0) {
    latestId = notifications[0].id;
  }

  return { notifications, latestId };
}

// ── Server ────────────────────────────────────────────────────────────────
const server = createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
  const path = url.pathname;

  // CORS
  if (path.startsWith("/api/")) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Authorization, Content-Type");
    if (req.method === "OPTIONS") {
      res.writeHead(204);
      res.end();
      return;
    }
  }

  // ── Push subscription API ────────────────────────────────────────────
  if (path === "/api/v1/push/subscription") {
    const authHeader = req.headers.authorization || "";
    const bearerToken = authHeader.startsWith("Bearer ")
      ? authHeader.slice(7)
      : url.searchParams.get("access_token");

    if (!bearerToken) {
      res.writeHead(401, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Unauthorized" }));
      return;
    }

    const tokenInfo = await verifyToken(bearerToken);
    if (!tokenInfo?.accountOwnerId) {
      res.writeHead(401, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Unauthorized" }));
      return;
    }

    if (tokenInfo.scopes.length > 0 && !tokenInfo.scopes.includes("push") && !tokenInfo.scopes.includes("read")) {
      res.writeHead(403, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Forbidden: missing push or read scope" }));
      return;
    }

    const id = tokenInfo.accountOwnerId;

    if (req.method === "GET") {
      const sub = await loadSubscription(id, bearerToken);
      if (!sub) {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({}));
        return;
      }
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        id, endpoint: sub.endpoint, alerts: sub.alerts,
        server_key: VAPID_PUBLIC_KEY, policy: "all",
      }));
      return;
    }

    if (req.method === "POST") {
      let body = "";
      for await (const chunk of req) body += chunk;
      let parsed;
      try { parsed = JSON.parse(body); } catch {
        res.writeHead(400); res.end(JSON.stringify({ error: "Invalid JSON" })); return;
      }
      const sub = parsed.subscription;
      const alertsData = parsed.data?.alerts || {};
      if (!sub?.endpoint || !sub?.keys?.p256dh || !sub?.keys?.auth) {
        res.writeHead(400); res.end(JSON.stringify({ error: "Invalid subscription" })); return;
      }
      await saveSubscription(id, sub, alertsData, bearerToken);
      pushAccounts.add(id);
      startPolling();
      logger.push("subscription saved", { account: id });
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        id, endpoint: sub.endpoint, alerts: alertsData,
        server_key: VAPID_PUBLIC_KEY, policy: "all",
      }));
      return;
    }

    if (req.method === "PUT") {
      let body = "";
      for await (const chunk of req) body += chunk;
      let parsed;
      try { parsed = JSON.parse(body); } catch {
        res.writeHead(400); res.end(JSON.stringify({ error: "Invalid JSON" })); return;
      }
      if (parsed.data?.alerts) await updateAlerts(id, parsed.data.alerts, bearerToken);
      res.writeHead(200); res.end(JSON.stringify({}));
      return;
    }

    if (req.method === "DELETE") {
      await deleteSubscription(id, bearerToken);
      const remaining = await loadSubscriptions(id);
      if (remaining.length === 0) pushAccounts.delete(id);
      logger.push("subscription deleted", { account: id });
      res.writeHead(200); res.end(JSON.stringify({}));
      return;
    }

    res.writeHead(405); res.end(JSON.stringify({ error: "Method not allowed" }));
    return;
  }

  // ── Mastodon instance info (inject streaming URL) ─────────────────────
  if ((path === "/api/v1/instance" || path === "/api/v2/instance") && req.method === "GET") {
    try {
      const apiVersion = path.startsWith("/api/v2") ? "v2" : "v1";
      const data = await fetchInstanceAPI(req, apiVersion);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(data));
    } catch (err) {
      logger.error("instance API proxy error", { error: err.message });
      res.writeHead(502, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Failed to fetch instance info" }));
    }
    return;
  }

  // ── Mastodon filters (Hollo does not support; return empty) ───────────
  if (path === "/api/v1/filters") {
    if (req.method === "OPTIONS") {
      res.writeHead(204);
      res.end();
      return;
    }
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify([]));
    return;
  }

  // ── Health check ──────────────────────────────────────────────────────
  if (path === "/health" && req.method === "GET") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("OK");
    return;
  }

  // ── SSE Streaming (HTTP long-lived) ─────────────────────────────────
  if (req.method === "GET" && path.startsWith("/api/v1/streaming/")) {
    const ssePath = path.replace("/api/v1/streaming/", "");
    const tokenFromQuery = url.searchParams.get("access_token");
    const authHeader = req.headers.authorization || "";
    const tokenFromHeader = authHeader.startsWith("Bearer ") ? authHeader.slice(7).trim() : null;
    const token = tokenFromQuery || tokenFromHeader;

    if (!token) {
      res.writeHead(401, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Unauthorized" }));
      return;
    }

    const tokenInfo = await verifyToken(token);
    if (!tokenInfo?.accountOwnerId) {
      res.writeHead(401, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Unauthorized" }));
      return;
    }

    if (tokenInfo.scopes.length > 0 && !tokenInfo.scopes.includes("read")) {
      res.writeHead(403, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Forbidden: missing read scope" }));
      return;
    }

    let stream, params;
    if (ssePath === "" || ssePath === "user") {
      stream = "user";
      params = {};
    } else if (ssePath === "user/notification") {
      stream = "user:notification";
      params = {};
    } else if (ssePath === "list") {
      const listId = url.searchParams.get("list");
      if (!listId) { res.writeHead(400, { "Content-Type": "application/json" }); res.end(JSON.stringify({ error: "Missing list parameter" })); return; }
      stream = "list";
      params = { list: listId };
    } else if (ssePath === "public" || ssePath === "public/local" || ssePath === "public/remote") {
      stream = ssePath.replace("/", ":");
      params = {};
    } else if (ssePath === "hashtag" || ssePath === "hashtag/local") {
      const tag = url.searchParams.get("tag");
      if (!tag) { res.writeHead(400, { "Content-Type": "application/json" }); res.end(JSON.stringify({ error: "Missing tag parameter" })); return; }
      stream = ssePath.replace("/", ":");
      params = { tag };
    } else if (ssePath === "direct") {
      stream = "direct";
      params = {};
    } else {
      res.writeHead(404, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Unknown stream" }));
      return;
    }

    const account = tokenInfo.accountOwnerId;
    const subListId = stream === "list" ? params.list : null;
    const subTag = (stream === "hashtag" || stream === "hashtag:local") ? params.tag : null;
    const key = subListId ? `list:${subListId}` : subTag ? `${stream}:${subTag}` : stream;
    const subscriptions = new Map();
    subscriptions.set(key, { stream, listId: subListId, tag: subTag });

    const sseEntry = { ws: null, subscriptions, initialized: false, userAgent: req.headers["user-agent"] || "", sse: true, token };

    sseEntry.send = (eventJson) => {
      try {
        const parsed = JSON.parse(eventJson);
        const event = parsed.event;
        const payload = parsed.payload;
        res.write(`event: ${event}\n`);
        res.write(`data: ${payload}\n\n`);
      } catch {}
    };

    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "private, no-store",
      "Connection": "keep-alive",
    });
    res.write(":ok\n\n");

    if (!activeStreams.has(account)) {
      activeStreams.set(account, new Set());
    }
    activeStreams.get(account).add(sseEntry);

    req.on("close", () => {
      const streams = activeStreams.get(account);
      if (streams) {
        streams.delete(sseEntry);
        if (streams.size === 0) activeStreams.delete(account);
      }
      logger.stream("sse disconnected", { account });
    });

    logger.stream("sse connected", { account, stream, listId: subListId, tag: subTag });
    startPolling();
    return;
  }

  // ── Default: 404 ─────────────────────────────────────────────────────
  res.writeHead(404, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ error: "Not found" }));
});

// ── WebSocket server ────────────────────────────────────────────────────
const wss = new WebSocketServer({ noServer: true });
wss.on("error", (err) => {
  logger.error("websocket server error", { error: err.message });
});
const activeStreams = new Map();
const pushAccounts = new Set();
const recentlySentNotifications = new Map();
const sentTlPosts = new Map();
const tlMaxIds = new Map();
const DEDUP_WINDOW_MS = 60_000;

server.on("upgrade", async (req, socket, head) => {
  let url;
  try {
    url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
  } catch (err) {
    logger.error("ws upgrade: invalid url", { url: req.url, error: err.message });
    socket.end("HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n");
    return;
  }

  const tokenFromQuery = url.searchParams.get("access_token");
  const authHeader = req.headers.authorization || "";
  const tokenFromHeader = authHeader.startsWith("Bearer ") ? authHeader.slice(7).trim() : null;
  const secProtocol = (req.headers["sec-websocket-protocol"] || "").split(",")[0]?.trim();
  const tokenFromProtocol = secProtocol || null;
  const token = tokenFromQuery || tokenFromHeader || tokenFromProtocol;
  const stream = url.searchParams.get("stream") || "user";
  const listId = stream === "list" ? url.searchParams.get("list") : null;
  const tag = (stream === "hashtag" || stream === "hashtag:local")
    ? url.searchParams.get("tag")
    : null;

  logger.stream("upgrade request", {
    ua: req.headers["user-agent"],
    tokenFrom: tokenFromQuery ? "query" : tokenFromHeader ? "header" : tokenFromProtocol ? "protocol" : "none",
    stream,
    listId,
  });

  if (!token) {
    socket.end("HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n");
    return;
  }

  if (stream === "list" && !listId) {
    socket.end("HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n");
    return;
  }

  if ((stream === "hashtag" || stream === "hashtag:local") && !tag) {
    socket.end("HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n");
    return;
  }

  const validStreams = ["user", "user:notification", "list", "public", "public:local", "public:remote", "hashtag", "hashtag:local"];
  if (!validStreams.includes(stream)) {
    socket.end("HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n");
    return;
  }

  let tokenInfo;
  try {
    tokenInfo = await verifyToken(token);
  } catch (err) {
    logger.error("ws upgrade: token verification error", { error: err.message });
    socket.end("HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n");
    return;
  }

  if (!tokenInfo?.accountOwnerId) {
    socket.end("HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n");
    return;
  }

  if (tokenInfo.scopes.length > 0 && !tokenInfo.scopes.includes("read")) {
    socket.end("HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n");
    return;
  }

  const account = tokenInfo.accountOwnerId;

  try {
    wss.handleUpgrade(req, socket, head, (ws) => {
      const subscriptions = new Map();

      function addSubscription(subStream, params = {}) {
        const subListId = subStream === "list" ? params.list : null;
        const subTag = (subStream === "hashtag" || subStream === "hashtag:local") ? params.tag : null;
        const key = subListId ? `list:${subListId}` : subTag ? `${subStream}:${subTag}` : subStream;
        subscriptions.set(key, { stream: subStream, listId: subListId, tag: subTag });
        logger.stream("subscribed", { account, stream: subStream, listId: subListId, tag: subTag });
      }

      function removeSubscription(subStream, params = {}) {
        const subListId = subStream === "list" ? params.list : null;
        const subTag = (subStream === "hashtag" || subStream === "hashtag:local") ? params.tag : null;
        const key = subListId ? `list:${subListId}` : subTag ? `${subStream}:${subTag}` : subStream;
        subscriptions.delete(key);
        logger.stream("unsubscribed", { account, stream: subStream, listId: subListId, tag: subTag });
      }

      function hasSubscription(subStream, listIdOrTag = null) {
        if (subStream === "list") return subscriptions.has(`list:${listIdOrTag}`);
        if (subStream === "hashtag" || subStream === "hashtag:local") return subscriptions.has(`${subStream}:${listIdOrTag}`);
        return subscriptions.has(subStream);
      }

      ws.on("message", (data, isBinary) => {
        if (isBinary) return;
        let json;
        try { json = JSON.parse(data.toString("utf8")); } catch { return; }
        if (!json || typeof json !== "object") return;
        const { type, stream: msgStream, ...params } = json;
        if (type === "subscribe" && typeof msgStream === "string") {
          addSubscription(msgStream, params);
          startPolling();
        } else if (type === "unsubscribe" && typeof msgStream === "string") {
          removeSubscription(msgStream, params);
        }
      });

      ws.on("error", (err) => {
        logger.error("websocket connection error", { account, error: err.message });
      });

      const streamEntry = { ws, subscriptions, initialized: false, userAgent: req.headers["user-agent"] || "", token };

      if (!activeStreams.has(account)) {
        activeStreams.set(account, new Set());
      }
      activeStreams.get(account).add(streamEntry);

      ws.on("close", () => {
        const streams = activeStreams.get(account);
        if (streams) {
          streams.delete(streamEntry);
          if (streams.size === 0) activeStreams.delete(account);
        }
        logger.stream("disconnected", { account });
      });

      if (stream) addSubscription(stream, { list: listId, tag });

      logger.stream("connected", { account });
      startPolling();
    });
  } catch (err) {
    logger.error("ws upgrade: handleUpgrade failed", { account, error: err.message });
    if (!socket.destroyed) {
      socket.end("HTTP/1.1 500 Internal Server Error\r\nConnection: close\r\n\r\n");
    }
  }
});

function markNotificationSent(accountOwnerId, notificationId, streamKey) {
  const compoundKey = `${accountOwnerId}:${streamKey}`;
  const now = Date.now();
  if (!recentlySentNotifications.has(compoundKey)) {
    recentlySentNotifications.set(compoundKey, new Map());
  }
  const map = recentlySentNotifications.get(compoundKey);
  for (const [id, ts] of map.entries()) {
    if (now - ts > DEDUP_WINDOW_MS) map.delete(id);
  }
  if (map.has(notificationId)) return true;
  map.set(notificationId, now);
  return false;
}

function markTlPostSent(accountOwnerId, postId, streamKey) {
  const compoundKey = `${accountOwnerId}:${streamKey}`;
  if (!sentTlPosts.has(compoundKey)) {
    sentTlPosts.set(compoundKey, new Set());
  }
  const set = sentTlPosts.get(compoundKey);
  if (set.has(String(postId))) return true;
  set.add(String(postId));
  return false;
}

// Hollo の status オブジェクトから、Mastodon クライアントが誤認識しやすい
// 非標準フィールドを取り除く
function sanitizeStatus(status) {
  if (!status || typeof status !== "object") return status;
  const sanitized = { ...status };
  delete sanitized.quote_id;
  delete sanitized.quote;
  delete sanitized.quote_approval;
  delete sanitized.quotes_count;
  delete sanitized.fedibird_capabilities;
  if (sanitized.filtered === null || sanitized.filtered === undefined) {
    sanitized.filtered = [];
  }
  if (sanitized.account && typeof sanitized.account === "object") {
    const account = { ...sanitized.account };
    delete account.fedibird_capabilities;
    sanitized.account = account;
  }
  if (sanitized.reblog && typeof sanitized.reblog === "object") {
    sanitized.reblog = sanitizeStatus(sanitized.reblog);
  }
  if (sanitized.quote && typeof sanitized.quote === "object") {
    sanitized.quote = sanitizeStatus(sanitized.quote);
  }
  return sanitized;
}

function sanitizeNotification(notification) {
  if (!notification || typeof notification !== "object") return notification;
  const sanitized = { ...notification };
  if (sanitized.status) {
    sanitized.status = sanitizeStatus(sanitized.status);
  }
  return sanitized;
}

function shouldSendPush(alerts, notification) {
  const type = notification.type;
  // Hollo の emoji_reaction は reaction / favourite アラートで制御
  if (type === "emoji_reaction" || type === "reaction") {
    return alerts.reaction !== false || alerts.favourite !== false;
  }
  const key = type.replace(/\./g, "_");
  return alerts[key] !== false;
}

function buildPushPayload(notification, accessToken) {
  const icon = notification.account?.avatar || notification.account?.avatar_static || "";
  return {
    access_token: accessToken,
    notification_id: String(notification.id),
    notification_type: notification.type,
    icon,
    title: "",
    body: "",
    notification,
  };
}

let pollTimer = null;

function startPolling() {
  if (pollTimer) return;

  const poll = async () => {
    // Collect all active stream entries
    const allStreamEntries = [];
    for (const streams of activeStreams.values()) {
      for (const s of streams) {
        allStreamEntries.push(s);
      }
    }

    // If no active streams and no push accounts, stop polling
    if (allStreamEntries.length === 0 && pushAccounts.size === 0) {
      pollTimer = null;
      return;
    }

    // Poll for each stream entry independently
    for (const streamEntry of allStreamEntries) {
      const accessToken = streamEntry.token;
      if (!accessToken) continue;

      const subscriptions = streamEntry.subscriptions;
      const hasUserStream = subscriptions.has("user") || subscriptions.has("user:notification");
      const hasTimelineStream = subscriptions.has("user");
      const listSubs = [...subscriptions.values()].filter((sub) => sub.stream === "list" && sub.listId);
      const listIds = [...new Set(listSubs.map((s) => s.listId))];

      // Get account ID from token
      const tokenInfo = await verifyToken(accessToken);
      if (!tokenInfo?.accountOwnerId) continue;
      const accountOwnerId = tokenInfo.accountOwnerId;

      // Notifications
      if (hasUserStream) {
        try {
          const sinceId = tlMaxIds.get(`notif_${accountOwnerId}_${accessToken}`) || null;
          const { notifications, latestId } = await fetchNotificationsAPI(accessToken, sinceId);

          if (notifications.length > 0) {
            if (latestId) tlMaxIds.set(`notif_${accountOwnerId}_${accessToken}`, latestId);

            if (!sinceId) {
              logger.stream("notification checkpoint set", {
                account: accountOwnerId, latestId, count: notifications.length,
              });
            } else {
              let wsSent = 0;
              let wsSkipped = 0;

              for (const n of notifications) {
                const sanitized = sanitizeNotification(n);
                if (!subscriptions.has("user") && !subscriptions.has("user:notification")) continue;

                const notifStream = subscriptions.has("user:notification") ? "user:notification" : "user";
                if (markNotificationSent(`${accountOwnerId}_${accessToken}`, String(n.id), notifStream)) {
                  wsSkipped++;
                  continue;
                }

                const eventJson = JSON.stringify({
                  stream: [notifStream],
                  event: "notification",
                  payload: JSON.stringify(sanitized),
                });
                if (streamEntry.sse) { streamEntry.send(eventJson); wsSent++; }
                else if (streamEntry.ws?.readyState === 1) { try { streamEntry.ws.send(eventJson); wsSent++; } catch (_) {} }
              }

              if (wsSent > 0 || wsSkipped > 0) {
                logger.stream("notification", {
                  account: accountOwnerId, sent: wsSent, skipped: wsSkipped,
                  fetched: notifications.length,
                });
              }
            }
          }
        } catch (err) {
          logger.error("notification poll error", {
            account: accountOwnerId, error: err.message,
          });
        }
      }

      // Timeline
      if (hasTimelineStream) {
        try {
          const sinceId = tlMaxIds.get(`${accountOwnerId}_${accessToken}`) || null;
          const { statuses, latestId } = await fetchHomeTimelineAPI(accessToken, sinceId);

          if (statuses.length > 0) {
            if (latestId) tlMaxIds.set(`${accountOwnerId}_${accessToken}`, latestId);

            if (!sinceId) {
              logger.stream("timeline checkpoint set", {
                account: accountOwnerId, latestId, count: statuses.length,
              });
            } else {
              let sent = 0;
              let skipped = 0;

              for (const status of statuses.reverse()) {
                if (markTlPostSent(`${accountOwnerId}_${accessToken}`, String(status.id), "user")) {
                  skipped++;
                  continue;
                }

                if (subscriptions.has("user")) {
                  const eventJson = JSON.stringify({
                    stream: ["user"],
                    event: "update",
                    payload: JSON.stringify(sanitizeStatus(status)),
                  });
                  if (streamEntry.sse) { streamEntry.send(eventJson); sent++; }
                  else if (streamEntry.ws?.readyState === 1) { try { streamEntry.ws.send(eventJson); sent++; } catch (_) {} }
                }
              }

              if (sent > 0 || skipped > 0) {
                logger.stream("update", {
                  account: accountOwnerId, count: statuses.length, sent, skipped,
                });
              }
            }
          }
        } catch (err) {
          logger.error("timeline poll error", {
            account: accountOwnerId, error: err.message,
          });
        }
      }

      // List Timelines
      for (const lid of listIds) {
        try {
          const sinceId = tlMaxIds.get(`list:${lid}_${accessToken}`) || null;
          const { statuses, latestId } = await fetchListTimelineAPI(accessToken, lid, sinceId);

          if (statuses.length > 0) {
            if (latestId) tlMaxIds.set(`list:${lid}_${accessToken}`, latestId);

            if (!sinceId) {
              logger.stream("list timeline checkpoint set", {
                account: accountOwnerId, listId: lid, latestId, count: statuses.length,
              });
            } else {
              let sent = 0;
              let skipped = 0;

              for (const status of statuses.reverse()) {
                if (markTlPostSent(`${accountOwnerId}_${accessToken}`, String(status.id), `list:${lid}`)) {
                  skipped++;
                  continue;
                }

                const sub = subscriptions.get(`list:${lid}`);
                if (sub) {
                  const eventJson = JSON.stringify({
                    stream: ["list", lid],
                    event: "update",
                    payload: JSON.stringify(sanitizeStatus(status)),
                  });
                  if (streamEntry.sse) { streamEntry.send(eventJson); sent++; }
                  else if (streamEntry.ws?.readyState === 1) { try { streamEntry.ws.send(eventJson); sent++; } catch (_) {} }
                }
              }

              if (sent > 0 || skipped > 0) {
                logger.stream("list update", {
                  account: accountOwnerId, listId: lid, count: statuses.length, sent, skipped,
                });
              }
            }
          }
        } catch (err) {
          logger.error("list timeline poll error", {
            account: accountOwnerId, listId: lid, error: err.message,
          });
        }
      }

      // Public timelines
      const publicVariants = new Map();
      ["public", "public:local", "public:remote"].forEach((st) => {
        if (subscriptions.has(st)) {
          publicVariants.set(st, { local: st === "public:local", remote: st === "public:remote" });
        }
      });
      for (const [streamKey, { local, remote }] of publicVariants) {
        try {
          const sinceId = tlMaxIds.get(`${accountOwnerId}:${streamKey}_${accessToken}`) || null;
          const { statuses, latestId } = await fetchPublicTimelineAPI(accessToken, { local, remote, sinceId });

          if (statuses.length > 0) {
            if (latestId) tlMaxIds.set(`${accountOwnerId}:${streamKey}_${accessToken}`, latestId);

            if (!sinceId) {
              logger.stream("public timeline checkpoint set", {
                account: accountOwnerId, stream: streamKey, latestId, count: statuses.length,
              });
            } else {
              let sent = 0;
              let skipped = 0;

              for (const status of statuses.reverse()) {
                if (markTlPostSent(`${accountOwnerId}_${accessToken}`, String(status.id), streamKey)) {
                  skipped++;
                  continue;
                }

                if (subscriptions.has(streamKey)) {
                  const eventJson = JSON.stringify({
                    stream: [streamKey],
                    event: "update",
                    payload: JSON.stringify(sanitizeStatus(status)),
                  });
                  if (streamEntry.sse) { streamEntry.send(eventJson); sent++; }
                  else if (streamEntry.ws?.readyState === 1) { try { streamEntry.ws.send(eventJson); sent++; } catch (_) {} }
                }
              }

              if (sent > 0 || skipped > 0) {
                logger.stream("public timeline", {
                  account: accountOwnerId, stream: streamKey, sent, skipped, fetched: statuses.length,
                });
              }
            }
          }
        } catch (err) {
          logger.error("public timeline poll error", {
            account: accountOwnerId, stream: streamKey, error: err.message,
          });
        }
      }

      // Hashtag timelines
      const hashtagGroups = new Map();
      for (const sub of subscriptions.values()) {
        if ((sub.stream === "hashtag" || sub.stream === "hashtag:local") && sub.tag) {
          const key = `${sub.stream}:${sub.tag}`;
          if (!hashtagGroups.has(key)) {
            hashtagGroups.set(key, { stream: sub.stream, tag: sub.tag, local: sub.stream === "hashtag:local" });
          }
        }
      }
      for (const [key, { stream: streamKey, tag: htTag, local }] of hashtagGroups) {
        try {
          const sinceId = tlMaxIds.get(`${accountOwnerId}:hashtag:${htTag}${local ? ":local" : ""}_${accessToken}`) || null;
          const { statuses, latestId } = await fetchHashtagTimelineAPI(accessToken, htTag, { local, sinceId });

          if (statuses.length > 0) {
            const dedupKey = `${accountOwnerId}:hashtag:${htTag}${local ? ":local" : ""}_${accessToken}`;
            if (latestId) tlMaxIds.set(dedupKey, latestId);

            if (!sinceId) {
              logger.stream("hashtag timeline checkpoint set", {
                account: accountOwnerId, tag: htTag, local, latestId, count: statuses.length,
              });
            } else {
              let sent = 0;
              let skipped = 0;

              for (const status of statuses.reverse()) {
                if (markTlPostSent(`${accountOwnerId}_${accessToken}`, String(status.id), dedupKey)) {
                  skipped++;
                  continue;
                }

                const sub = subscriptions.get(`${streamKey}:${htTag}`);
                if (sub && sub.tag === htTag) {
                  const eventJson = JSON.stringify({
                    stream: [streamKey, htTag],
                    event: "update",
                    payload: JSON.stringify(sanitizeStatus(status)),
                  });
                  if (streamEntry.sse) { streamEntry.send(eventJson); sent++; }
                  else if (streamEntry.ws?.readyState === 1) { try { streamEntry.ws.send(eventJson); sent++; } catch (_) {} }
                }
              }

              if (sent > 0 || skipped > 0) {
                logger.stream("hashtag timeline", {
                  account: accountOwnerId, tag: htTag, local, sent, skipped, fetched: statuses.length,
                });
              }
            }
          }
        } catch (err) {
          logger.error("hashtag timeline poll error", {
            account: accountOwnerId, tag: htTag, local, error: err.message,
          });
        }
      }
    }

    // Poll for push subscriptions
    for (const accountOwnerId of pushAccounts) {
      try {
        const subs = await loadSubscriptions(accountOwnerId);
        if (subs.length === 0) {
          pushAccounts.delete(accountOwnerId);
          continue;
        }

        // Use the first subscription's token for polling
        const accessToken = subs[0].access_token;
        if (!accessToken) continue;

        const sinceId = tlMaxIds.get(`notif_${accountOwnerId}_push`) || null;
        const { notifications, latestId } = await fetchNotificationsAPI(accessToken, sinceId);

        if (notifications.length > 0) {
          if (latestId) tlMaxIds.set(`notif_${accountOwnerId}_push`, latestId);

          if (!sinceId) {
            logger.stream("notification checkpoint set", {
              account: accountOwnerId, latestId, count: notifications.length,
            });
          } else {
            let pushSent = 0;

            for (const n of notifications) {
              const sanitized = sanitizeNotification(n);

              for (let i = subs.length - 1; i >= 0; i--) {
                const sub = subs[i];
                if (!shouldSendPush(sub.alerts, sanitized)) continue;

                const payload = buildPushPayload(sanitized, sub.access_token);
                const result = await sendPushNotification(sub, payload);
                if (result.removed) {
                  await removePushSubscription(sub.endpoint);
                  subs.splice(i, 1);
                } else if (result.ok) {
                  pushSent++;
                }
              }
            }

            if (pushSent > 0) {
              logger.push("notification", {
                account: accountOwnerId, sent: pushSent,
                fetched: notifications.length,
              });
            }
            if (subs.length === 0) {
              pushAccounts.delete(accountOwnerId);
            }
          }
        }
      } catch (err) {
        logger.error("push notification poll error", {
          account: accountOwnerId, error: err.message,
        });
      }
    }

    // Continue polling if there are still active streams or push accounts
    if (allStreamEntries.length > 0 || pushAccounts.size > 0) {
      pollTimer = setTimeout(poll, POLL_INTERVAL_MS);
    } else {
      pollTimer = null;
    }
  };

  pollTimer = setTimeout(poll, POLL_INTERVAL_MS);
}

async function sendPushNotification(sub, payload) {
  if (!VAPID_PRIVATE_KEY) {
    return { ok: false, code: null, message: "VAPID_PRIVATE_KEY not configured" };
  }
  try {
    await webpush.sendNotification(
      { endpoint: sub.endpoint, keys: sub.keys },
      JSON.stringify(payload),
    );
    return { ok: true };
  } catch (err) {
    if (err.statusCode === 410 || err.statusCode === 404) {
      logger.push("expired subscription removed", { endpoint: sub.endpoint });
      return { removed: true };
    }
    logger.error("push send failed", {
      endpoint: sub.endpoint, code: err.statusCode, message: err.message,
    });
    return { ok: false, code: err.statusCode, message: err.message };
  }
}

async function loadExistingPushSubscriptions() {
  const data = await loadSubsFile();
  for (const accountId of Object.keys(data.subscriptions || {})) {
    if (data.subscriptions[accountId].length > 0) {
      pushAccounts.add(accountId);
    }
  }
  if (pushAccounts.size > 0) {
    logger.push("loaded existing subscriptions", { accounts: pushAccounts.size });
  }
}

// ─ Start server ──────────────────────────────────────────────────────────
await loadExistingPushSubscriptions();
if (pushAccounts.size > 0) startPolling();
server.listen(PORT, () => {
  logger.info(`Hollo Stream Proxy listening on port ${PORT}`);
  logger.info(`Hollo URL: ${HOLLO_URL}`);
});

async function shutdown() {
  logger.info("shutting down...");
  clearTimeout(pollTimer);
  const forceExit = setTimeout(() => {
    logger.warn("forced shutdown: existing connections remain");
    process.exit(1);
  }, 5000);
  server.close(() => {
    clearTimeout(forceExit);
    process.exit(0);
  });
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
