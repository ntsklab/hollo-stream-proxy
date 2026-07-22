#!/usr/bin/env node
/**
 * Hollo Stream Proxy
 *
 * 機能:
 * 1. OAuth ログイン (authorization code を手動入力)
 * 2. WebSocket Streaming (/api/v1/streaming)
 * 3. WebPush Subscription API (/api/v1/push/subscription)
 *
 * 認証方式:
 * - ログイン: OAuth authorization code → token 交換
 * - WebSocket/Push: DBのoauth_tokensテーブルでトークン検証
 * - データ取得: Hollo API (/timelines/home, /notifications) をポーリング
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
const OAUTH_SESSIONS_FILE = `${DATA_DIR}/oauth_sessions.json`;
const CLIENT_CREDENTIALS_FILE = `${DATA_DIR}/client_credentials.json`;

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
  auth: (m, e) => log("auth", m, e),
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
const TOKEN_CACHE_TTL_MS = 300_000; // 5 min
const TOKEN_CACHE_NEG_TTL_MS = 60_000; // invalid token: 1 min

// ── OAuth sessions (補助情報) ──────────────────────────────────────────
async function loadOAuthSessions() {
  try {
    return JSON.parse(await readFile(OAUTH_SESSIONS_FILE, "utf8"));
  } catch {
    return { sessions: {} };
  }
}

async function saveOAuthSessions(data) {
  const tmp = `${OAUTH_SESSIONS_FILE}.tmp`;
  await writeFile(tmp, JSON.stringify(data, null, 2));
  await rename(tmp, OAUTH_SESSIONS_FILE);
}

async function addOAuthSession(tokenData) {
  const data = await loadOAuthSessions();
  data.sessions[tokenData.account_id] = {
    ...tokenData,
    created_at: new Date().toISOString(),
  };
  await saveOAuthSessions(data);
  logger.auth("session saved", { account: tokenData.account_handle });
}

async function listOAuthSessions() {
  const data = await loadOAuthSessions();
  return data.sessions;
}

async function removeOAuthSession(accountId) {
  const data = await loadOAuthSessions();
  delete data.sessions[accountId];
  await saveOAuthSessions(data);
  logger.auth("session removed", { account: accountId });
}

// ── Client credentials (OAuth app registration) ─────────────────────────
let clientCredentials = null;

async function loadClientCredentials() {
  try {
    return JSON.parse(await readFile(CLIENT_CREDENTIALS_FILE, "utf8"));
  } catch {
    return null;
  }
}

async function saveClientCredentials(data) {
  await writeFile(CLIENT_CREDENTIALS_FILE, JSON.stringify(data, null, 2));
}

async function registerApp() {
  const existing = await loadClientCredentials();
  if (existing?.client_id && existing?.client_secret) {
    clientCredentials = existing;
    logger.info("using existing client credentials", { client_id: existing.client_id });
    return;
  }

  logger.info("registering OAuth app with Hollo...");
  const res = await fetch(`${HOLLO_URL}/api/v1/apps`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      client_name: "Hollo Stream Proxy",
      redirect_uris: "urn:ietf:wg:oauth:2.0:oob",
      scopes: "read",
      website: HOLLO_URL,
    }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`App registration failed: ${res.status} - ${text}`);
  }

  const data = await res.json();
  clientCredentials = {
    client_id: data.client_id,
    client_secret: data.client_secret,
  };
  await saveClientCredentials(clientCredentials);
  logger.info("app registered", { client_id: clientCredentials.client_id });
}

// ── Token validation via Hollo API ──────────────────────────────────────
async function verifyToken(token) {
  if (!token) return null;

  const cached = tokenCache.get(token);
  if (cached) {
    if (cached.valid && cached.expiresAt > Date.now()) return cached;
    if (!cached.valid && cached.expiresAt > Date.now()) return null;
  }

  try {
    const res = await fetch(`${HOLLO_INTERNAL_URL}/api/v1/accounts/verify_credentials`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    if (!res.ok) {
      tokenCache.set(token, { valid: false, expiresAt: Date.now() + TOKEN_CACHE_NEG_TTL_MS });
      return null;
    }
    const account = await res.json();
    const info = {
      accountOwnerId: account.id,
      scopes: [],
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

// ── Exchange authorization code for token ────────────────────────────────
async function exchangeCodeForToken(code) {
  // OAuth authorization code → token 交換
  const res = await fetch(`${HOLLO_URL}/oauth/token`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      grant_type: "authorization_code",
      code: code,
      client_id: clientCredentials.client_id,
      client_secret: clientCredentials.client_secret,
      scope: "read",
      redirect_uri: "urn:ietf:wg:oauth:2.0:oob",
    }),
  });
  
  if (!res.ok) {
    const errorText = await res.text();
    throw new Error(`Token exchange failed: ${res.status} - ${errorText}`);
  }
  
  return await res.json();
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

async function loadSubscription(accountId) {
  const data = await loadSubsFile();
  const subs = data.subscriptions[accountId];
  return subs && subs.length > 0 ? subs[0] : null;
}

async function loadSubscriptions(accountId) {
  const data = await loadSubsFile();
  return data.subscriptions[accountId] || [];
}

async function deleteSubscription(accountId) {
  const data = await loadSubsFile();
  delete data.subscriptions[accountId];
  await saveSubsFile(data);
}

async function updateAlerts(accountId, alertsData) {
  const data = await loadSubsFile();
  const subs = data.subscriptions[accountId];
  if (!subs) return;
  for (const sub of subs) {
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
  }
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

// ── HTML Pages ────────────────────────────────────────────────────────────

let loginTemplate = "";
let tokenTemplate = "";

async function loadTemplates() {
  const dir = new URL(".", import.meta.url).pathname;
  loginTemplate = await readFile(`${dir}pages/login.html`, "utf-8");
  tokenTemplate = await readFile(`${dir}pages/token.html`, "utf-8");
}

function renderLoginPage(sessions, message = null, error = null) {
  const rows = Object.entries(sessions)
    .map(([id, s]) => `<tr><td>${s.account_handle}</td><td>${s.account_display_name || "-"}</td><td>${new Date(s.created_at).toISOString().slice(0, 10)}</td><td><button onclick="if(confirm('Delete?')){fetch('/api/v1/sessions/${id}',{method:'DELETE'}).then(()=>location.reload())}">Delete</button></td></tr>`)
    .join("");
  const list = rows
    ? `<table><tr><th>Handle</th><th>Name</th><th>Date</th><th></th></tr>${rows}</table>`
    : "<p>No sessions</p>";
  const authUrl = `${HOLLO_URL}/oauth/authorize?response_type=code&client_id=${clientCredentials.client_id}&redirect_uri=urn:ietf:wg:oauth:2.0:oob&scope=read`;
  return loginTemplate
    .replace("{{MESSAGE}}", message ? `<div class="msg msg-ok">${message}</div>` : "")
    .replace("{{ERROR}}", error ? `<div class="msg msg-err">${error}</div>` : "")
    .replace("{{AUTH_URL}}", authUrl)
    .replace("{{SESSION_LIST}}", list)
    .replace("{{PORT}}", String(PORT));
}

function renderTokenPage(tokenData, account) {
  return tokenTemplate
    .replace("{{DISPLAY_NAME}}", account.display_name || account.acct)
    .replace("{{ACCT}}", account.acct)
    .replace("{{ACCESS_TOKEN}}", tokenData.access_token);
}

// ── Server ────────────────────────────────────────────────────────────────
const server = createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
  const path = url.pathname;
  
  // CORS
  if (path.startsWith("/api/") || path.startsWith("/auth/")) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Authorization, Content-Type");
    if (req.method === "OPTIONS") {
      res.writeHead(204);
      res.end();
      return;
    }
  }

  // ── Login page ────────────────────────────────────────────────────────
  if (path === "/" && req.method === "GET") {
    const sessions = await listOAuthSessions();
    const html = renderLoginPage(sessions);
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(html);
    return;
  }

  // ── OAuth: Login with code ────────────────────────────────────────────
  if (path === "/auth/login" && req.method === "POST") {
    let body = "";
    for await (const chunk of req) body += chunk;
    
    const params = new URLSearchParams(body);
    const code = params.get("code");
    
    if (!code) {
      const sessions = await listOAuthSessions();
      const html = renderLoginPage(sessions, null, "Authorization codeが必要です");
      res.writeHead(400, { "Content-Type": "text/html; charset=utf-8" });
      res.end(html);
      return;
    }
    
    try {
      // Exchange code for token
      const tokenData = await exchangeCodeForToken(code);
      
      // Validate token via DB to get account_id
      const tokenInfo = await verifyToken(tokenData.access_token);
      if (!tokenInfo?.accountOwnerId) {
        throw new Error("Token validation failed");
      }
      const account = tokenInfo.account;

      // Save session
      await addOAuthSession({
        access_token: tokenData.access_token,
        account_id: account.id,
        account_handle: account.acct,
        account_display_name: account.display_name,
        account_avatar: account.avatar,
        scopes: tokenData.scope?.split(" ") || [],
      });
      
      logger.auth("login success", { account: account.acct });
      
      const html = renderTokenPage(tokenData, account);
      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
      res.end(html);
    } catch (err) {
      logger.error("login failed", { error: err.message });
      const sessions = await listOAuthSessions();
      const html = renderLoginPage(sessions, null, `ログイン失敗: ${err.message}`);
      res.writeHead(400, { "Content-Type": "text/html; charset=utf-8" });
      res.end(html);
    }
    return;
  }

  // ── Remove session ────────────────────────────────────────────────────
  if (path.match(/^\/api\/v1\/sessions\/(.+)$/) && req.method === "DELETE") {
    const accountId = path.match(/^\/api\/v1\/sessions\/(.+)$/)[1];
    await removeOAuthSession(accountId);
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ success: true }));
    return;
  }

  // ── List sessions (API) ──────────────────────────────────────────────
  if (path === "/api/v1/sessions" && req.method === "GET") {
    const sessions = await listOAuthSessions();
    const list = Object.entries(sessions).map(([id, s]) => ({
      account_id: id,
      account_handle: s.account_handle,
      account_display_name: s.account_display_name,
      account_avatar: s.account_avatar,
      created_at: s.created_at,
    }));
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(list));
    return;
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
    
    const id = tokenInfo.accountOwnerId;

    if (req.method === "GET") {
      const sub = await loadSubscription(id);
      if (!sub) {
        res.writeHead(404, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Push subscription not found" }));
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
      if (parsed.data?.alerts) await updateAlerts(id, parsed.data.alerts);
      res.writeHead(200); res.end(JSON.stringify({}));
      return;
    }

    if (req.method === "DELETE") {
      await deleteSubscription(id);
      pushAccounts.delete(id);
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

  // ── Default: 404 ─────────────────────────────────────────────────────
  res.writeHead(404, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ error: "Not found" }));
});

// ── WebSocket server ────────────────────────────────────────────────────
const wss = new WebSocketServer({ noServer: true });
const activeStreams = new Map();
const pushAccounts = new Set();
const recentlySentNotifications = new Map();
const sentTlPosts = new Map();
const tlMaxIds = new Map();
const DEDUP_WINDOW_MS = 60_000;

server.on("upgrade", async (req, socket, head) => {
  const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
  const tokenFromQuery = url.searchParams.get("access_token");
  const authHeader = req.headers.authorization || "";
  const tokenFromHeader = authHeader.startsWith("Bearer ") ? authHeader.slice(7).trim() : null;
  const token = tokenFromQuery || tokenFromHeader;
  const stream = url.searchParams.get("stream") || "user";
  const listId = stream === "list" ? url.searchParams.get("list") : null;
  const tag = (stream === "hashtag" || stream === "hashtag:local")
    ? url.searchParams.get("tag")
    : null;

  logger.stream("upgrade request", {
    ua: req.headers["user-agent"],
    tokenFrom: tokenFromQuery ? "query" : tokenFromHeader ? "header" : "none",
    stream,
    listId,
  });
  
  if (!token) {
    socket.write("HTTP/1.1 401 Unauthorized\r\n\r\n");
    socket.destroy();
    return;
  }

  if (stream === "list" && !listId) {
    socket.write("HTTP/1.1 400 Bad Request\r\n\r\n");
    socket.destroy();
    return;
  }

  if ((stream === "hashtag" || stream === "hashtag:local") && !tag) {
    socket.write("HTTP/1.1 400 Bad Request\r\n\r\n");
    socket.destroy();
    return;
  }

  const validStreams = ["user", "user:notification", "list", "public", "public:local", "public:remote", "hashtag", "hashtag:local"];
  if (!validStreams.includes(stream)) {
    socket.write("HTTP/1.1 400 Bad Request\r\n\r\n");
    socket.destroy();
    return;
  }
  
  const tokenInfo = await verifyToken(token);
  if (!tokenInfo?.accountOwnerId) {
    socket.write("HTTP/1.1 401 Unauthorized\r\n\r\n");
    socket.destroy();
    return;
  }
  
  const account = tokenInfo.accountOwnerId;
  
  wss.handleUpgrade(req, socket, head, (ws) => {
    const streamEntry = { ws, stream, listId, tag, initialized: false, userAgent: req.headers["user-agent"] || "" };
    
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
    
    logger.stream("connected", { account, stream, listId });
    startPolling();
  });
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
    const accountIds = new Set([...activeStreams.keys(), ...pushAccounts]);
    if (accountIds.size === 0) { pollTimer = null; return; }
    
    for (const accountOwnerId of accountIds) {
      const streams = activeStreams.get(accountOwnerId);
      const hasUserStream = streams && [...streams].some(
        (s) => s.stream === "user" || s.stream === "user:notification",
      );
      const hasTimelineStream = streams && [...streams].some(
        (s) => s.stream === "user",
      );
      const listStreams = streams
        ? [...streams].filter((s) => s.stream === "list" && s.listId)
        : [];
      const listIds = [...new Set(listStreams.map((s) => s.listId))];
      
      // Get access token from OAuth sessions (補助情報)
      const sessions = await listOAuthSessions();
      const session = sessions[accountOwnerId];
      if (!session?.access_token) {
        logger.error("no access token for account", { account: accountOwnerId });
        continue;
      }
      
      const accessToken = session.access_token;
      
      // Notifications
      if (hasUserStream || pushAccounts.has(accountOwnerId)) {
        try {
          const sinceId = tlMaxIds.get(`notif_${accountOwnerId}`) || null;
          const { notifications, latestId } = await fetchNotificationsAPI(accessToken, sinceId);
          
          if (notifications.length > 0) {
            if (latestId) tlMaxIds.set(`notif_${accountOwnerId}`, latestId);
            
            // 初回接続時は既存通知を送信せず、チェックポイントのみ設定
            if (!sinceId) {
              logger.stream("notification checkpoint set", {
                account: accountOwnerId, latestId, count: notifications.length,
              });
            } else {
              let wsSent = 0;
              let wsSkipped = 0;
              let pushSent = 0;
              const subs = pushAccounts.has(accountOwnerId)
                ? await loadSubscriptions(accountOwnerId)
                : [];
              
              for (const n of notifications) {
                const sanitized = sanitizeNotification(n);

                for (const s of streams || []) {
                  if (s.ws.readyState !== 1) continue;
                  if (s.stream !== "user" && s.stream !== "user:notification") continue;

                  if (markNotificationSent(accountOwnerId, String(n.id), s.stream)) {
                    wsSkipped++;
                    continue;
                  }

                  const eventJson = JSON.stringify({
                    stream: [s.stream],
                    event: "notification",
                    payload: JSON.stringify(sanitized),
                  });
                  try { s.ws.send(eventJson); wsSent++; } catch (_) {}
                }
                
                if (subs.length > 0) {
                  const payload = buildPushPayload(sanitized, accessToken);
                  let sentForNotification = 0;
                  for (let i = subs.length - 1; i >= 0; i--) {
                    const sub = subs[i];
                    if (!shouldSendPush(sub.alerts, sanitized)) continue;
                    
                    const result = await sendPushNotification(sub, payload);
                    if (result.removed) {
                      await removePushSubscription(sub.endpoint);
                      subs.splice(i, 1);
                    } else if (result.ok) {
                      sentForNotification++;
                    }
                  }
                  pushSent += sentForNotification;
                  if (subs.length === 0) {
                    pushAccounts.delete(accountOwnerId);
                  }
                }
              }
              
              if (wsSent > 0 || wsSkipped > 0) {
                logger.stream("notification", {
                  account: accountOwnerId, sent: wsSent, skipped: wsSkipped,
                  fetched: notifications.length,
                });
              }
              if (pushSent > 0) {
                logger.push("notification", {
                  account: accountOwnerId, sent: pushSent,
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
          const sinceId = tlMaxIds.get(accountOwnerId) || null;
          const { statuses, latestId } = await fetchHomeTimelineAPI(accessToken, sinceId);

          if (statuses.length > 0) {
            if (latestId) tlMaxIds.set(accountOwnerId, latestId);

            // 初回接続時は既存投稿を送信せず、チェックポイントのみ設定
            if (!sinceId) {
              logger.stream("timeline checkpoint set", {
                account: accountOwnerId, latestId, count: statuses.length,
              });
            } else {
              let sent = 0;
              let skipped = 0;

              for (const status of statuses.reverse()) {
                if (markTlPostSent(accountOwnerId, String(status.id), "user")) {
                  skipped++;
                  continue;
                }

                for (const s of streams) {
                  if (s.stream === "user" && s.ws.readyState === 1) {
                    const eventJson = JSON.stringify({
                      stream: [s.stream],
                      event: "update",
                      payload: JSON.stringify(sanitizeStatus(status)),
                    });
                    try { s.ws.send(eventJson); sent++; } catch (_) {}
                  }
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
          const sinceId = tlMaxIds.get(`list:${lid}`) || null;
          const { statuses, latestId } = await fetchListTimelineAPI(accessToken, lid, sinceId);

          if (statuses.length > 0) {
            if (latestId) tlMaxIds.set(`list:${lid}`, latestId);

            if (!sinceId) {
              logger.stream("list timeline checkpoint set", {
                account: accountOwnerId, listId: lid, latestId, count: statuses.length,
              });
            } else {
              let sent = 0;
              let skipped = 0;

              for (const status of statuses.reverse()) {
                if (markTlPostSent(accountOwnerId, String(status.id), `list:${lid}`)) {
                  skipped++;
                  continue;
                }

                for (const s of listStreams) {
                  if (s.listId === lid && s.ws.readyState === 1) {
                    const eventJson = JSON.stringify({
                      stream: ["list"],
                      event: "update",
                      payload: JSON.stringify(sanitizeStatus(status)),
                    });
                    try { s.ws.send(eventJson); sent++; } catch (_) {}
                  }
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
      const publicStreams = streams
        ? [...streams].filter((s) =>
            s.stream === "public" ||
            s.stream === "public:local" ||
            s.stream === "public:remote")
        : [];
      const publicVariants = new Map();
      for (const s of publicStreams) {
        publicVariants.set(s.stream, { local: s.stream === "public:local", remote: s.stream === "public:remote" });
      }
      for (const [streamKey, { local, remote }] of publicVariants) {
        try {
          const sinceId = tlMaxIds.get(`${accountOwnerId}:${streamKey}`) || null;
          const { statuses, latestId } = await fetchPublicTimelineAPI(accessToken, { local, remote, sinceId });

          if (statuses.length > 0) {
            if (latestId) tlMaxIds.set(`${accountOwnerId}:${streamKey}`, latestId);

            if (!sinceId) {
              logger.stream("public timeline checkpoint set", {
                account: accountOwnerId, stream: streamKey, latestId, count: statuses.length,
              });
            } else {
              let sent = 0;
              let skipped = 0;

              for (const status of statuses.reverse()) {
                if (markTlPostSent(accountOwnerId, String(status.id), streamKey)) {
                  skipped++;
                  continue;
                }

                const applicableStreams = streams
                  ? [...streams].filter((s) => s.stream === streamKey)
                  : [];
                for (const s of applicableStreams) {
                  if (s.ws.readyState === 1) {
                    const eventJson = JSON.stringify({
                      stream: [streamKey],
                      event: "update",
                      payload: JSON.stringify(sanitizeStatus(status)),
                    });
                    try { s.ws.send(eventJson); sent++; } catch (_) {}
                  }
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
      const hashtagStreams = streams
        ? [...streams].filter((s) =>
            (s.stream === "hashtag" || s.stream === "hashtag:local") && s.tag)
        : [];
      const hashtagGroups = new Map();
      for (const s of hashtagStreams) {
        const key = `${s.stream}:${s.tag}`;
        if (!hashtagGroups.has(key)) {
          hashtagGroups.set(key, { stream: s.stream, tag: s.tag, local: s.stream === "hashtag:local" });
        }
      }
      for (const [key, { stream: streamKey, tag: htTag, local }] of hashtagGroups) {
        try {
          const sinceId = tlMaxIds.get(`${accountOwnerId}:hashtag:${htTag}${local ? ":local" : ""}`) || null;
          const { statuses, latestId } = await fetchHashtagTimelineAPI(accessToken, htTag, { local, sinceId });

          if (statuses.length > 0) {
            const dedupKey = `${accountOwnerId}:hashtag:${htTag}${local ? ":local" : ""}`;
            if (latestId) tlMaxIds.set(dedupKey, latestId);

            if (!sinceId) {
              logger.stream("hashtag timeline checkpoint set", {
                account: accountOwnerId, tag: htTag, local, latestId, count: statuses.length,
              });
            } else {
              let sent = 0;
              let skipped = 0;

              for (const status of statuses.reverse()) {
                if (markTlPostSent(accountOwnerId, String(status.id), dedupKey)) {
                  skipped++;
                  continue;
                }

                const applicableStreams = streams
                  ? [...streams].filter((s) => s.stream === streamKey && s.tag === htTag)
                  : [];
                for (const s of applicableStreams) {
                  if (s.ws.readyState === 1) {
                    const eventJson = JSON.stringify({
                      stream: ["hashtag"],
                      event: "update",
                      payload: JSON.stringify(sanitizeStatus(status)),
                    });
                    try { s.ws.send(eventJson); sent++; } catch (_) {}
                  }
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
    
    if (activeStreams.size === 0 && pushAccounts.size === 0) {
      pollTimer = null;
      return;
    }
    pollTimer = setTimeout(poll, POLL_INTERVAL_MS);
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
// Start server after app registration
registerApp().then(async () => {
  await loadTemplates();
  await loadExistingPushSubscriptions();
  if (pushAccounts.size > 0) startPolling();
  server.listen(PORT, () => {
    logger.info(`Hollo Stream Proxy listening on port ${PORT}`);
    logger.info(`Hollo URL: ${HOLLO_URL}`);
    logger.info(`Login page: http://localhost:${PORT}/`);
  });
}).catch((err) => {
  logger.error("failed to start: app registration error", { error: err.message });
  process.exit(1);
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
