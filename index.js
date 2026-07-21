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
import pg from "pg";

// ─ Config ────────────────────────────────────────────────────────────────
const PORT = parseInt(process.env.PORT || "3001", 10);
const HOLLO_URL = process.env.HOLLO_URL;
const HOLLO_INTERNAL_URL = process.env.HOLLO_INTERNAL_URL || HOLLO_URL;
const DATABASE_URL = process.env.DATABASE_URL;
if (!HOLLO_URL) {
  console.error("FATAL: HOLLO_URL environment variable is required");
  process.exit(1);
}
if (!DATABASE_URL) {
  console.error("FATAL: DATABASE_URL environment variable is required");
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

// ── PostgreSQL pool ───────────────────────────────────────────────────────
const pool = new pg.Pool({
  connectionString: DATABASE_URL,
  max: 10,
  idleTimeoutMillis: 60_000,
});

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

// ── Validate token via DB (oauth_tokensテーブル) ───────────────────────
async function validateToken(token) {
  if (!token) return null;
  
  // キャッシュ確認
  const cached = tokenCache.get(token);
  if (cached) {
    if (cached.valid && cached.expiresAt > Date.now()) return cached;
    if (!cached.valid && cached.expiresAt > Date.now()) return null;
  }
  
  try {
    // access_tokensテーブルでトークンを検索（Holloの正しいテーブル名）
    const result = await pool.query(
      `SELECT account_owner_id, scopes, created
       FROM access_tokens
       WHERE code = $1`,
      [token],
    );
    
    if (result.rows.length === 0) {
      // トークンが見つからない
      const info = { valid: false, expiresAt: Date.now() + TOKEN_CACHE_NEG_TTL_MS };
      tokenCache.set(token, info);
      return null;
    }
    
    const row = result.rows[0];
    const info = {
      accountOwnerId: row.account_owner_id,
      scopes: row.scopes ? row.scopes : [],
      valid: true,
      expiresAt: Date.now() + TOKEN_CACHE_TTL_MS,
    };
    tokenCache.set(token, info);
    return info;
  } catch (err) {
    logger.error("token validation failed", { error: err.message });
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

// ── Get account info from DB ─────────────────────────────────────────────
async function getAccountInfo(accountId) {
  try {
    const result = await pool.query(
      `SELECT id, handle, name, avatar_url
       FROM accounts
       WHERE id = $1`,
      [accountId],
    );
    
    if (result.rows.length === 0) return null;
    
    const row = result.rows[0];
    return {
      id: row.id,
      acct: row.handle,
      display_name: row.name,
      avatar: row.avatar_url,
    };
  } catch (err) {
    logger.error("get account info failed", { error: err.message });
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
      reblog: alertsData.reblog !== false,
      favourite: alertsData.favourite !== false,
      follow: alertsData.follow !== false,
      poll: alertsData.poll !== false,
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
      reblog: alertsData.reblog !== false,
      favourite: alertsData.favourite !== false,
      follow: alertsData.follow !== false,
      poll: alertsData.poll !== false,
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
function renderLoginPage(sessions, message = null, error = null) {
  const sessionList = Object.entries(sessions)
    .map(([id, s]) => `
      <li class="session-item">
        <div class="session-info">
          <img src="${s.account_avatar || ''}" alt="" class="avatar" onerror="this.style.display='none'">
          <div>
            <strong>${s.account_display_name || s.account_handle}</strong>
            <div class="handle">${s.account_handle}</div>
            <div class="time">ログイン: ${new Date(s.created_at).toLocaleString('ja-JP')}</div>
          </div>
        </div>
        <button onclick="if(confirm('このセッションを削除しますか？')){fetch('/api/v1/sessions/${id}',{method:'DELETE'}).then(()=>location.reload())}" class="btn-danger">削除</button>
      </li>
    `)
    .join("");
  
  return `<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Hollo Stream Proxy</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; max-width: 700px; margin: 0 auto; padding: 20px; background: #f8f9fa; color: #333; }
    h1 { color: #6364ff; margin-bottom: 20px; font-size: 24px; }
    .card { background: white; border-radius: 8px; padding: 20px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
    .btn { display: inline-block; padding: 10px 20px; background: #6364ff; color: white; border: none; border-radius: 6px; cursor: pointer; text-decoration: none; font-size: 14px; }
    .btn:hover { background: #5253e0; }
    .btn-danger { background: #dc3545; }
    .btn-danger:hover { background: #c82333; }
    .btn-success { background: #28a745; }
    .btn-success:hover { background: #218838; }
    input[type="text"] { width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 6px; font-size: 14px; margin: 10px 0; font-family: monospace; }
    .session-list { list-style: none; }
    .session-item { display: flex; justify-content: space-between; align-items: center; padding: 15px 0; border-bottom: 1px solid #eee; }
    .session-item:last-child { border-bottom: none; }
    .session-info { display: flex; gap: 12px; align-items: center; }
    .avatar { width: 48px; height: 48px; border-radius: 50%; object-fit: cover; }
    .handle { color: #666; font-size: 13px; }
    .time { color: #999; font-size: 12px; margin-top: 4px; }
    .message { padding: 12px; border-radius: 6px; margin-bottom: 15px; }
    .message.success { background: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
    .message.error { background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
    .token-display { background: #f8f9fa; padding: 15px; border-radius: 6px; margin: 15px 0; word-break: break-all; font-family: monospace; font-size: 12px; border: 1px solid #e9ecef; }
    .info { color: #666; font-size: 14px; line-height: 1.6; }
    .warning { background: #fff3cd; color: #856404; padding: 12px; border-radius: 6px; margin: 15px 0; border: 1px solid #ffeaa7; font-size: 13px; }
    .step { background: #e7f3ff; padding: 12px; border-radius: 6px; margin: 10px 0; border-left: 4px solid #6364ff; }
    .step strong { color: #6364ff; }
    .copy-btn { padding: 4px 10px; font-size: 12px; margin-left: 8px; cursor: pointer; background: #6c757d; color: white; border: none; border-radius: 4px; }
    .copy-btn:hover { background: #5a6268; }
    h2 { font-size: 18px; margin-bottom: 15px; color: #444; }
    h3 { font-size: 15px; margin: 15px 0 10px; color: #555; }
    pre { background: #f8f9fa; padding: 15px; border-radius: 6px; overflow-x: auto; font-size: 12px; border: 1px solid #e9ecef; }
  </style>
</head>
<body>
  <h1>🔵 Hollo Stream Proxy</h1>
  
  ${message ? `<div class="message success">${message}</div>` : ''}
  ${error ? `<div class="message error">${error}</div>` : ''}
  
  <div class="card">
    <h2>新規ログイン</h2>
    <div class="step">
      <strong>ステップ1:</strong> 下のリンクをクリックしてHolloで認証し、表示される<b>authorization code</b>をコピーしてください
    </div>
    <a href="${HOLLO_URL}/oauth/authorize?response_type=code&client_id=${clientCredentials.client_id}&redirect_uri=urn:ietf:wg:oauth:2.0:oob&scope=read" target="_blank" class="btn">Holloで認証する</a>
    
    <div class="step" style="margin-top: 15px;">
      <strong>ステップ2:</strong> コピーしたcodeを下のフォームに貼り付けて送信してください
    </div>
    <form method="POST" action="/auth/login">
      <input type="text" name="code" placeholder="Authorization codeを貼り付け..." required autocomplete="off">
      <button type="submit" class="btn btn-success">ログイン</button>
    </form>
  </div>
  
  <div class="card">
    <h2>ログイン済みセッション</h2>
    ${sessionList || '<p class="info">ログイン済みセッションはありません</p>'}
  </div>
  
  <div class="card">
    <h2>API エンドポイント</h2>
    <div class="info">
      <p>
        以下の接続には、<strong>Hollo本体で各クライアントに発行した既存のアクセストークン</strong>を使用してください。<br>
        （ここでOAuthログインして取得するトークンはproxyのポーリング用です。）
      </p>
      <p><strong>WebSocket Streaming:</strong></p>
      <div class="token-display">
        ws://localhost:${PORT}/api/v1/streaming?access_token=<em>Holloのアクセストークン</em>&stream=user
      </div>
      <p><strong>WebPush Subscription:</strong></p>
      <div class="token-display">
        POST http://localhost:${PORT}/api/v1/push/subscription
      </div>
    </div>
  </div>
</body>
</html>`;
}

function renderTokenPage(tokenData, account) {
  return `<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Login Success - Hollo Stream Proxy</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; max-width: 700px; margin: 0 auto; padding: 20px; background: #f8f9fa; color: #333; }
    h1 { color: #6364ff; margin-bottom: 20px; font-size: 24px; }
    .card { background: white; border-radius: 8px; padding: 20px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
    .btn { display: inline-block; padding: 10px 20px; background: #6364ff; color: white; border: none; border-radius: 6px; cursor: pointer; text-decoration: none; font-size: 14px; }
    .btn:hover { background: #5253e0; }
    .token-display { background: #f8f9fa; padding: 15px; border-radius: 6px; margin: 15px 0; word-break: break-all; font-family: monospace; font-size: 12px; border: 1px solid #e9ecef; position: relative; }
    .copy-btn { padding: 4px 10px; font-size: 12px; margin-left: 8px; cursor: pointer; background: #6c757d; color: white; border: none; border-radius: 4px; }
    .copy-btn:hover { background: #5a6268; }
    .info { color: #666; font-size: 14px; line-height: 1.6; }
    .success { background: #d4edda; color: #155724; padding: 15px; border-radius: 6px; margin: 15px 0; border: 1px solid #c3e6cb; }
    h2 { font-size: 18px; margin-bottom: 15px; color: #444; }
    pre { background: #f8f9fa; padding: 15px; border-radius: 6px; overflow-x: auto; font-size: 12px; border: 1px solid #e9ecef; }
  </style>
</head>
<body>
  <h1>✅ ログイン成功</h1>
  <div class="card">
    <div class="success">
      <strong>${account.display_name || account.acct}</strong> (@${account.acct}) としてログインしました。
    </div>
    
    <h2>ポーリング用トークン</h2>
    <p class="info">
      このトークンは <strong>stream-proxyがHollo APIをポーリングするため</strong> に使用されます。<br>
      WebSocketクライアントやMastodonアプリで使用するトークンとは<strong>別物</strong>です。
    </p>
    <div class="token-display">
      ${tokenData.access_token}
      <button class="copy-btn" onclick="navigator.clipboard.writeText('${tokenData.access_token}').then(()=>this.textContent='コピーしました！')">コピー</button>
    </div>

    <h2>WebSocketクライアント接続例</h2>
    <p class="info">
      WebSocketに接続する際は、<strong>Hollo本体で既に発行されている各クライアント用のアクセストークン</strong>を使用してください。
    </p>
    <pre>WebSocket: ws://localhost:${PORT}/api/v1/streaming?access_token=<em>あなたのHolloアクセストークン</em>&stream=user</pre>

    <a href="/" class="btn">戻る</a>
  </div>
</body>
</html>`;
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
      const tokenInfo = await validateToken(tokenData.access_token);
      if (!tokenInfo?.accountOwnerId) {
        throw new Error("Token validation failed");
      }
      
      // Get account info from DB
      const account = await getAccountInfo(tokenInfo.accountOwnerId);
      if (!account) {
        throw new Error("Account not found");
      }
      
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
    
    const tokenInfo = await validateToken(bearerToken);
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
  
  logger.stream("upgrade request", {
    ua: req.headers["user-agent"],
    tokenFrom: tokenFromQuery ? "query" : tokenFromHeader ? "header" : "none",
    stream,
  });
  
  if (!token) {
    socket.write("HTTP/1.1 401 Unauthorized\r\n\r\n");
    socket.destroy();
    return;
  }
  
  const tokenInfo = await validateToken(token);
  if (!tokenInfo?.accountOwnerId) {
    socket.write("HTTP/1.1 401 Unauthorized\r\n\r\n");
    socket.destroy();
    return;
  }
  
  const account = tokenInfo.accountOwnerId;
  
  wss.handleUpgrade(req, socket, head, (ws) => {
    const streamEntry = { ws, stream, initialized: false, userAgent: req.headers["user-agent"] || "" };
    
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
    
    logger.stream("connected", { account, stream });
    startPolling();
  });
});

function markNotificationSent(accountOwnerId, notificationId) {
  const now = Date.now();
  if (!recentlySentNotifications.has(accountOwnerId)) {
    recentlySentNotifications.set(accountOwnerId, new Map());
  }
  const map = recentlySentNotifications.get(accountOwnerId);
  for (const [id, ts] of map.entries()) {
    if (now - ts > DEDUP_WINDOW_MS) map.delete(id);
  }
  if (map.has(notificationId)) return true;
  map.set(notificationId, now);
  return false;
}

function markTlPostSent(accountOwnerId, postId) {
  if (!sentTlPosts.has(accountOwnerId)) {
    sentTlPosts.set(accountOwnerId, new Set());
  }
  const set = sentTlPosts.get(accountOwnerId);
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
            } else if (streams && streams.size > 0) {
              let wsSent = 0;
              let wsSkipped = 0;
              
              for (const n of notifications) {
                if (markNotificationSent(accountOwnerId, String(n.id))) {
                  wsSkipped++;
                  continue;
                }
                
                const sanitized = sanitizeNotification(n);
                
                for (const s of streams) {
                  if (
                    s.ws.readyState === 1 &&
                    (s.stream === "user" || s.stream === "user:notification")
                  ) {
                    const eventJson = JSON.stringify({
                      stream: [s.stream],
                      event: "notification",
                      payload: JSON.stringify(sanitized),
                    });
                    try { s.ws.send(eventJson); wsSent++; } catch (_) {}
                  }
                }
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
                if (markTlPostSent(accountOwnerId, String(status.id))) {
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

// ─ Start server ──────────────────────────────────────────────────────────
// Start server after app registration
registerApp().then(() => {
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
  clearInterval(pollTimer);
  await pool.end();
  server.close();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
