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

import { mkdir } from "node:fs/promises";
import { createServer } from "node:http";
import { PORT, DATA_DIR } from "./lib/config.js";
import { logger } from "./lib/logger.js";
import { verifyToken } from "./lib/verifyClientToken.js";
import { fetchInstanceAPI } from "./lib/holloApiClient.js";
import {
  saveSubscription,
  loadSubscription,
  loadSubscriptions,
  deleteSubscription,
  updateAlerts,
} from "./lib/pushSubscriptions.js";
import {
  wss,
  activeStreams,
  pushAccounts,
  VAPID_PUBLIC_KEY,
} from "./lib/streaming.js";
import { startPolling, getPollTimer, setPollTimer } from "./lib/polling.js";

await mkdir(DATA_DIR, { recursive: true });

const server = createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
  const path = url.pathname;

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

    const tokenInfo = await verifyToken(bearerToken, "notifications");
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

  if (path === "/health" && req.method === "GET") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("OK");
    return;
  }

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

  res.writeHead(404, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ error: "Not found" }));
});

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

async function loadExistingPushSubscriptions() {
  const { loadSubsFile } = await import("./lib/pushSubscriptions.js");
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

await loadExistingPushSubscriptions();
if (pushAccounts.size > 0) startPolling();
server.listen(PORT, () => {
  logger.info(`Hollo Stream Proxy listening on port ${PORT}`);
  logger.info(`Hollo URL: ${process.env.HOLLO_URL}`);
});

async function shutdown() {
  logger.info("shutting down...");
  const timer = getPollTimer();
  if (timer) clearTimeout(timer);
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
