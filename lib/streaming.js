import { WebSocketServer } from "ws";
import webpush from "web-push";
import { VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY, VAPID_SUBJECT, DEDUP_WINDOW_MS, WS_MAX_PAYLOAD, WS_MAX_CONNECTIONS, WS_MAX_MESSAGES_PER_MINUTE } from "./config.js";
import { logger } from "./logger.js";
import { validateEndpointHost } from "./pushSubscriptions.js";

if (VAPID_PRIVATE_KEY) {
  webpush.setVapidDetails(VAPID_SUBJECT, VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);
  logger.info("web-push VAPID initialized");
} else {
  logger.warn("VAPID_PRIVATE_KEY not set — push delivery disabled");
}

export const wss = new WebSocketServer({ noServer: true, maxPayload: WS_MAX_PAYLOAD });
wss.on("error", (err) => {
  logger.error("websocket server error", { error: err.message });
});

let wsConnectionCount = 0;

export function tryAcquireWsSlot() {
  if (wsConnectionCount >= WS_MAX_CONNECTIONS) return false;
  wsConnectionCount++;
  return true;
}

export function releaseWsSlot() {
  if (wsConnectionCount > 0) wsConnectionCount--;
}

export function isWsMessageAllowed(state) {
  const now = Date.now();
  if (now - state.windowStart > 60_000) {
    state.windowStart = now;
    state.count = 0;
  }
  state.count++;
  return state.count <= WS_MAX_MESSAGES_PER_MINUTE;
}

export const activeStreams = new Map();
export const pushAccounts = new Set();

export const VALID_STREAMS = [
  "user",
  "user:notification",
  "list",
  "public",
  "public:local",
  "public:remote",
  "hashtag",
  "hashtag:local",
  "direct",
];

const MAX_STREAM_PARAM_LENGTH = 256;

export function subscriptionKey(stream, { list = null, tag = null } = {}) {
  return JSON.stringify([stream, list ?? null, tag ?? null]);
}

export function validateStreamRequest(stream, params = {}) {
  if (typeof stream !== "string" || !VALID_STREAMS.includes(stream)) {
    return "Unknown stream type";
  }
  if (stream === "list") {
    const listId = params.list;
    if (typeof listId !== "string" || listId.length === 0 || listId.length > MAX_STREAM_PARAM_LENGTH) {
      return "Missing list parameter";
    }
  }
  if (stream === "hashtag" || stream === "hashtag:local") {
    const tag = params.tag;
    if (typeof tag !== "string" || tag.length === 0 || tag.length > MAX_STREAM_PARAM_LENGTH) {
      return "Missing tag parameter";
    }
  }
  return null;
}

const recentlySentNotifications = new Map();
const sentTlPosts = new Map();
export const tlMaxIds = new Map();

export function markNotificationSent(accountOwnerId, notificationId, streamKey) {
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

export function markTlPostSent(accountOwnerId, postId, streamKey) {
  const compoundKey = `${accountOwnerId}:${streamKey}`;
  if (!sentTlPosts.has(compoundKey)) {
    sentTlPosts.set(compoundKey, new Set());
  }
  const set = sentTlPosts.get(compoundKey);
  if (set.has(String(postId))) return true;
  set.add(String(postId));
  return false;
}

export async function sendPushNotification(sub, payload) {
  if (!VAPID_PRIVATE_KEY) {
    return { ok: false, code: null, message: "VAPID_PRIVATE_KEY not configured" };
  }
  if (await validateEndpointHost(sub.endpoint)) {
    logger.push("endpoint no longer resolves to a public address, send skipped", { endpoint: sub.endpoint });
    return { ok: false, code: null, message: "endpoint blocked" };
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

export { VAPID_PUBLIC_KEY };
