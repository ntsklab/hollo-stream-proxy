import { WebSocketServer } from "ws";
import webpush from "web-push";
import { VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY, VAPID_SUBJECT, DEDUP_WINDOW_MS } from "./config.js";
import { logger } from "./logger.js";

if (VAPID_PRIVATE_KEY) {
  webpush.setVapidDetails(VAPID_SUBJECT, VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);
  logger.info("web-push VAPID initialized");
} else {
  logger.warn("VAPID_PRIVATE_KEY not set — push delivery disabled");
}

export const wss = new WebSocketServer({ noServer: true });
wss.on("error", (err) => {
  logger.error("websocket server error", { error: err.message });
});

export const activeStreams = new Map();
export const pushAccounts = new Set();
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
