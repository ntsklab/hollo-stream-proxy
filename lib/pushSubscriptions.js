import { readFile, writeFile, mkdir, rename } from "node:fs/promises";
import { lookup } from "node:dns/promises";
import { isIP } from "node:net";
import { DATA_DIR, SUBS_FILE } from "./config.js";
import { logger } from "./logger.js";

export async function loadSubsFile() {
  try {
    return JSON.parse(await readFile(SUBS_FILE, "utf8"));
  } catch {
    return { subscriptions: {}, push_since: {} };
  }
}

export async function saveSubsFile(data) {
  await mkdir(DATA_DIR, { recursive: true });
  const tmp = `${SUBS_FILE}.tmp`;
  await writeFile(tmp, JSON.stringify(data, null, 2));
  await rename(tmp, SUBS_FILE);
}

function isBlockedIp(addr) {
  const family = isIP(addr);
  if (family === 4) {
    const p = addr.split(".").map(Number);
    if (p[0] === 10) return true;
    if (p[0] === 172 && p[1] >= 16 && p[1] <= 31) return true;
    if (p[0] === 192 && p[1] === 168) return true;
    if (p[0] === 127) return true;
    if (p[0] === 169 && p[1] === 254) return true;
    if (p[0] === 0) return true;
    return false;
  }
  if (family === 6) {
    const a = addr.toLowerCase();
    if (a === "::1" || a === "::") return true;
    if (a.startsWith("fc") || a.startsWith("fd")) return true;
    if (a.startsWith("fe80:")) return true;
    if (a.startsWith("::ffff:")) return isBlockedIp(a.slice(7));
    return false;
  }
  return true;
}

function decodeKey(value, maxBytes) {
  if (typeof value !== "string" || value.length > 512) return null;
  const normalized = value.replace(/-/g, "+").replace(/_/g, "/");
  const padded = normalized + "=".repeat((4 - (normalized.length % 4)) % 4);
  let buf;
  try {
    buf = Buffer.from(padded, "base64");
  } catch {
    return null;
  }
  if (buf.length === 0 || buf.length > maxBytes) return null;
  return buf;
}

export async function validatePushSubscription(sub) {
  if (!sub?.endpoint || typeof sub.endpoint !== "string" || sub.endpoint.length > 2048) {
    return "Invalid subscription";
  }
  let url;
  try {
    url = new URL(sub.endpoint);
  } catch {
    return "Invalid subscription";
  }
  if (url.protocol !== "https:") return "Invalid subscription";
  if (url.username || url.password) return "Invalid subscription";
  if (!decodeKey(sub.keys?.p256dh, 128)) return "Invalid subscription";
  if (!decodeKey(sub.keys?.auth, 64)) return "Invalid subscription";
  const host = url.hostname.replace(/^\[|\]$/g, "");
  if (isIP(host)) {
    if (isBlockedIp(host)) return "Invalid subscription";
  } else {
    let addrs;
    try {
      addrs = await lookup(host, { all: true });
    } catch {
      return "Invalid subscription";
    }
    if (addrs.length === 0 || addrs.some((a) => isBlockedIp(a.address))) {
      return "Invalid subscription";
    }
  }
  return null;
}

export async function saveSubscription(accountId, sub, alertsData, accessToken) {
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

export async function loadSubscription(accountId, accessToken) {
  const data = await loadSubsFile();
  const subs = data.subscriptions[accountId];
  if (!subs || subs.length === 0) return null;
  return subs.find(s => s.access_token === accessToken) || null;
}

export async function loadSubscriptions(accountId) {
  const data = await loadSubsFile();
  return data.subscriptions[accountId] || [];
}

export async function deleteSubscription(accountId, accessToken) {
  const data = await loadSubsFile();
  const subs = data.subscriptions[accountId];
  if (!subs) return;
  data.subscriptions[accountId] = subs.filter(s => s.access_token !== accessToken);
  if (data.subscriptions[accountId].length === 0) {
    delete data.subscriptions[accountId];
  }
  await saveSubsFile(data);
}

export async function updateAlerts(accountId, alertsData, accessToken) {
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

export async function removePushSubscription(endpoint) {
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

export function shouldSendPush(alerts, notification) {
  const type = notification.type;
  if (type === "emoji_reaction" || type === "reaction") {
    return alerts.reaction !== false || alerts.favourite !== false;
  }
  const key = type.replace(/\./g, "_");
  return alerts[key] !== false;
}

export function buildPushPayload(notification, accessToken) {
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
