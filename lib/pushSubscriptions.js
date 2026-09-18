import { readFile, writeFile, mkdir, rename, chmod } from "node:fs/promises";
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
  await writeFile(tmp, JSON.stringify(data, null, 2), { mode: 0o600 });
  await rename(tmp, SUBS_FILE);
  try {
    await chmod(SUBS_FILE, 0o600);
  } catch (err) {
    logger.warn("failed to restrict subscription file permissions", { error: err.message });
  }
}

const BLOCKED_V4 = [
  ["0.0.0.0", 8],
  ["10.0.0.0", 8],
  ["100.64.0.0", 10],
  ["127.0.0.0", 8],
  ["169.254.0.0", 16],
  ["172.16.0.0", 12],
  ["192.0.0.0", 24],
  ["192.0.2.0", 24],
  ["192.168.0.0", 16],
  ["198.18.0.0", 15],
  ["198.51.100.0", 24],
  ["203.0.113.0", 24],
  ["224.0.0.0", 4],
  ["240.0.0.0", 4],
].map(([net, bits]) => [ipv4ToInt(net), bits]);

function ipv4ToInt(addr) {
  const p = addr.split(".").map(Number);
  return ((p[0] * 256 + p[1]) * 256 + p[2]) * 256 + p[3];
}

function isBlockedIpv4(addr) {
  const n = ipv4ToInt(addr);
  return BLOCKED_V4.some(([net, bits]) => bits === 0 || (n >>> (32 - bits)) === (net >>> (32 - bits)));
}

function expandIPv6(addr) {
  const halves = addr.split("::");
  if (halves.length > 2) return null;
  const parseSide = (s) => {
    if (s === "") return [];
    const out = [];
    for (const g of s.split(":")) {
      const v4 = g.match(/^(\d+)\.(\d+)\.(\d+)\.(\d+)$/);
      if (v4) {
        const b = v4.slice(1).map(Number);
        if (b.some((x) => !Number.isInteger(x) || x < 0 || x > 255)) return null;
        out.push((b[0] * 256 + b[1]).toString(16), (b[2] * 256 + b[3]).toString(16));
      } else {
        if (!/^[0-9a-f]{1,4}$/.test(g)) return null;
        out.push(g);
      }
    }
    return out;
  };
  const head = parseSide(halves[0]);
  if (head === null) return null;
  if (halves.length === 1) return head.length === 8 ? head : null;
  const tail = parseSide(halves[1]);
  if (tail === null || head.length + tail.length > 8) return null;
  return [...head, ...new Array(8 - head.length - tail.length).fill("0"), ...tail];
}

const BLOCKED_V6 = [
  [[0, 0, 0, 0, 0, 0, 0, 0], 128],
  [[0, 0, 0, 0, 0, 0, 0, 1], 128],
  [[0xfc00], 7],
  [[0xfe80], 10],
  [[0xfec0], 10],
  [[0xff00], 8],
  [[0x2001, 0xdb8], 32],
  [[0x2001, 0x2], 48],
  [[0x2002], 16],
];

function isBlockedIpv6(addr) {
  const groups = expandIPv6(addr);
  if (groups === null) return true;
  const nums = groups.map((g) => parseInt(g, 16));
  if (nums[0] === 0 && nums[1] === 0 && nums[2] === 0 && nums[3] === 0 && nums[4] === 0 && nums[5] === 0xffff) {
    return isBlockedIpv4(`${nums[6] >>> 8}.${nums[6] & 0xff}.${nums[7] >>> 8}.${nums[7] & 0xff}`);
  }
  if (nums[0] === 0x64 && nums[1] === 0xff9b && nums[2] === 0 && nums[3] === 0 && nums[4] === 0 && nums[5] === 0) {
    return isBlockedIpv4(`${nums[6] >>> 8}.${nums[6] & 0xff}.${nums[7] >>> 8}.${nums[7] & 0xff}`);
  }
  return BLOCKED_V6.some(([prefix, bits]) => {
    const full = Math.floor(bits / 16);
    for (let i = 0; i < full; i++) {
      if (nums[i] !== (prefix[i] ?? 0)) return false;
    }
    const rem = bits % 16;
    if (rem > 0) {
      const mask = (0xffff << (16 - rem)) & 0xffff;
      if ((nums[full] & mask) !== (prefix[full] & mask)) return false;
    }
    return true;
  });
}

function isBlockedIp(addr) {
  const family = isIP(addr);
  if (family === 4) return isBlockedIpv4(addr);
  if (family === 6) return isBlockedIpv6(addr.toLowerCase());
  return true;
}

function decodeKey(value, exactBytes) {
  if (typeof value !== "string" || value.length > 512) return null;
  const normalized = value.replace(/-/g, "+").replace(/_/g, "/");
  const padded = normalized + "=".repeat((4 - (normalized.length % 4)) % 4);
  let buf;
  try {
    buf = Buffer.from(padded, "base64");
  } catch {
    return null;
  }
  if (buf.length !== exactBytes) return null;
  return buf;
}

export async function validateEndpointHost(endpoint) {
  if (!endpoint || typeof endpoint !== "string" || endpoint.length > 2048) {
    return "Invalid subscription";
  }
  let url;
  try {
    url = new URL(endpoint);
  } catch {
    return "Invalid subscription";
  }
  if (url.protocol !== "https:") return "Invalid subscription";
  if (url.username || url.password) return "Invalid subscription";
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

export async function validatePushSubscription(sub) {
  const hostError = await validateEndpointHost(sub?.endpoint);
  if (hostError) return hostError;
  if (!decodeKey(sub.keys?.p256dh, 65)) return "Invalid subscription";
  if (!decodeKey(sub.keys?.auth, 16)) return "Invalid subscription";
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
