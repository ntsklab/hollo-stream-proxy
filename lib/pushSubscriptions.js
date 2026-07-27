import { readFile, writeFile, mkdir, rename } from "node:fs/promises";
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
