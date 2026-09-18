import { HOLLO_URL, HOLLO_INTERNAL_URL, PUBLIC_URL } from "./config.js";
import { logger } from "./logger.js";

const FETCH_TIMEOUT_MS = 10_000;

async function fetchTimelineJson(url, accessToken) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    return await fetch(url, {
      headers: { Authorization: `Bearer ${accessToken}` },
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timeout);
  }
}

function applyPagination(params, { minId = null, sinceId = null, maxId = null } = {}) {
  if (minId) params.set("min_id", minId);
  else if (sinceId) params.set("since_id", sinceId);
  if (maxId) params.set("max_id", maxId);
}

export async function fetchHomeTimelineAPI(accessToken, { minId = null, sinceId = null, maxId = null } = {}) {
  const params = new URLSearchParams({ limit: "40" });
  applyPagination(params, { minId, sinceId, maxId });

  const res = await fetchTimelineJson(
    `${HOLLO_URL}/api/v1/timelines/home?${params}`,
    accessToken,
  );

  if (!res.ok) {
    logger.error("timeline API error", { status: res.status });
    return { statuses: [], latestId: null };
  }

  const statuses = await res.json();
  const latestId = statuses.length > 0 ? statuses[0].id : null;

  return { statuses, latestId };
}

export async function fetchListTimelineAPI(accessToken, listId, { minId = null, sinceId = null, maxId = null } = {}) {
  const params = new URLSearchParams({ limit: "40" });
  applyPagination(params, { minId, sinceId, maxId });

  const res = await fetchTimelineJson(
    `${HOLLO_URL}/api/v1/timelines/list/${encodeURIComponent(listId)}?${params}`,
    accessToken,
  );

  if (!res.ok) {
    logger.error("list timeline API error", { status: res.status, listId });
    return { statuses: [], latestId: null };
  }

  const statuses = await res.json();
  const latestId = statuses.length > 0 ? statuses[0].id : null;

  return { statuses, latestId };
}

function stripPort(host) {
  if (host.startsWith("[")) {
    const end = host.indexOf("]");
    return end >= 0 ? host.slice(1, end) : host;
  }
  const first = host.indexOf(":");
  if (first >= 0 && first === host.lastIndexOf(":")) return host.slice(0, first);
  return host;
}

let cachedPublicOrigin;
function getPublicOrigin(req) {
  if (cachedPublicOrigin === undefined) {
    cachedPublicOrigin = null;
    if (PUBLIC_URL) {
      try {
        const parsed = new URL(PUBLIC_URL);
        cachedPublicOrigin = { host: parsed.host, domain: parsed.hostname.replace(/^\[|\]$/g, "") };
      } catch {
        logger.warn("ignoring invalid PUBLIC_URL", { PUBLIC_URL });
      }
    }
  }
  if (cachedPublicOrigin) return cachedPublicOrigin;
  const host = req.headers.host || "localhost";
  return { host, domain: stripPort(host).toLowerCase() };
}

export async function fetchInstanceAPI(req, apiVersion = "v1") {
  const res = await fetch(`${HOLLO_INTERNAL_URL}/api/${apiVersion}/instance`, {
    headers: { Accept: "application/json" },
  });
  if (!res.ok) {
    throw new Error(`instance API error: ${res.status}`);
  }
  const data = await res.json();

  const { host: publicHost, domain: publicDomain } = getPublicOrigin(req);
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

export async function fetchPublicTimelineAPI(accessToken, { local = false, remote = false, minId = null, sinceId = null, maxId = null } = {}) {
  const params = new URLSearchParams({ limit: "40" });
  if (local) params.set("local", "true");
  if (remote) params.set("remote", "true");
  applyPagination(params, { minId, sinceId, maxId });

  const res = await fetchTimelineJson(
    `${HOLLO_URL}/api/v1/timelines/public?${params}`,
    accessToken,
  );

  if (!res.ok) {
    logger.error("public timeline API error", { status: res.status, local, remote });
    return { statuses: [], latestId: null };
  }

  const statuses = await res.json();
  const latestId = statuses.length > 0 ? statuses[0].id : null;
  return { statuses, latestId };
}

export async function fetchHashtagTimelineAPI(accessToken, tag, { local = false, minId = null, sinceId = null, maxId = null } = {}) {
  const params = new URLSearchParams({ limit: "40" });
  if (local) params.set("local", "true");
  applyPagination(params, { minId, sinceId, maxId });

  const res = await fetchTimelineJson(
    `${HOLLO_URL}/api/v1/timelines/tag/${encodeURIComponent(tag)}?${params}`,
    accessToken,
  );

  if (!res.ok) {
    logger.error("hashtag timeline API error", { status: res.status, tag, local });
    return { statuses: [], latestId: null };
  }

  const statuses = await res.json();
  const latestId = statuses.length > 0 ? statuses[0].id : null;
  return { statuses, latestId };
}

export async function fetchNotificationsAPI(accessToken, { minId = null, sinceId = null, maxId = null } = {}) {
  const params = new URLSearchParams({ limit: "30" });
  applyPagination(params, { minId, sinceId, maxId });

  const res = await fetchTimelineJson(
    `${HOLLO_URL}/api/v1/notifications?${params}`,
    accessToken,
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

export function sanitizeStatus(status) {
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

export function sanitizeNotification(notification) {
  if (!notification || typeof notification !== "object") return notification;
  const sanitized = { ...notification };
  if (sanitized.status) {
    sanitized.status = sanitizeStatus(sanitized.status);
  }
  return sanitized;
}
