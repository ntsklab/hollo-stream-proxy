import { HOLLO_URL, HOLLO_INTERNAL_URL } from "./config.js";
import { logger } from "./logger.js";

export async function fetchHomeTimelineAPI(accessToken, sinceId = null) {
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
  const latestId = statuses.length > 0 ? statuses[0].id : null;

  return { statuses, latestId };
}

export async function fetchListTimelineAPI(accessToken, listId, sinceId = null) {
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

export async function fetchInstanceAPI(req, apiVersion = "v1") {
  const res = await fetch(`${HOLLO_INTERNAL_URL}/api/${apiVersion}/instance`, {
    headers: { Accept: "application/json" },
  });
  if (!res.ok) {
    throw new Error(`instance API error: ${res.status}`);
  }
  const data = await res.json();

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

export async function fetchPublicTimelineAPI(accessToken, { local = false, remote = false, sinceId = null } = {}) {
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

export async function fetchHashtagTimelineAPI(accessToken, tag, { local = false, sinceId = null } = {}) {
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

export async function fetchNotificationsAPI(accessToken, sinceId = null) {
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
