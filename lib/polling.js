import { POLL_INTERVAL_MS } from "./config.js";
import { logger } from "./logger.js";
import { verifyToken } from "./verifyClientToken.js";
import {
  fetchHomeTimelineAPI,
  fetchListTimelineAPI,
  fetchPublicTimelineAPI,
  fetchHashtagTimelineAPI,
  fetchNotificationsAPI,
  sanitizeStatus,
  sanitizeNotification,
} from "./holloApiClient.js";
import {
  loadSubscriptions,
  removePushSubscription,
  shouldSendPush,
  buildPushPayload,
} from "./pushSubscriptions.js";
import {
  activeStreams,
  pushAccounts,
  tlMaxIds,
  markNotificationSent,
  markTlPostSent,
  sendPushNotification,
  subscriptionKey,
} from "./streaming.js";

let pollTimer = null;

const TIMELINE_PAGE_LIMIT = 40;
const NOTIF_PAGE_LIMIT = 30;
const MAX_PAGES = 5;

function sendToEntry(streamEntry, eventJson) {
  if (streamEntry.sse) {
    try {
      streamEntry.send(eventJson);
      return true;
    } catch {
      return false;
    }
  }
  if (streamEntry.ws?.readyState === 1) {
    try {
      streamEntry.ws.send(eventJson);
      return true;
    } catch {
      return false;
    }
  }
  return false;
}

async function drainTimelinePages(fetchPage, cursorKey) {
  const sinceCursor = tlMaxIds.get(cursorKey) || null;
  if (!sinceCursor) {
    const { statuses, latestId } = await fetchPage(null);
    if (statuses.length > 0 && latestId) tlMaxIds.set(cursorKey, latestId);
    return { checkpointed: true, ordered: [], newestId: latestId || null, count: statuses.length };
  }
  const pages = [];
  let minId = sinceCursor;
  let newestId = null;
  for (let p = 0; p < MAX_PAGES; p++) {
    const { statuses, latestId } = await fetchPage(minId);
    if (statuses.length === 0) break;
    pages.push(statuses);
    if (latestId) newestId = latestId;
    if (statuses.length < TIMELINE_PAGE_LIMIT) break;
    minId = statuses[0].id;
  }
  if (pages.length === 0) return { checkpointed: false, ordered: [], newestId: null, count: 0 };
  if (newestId) tlMaxIds.set(cursorKey, newestId);
  const ordered = [];
  for (const pg of pages) {
    for (let i = pg.length - 1; i >= 0; i--) ordered.push(pg[i]);
  }
  return { checkpointed: false, ordered, newestId, count: ordered.length };
}

async function drainNotificationPages(fetchPage, cursorKey) {
  const sinceCursor = tlMaxIds.get(cursorKey) || null;
  if (!sinceCursor) {
    const { notifications, latestId } = await fetchPage(null, null);
    if (notifications.length > 0 && latestId) tlMaxIds.set(cursorKey, latestId);
    return { checkpointed: true, ordered: [], newestId: latestId || null, count: notifications.length };
  }
  const pages = [];
  let maxId = null;
  let newestId = null;
  let capped = false;
  for (let p = 0; p < MAX_PAGES; p++) {
    const { notifications, latestId } = await fetchPage(sinceCursor, maxId);
    if (notifications.length === 0) break;
    pages.push(notifications);
    if (!newestId) newestId = latestId;
    if (notifications.length < NOTIF_PAGE_LIMIT) break;
    maxId = notifications[notifications.length - 1].id;
    if (p === MAX_PAGES - 1) capped = true;
  }
  if (pages.length === 0) return { checkpointed: false, ordered: [], newestId: null, count: 0 };
  if (newestId && !capped) {
    tlMaxIds.set(cursorKey, newestId);
  } else if (capped) {
    const lastPage = pages[pages.length - 1];
    tlMaxIds.set(cursorKey, lastPage[lastPage.length - 1].id);
  }
  const ordered = [];
  for (let p = pages.length - 1; p >= 0; p--) {
    const pg = pages[p];
    for (let i = pg.length - 1; i >= 0; i--) ordered.push(pg[i]);
  }
  return { checkpointed: false, ordered, newestId, count: ordered.length };
}

export function startPolling() {
  if (pollTimer) return;

  const poll = async () => {
    try {
      await pollOnce();
    } catch (err) {
      logger.error("poll error", { error: err.message });
    } finally {
      const remaining = [];
      for (const streams of activeStreams.values()) {
        for (const s of streams) remaining.push(s);
      }
      if (remaining.length > 0 || pushAccounts.size > 0) {
        pollTimer = setTimeout(poll, POLL_INTERVAL_MS);
      } else {
        pollTimer = null;
      }
    }
  };

  pollTimer = setTimeout(poll, POLL_INTERVAL_MS);
}

async function pollOnce() {
  const allStreamEntries = [];
  for (const streams of activeStreams.values()) {
    for (const s of streams) {
      allStreamEntries.push(s);
    }
  }

  if (allStreamEntries.length === 0 && pushAccounts.size === 0) {
    return;
  }

  const groups = new Map();
  for (const streamEntry of allStreamEntries) {
    const accessToken = streamEntry.token;
    if (!accessToken) continue;
    if (!groups.has(accessToken)) groups.set(accessToken, { accessToken, entries: [] });
    groups.get(accessToken).entries.push(streamEntry);
  }

  const userKey = subscriptionKey("user");
  const userNotifKey = subscriptionKey("user:notification");

  for (const { accessToken, entries } of groups.values()) {
    let tokenInfo;
    try {
      tokenInfo = await verifyToken(accessToken);
    } catch (err) {
      logger.error("token verify poll error", { error: err.message });
      continue;
    }
    if (!tokenInfo?.accountOwnerId) continue;
    const accountOwnerId = tokenInfo.accountOwnerId;
    const compound = `${accountOwnerId}_${accessToken}`;

    let needsHome = false;
    let needsNotif = false;
    const listIds = new Set();
    const publicVariants = new Map();
    const hashtagGroups = new Map();
    for (const streamEntry of entries) {
      const subscriptions = streamEntry.subscriptions;
      if (subscriptions.has(userKey)) needsHome = true;
      if (subscriptions.has(userKey) || subscriptions.has(userNotifKey)) needsNotif = true;
      for (const sub of subscriptions.values()) {
        if (sub.stream === "list" && sub.listId) listIds.add(sub.listId);
        if (sub.stream === "public" || sub.stream === "public:local" || sub.stream === "public:remote") {
          if (!publicVariants.has(sub.stream)) {
            publicVariants.set(sub.stream, { local: sub.stream === "public:local", remote: sub.stream === "public:remote" });
          }
        }
        if ((sub.stream === "hashtag" || sub.stream === "hashtag:local") && sub.tag) {
          const key = subscriptionKey(sub.stream, { tag: sub.tag });
          if (!hashtagGroups.has(key)) {
            hashtagGroups.set(key, { stream: sub.stream, tag: sub.tag, local: sub.stream === "hashtag:local" });
          }
        }
      }
    }

    const subscribedEntries = (predicate) => entries.filter((e) => predicate(e.subscriptions));

    if (needsNotif) {
      try {
        const cursorKey = `notif_${accountOwnerId}_${accessToken}`;
        const { checkpointed, ordered, newestId, count } = await drainNotificationPages(
          (sinceId, maxId) => fetchNotificationsAPI(accessToken, { sinceId, maxId }),
          cursorKey,
        );
        if (checkpointed) {
          logger.stream("notification checkpoint set", {
            account: accountOwnerId, latestId: newestId, count,
          });
        } else if (ordered.length > 0) {
          const fresh = [];
          for (const n of ordered) {
            if (markNotificationSent(compound, String(n.id), "user")) continue;
            fresh.push(n);
          }
          let sent = 0;
          const skipped = ordered.length - fresh.length;
          for (const n of fresh) {
            const sanitized = sanitizeNotification(n);
            const payload = JSON.stringify(sanitized);
            for (const streamEntry of entries) {
              const subs = streamEntry.subscriptions;
              if (!subs.has(userKey) && !subs.has(userNotifKey)) continue;
              const notifStream = subs.has(userNotifKey) ? "user:notification" : "user";
              const eventJson = JSON.stringify({
                stream: [notifStream],
                event: "notification",
                payload,
              });
              if (sendToEntry(streamEntry, eventJson)) sent++;
            }
          }
          if (sent > 0 || skipped > 0) {
            logger.stream("notification", {
              account: accountOwnerId, sent, skipped, fetched: ordered.length,
            });
          }
        }
      } catch (err) {
        logger.error("notification poll error", {
          account: accountOwnerId, error: err.message,
        });
      }
    }

    if (needsHome) {
      try {
        const cursorKey = `${accountOwnerId}_${accessToken}`;
        const { checkpointed, ordered, newestId, count } = await drainTimelinePages(
          (minId) => fetchHomeTimelineAPI(accessToken, minId ? { minId } : {}),
          cursorKey,
        );
        if (checkpointed) {
          logger.stream("timeline checkpoint set", {
            account: accountOwnerId, latestId: newestId, count,
          });
        } else if (ordered.length > 0) {
          const targets = subscribedEntries((subs) => subs.has(userKey));
          const fresh = [];
          for (const status of ordered) {
            if (markTlPostSent(compound, String(status.id), "user")) continue;
            fresh.push(status);
          }
          let sent = 0;
          const skipped = ordered.length - fresh.length;
          for (const status of fresh) {
            const eventJson = JSON.stringify({
              stream: ["user"],
              event: "update",
              payload: JSON.stringify(sanitizeStatus(status)),
            });
            for (const streamEntry of targets) {
              if (sendToEntry(streamEntry, eventJson)) sent++;
            }
          }
          if (sent > 0 || skipped > 0) {
            logger.stream("update", {
              account: accountOwnerId, count: ordered.length, sent, skipped,
            });
          }
        }
      } catch (err) {
        logger.error("timeline poll error", {
          account: accountOwnerId, error: err.message,
        });
      }
    }

    for (const lid of listIds) {
      try {
        const cursorKey = `list:${lid}_${accessToken}`;
        const { checkpointed, ordered, newestId, count } = await drainTimelinePages(
          (minId) => fetchListTimelineAPI(accessToken, lid, minId ? { minId } : {}),
          cursorKey,
        );
        if (checkpointed) {
          logger.stream("list timeline checkpoint set", {
            account: accountOwnerId, listId: lid, latestId: newestId, count,
          });
        } else if (ordered.length > 0) {
          const key = subscriptionKey("list", { list: lid });
          const targets = subscribedEntries((subs) => subs.has(key));
          if (targets.length === 0) continue;
          const fresh = [];
          for (const status of ordered) {
            if (markTlPostSent(compound, String(status.id), `list:${lid}`)) continue;
            fresh.push(status);
          }
          let sent = 0;
          const skipped = ordered.length - fresh.length;
          for (const status of fresh) {
            const eventJson = JSON.stringify({
              stream: ["list", lid],
              event: "update",
              payload: JSON.stringify(sanitizeStatus(status)),
            });
            for (const streamEntry of targets) {
              if (sendToEntry(streamEntry, eventJson)) sent++;
            }
          }
          if (sent > 0 || skipped > 0) {
            logger.stream("list update", {
              account: accountOwnerId, listId: lid, count: ordered.length, sent, skipped,
            });
          }
        }
      } catch (err) {
        logger.error("list timeline poll error", {
          account: accountOwnerId, listId: lid, error: err.message,
        });
      }
    }

    for (const [streamKey, { local, remote }] of publicVariants) {
      try {
        const cursorKey = `${accountOwnerId}:${streamKey}_${accessToken}`;
        const { checkpointed, ordered, newestId, count } = await drainTimelinePages(
          (minId) => fetchPublicTimelineAPI(accessToken, minId ? { local, remote, minId } : { local, remote }),
          cursorKey,
        );
        if (checkpointed) {
          logger.stream("public timeline checkpoint set", {
            account: accountOwnerId, stream: streamKey, latestId: newestId, count,
          });
        } else if (ordered.length > 0) {
          const key = subscriptionKey(streamKey);
          const targets = subscribedEntries((subs) => subs.has(key));
          if (targets.length === 0) continue;
          const fresh = [];
          for (const status of ordered) {
            if (markTlPostSent(compound, String(status.id), streamKey)) continue;
            fresh.push(status);
          }
          let sent = 0;
          const skipped = ordered.length - fresh.length;
          for (const status of fresh) {
            const eventJson = JSON.stringify({
              stream: [streamKey],
              event: "update",
              payload: JSON.stringify(sanitizeStatus(status)),
            });
            for (const streamEntry of targets) {
              if (sendToEntry(streamEntry, eventJson)) sent++;
            }
          }
          if (sent > 0 || skipped > 0) {
            logger.stream("public timeline", {
              account: accountOwnerId, stream: streamKey, sent, skipped, fetched: ordered.length,
            });
          }
        }
      } catch (err) {
        logger.error("public timeline poll error", {
          account: accountOwnerId, stream: streamKey, error: err.message,
        });
      }
    }

    for (const [key, { stream: streamKey, tag: htTag, local }] of hashtagGroups) {
      try {
        const dedupKey = `${accountOwnerId}:hashtag:${htTag}${local ? ":local" : ""}_${accessToken}`;
        const { checkpointed, ordered, newestId, count } = await drainTimelinePages(
          (minId) => fetchHashtagTimelineAPI(accessToken, htTag, minId ? { local, minId } : { local }),
          dedupKey,
        );
        if (checkpointed) {
          logger.stream("hashtag timeline checkpoint set", {
            account: accountOwnerId, tag: htTag, local, latestId: newestId, count,
          });
        } else if (ordered.length > 0) {
          const subKey = subscriptionKey(streamKey, { tag: htTag });
          const targets = subscribedEntries((subs) => {
            const sub = subs.get(subKey);
            return sub && sub.tag === htTag;
          });
          if (targets.length === 0) continue;
          const fresh = [];
          for (const status of ordered) {
            if (markTlPostSent(compound, String(status.id), dedupKey)) continue;
            fresh.push(status);
          }
          let sent = 0;
          const skipped = ordered.length - fresh.length;
          for (const status of fresh) {
            const eventJson = JSON.stringify({
              stream: [streamKey, htTag],
              event: "update",
              payload: JSON.stringify(sanitizeStatus(status)),
            });
            for (const streamEntry of targets) {
              if (sendToEntry(streamEntry, eventJson)) sent++;
            }
          }
          if (sent > 0 || skipped > 0) {
            logger.stream("hashtag timeline", {
              account: accountOwnerId, tag: htTag, local, sent, skipped, fetched: ordered.length,
            });
          }
        }
      } catch (err) {
        logger.error("hashtag timeline poll error", {
          account: accountOwnerId, tag: htTag, local, error: err.message,
        });
      }
    }
  }

    for (const accountOwnerId of pushAccounts) {
      try {
        const subs = await loadSubscriptions(accountOwnerId);
        if (subs.length === 0) {
          pushAccounts.delete(accountOwnerId);
          continue;
        }

        const accessToken = subs[0].access_token;
        if (!accessToken) continue;

        const tokenInfo = await verifyToken(accessToken, "notifications");
        if (!tokenInfo) {
          logger.push("invalid token, removing subscriptions", { account: accountOwnerId });
          for (const sub of subs) {
            await removePushSubscription(sub.endpoint);
          }
          pushAccounts.delete(accountOwnerId);
          continue;
        }

        const cursorKey = `notif_${accountOwnerId}_push`;
        const { checkpointed, ordered, newestId, count } = await drainNotificationPages(
          (sinceId, maxId) => fetchNotificationsAPI(accessToken, { sinceId, maxId }),
          cursorKey,
        );

        if (checkpointed) {
          if (count > 0) {
            logger.stream("notification checkpoint set", {
              account: accountOwnerId, latestId: newestId, count,
            });
          }
          continue;
        }

        if (ordered.length > 0) {
          let pushSent = 0;

          for (const n of ordered) {
            const sanitized = sanitizeNotification(n);

            for (let i = subs.length - 1; i >= 0; i--) {
              const sub = subs[i];
              if (!shouldSendPush(sub.alerts, sanitized)) continue;

              const payload = buildPushPayload(sanitized, sub.access_token);
              const result = await sendPushNotification(sub, payload);
              if (result.removed) {
                await removePushSubscription(sub.endpoint);
                subs.splice(i, 1);
              } else if (result.ok) {
                pushSent++;
              }
            }
          }

          if (pushSent > 0) {
            logger.push("notification", {
              account: accountOwnerId, sent: pushSent,
              fetched: ordered.length,
            });
          }
          if (subs.length === 0) {
            pushAccounts.delete(accountOwnerId);
          }
        }
      } catch (err) {
        logger.error("push notification poll error", {
          account: accountOwnerId, error: err.message,
        });
      }
    }
}

export function getPollTimer() {
  return pollTimer;
}

export function setPollTimer(timer) {
  pollTimer = timer;
}
