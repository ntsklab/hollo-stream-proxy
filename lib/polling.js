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
} from "./streaming.js";

let pollTimer = null;

export function startPolling() {
  if (pollTimer) return;

  const poll = async () => {
    const allStreamEntries = [];
    for (const streams of activeStreams.values()) {
      for (const s of streams) {
        allStreamEntries.push(s);
      }
    }

    if (allStreamEntries.length === 0 && pushAccounts.size === 0) {
      pollTimer = null;
      return;
    }

    for (const streamEntry of allStreamEntries) {
      const accessToken = streamEntry.token;
      if (!accessToken) continue;

      const subscriptions = streamEntry.subscriptions;
      const hasUserStream = subscriptions.has("user") || subscriptions.has("user:notification");
      const hasTimelineStream = subscriptions.has("user");
      const listSubs = [...subscriptions.values()].filter((sub) => sub.stream === "list" && sub.listId);
      const listIds = [...new Set(listSubs.map((s) => s.listId))];

      const tokenInfo = await verifyToken(accessToken);
      if (!tokenInfo?.accountOwnerId) continue;
      const accountOwnerId = tokenInfo.accountOwnerId;

      if (hasUserStream) {
        try {
          const sinceId = tlMaxIds.get(`notif_${accountOwnerId}_${accessToken}`) || null;
          const { notifications, latestId } = await fetchNotificationsAPI(accessToken, sinceId);

          if (notifications.length > 0) {
            if (latestId) tlMaxIds.set(`notif_${accountOwnerId}_${accessToken}`, latestId);

            if (!sinceId) {
              logger.stream("notification checkpoint set", {
                account: accountOwnerId, latestId, count: notifications.length,
              });
            } else {
              let wsSent = 0;
              let wsSkipped = 0;

              for (const n of notifications) {
                const sanitized = sanitizeNotification(n);
                if (!subscriptions.has("user") && !subscriptions.has("user:notification")) continue;

                const notifStream = subscriptions.has("user:notification") ? "user:notification" : "user";
                if (markNotificationSent(`${accountOwnerId}_${accessToken}`, String(n.id), notifStream)) {
                  wsSkipped++;
                  continue;
                }

                const eventJson = JSON.stringify({
                  stream: [notifStream],
                  event: "notification",
                  payload: JSON.stringify(sanitized),
                });
                if (streamEntry.sse) { streamEntry.send(eventJson); wsSent++; }
                else if (streamEntry.ws?.readyState === 1) { try { streamEntry.ws.send(eventJson); wsSent++; } catch (_) {} }
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

      if (hasTimelineStream) {
        try {
          const sinceId = tlMaxIds.get(`${accountOwnerId}_${accessToken}`) || null;
          const { statuses, latestId } = await fetchHomeTimelineAPI(accessToken, sinceId);

          if (statuses.length > 0) {
            if (latestId) tlMaxIds.set(`${accountOwnerId}_${accessToken}`, latestId);

            if (!sinceId) {
              logger.stream("timeline checkpoint set", {
                account: accountOwnerId, latestId, count: statuses.length,
              });
            } else {
              let sent = 0;
              let skipped = 0;

              for (const status of statuses.reverse()) {
                if (markTlPostSent(`${accountOwnerId}_${accessToken}`, String(status.id), "user")) {
                  skipped++;
                  continue;
                }

                if (subscriptions.has("user")) {
                  const eventJson = JSON.stringify({
                    stream: ["user"],
                    event: "update",
                    payload: JSON.stringify(sanitizeStatus(status)),
                  });
                  if (streamEntry.sse) { streamEntry.send(eventJson); sent++; }
                  else if (streamEntry.ws?.readyState === 1) { try { streamEntry.ws.send(eventJson); sent++; } catch (_) {} }
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

      for (const lid of listIds) {
        try {
          const sinceId = tlMaxIds.get(`list:${lid}_${accessToken}`) || null;
          const { statuses, latestId } = await fetchListTimelineAPI(accessToken, lid, sinceId);

          if (statuses.length > 0) {
            if (latestId) tlMaxIds.set(`list:${lid}_${accessToken}`, latestId);

            if (!sinceId) {
              logger.stream("list timeline checkpoint set", {
                account: accountOwnerId, listId: lid, latestId, count: statuses.length,
              });
            } else {
              let sent = 0;
              let skipped = 0;

              for (const status of statuses.reverse()) {
                if (markTlPostSent(`${accountOwnerId}_${accessToken}`, String(status.id), `list:${lid}`)) {
                  skipped++;
                  continue;
                }

                const sub = subscriptions.get(`list:${lid}`);
                if (sub) {
                  const eventJson = JSON.stringify({
                    stream: ["list", lid],
                    event: "update",
                    payload: JSON.stringify(sanitizeStatus(status)),
                  });
                  if (streamEntry.sse) { streamEntry.send(eventJson); sent++; }
                  else if (streamEntry.ws?.readyState === 1) { try { streamEntry.ws.send(eventJson); sent++; } catch (_) {} }
                }
              }

              if (sent > 0 || skipped > 0) {
                logger.stream("list update", {
                  account: accountOwnerId, listId: lid, count: statuses.length, sent, skipped,
                });
              }
            }
          }
        } catch (err) {
          logger.error("list timeline poll error", {
            account: accountOwnerId, listId: lid, error: err.message,
          });
        }
      }

      const publicVariants = new Map();
      ["public", "public:local", "public:remote"].forEach((st) => {
        if (subscriptions.has(st)) {
          publicVariants.set(st, { local: st === "public:local", remote: st === "public:remote" });
        }
      });
      for (const [streamKey, { local, remote }] of publicVariants) {
        try {
          const sinceId = tlMaxIds.get(`${accountOwnerId}:${streamKey}_${accessToken}`) || null;
          const { statuses, latestId } = await fetchPublicTimelineAPI(accessToken, { local, remote, sinceId });

          if (statuses.length > 0) {
            if (latestId) tlMaxIds.set(`${accountOwnerId}:${streamKey}_${accessToken}`, latestId);

            if (!sinceId) {
              logger.stream("public timeline checkpoint set", {
                account: accountOwnerId, stream: streamKey, latestId, count: statuses.length,
              });
            } else {
              let sent = 0;
              let skipped = 0;

              for (const status of statuses.reverse()) {
                if (markTlPostSent(`${accountOwnerId}_${accessToken}`, String(status.id), streamKey)) {
                  skipped++;
                  continue;
                }

                if (subscriptions.has(streamKey)) {
                  const eventJson = JSON.stringify({
                    stream: [streamKey],
                    event: "update",
                    payload: JSON.stringify(sanitizeStatus(status)),
                  });
                  if (streamEntry.sse) { streamEntry.send(eventJson); sent++; }
                  else if (streamEntry.ws?.readyState === 1) { try { streamEntry.ws.send(eventJson); sent++; } catch (_) {} }
                }
              }

              if (sent > 0 || skipped > 0) {
                logger.stream("public timeline", {
                  account: accountOwnerId, stream: streamKey, sent, skipped, fetched: statuses.length,
                });
              }
            }
          }
        } catch (err) {
          logger.error("public timeline poll error", {
            account: accountOwnerId, stream: streamKey, error: err.message,
          });
        }
      }

      const hashtagGroups = new Map();
      for (const sub of subscriptions.values()) {
        if ((sub.stream === "hashtag" || sub.stream === "hashtag:local") && sub.tag) {
          const key = `${sub.stream}:${sub.tag}`;
          if (!hashtagGroups.has(key)) {
            hashtagGroups.set(key, { stream: sub.stream, tag: sub.tag, local: sub.stream === "hashtag:local" });
          }
        }
      }
      for (const [key, { stream: streamKey, tag: htTag, local }] of hashtagGroups) {
        try {
          const sinceId = tlMaxIds.get(`${accountOwnerId}:hashtag:${htTag}${local ? ":local" : ""}_${accessToken}`) || null;
          const { statuses, latestId } = await fetchHashtagTimelineAPI(accessToken, htTag, { local, sinceId });

          if (statuses.length > 0) {
            const dedupKey = `${accountOwnerId}:hashtag:${htTag}${local ? ":local" : ""}_${accessToken}`;
            if (latestId) tlMaxIds.set(dedupKey, latestId);

            if (!sinceId) {
              logger.stream("hashtag timeline checkpoint set", {
                account: accountOwnerId, tag: htTag, local, latestId, count: statuses.length,
              });
            } else {
              let sent = 0;
              let skipped = 0;

              for (const status of statuses.reverse()) {
                if (markTlPostSent(`${accountOwnerId}_${accessToken}`, String(status.id), dedupKey)) {
                  skipped++;
                  continue;
                }

                const sub = subscriptions.get(`${streamKey}:${htTag}`);
                if (sub && sub.tag === htTag) {
                  const eventJson = JSON.stringify({
                    stream: [streamKey, htTag],
                    event: "update",
                    payload: JSON.stringify(sanitizeStatus(status)),
                  });
                  if (streamEntry.sse) { streamEntry.send(eventJson); sent++; }
                  else if (streamEntry.ws?.readyState === 1) { try { streamEntry.ws.send(eventJson); sent++; } catch (_) {} }
                }
              }

              if (sent > 0 || skipped > 0) {
                logger.stream("hashtag timeline", {
                  account: accountOwnerId, tag: htTag, local, sent, skipped, fetched: statuses.length,
                });
              }
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

        const sinceId = tlMaxIds.get(`notif_${accountOwnerId}_push`) || null;
        const { notifications, latestId } = await fetchNotificationsAPI(accessToken, sinceId);

        if (notifications.length > 0) {
          if (latestId) tlMaxIds.set(`notif_${accountOwnerId}_push`, latestId);

          if (!sinceId) {
            logger.stream("notification checkpoint set", {
              account: accountOwnerId, latestId, count: notifications.length,
            });
          } else {
            let pushSent = 0;

            for (const n of notifications) {
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
                fetched: notifications.length,
              });
            }
            if (subs.length === 0) {
              pushAccounts.delete(accountOwnerId);
            }
          }
        }
      } catch (err) {
        logger.error("push notification poll error", {
          account: accountOwnerId, error: err.message,
        });
      }
    }

    if (allStreamEntries.length > 0 || pushAccounts.size > 0) {
      pollTimer = setTimeout(poll, POLL_INTERVAL_MS);
    } else {
      pollTimer = null;
    }
  };

  pollTimer = setTimeout(poll, POLL_INTERVAL_MS);
}

export function getPollTimer() {
  return pollTimer;
}

export function setPollTimer(timer) {
  pollTimer = timer;
}
