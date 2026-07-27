export const PORT = parseInt(process.env.PORT || "3001", 10);
export const HOLLO_URL = process.env.HOLLO_URL;
export const HOLLO_INTERNAL_URL = process.env.HOLLO_INTERNAL_URL || HOLLO_URL;

if (!HOLLO_URL) {
  console.error("FATAL: HOLLO_URL environment variable is required");
  process.exit(1);
}

export const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL || "5000", 10);
export const DATA_DIR = process.env.DATA_DIR || "/data";
export const SUBS_FILE = `${DATA_DIR}/push_subscriptions.json`;

export const VAPID_PUBLIC_KEY = process.env.VAPID_PUBLIC_KEY || "";
export const VAPID_PRIVATE_KEY = process.env.VAPID_PRIVATE_KEY || "";
export const VAPID_SUBJECT = process.env.VAPID_SUBJECT || "https://example.com";

export const TOKEN_CACHE_TTL_MS = 30_000;
export const TOKEN_CACHE_NEG_TTL_MS = 30_000;
export const DEDUP_WINDOW_MS = 60_000;
