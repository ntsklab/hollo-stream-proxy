import { HOLLO_INTERNAL_URL, TOKEN_CACHE_TTL_MS, TOKEN_CACHE_NEG_TTL_MS } from "./config.js";
import { logger } from "./logger.js";

const tokenCache = new Map();

export async function verifyToken(token, testEndpoint = "timeline") {
  if (!token) return null;

  const cacheKey = `${token}:${testEndpoint}`;
  const cached = tokenCache.get(cacheKey);
  if (cached) {
    if (cached.valid && cached.expiresAt > Date.now()) return cached;
    if (!cached.valid && cached.expiresAt > Date.now()) return null;
  }

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15_000);
    
    const res = await fetch(`${HOLLO_INTERNAL_URL}/api/v1/accounts/verify_credentials`, {
      headers: { Authorization: `Bearer ${token}` },
      signal: controller.signal,
    });
    clearTimeout(timeout);
    
    if (!res.ok) {
      tokenCache.set(cacheKey, { valid: false, expiresAt: Date.now() + TOKEN_CACHE_NEG_TTL_MS });
      return null;
    }
    
    const account = await res.json();
    
    let testUrl;
    if (testEndpoint === "notifications") {
      testUrl = `${HOLLO_INTERNAL_URL}/api/v1/notifications?limit=1`;
    } else {
      testUrl = `${HOLLO_INTERNAL_URL}/api/v1/timelines/home?limit=1`;
    }
    
    const testController = new AbortController();
    const testTimeout = setTimeout(() => testController.abort(), 15_000);
    const testRes = await fetch(testUrl, {
      headers: { Authorization: `Bearer ${token}` },
      signal: testController.signal,
    });
    clearTimeout(testTimeout);
    
    let scopes = [];
    if (testRes.status === 200) {
      scopes = ["read", "read:statuses"];
    } else if (testRes.status === 403) {
      scopes = ["read:accounts"];
    }
    
    const info = {
      accountOwnerId: account.id,
      scopes,
      valid: true,
      expiresAt: Date.now() + TOKEN_CACHE_TTL_MS,
      account: {
        id: account.id,
        acct: account.acct || account.username,
        display_name: account.display_name || account.username,
        avatar: account.avatar || "",
      },
    };
    tokenCache.set(cacheKey, info);
    return info;
  } catch (err) {
    logger.error("token verification failed", { error: err.message });
    return null;
  }
}
