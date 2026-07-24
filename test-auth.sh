#!/usr/bin/env bash
# hollo-stream-proxy: 認証・認可の境界試験
#
# Usage:
#   ./test-auth.sh [base_url]
#
#   base_url defaults to https://hl.oyasumi.dev
#
# Prerequisites: node (>=21)
#
# Tests:
#   WebSocket: upgrade request with various token scenarios
#   SSE:       HTTP GET streaming with various token scenarios
#   Push API:  subscription endpoints with various token scenarios

set -euo pipefail

BASE_URL="${1:-https://hl.oyasumi.dev}"
STREAM_PATH="/api/v1/streaming"
PUSH_PATH="/api/v1/push/subscription"

# Convert https:// to wss:// for WebSocket tests
WS_BASE="${BASE_URL/https:/wss:}"

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

PASS=0
FAIL=0

pass() { echo -e "${GREEN}PASS${NC} $1"; PASS=$((PASS + 1)); }
fail() { echo -e "${RED}FAIL${NC} $1 (expected $2, got $3)"; FAIL=$((FAIL + 1)); }

# ── WebSocket helper ────────────────────────────────────────────────
# $1: test description
# $2: expected http status (or "error" for connection refused)
# $3..: URL with optional query params
ws_test() {
  local desc="$1"
  local expected="$2"
  shift 2
  local url="${WS_BASE}${STREAM_PATH}?$*"

  local result
  result=$(node --input-type=module -e "
    const ws = new WebSocket('${url}');
    ws.onerror = () => { console.log('error'); process.exit(0); };
    ws.onopen = () => { ws.close(); console.log('open'); };
    setTimeout(() => { console.log('timeout'); process.exit(0); }, 3000);
  " 2>/dev/null || echo "error")

  if [ "$expected" = "error" ] && [ "$result" = "error" ]; then
    pass "$desc → connection refused (expected)"
  elif [ "$expected" != "error" ] && [ "$result" = "open" ]; then
    fail "$desc" "$expected" "open (token accepted)"
  elif [ "$expected" != "error" ] && [ "$result" = "error" ]; then
    pass "$desc → connection refused (expected $expected)"
  else
    fail "$desc" "$expected" "$result"
  fi
}

# ── WebSocket Tests ─────────────────────────────────────────────────
echo "=== WebSocket Tests ==="
echo ""

echo "--- WS: no token ---"
ws_test "WS no token" "error" "stream=user"
echo ""

echo "--- WS: invalid token ---"
ws_test "WS invalid token" "error" "stream=user&access_token=invalid-token-deadbeef"
echo ""

echo "--- WS: missing list param ---"
ws_test "WS list without list" "error" "stream=list&access_token=invalid-token-deadbeef"
echo ""

echo "--- WS: invalid stream type ---"
ws_test "WS invalid stream" "error" "stream=invalid&access_token=invalid-token-deadbeef"
echo ""

# ── SSE Tests ───────────────────────────────────────────────────────
echo "=== SSE Tests ==="
echo ""

echo "--- SSE: no token ---"
HTTP_OUT=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 \
  "${BASE_URL}${STREAM_PATH}/user" 2>/dev/null || echo "000")
if [ "$HTTP_OUT" = "401" ]; then
  pass "SSE no token → 401"
else
  fail "SSE no token" "401" "$HTTP_OUT"
fi
echo ""

echo "--- SSE: invalid token ---"
HTTP_OUT=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 \
  "${BASE_URL}${STREAM_PATH}/user?access_token=invalid-token-deadbeef" 2>/dev/null || echo "000")
if [ "$HTTP_OUT" = "401" ]; then
  pass "SSE invalid token → 401"
else
  fail "SSE invalid token" "401" "$HTTP_OUT"
fi
echo ""

echo "--- SSE: unknown stream ---"
HTTP_OUT=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 \
  "${BASE_URL}${STREAM_PATH}/unknown?access_token=invalid-token-deadbeef" 2>/dev/null || echo "000")
if [ "$HTTP_OUT" = "401" ]; then
  pass "SSE unknown stream → 401 (token rejected first)"
else
  fail "SSE unknown stream" "401" "$HTTP_OUT"
fi
echo ""

# ── Push Subscription API Tests ─────────────────────────────────────
echo "=== Push Subscription API Tests ==="
echo ""

echo "--- Push: no token ---"
HTTP_OUT=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 \
  "${BASE_URL}${PUSH_PATH}" 2>/dev/null || echo "000")
if [ "$HTTP_OUT" = "401" ]; then
  pass "Push GET no token → 401"
else
  fail "Push GET no token" "401" "$HTTP_OUT"
fi
echo ""

echo "--- Push: invalid token ---"
HTTP_OUT=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 \
  -H "Authorization: Bearer invalid-token-deadbeef" \
  "${BASE_URL}${PUSH_PATH}" 2>/dev/null || echo "000")
if [ "$HTTP_OUT" = "401" ]; then
  pass "Push GET invalid token → 401"
else
  fail "Push GET invalid token" "401" "$HTTP_OUT"
fi
echo ""

# ── Summary ─────────────────────────────────────────────────────────
echo "========================================="
echo -e "Results: ${GREEN}${PASS} passed${NC}, ${RED}${FAIL} failed${NC}"
echo "========================================="

[ "$FAIL" -eq 0 ] || exit 1
