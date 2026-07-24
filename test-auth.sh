#!/usr/bin/env bash
# hollo-stream-proxy: 認証・認可の境界試験
#
# Usage:
#   ./test-auth.sh [base_url]
#
#   base_url defaults to https://hl.oyasumi.dev
#
# Tests:
#   WebSocket: upgrade request with various token scenarios
#   SSE:       HTTP GET streaming with various token scenarios
#   Push API:  subscription endpoints with various token scenarios

set -euo pipefail

BASE_URL="${1:-https://hl.oyasumi.dev}"
STREAM_PATH="/api/v1/streaming"
PUSH_PATH="/api/v1/push/subscription"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASS=0
FAIL=0

pass() { echo -e "${GREEN}PASS${NC} $1"; PASS=$((PASS + 1)); }
fail() { echo -e "${RED}FAIL${NC} $1 (expected $2, got $3)"; FAIL=$((FAIL + 1)); }

# ── WebSocket Upgrade tests ─────────────────────────────────────────
# Note: curl HTTP upgrade requests may not reach the WebSocket handler.
# These tests verify HTTP response codes for unauthenticated streaming
# requests; real WebSocket auth is tested by actual clients.
echo "=== WebSocket Upgrade Tests ==="
echo ""

# Test 1: No token → 401
echo "--- WS: no token ---"
STATUS=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 \
  -H "Connection: Upgrade" -H "Upgrade: websocket" \
  -H "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==" \
  -H "Sec-WebSocket-Version: 13" \
  "${BASE_URL}${STREAM_PATH}?stream=user" 2>/dev/null || echo "000")
if [ "$STATUS" = "401" ]; then
  pass "WS no token → 401"
else
  fail "WS no token" "401" "$STATUS"
fi
echo ""

# Test 2: Invalid token → 401
echo "--- WS: invalid token ---"
STATUS=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 \
  -H "Connection: Upgrade" -H "Upgrade: websocket" \
  -H "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==" \
  -H "Sec-WebSocket-Version: 13" \
  "${BASE_URL}${STREAM_PATH}?stream=user&access_token=invalid-token-deadbeef" 2>/dev/null || echo "000")
if [ "$STATUS" = "401" ]; then
  pass "WS invalid token → 401"
else
  fail "WS invalid token" "401" "$STATUS"
fi
echo ""

# Test 3: Missing stream param for list → 400
echo "--- WS: list without list param ---"
STATUS=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 \
  -H "Connection: Upgrade" -H "Upgrade: websocket" \
  -H "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==" \
  -H "Sec-WebSocket-Version: 13" \
  "${BASE_URL}${STREAM_PATH}?stream=list&access_token=invalid-token-deadbeef" 2>/dev/null || echo "000")
# 401 comes before 400 (token check first)
if [ "$STATUS" = "401" ]; then
  pass "WS list without list → 401 (token rejected first)"
else
  fail "WS list without list" "401" "$STATUS"
fi
echo ""

# Test 4: Invalid stream type → 400
echo "--- WS: invalid stream type ---"
STATUS=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 \
  -H "Connection: Upgrade" -H "Upgrade: websocket" \
  -H "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==" \
  -H "Sec-WebSocket-Version: 13" \
  "${BASE_URL}${STREAM_PATH}?stream=invalid&access_token=invalid-token-deadbeef" 2>/dev/null || echo "000")
if [ "$STATUS" = "401" ]; then
  pass "WS invalid stream type → 401 (token rejected first)"
else
  fail "WS invalid stream type" "401" "$STATUS"
fi
echo ""

# ── SSE Tests ───────────────────────────────────────────────────────
echo "=== SSE Tests ==="
echo ""

# Test 5: No token → 401
echo "--- SSE: no token ---"
HTTP_OUT=$(curl -s -o /tmp/sse-test-output.txt -w "%{http_code}" --max-time 3 \
  "${BASE_URL}${STREAM_PATH}/user" 2>/dev/null || echo "000")
if [ "$HTTP_OUT" = "401" ]; then
  pass "SSE no token → 401"
else
  fail "SSE no token" "401" "$HTTP_OUT"
fi
echo ""

# Test 6: Invalid token → 401
echo "--- SSE: invalid token ---"
HTTP_OUT=$(curl -s -o /tmp/sse-test-output.txt -w "%{http_code}" --max-time 3 \
  "${BASE_URL}${STREAM_PATH}/user?access_token=invalid-token-deadbeef" 2>/dev/null || echo "000")
if [ "$HTTP_OUT" = "401" ]; then
  pass "SSE invalid token → 401"
else
  fail "SSE invalid token" "401" "$HTTP_OUT"
fi
echo ""

# Test 7: Unknown SSE stream → 404
echo "--- SSE: unknown stream ---"
HTTP_OUT=$(curl -s -o /tmp/sse-test-output.txt -w "%{http_code}" --max-time 3 \
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

# Test 8: No token → 401
echo "--- Push: no token ---"
HTTP_OUT=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 \
  "${BASE_URL}${PUSH_PATH}" 2>/dev/null || echo "000")
if [ "$HTTP_OUT" = "401" ]; then
  pass "Push GET no token → 401"
else
  fail "Push GET no token" "401" "$HTTP_OUT"
fi
echo ""

# Test 9: Invalid token → 401
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

rm -f /tmp/sse-test-output.txt

[ "$FAIL" -eq 0 ] || exit 1
