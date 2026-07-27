# AGENTS.md

## Project Overview

Hollo Stream Proxy is a Node.js (ESM) proxy that adds WebSocket streaming and WebPush support for [Hollo](https://github.com/fedify-dev/hollo), a Mastodon-compatible ActivityPub server that lacks native streaming.

Single-file architecture: all logic lives in `index.js`.

## Development

```bash
npm install
npm run dev          # node --watch index.js
```

Required env var: `HOLLO_URL`. See `README.md` for full env var list.

## Testing

No test framework. Verify via:
- `node --check index.js` (syntax check)
- Manual testing with a running Hollo instance

## Code Conventions

- **Language**: JavaScript (ESM, `"type": "module"`)
- **Runtime**: Node.js 24+
- **No transpilation**: raw `.js`, no TypeScript
- **No comments** unless explicitly requested
- **No external frameworks**: only `ws` and `web-push` as dependencies
- **Logging**: use `logger.info/warn/error/stream/push` (not `console.log`)
- **Naming**: camelCase for variables/functions, UPPER_SNAKE for constants
- **Async**: prefer `async/await`, avoid callbacks

## Architecture Notes

- Push subscriptions are persisted as JSON files in `DATA_DIR`
- Token validation caches results in memory (`tokenCache` Map) with 30s TTL
- Token scopes are extracted from X-OAuth-Scopes response header
- Streaming endpoints require `read` scope; push endpoints require `read` or `push` scope
- Push subscriptions are scoped per access_token (multi-client support)
- Each client connection (WebSocket/SSE) stores its own access_token and polls independently
- Polling uses each client's token to fetch data from Hollo API
- Multiple Hollo accounts per user are supported (each with its own token)
- Dedup uses compound keys (`accountId:token:streamKey`) per stream type:
  - `"user"` for home timeline / user notification streams
  - `"user:notification"` for notification-only streams
  - `"list:<listId>"` for list timeline streams
  - `"public"`, `"public:local"`, `"public:remote"` for public timelines
  - `"hashtag:<tag>"`, `"hashtag:<tag>:local"` for hashtag timelines
- Polling runs on a single timer (`setTimeout` chain), not `setInterval`
- WebSocket upgrade handles auth via query param `access_token`, `Authorization: Bearer` header, or `Sec-WebSocket-Protocol` header

## Git Workflow

- `git commit` は適宜行う（变更がまとまったら都度コミットする）
- コミットメッセージは変更内容を簡潔に記述する（例: `fix: ...`, `feat: ...`, `refactor: ...`）
  - 破壊的変更がなければ patch increment（`0.9.x` → `0.9.x+1`）
  - 新機能追加は minor increment（`0.9.x` → `0.10.0`）
  - version bump 時は下記ファイルのバージョン表記も合わせて更新する:
    - `README.md` — コンテナイメージのタグ
    - `hollo-stream-proxy.yaml.sample` — Deployment のイメージタグ

## Key Patterns

- **Stream types**: `user`, `user:notification`, `list` (with `list` query param), `public`, `public:local`, `public:remote`, `hashtag`, `hashtag:local` (with `tag` query param)
- **Hollo API calls**: always use `HOLLO_URL` (public) or `HOLLO_INTERNAL_URL` (internal)
- **Status sanitization**: `sanitizeStatus()` strips Hollo-specific fields before sending to clients
- **File writes**: atomic via `.tmp` + `rename` pattern

## Reference Implementations

- **Hollo source**: https://github.com/fedify-dev/hollo
  - Timelines API: `src/api/v1/timelines.ts` (`GET /api/v1/timelines/list/:id`)
  - Lists API: `src/api/v1/lists.ts`
- **Mastodon streaming (Node.js)**: https://github.com/mastodon/mastodon/tree/main/streaming
  - Main entry: `streaming/index.js`
  - Request handler: `streaming/handler.js`
  - Worker: `streaming/worker.js`
- **Mastodon push notifications**: https://github.com/mastodon/mastodon/blob/main/app/workers/web_push_notification_worker.rb
