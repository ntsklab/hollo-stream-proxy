# Hollo Stream Proxy

[Hollo](https://github.com/fedify-dev/hollo) 用の Streaming / WebSocket & WebPush プロキシです。

Push 通知および Streaming 非対応の Hollo で、リアルタイム通知を実現します。
また Instance API の補完（`urls.streaming_api` 注入）と Filters エンドポイントも提供します。

## 機能

| 機能 | エンドポイント | 説明 |
|---|---|---|
| OAuth ログイン | `/` | Hollo アカウントでログインし、ポーリング用トークンを取得 |
| WebSocket Streaming | `GET /api/v1/streaming` | Mastodon 互換ストリーミング API。`access_token` 認証 |
| WebPush Subscription | `/api/v1/push/subscription` | Mastodon 互換プッシュ通知購読 API（CRUD） |
| Instance API | `/api/v1/instance`, `/api/v2/instance` | Hollo のインスタンス情報をプロキシし `urls.streaming_api` を注入 |
| Filters | `/api/v1/filters`, `/api/v2/filters` | 空配列 `[]` を返す（Hollo 非対応のため） |

- 認証: `access_token`（WS はクエリパラメータ、HTTP は `Authorization: Bearer`）
- 通知取得: Hollo REST API（`/api/v1/notifications`, `/api/v1/timelines/home`）をポーリング
- プッシュ通知: `web-push` ライブラリで VAPID 署名 + 送信
- 購読情報: PVC 上の JSON ファイルで永続化管理

## 環境変数

| 変数 | 必須 | デフォルト | 説明 |
|---|---|---|---|
| `HOLLO_URL` | **必須** | — | Hollo の公開 URL |
| `HOLLO_INTERNAL_URL` | | `HOLLO_URL` | Hollo の内部 URL（K8s Service DNS 等） |
| `DATABASE_URL` | **必須** | — | Hollo の PostgreSQL 接続文字列（トークン検証用） |
| `PORT` | | `3001` | リッスンポート |
| `POLL_INTERVAL` | | `3000` | ポーリング間隔（ミリ秒） |
| `DATA_DIR` | | `/data` | セッション・購読データの保存先 |
| `VAPID_PUBLIC_KEY` | | — | WebPush VAPID 公開鍵 |
| `VAPID_PRIVATE_KEY` | | — | WebPush VAPID 秘密鍵（未設定時はプッシュ配信無効） |
| `VAPID_SUBJECT` | | `https://example.com` | VAPID subject |

## ヘルスチェック

`GET /health` — `200 OK`

## コンテナイメージ

```
nrt.vultrcr.com/ntlab1/hollo-stream-proxy:0.9.15
```

## デプロイ

### 1. VAPID 鍵生成

```bash
node gen-vapid-keys.mjs
```

### 2. マニフェスト準備

```bash
cp hollo-stream-proxy.yaml.sample hollo-stream-proxy.yaml
```

`<NAMESPACE>` を置換し、`DATABASE_URL`、`HOLLO_URL`、VAPID 鍵を設定。

### 3. デプロイ

```bash
kubectl apply -f hollo-stream-proxy.yaml
```

### 4. HAProxy 設定

`haproxy.conf.sample` を参照し、instance / filters / streaming / push を本プロキシに振り分け。

## API リファレンス

### OAuth ログイン

```
GET  /           — ログイン画面
POST /auth/login — 認証コード送信（code パラメータ）
```

### WebSocket Streaming

```
GET /api/v1/streaming?access_token=xxx&stream=user
```

| パラメータ | 説明 |
|---|---|
| `access_token` | アクセストークン（**必須**） |
| `stream` | ストリーム種別（`user`） |

### WebPush Subscription

```
POST   /api/v1/push/subscription  — 購読登録
GET    /api/v1/push/subscription  — 購読情報取得
PUT    /api/v1/push/subscription  — アラート設定更新
DELETE /api/v1/push/subscription  — 購読解除
```

`Authorization: Bearer <access_token>` 必須。

### Instance API

```
GET /api/v1/instance  — Mastodon v1 Instance API
GET /api/v2/instance  — Mastodon v2 Instance API
```

### Filters

```
GET /api/v1/filters  — 空配列
GET /api/v2/filters  — 空配列
```

## ファイル構成

```
.
├── index.js                        # メインアプリケーション
├── package.json
├── Dockerfile
├── gen-vapid-keys.mjs              # VAPID 鍵生成
├── hollo-stream-proxy.yaml.sample  # K8s マニフェスト
├── haproxy.conf.sample             # HAProxy 設定
└── README.md
```
