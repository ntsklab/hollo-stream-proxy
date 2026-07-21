# Hollo Stream Proxy - セットアップ手順

## 概要

Holloのstreaming/push通知機能を補完するproxyです。Hollo本体で未実装のWebSocket streamingとWebPushを、正規APIをポーリングすることで実現します。

## アーキテクチャ

```
┌─────────────────────────────────────────────────────────┐
│  Hollo Stream Proxy (:3001)                             │
│                                                         │
│  GET  /                      → ログイン画面             │
│  POST /auth/login            → code → token 交換         │
│  GET  /api/v1/streaming      → WebSocket Streaming      │
│  POST /api/v1/push/subscription → WebPush Subscription  │
│                                                         │
│  認証: Hollo DB (access_tokensテーブル) を直接参照        │
│  データ: Hollo API (/timelines/home, /notifications)    │
└─────────────────────────────────────────────────────────┘
```

## 環境変数

| 変数名 | 説明 | 例 |
|--------|------|-----|
| `HOLLO_URL` | Hollo本体のURL | `https://hl.oyasumi.dev` |
| `DATABASE_URL` | Hollo DB接続文字列 | `postgres://...` |
| `POLL_INTERVAL` | ポーリング間隔(ms) | `5000` |
| `VAPID_PUBLIC_KEY` | WebPush用公開鍵 | `BC1TQZ3...` |
| `VAPID_PRIVATE_KEY` | WebPush用秘密鍵 | `yf0csIg...` |
| `VAPID_SUBJECT` | WebPush用mailto | `mailto:admin@example.com` |
| `PORT` | リスンポート | `3001` |
| `DATA_DIR` | データ保存先 | `/data` |

## セットアップ手順

### 1. VAPIDキーの生成

```bash
cd hollo-stream-proxy
node gen-vapid-keys.mjs
```

出力された公開鍵と秘密鍵を環境変数に設定します。

### 2. Kubernetes設定の更新

`hollo-stream-proxy.yaml`を編集し、以下の環境変数を設定します：

```yaml
env:
  - name: HOLLO_URL
    value: "https://your-hollo-instance.example.com"
  - name: DATABASE_URL
    valueFrom:
      secretKeyRef:
        name: hollo-db-secret
        key: connection-string
  - name: VAPID_PUBLIC_KEY
    value: "生成した公開鍵"
  - name: VAPID_PRIVATE_KEY
    value: "生成した秘密鍵"
```

### 3. デプロイ

```bash
cd hollo-stream-proxy
VERSION=0.9.8 ./build.sh
cd ..
kubectl apply -f hollo-stream-proxy.yaml
```

WebSocket と Push API のルーティングは `haproxy.yaml` でパスベースに設定済みです:

- `/api/v1/streaming` → `hollo-stream-proxy:3001`
- `/api/v1/push/subscription` → `hollo-stream-proxy:3001`

追加のIngress設定は不要です。

## ログイン手順

### TheDesk v25風 OAuthフロー

1. **ポートフォワード**
   ```bash
   kubectl port-forward -n hollo-1 svc/hollo-stream-proxy 3001:3001
   ```

2. **ログイン画面にアクセス**
   ```
   http://localhost:3001/
   ```

2. **Holloで認証**
   - 「Holloで認証する」ボタンをクリック
   - Hollo本体の認証画面でログイン
   - 表示される **authorization code** をコピー

3. **codeを入力**
   - ログイン画面のフォームにcodeを貼り付け
   - 「ログイン」ボタンをクリック
   - アクセストークンが表示される

4. **トークンを保存**
   - 表示されたトークンをクライアントに設定
   - 例: `ws://stream.example.com/api/v1/streaming?access_token=XXX&stream=user`

### トークンの仕組み

このproxyでは2種類のトークンが使用されます：

1. **対クライアント用トークン**（WebSocket/Push認証用）
   - 各Mastodonクライアント（Subway Tooter、TheDesk等）がHolloで発行したアクセストークン
   - Hollo DBの `access_tokens` テーブルで検証される
   - WebSocket接続時とPush subscription登録時に使用

2. **ポーリング用トークン**（Hollo API呼び出し用）
   - このproxyがStreamingデータを取得するために、Holloに対してOAuthフローで発行したトークン
   - `oauth_sessions.json` に保存される
   - Hollo API（`/api/v1/timelines/home`、`/api/v1/notifications`）を叩くのに使用
   - **このトークンはクライアント側に渡されない**

両者は完全に独立しています。クライアントは自分のHolloトークンを直接proxyに渡し、proxyは別のトークンでHollo APIをポーリングします。

## クライアント設定例

### Subway Tooter / TheDesk

```
ストリーミングURL: wss://stream.example.com/api/v1/streaming?access_token=TOKEN&stream=user
```

### curlテスト

事前にポートフォワードが必要です:
```bash
kubectl port-forward -n hollo-1 svc/hollo-stream-proxy 3001:3001
```

```bash
# WebSocket接続テスト
wscat -c "ws://localhost:3001/api/v1/streaming?access_token=TOKEN&stream=user"

# WebPush購読
curl -X POST http://localhost:3001/api/v1/push/subscription \
  -H "Authorization: Bearer TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "subscription": {
      "endpoint": "https://push.example.com/endpoint",
      "keys": {
        "p256dh": "...",
        "auth": "..."
      }
    },
    "data": {
      "alerts": {
        "mention": true,
        "favourite": true,
        "follow": true,
        "reblog": true
      }
    }
  }'
```

## 動作確認

### ポーリング確認

```bash
kubectl logs -n hollo-1 -l app=hollo-stream-proxy -f
```

以下のログが出力されれば正常動作しています：

```
[2026-07-18 12:00:00] [stream] connected {"account":"xxx","stream":"user"}
[2026-07-18 12:00:05] [stream] update {"account":"xxx","count":1}
```

### データベース確認

```sql
-- アクセストークン一覧
SELECT code, account_owner_id, scopes, created FROM access_tokens;

-- ポーリング対象アカウント
SELECT DISTINCT account_owner_id FROM access_tokens;
```

## トラブルシューティング

### WebSocket接続失敗

- トークンがDBに存在するか確認
- `access_tokens`テーブルの`code`カラムとトークンが一致するか確認

### ポーリングが動作しない

- HOLLO_URLが正しいか確認
- Hollo APIにアクセスできるか確認
- ポーリング間隔（POLL_INTERVAL）が適切か確認

### トークン無効エラー

- Hollo本体でトークンが失効していないか確認
- DB接続が正常か確認

## 注意事項

- **DB直接参照**: 認証はHollo DBを直接参照します。DBスキーマが変更されると動作しなくなる可能性があります。
- **ポーリング方式**: リアルタイム性はPolling間隔に依存します（デフォルト5秒）。
- **スケーラビリティ**: 現在は単一Podのみ対応。複数Pod構成ではセッション共有が必要です。

## 技術詳細

### テーブル構造

#### access_tokens (Hollo本体)

```sql
CREATE TABLE access_tokens (
  code TEXT PRIMARY KEY,
  application_id UUID NOT NULL,
  account_owner_id UUID,
  grant_type grant_type NOT NULL DEFAULT 'authorization_code',
  scopes scope[] NOT NULL,
  created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

### ポーリング対象API

- `GET /api/v1/timelines/home` - ホームタイムライン
- `GET /api/v1/notifications` - 通知

## 開発

### ローカル実行

```bash
export HOLLO_URL=https://hl.oyasumi.dev
export DATABASE_URL=postgres://localhost:5432/hollo
export POLL_INTERVAL=5000

cd hollo-stream-proxy
npm install
npm run dev
```

### ビルド

```bash
cd hollo-stream-proxy
VERSION=0.9.8 ./build.sh
```

## ライセンス

AGPL-3.0 (Hollo本体に準拠)
