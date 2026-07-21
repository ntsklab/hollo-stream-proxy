# Hollo Stream Proxy - ログイン手順

## 概要

HolloのWebSocket streaming/push通知を補完するproxyです。  
**後日対応予定**: セットアップとデプロイ

---

## クイックスタート

### 1. ログイン画面にアクセス

```
http://localhost:3001/
```

### 2. Holloで認証

1. 「Holloで認証する」ボタンをクリック
2. Hollo本体の認証画面でログイン
3. 表示される **authorization code** をコピー

### 3. codeを入力

1. ログイン画面のフォームにcodeを貼り付け
2. 「ログイン」ボタンをクリック
3. アクセストークンが表示される

### 4. クライアントに設定

表示されたトークンをSubway Tooterなどのクライアントに設定：

```
wss://stream.example.com/api/v1/streaming?access_token=TOKEN&stream=user
```

---

## 既存トークンの利用

Hollo本体で既に発行されているアクセストークンも使用可能です。

```sql
-- DBでトークン確認
SELECT code, scopes FROM access_tokens;
```

---

## 環境変数（後日設定）

| 変数名 | 説明 | 必須 |
|--------|------|------|
| `HOLLO_URL` | Hollo本体のURL | ✅ |
| `DATABASE_URL` | Hollo DB接続文字列 | ✅ |
| `VAPID_PUBLIC_KEY` | WebPush用公開鍵 | ⚠️ Push使用時 |
| `VAPID_PRIVATE_KEY` | WebPush用秘密鍵 | ⚠️ Push使用時 |

---

## 動作確認

```bash
# ログ確認
kubectl logs -n hollo-1 -l app=hollo-stream-proxy -f

# WebSocketテスト
wscat -c "ws://localhost:3001/api/v1/streaming?access_token=TOKEN&stream=user"
```

---

## 注意事項

- 認証はHollo DBの`access_tokens`テーブルを直接参照
- ポーリング間隔: 5秒（デフォルト）
- セットアップ詳細: `SETUP.md` を参照

---

**最終更新**: 2026-07-18  
**バージョン**: 0.9.8
