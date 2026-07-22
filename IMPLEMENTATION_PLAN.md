# Hollo Stream Proxy 実装計画

## Hollo API 実装状況（ソースコード確認済み）

### タイムラインAPI（`src/api/v1/timelines.ts`）

| エンドポイント | 実装 | パラメータ | 備考 |
|---|---|---|---|
| `GET /api/v1/timelines/public` | ✅ 完全実装 | `local`, `remote`, `max_id`, `since_id`, `min_id`, `limit` | `only_media` 非対応 |
| `GET /api/v1/timelines/home` | ✅ 完全実装 | `max_id`, `since_id`, `min_id`, `limit` | |
| `GET /api/v1/timelines/list/:list_id` | ✅ 完全実装 | `local`, `remote`, `max_id`, `since_id`, `min_id`, `limit` | |
| `GET /api/v1/timelines/tag/:hashtag` | ✅ 完全実装 | `local`, `remote`, `max_id`, `since_id`, `min_id`, `limit` | `posts.tags` でJSONBクエリ。stubではない |

### Stub/未実装API

| エンドポイント | 状態 | 詳細 |
|---|---|---|
| `GET /api/v1/conversations` | ❌ 存在しない | ファイル自体がない。`direct`ストリームは実装不可 |
| `GET /api/v1/announcements` | ⚠️ stub | `return c.json([])` を返すだけ。announcement系イベントも不要 |
| `GET /api/v1/trends/*` | ⚠️ stub | すべて空配列を返す |
| `GET /api/v1/suggestions` | ⚠️ stub | 空配列を返す |

### メディアフィルタ（`only_media`）

- `timelines/public`, `timelines/tag/:hashtag` → **非対応**（`publicTimelineQuerySchema`に存在しない）
- `accounts/:id/statuses` → 対応（個別に`timelineQuerySchema.extend`で追加）
- したがって `public:media`, `public:local:media`, `public:remote:media` は **実装不可**

---

## 現在の実装（proxy）

- `user` — ホームTL + 通知（`/api/v1/timelines/home` + `/api/v1/notifications`）
- `user:notification` — 通知のみ（`/api/v1/notifications`）
- `list` — リストTL（`/api/v1/timelines/list/:list_id`）

---

## 追加可能なストリーム種別

### Phase 1: 基本（高優先度）

#### 1. `public` — 連合タイムライン
- **Hollo API**: `GET /api/v1/timelines/public`（認証必須、`withAccountOwner`）
- **クエリパラメータ**: なし（デフォルト: 全公開投稿）
- **ポーリング**: アカウント単位のポーリングが必要（認証必須のため）
- **重複排除キー**: `"public"`
- **配信イベント**: `update`, `delete`

#### 2. `public:local` — ローカルタイムライン
- **Hollo API**: `GET /api/v1/timelines/public?local=true`
- **重複排除キー**: `"public:local"`

#### 3. `hashtag` — ハッシュタグタイムライン
- **Hollo API**: `GET /api/v1/timelines/tag/:hashtag`
- **追加クエリ**: `tag` パラメータが必要
- **重複排除キー**: `"hashtag:<tag>"`
- **配信イベント**: `update`, `delete`

#### 4. `hashtag:local` — ローカルハッシュタグタイムライン
- **Hollo API**: `GET /api/v1/timelines/tag/:hashtag?local=true`
- **重複排除キー**: `"hashtag:local:<tag>"`

### Phase 2: 拡張（中優先度）

#### 5. `public:remote` — リモートタイムライン
- **Hollo API**: `GET /api/v1/timelines/public?remote=true`
- **重複排除キー**: `"public:remote"`

### 実装不可（Holloの制限）

| ストリーム | 理由 |
|---|---|
| `public:media` | Holloが`only_media`非対応 |
| `public:local:media` | 同上 |
| `public:remote:media` | 同上 |
| `direct` | Holloにconversations APIが存在しない |

---

## 技術的実装詳細

### 1. WebSocket upgrade ハンドラの拡張

```javascript
// 現行
const stream = url.searchParams.get("stream") || "user";
const listId = stream === "list" ? url.searchParams.get("list") : null;

// 変更後
const stream = url.searchParams.get("stream") || "user";
const listId = (stream === "list") ? url.searchParams.get("list") : null;
const tag = (stream === "hashtag" || stream === "hashtag:local")
  ? url.searchParams.get("tag")
  : null;

// バリデーション
if (stream === "list" && !listId) { /* 400 */ }
if ((stream === "hashtag" || stream === "hashtag:local") && !tag) { /* 400 */ }
```

### 2. ストリームエントリーの拡張

```javascript
const streamEntry = {
  ws,
  initialized: false,
  userAgent: ua,
  stream,      // "user", "public", "hashtag" 等
  listId,      // list ストリーム用
  tag,         // hashtag ストリーム用
};
```

### 3. 新規APIフェッチ関数

```javascript
async function fetchPublicTimelineAPI({ accessToken, local, remote, sinceId }) {
  const params = new URLSearchParams({ limit: "40" });
  if (local) params.set("local", "true");
  if (remote) params.set("remote", "true");
  if (sinceId) params.set("since_id", sinceId);

  const res = await fetch(
    `${HOLLO_URL}/api/v1/timelines/public?${params}`,
    { headers: { Authorization: `Bearer ${accessToken}` } }
  );
  if (!res.ok) return { statuses: [], latestId: null };
  const statuses = await res.json();
  const latestId = statuses.length > 0 ? statuses[0].id : null;
  return { statuses, latestId };
}

async function fetchHashtagTimelineAPI({ accessToken, tag, local, sinceId }) {
  const params = new URLSearchParams({ limit: "40" });
  if (local) params.set("local", "true");
  if (sinceId) params.set("since_id", sinceId);

  const res = await fetch(
    `${HOLLO_URL}/api/v1/timelines/tag/${encodeURIComponent(tag)}?${params}`,
    { headers: { Authorization: `Bearer ${accessToken}` } }
  );
  if (!res.ok) return { statuses: [], latestId: null };
  const statuses = await res.json();
  const latestId = statuses.length > 0 ? statuses[0].id : null;
  return { statuses, latestId };
}
```

### 4. ポーリングロジックの拡張

```javascript
// 既存のストリーム分類に追加:
const publicStreams = streams
  ? [...streams].filter(s =>
      s.stream === "public" ||
      s.stream === "public:local" ||
      s.stream === "public:remote"
    )
  : [];

const hashtagStreams = streams
  ? [...streams].filter(s =>
      s.stream === "hashtag" || s.stream === "hashtag:local"
    )
  : [];

// public ストリーム用のポーリング
// variant ごとにフェッチし、該当ストリームに配信
const publicVariants = [];
if (publicStreams.some(s => s.stream === "public"))
  publicVariants.push({ stream: "public", local: false, remote: false });
if (publicStreams.some(s => s.stream === "public:local"))
  publicVariants.push({ stream: "public:local", local: true, remote: false });
if (publicStreams.some(s => s.stream === "public:remote"))
  publicVariants.push({ stream: "public:remote", local: false, remote: true });

// hashtag ストリーム用のポーリング
// tag + local の組み合わせごとにフェッチ
const hashtagGroups = new Map();
for (const s of hashtagStreams) {
  const key = `${s.stream}:${s.tag}`;
  if (!hashtagGroups.has(key)) {
    const local = s.stream === "hashtag:local";
    hashtagGroups.set(key, { stream: s.stream, tag: s.tag, local });
  }
}
```

### 5. 重複排除

```javascript
// tlMaxIds のキー設計
// 既存: `${accountId}:user`, `${accountId}:list:<listId>`
// 追加: `${accountId}:public`, `${accountId}:public:local`, `${accountId}:public:remote`
//        `${accountId}:hashtag:<tag>`, `${accountId}:hashtag:local:<tag>`
```

### 6. 配信イベント

| イベント | 対象ストリーム | 備考 |
|---|---|---|
| `update` | public, public:local, public:remote, hashtag, hashtag:local, list | |
| `delete` | 上記同上 | 現状のproxyはdeleteイベントを配信していない可能性がある |

---

## 実装不可のイベント（Holloの制限）

| イベント | 理由 |
|---|---|
| `conversation` | Holloにconversations APIなし |
| `announcement` | `/announcements`がstub（空配列） |
| `announcement.reaction` | 同上 |
| `announcement.delete` | 同上 |
| `filters_changed` | Holloにkeyword filter APIの確認が必要 |
| `status.update` | Holloの編集機能の確認が必要 |
| `notifications_merged` | Holloのnotification merge機能の確認が必要 |

---

## 検証計画

1. 各ストリームタイプでWebSocket接続が確立できるか
2. `tag`/`list` パラメータのバリデーション
3. 認証付きAPIコールが正しく動作するか（public/hashtag はトークン必須）
4. 重複排除が正しく機能するか
5. Mastodonクライアントでの動作確認

## 次のステップ

1. `public` + `public:local` を実装（最も需要が高い）
2. `hashtag` + `hashtag:local` を実装
3. `public:remote` を実装
4. テストクライアントで検証
