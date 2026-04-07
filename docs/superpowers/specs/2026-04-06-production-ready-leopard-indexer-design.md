# Production-Ready Leopard Indexer — Design Spec

**Date:** 2026-04-06
**Status:** Approved for implementation planning

---

## 1. Goals

Transform `openfga-indexer` from a single-process library into a production-ready, scalable, multi-cluster system for any OpenFGA deployment. The system must be:

- **Scalable** — independent horizontal scaling of each component
- **Reliable** — no single point of failure; graceful degradation when dependencies are unavailable
- **Correct** — fix known bugs in the existing index implementation before building on top of them
- **Observable** — full OTel traces + Prometheus metrics out of the box
- **Flexible** — pluggable messaging backend; supports both PostgreSQL and MySQL

---

## 2. Architecture Overview

### Two binaries

| Binary | Role |
|---|---|
| `cmd/write-proxy` | Intercepts all FGA writes; detects model changes; triggers Offline Pipeline |
| `cmd/indexer-server` | Runs in master or replica mode; serves Check/ListObjects via gRPC + HTTP |

### Four components

| Component | Binary | Master | Replica |
|---|---|---|---|
| **Write Proxy** | `write-proxy` | Active | Not deployed |
| **Offline Pipeline** | `indexer-server` | Active | Dormant (activates on promotion) |
| **Live Index Compute Tier** | `indexer-server` | Active | Dormant (activates on promotion) |
| **Check Gateway** | `indexer-server` | Active | Active |

### Full data flow

```
Application
    │
    ├──WRITES──► Write Proxy ──────────────────► FGA
    │                │
    │        model change detected?
    │                │ yes
    │                ▼
    │        Offline Pipeline
    │          │
    │          ├─ read all tuples from FGA
    │          ├─ build roaring bitmap shards (per type, per epoch)
    │          ├─ assign stable uint32 IDs via PostgreSQL/MySQL
    │          ├─ write shards to Object Storage
    │          └─ publish RotationEvent to MessageBus
    │                         │
    │                         ▼
    │          Live Index Compute Tier
    │          │ polls ReadChanges from FGA
    │          │ applies tuple deltas to Redis (L2 cache)
    │          │ incremental, continuous
    │
    └──READS───► Check Gateway
                    │
                    ├─ L1: in-memory roaring bitmap cache (process-local)
                    ├─ L2: Redis (cluster-local, populated from Object Storage)
                    └─ L3: FGA Check API (fallback, circuit-breaker protected)
```

---

## 3. Package Layout

```
openfga-indexer/
├── index/              # In-memory index — roaring bitmaps, directEdges, configurable config
├── cache/              # L1 in-memory shard cache (per-store, per-type)
├── redis/              # Redis adapter — L2 cache, watch tokens, master/heartbeat keys
├── stringtable/        # Stable string↔uint32 mapping (PostgreSQL + MySQL, Redis cache)
├── storage/            # Object storage adapter — shard read/write (S3/GCS/MinIO)
├── messaging/          # MessageBus interface + NATS JetStream + Redis Streams adapters
├── pipeline/           # Offline Pipeline — full rebuild, type-sharded, epoch-based
├── compute/            # Live Index Compute Tier — ReadChanges poller, delta application
├── proxy/              # Write Proxy — FGA write interceptor, model-change detection
├── gateway/            # Check Gateway — L1→L2→L3 lookup chain with circuit breakers
├── manager/            # StoreManager — owns one (pipeline+compute+cache) set per store
├── proto/              # Protobuf definitions + generated gRPC stubs
├── server/             # gRPC server + grpc-gateway HTTP/JSON handler
├── metrics/            # OTel SDK setup + Prometheus exporter + named instruments
├── admin/              # /admin/promote, /admin/status, /admin/rebuild HTTP endpoints
├── migrations/         # Versioned SQL files (migrations/postgres/, migrations/mysql/)
├── client/             # (existing, extended) FGAClient interface + OpenFGA SDK wrapper
└── cmd/
    ├── write-proxy/    # Write Proxy binary
    └── indexer-server/ # Indexer + Check Gateway binary (master/replica mode)
```

---

## 4. Index Correctness Fixes (`index/`)

### 4.1 Add `directEdges` map (fix broken delete)

The current `rebuildGroupToGroup` is a stub that does not correctly recompute transitive closure after a group-in-group edge deletion. Fix by storing direct child→parent edges explicitly:

```go
type leopardIndex struct {
    mu            sync.RWMutex
    memberToGroup map[uint32]*roaring.Bitmap // MEMBER2GROUP: userID → direct parent groups
    groupToGroup  map[uint32]*roaring.Bitmap // GROUP2GROUP: groupID → all descendants (transitive)
    directEdges   map[uint32]*roaring.Bitmap // child groupID → direct parent groups
}
```

Delete flow:
1. Remove edge from `directEdges`
2. Clear `groupToGroup` for affected ancestors
3. BFS-recompute `groupToGroup` from `directEdges` for the affected subgraph

### 4.2 Replace `sortedSet []string` with roaring bitmaps

All posting lists use `*roaring.Bitmap` (`github.com/RoaringBitmap/roaring`). Maps are keyed by `uint32` (string table ID).

Benefits:
- Set intersection via `And()` — SIMD-accelerated bitwise AND
- Automatic density adaptation (array vs bitmap containers)
- Compact portable serialization via `WriteTo` / `ReadFrom`

### 4.3 Configurable relation name and group prefix

```go
type IndexConfig struct {
    GroupRelation string // default: "member"
    GroupPrefix   string // default: "group:"
    ObjectType    string // the type this shard covers, e.g. "group"
}

func NewWithConfig(cfg IndexConfig) Index { ... }
func New() Index { return NewWithConfig(IndexConfig{}) } // backwards compatible
```

---

## 5. String Table (`stringtable/`)

### 5.1 Purpose

Maps arbitrary string IDs (e.g. `"user:alice"`, `"group:engineering"`) to stable `uint32` integers. IDs are stable across full rebuilds to support auditing and debugging.

### 5.2 Storage

**Authoritative store: PostgreSQL or MySQL** (same DB instance as OpenFGA)

```sql
-- PostgreSQL
CREATE TABLE leopard_string_table (
    id         BIGSERIAL PRIMARY KEY,
    store_id   VARCHAR(26) NOT NULL,
    value      TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (store_id, value)
);

-- MySQL
CREATE TABLE leopard_string_table (
    id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    store_id   VARCHAR(26) NOT NULL,
    value      TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_store_value (store_id, value(768))
);
```

ID assignment is idempotent:
- PostgreSQL: `INSERT ... ON CONFLICT DO NOTHING RETURNING id`
- MySQL: `INSERT IGNORE ... ; SELECT id FROM leopard_string_table WHERE store_id=? AND value=?`

**Cache: Redis Hash** (per store, loaded at startup, updated incrementally)

```
stringtable:fwd:<storeID>   HASH  →  { "user:alice": 1, "group:eng": 2, ... }
stringtable:rev:<storeID>   HASH  →  { "1": "user:alice", "2": "group:eng", ... }
```

**In-memory: two structures per loaded shard**
- `fwd map[string]uint32` — string → ID (forward lookup)
- `rev []string` — index = uint32 ID (O(1) reverse lookup)

### 5.3 Scope

String table is **per-store, shared across all type shards**. The same string always maps to the same uint32 within a store, enabling cross-type queries without re-encoding.

---

## 6. Redis Adapter (`redis/`)

Redis serves as a **cluster-local L2 cache and coordination store** only. It is not the authoritative store for any data.

```go
type Store interface {
    // L2 shard cache
    CacheShard(ctx context.Context, storeID, objectType string, epoch uint64, data []byte) error
    LoadCachedShard(ctx context.Context, storeID, objectType string, epoch uint64) ([]byte, error)

    // Watch token coordination (Live Index Compute Tier)
    SetWatchToken(ctx context.Context, storeID, token string) error
    GetWatchToken(ctx context.Context, storeID string) (string, error)

    // Master/replica coordination
    SetMaster(ctx context.Context, clusterID, instanceID string) error
    GetMaster(ctx context.Context) (clusterID, instanceID string, err error)
    RefreshHeartbeat(ctx context.Context, clusterID string, ttl time.Duration) error
    WatchHeartbeat(ctx context.Context, clusterID string) (<-chan struct{}, error)
}
```

**Redis key schema:**

```
shard:cache:<storeID>:<type>:<epoch>   → binary roaring bitmap shard (L2 cache)
watchtoken:<storeID>                   → ReadChanges continuation token
master:<clusterID>                     → instanceID of current master
heartbeat:<clusterID>                  → empty, TTL = 2× heartbeat interval
stringtable:fwd:<storeID>              → HASH of string→uint32
stringtable:rev:<storeID>              → HASH of uint32→string
```

---

## 7. Object Storage Adapter (`storage/`)

Shards are **written by the master's Offline Pipeline** and **read by all instances** on rotation. Object storage is the durable shard store.

```go
type ShardStore interface {
    WriteShard(ctx context.Context, storeID, objectType string, epoch uint64, data []byte) error
    ReadShard(ctx context.Context, storeID, objectType string, epoch uint64) ([]byte, error)
    DeleteShard(ctx context.Context, storeID, objectType string, epoch uint64) error
    ListEpochs(ctx context.Context, storeID, objectType string) ([]uint64, error)
}
```

Object path: `leopard/<storeID>/<objectType>/<epoch>.shard`

Supported backends: S3, GCS, MinIO (configurable via `storage.Config`).

Old epochs are garbage-collected after a configurable retention period (default: 3 most recent epochs per type).

---

## 8. Messaging Adapter (`messaging/`)

Provides reliable, durable delivery of control signals across clusters. Decoupled from shard data transport (which uses object storage).

### 8.1 Interface

```go
type MessageBus interface {
    // Shard rotation — published by Offline Pipeline, consumed by all Check Gateways
    PublishRotation(ctx context.Context, event RotationEvent) error
    SubscribeRotation(ctx context.Context, storeID string, handler func(RotationEvent)) (Subscription, error)

    // Rebuild trigger — published by Write Proxy, consumed by Offline Pipeline
    PublishRebuild(ctx context.Context, event RebuildEvent) error
    SubscribeRebuild(ctx context.Context, storeID string, handler func(RebuildEvent)) (Subscription, error)

    // Promotion — published by admin API, consumed by all instances
    PublishPromotion(ctx context.Context, event PromotionEvent) error
    SubscribePromotion(ctx context.Context, handler func(PromotionEvent)) (Subscription, error)

    Close() error
}

type RotationEvent struct {
    StoreID    string
    Epoch      uint64
    Types      []string  // object types included in this epoch
    PublishedAt time.Time
}

type RebuildEvent struct {
    StoreID  string
    ModelID  string
    TriggeredAt time.Time
}

type PromotionEvent struct {
    ClusterID  string
    InstanceID string
    PromotedAt time.Time
}
```

### 8.2 Implementations

**NATS JetStream** (`messaging/nats/`)
- Streams: `LEOPARD_ROTATIONS`, `LEOPARD_REBUILDS`, `LEOPARD_PROMOTIONS`
- Durable consumer groups per cluster
- At-least-once delivery with ACK
- Dependency: `github.com/nats-io/nats.go`

**Redis Streams** (`messaging/redisstream/`)
- Streams: `leopard:rotations`, `leopard:rebuilds`, `leopard:promotions`
- Consumer groups with `XACK`
- At-least-once delivery
- No new dependency (reuses existing Redis connection)

Selection via config:

```yaml
messaging:
  backend: nats          # or "redis_streams"
  nats:
    url: nats://nats:4222
  redis_streams:
    # uses the same redis config block
```

---

## 9. Offline Pipeline (`pipeline/`)

Triggered by Write Proxy via MessageBus `RebuildEvent`. Runs on master only.

### 9.1 Flow

```
Receive RebuildEvent (storeID, modelID)
    │
    ├─ acquire per-store mutex (one build at a time)
    │   if build in progress: queue exactly one pending rebuild (buffered chan depth 1)
    │
    ├─ allocate epoch = next sequence number (atomic, persisted in DB)
    │
    ├─ ReadTuples from FGA (full snapshot)
    │
    ├─ partition tuples by object type
    │   skip types not in config.IndexedTypes
    │
    ├─ FOR each indexed type:
    │     assign/fetch uint32 IDs from stringtable (PostgreSQL/MySQL)
    │     build roaring bitmap index shard
    │     serialize shard to bytes
    │     write to Object Storage: leopard/<storeID>/<type>/<epoch>.shard
    │
    ├─ if any shard write fails:
    │     delete all written shards for this epoch (compensating rollback)
    │     return error, do not publish rotation
    │
    ├─ update watchtoken:<storeID> in Redis to current ReadChanges position
    │
    └─ publish RotationEvent { storeID, epoch, types } to MessageBus
```

### 9.2 Consistency (epoch-gated rotation)

Replicas receiving a `RotationEvent`:
1. Download all shards for the epoch from Object Storage
2. Cache in local Redis (`shard:cache:<storeID>:<type>:<epoch>`)
3. Only after all shards confirmed: atomically swap L1 in-memory cache
4. If a shard download times out (replication/network lag): retry with exponential backoff; continue serving old shards
5. ACK the rotation message only after successful L1 swap

---

## 10. Live Index Compute Tier (`compute/`)

Runs on master only (dormant on replicas until promotion). Polls `ReadChanges` and applies tuple deltas incrementally to the current epoch shard in Redis and propagates them to replica clusters via the MessageBus.

```go
type ComputeTier struct {
    fga      client.FGAClient
    redis    redis.Store
    bus      messaging.MessageBus
    opts     ComputeOptions
}
```

- Listens on `WatchToken` channel; re-anchors after each Offline Pipeline rotation
- Applies `ApplyTupleWrite` / `ApplyTupleDelete` to in-memory shard, then persists delta to local Redis
- Publishes `DeltaEvent` to MessageBus for each change batch so replica clusters can apply them to their own local Redis without polling FGA
- Exponential backoff on `ReadChanges` errors (base: 1s, max: 60s, jitter: ±20%)
- Replicas subscribe to `DeltaEvent` stream and apply deltas locally; eventual consistency is acceptable

The MessageBus interface gains two methods:

```go
// Delta propagation — published by master Compute Tier, consumed by replica Check Gateways
PublishDelta(ctx context.Context, event DeltaEvent) error
SubscribeDelta(ctx context.Context, storeID string, handler func(DeltaEvent)) (Subscription, error)

type DeltaEvent struct {
    StoreID   string
    Epoch     uint64
    Changes   []TupleChange // reuses client.TupleChange
    AppliedAt time.Time
}
```

This keeps replica clusters independent from FGA's `ReadChanges` endpoint while staying current between full rebuilds.

---

## 11. Write Proxy (`proxy/`)

Separate binary (`cmd/write-proxy`). Deployed only in master cluster.

### 11.1 Responsibilities

1. Forward all FGA write requests unchanged
2. Detect `WriteAuthorizationModel` calls
3. Publish `RebuildEvent` to MessageBus on successful model write

### 11.2 Interface (gRPC + HTTP/JSON gateway)

Mirrors FGA's write API surface:

```protobuf
service WriteProxy {
    rpc WriteTuples(WriteTuplesRequest) returns (WriteTuplesResponse);
    rpc DeleteTuples(DeleteTuplesRequest) returns (DeleteTuplesResponse);
    rpc WriteAuthorizationModel(WriteAuthorizationModelRequest)
        returns (WriteAuthorizationModelResponse);
}
```

### 11.3 Model change handling

- Forward `WriteAuthorizationModel` to FGA
- On success: publish `RebuildEvent { storeID, modelID }` to MessageBus
- If MessageBus publish fails: log error, do not fail the original write (FGA write already succeeded)
- Rebuild signal uses buffered channel depth 1: newer model supersedes queued older one

---

## 12. Check Gateway (`gateway/`)

Serves `Check` and `ListObjects` from all instances (master and replica).

### 12.1 Three-tier lookup

```
Check(ctx, user, relation, object)
    │
    ├─ is relation indexable? (relation == GroupRelation && object type in IndexedTypes)
    │     │ yes
    │     ├─ L1: in-memory cache hit?  → return immediately
    │     ├─ L2: Redis shard exists?   → load into L1, return
    │     └─ L3: FGA Check API        → return (circuit breaker protected)
    │
    └─ not indexable → L3: FGA Check API directly
```

### 12.2 Circuit breakers

| Boundary | Open condition | Half-open probe |
|---|---|---|
| Gateway → FGA (L3) | 5 consecutive failures | 1 probe every 10s |
| Gateway → Redis (L2) | 3 consecutive timeouts | 1 probe every 5s |
| Write Proxy → FGA | 5 consecutive failures | 1 probe every 15s |

When FGA circuit is open: return last known L2 result if available, else return error with `503 Service Unavailable`.

Implementation: `github.com/sony/gobreaker`

---

## 13. Cluster Roles and Promotion (`admin/`)

### 13.1 Role detection

Each `indexer-server` instance determines its role at startup from config (`role: master` or `role: replica`). Roles can also be dynamically changed via the admin API without restart.

### 13.2 Promotion flow

```
1. Master heartbeat key expires in replica's local Redis
        │
2. All replicas → DEGRADED mode
   - continue serving reads from last known L1/L2 state
   - GET /admin/status returns:
     { "state": "degraded", "role": "replica", "last_master_seen": "<timestamp>" }
        │
3. Admin identifies healthiest replica via /admin/status
        │
4. Admin calls POST /admin/promote on chosen replica
        │
5. Chosen replica:
   a. Checks no live master exists (heartbeat key absent or expired)
   b. Writes master:<clusterID> = <instanceID> to local Redis
   c. Publishes PromotionEvent to MessageBus
   d. Starts pipeline + compute goroutines live (no restart)
   e. Runs immediate full rebuild to catch up on missed deltas
        │
6. Other replicas receive PromotionEvent, remain in replica mode
        │
7. Old master comes back:
   - Sees master:<clusterID> ≠ its instanceID
   - Enters replica mode automatically
   - Drains in-flight requests before switching
```

### 13.3 Split-brain protection

A replica refuses self-promotion if it can still read a live heartbeat key for the existing master — promotion only proceeds when the heartbeat has genuinely expired.

### 13.4 Admin endpoints

```
GET  /admin/status           → cluster role, state, last master seen, index freshness
POST /admin/promote          → promote this replica to master (guarded by split-brain check)
POST /admin/rebuild?store=X  → manually trigger Offline Pipeline for store X (master only)
GET  /admin/stores           → list all registered stores and their index state
```

---

## 14. Service API (`proto/` + `server/`)

### 14.1 gRPC service definition

```protobuf
syntax = "proto3";
package leopard.v1;

service IndexerService {
    // Authorization checks
    rpc Check(CheckRequest) returns (CheckResponse);
    rpc ListObjects(ListObjectsRequest) returns (ListObjectsResponse);

    // Index introspection
    rpc GetStoreStatus(GetStoreStatusRequest) returns (GetStoreStatusResponse);
}

message CheckRequest {
    string store_id  = 1;
    string user      = 2;
    string relation  = 3;
    string object    = 4;
}

message CheckResponse {
    bool   allowed     = 1;
    string source      = 2;  // "index_l1" | "index_l2" | "fga_api"
    string trace_id    = 3;
}

message ListObjectsRequest {
    string store_id    = 1;
    string user        = 2;
    string relation    = 3;
    string object_type = 4;
}

message ListObjectsResponse {
    repeated string objects = 1;
    string          source  = 2;
}
```

HTTP/JSON gateway via `grpc-gateway` is auto-generated from proto annotations.

### 14.2 Health endpoints

Standard gRPC health protocol (`grpc.health.v1`):
- `SERVING` — at least one store has a loaded shard
- `NOT_SERVING` — no shard loaded (starting up or all stores degraded)

HTTP aliases: `GET /healthz` (liveness), `GET /readyz` (readiness).

---

## 15. StoreManager (`manager/`)

Owns one `(pipeline, compute, cache)` set per registered FGA store. Handles lifecycle: start, stop, rebuild, rotation.

```go
type StoreManager struct {
    stores  map[string]*storeEntry  // storeID → entry
    mu      sync.RWMutex
}

type storeEntry struct {
    pipeline *pipeline.Pipeline
    compute  *compute.ComputeTier
    cache    *cache.Cache
    cancel   context.CancelFunc
}

func (m *StoreManager) RegisterStore(ctx context.Context, storeID string) error
func (m *StoreManager) DeregisterStore(ctx context.Context, storeID string) error
func (m *StoreManager) TriggerRebuild(ctx context.Context, storeID string) error
```

Stores are registered from config at startup. Dynamic registration is supported via the admin API.

---

## 16. Observability (`metrics/`)

### 16.1 OTel traces

Spans on every inbound RPC (`Check`, `ListObjects`, `WriteTuples`, etc.) with attributes:
- `store_id`, `user`, `relation`, `object_type`, `result_source` (`l1/l2/l3`)
- `db.system` on PostgreSQL/MySQL calls
- `messaging.system` on MessageBus publish/subscribe

### 16.2 Prometheus metrics

| Metric | Type | Labels |
|---|---|---|
| `leopard_check_total` | Counter | store_id, source, allowed |
| `leopard_check_duration_seconds` | Histogram | store_id, source |
| `leopard_rebuild_duration_seconds` | Histogram | store_id, status |
| `leopard_rebuild_tuple_count` | Gauge | store_id |
| `leopard_shard_age_seconds` | Gauge | store_id, type |
| `leopard_watcher_lag_changes` | Gauge | store_id |
| `leopard_circuit_breaker_state` | Gauge | target, state |
| `leopard_string_table_size` | Gauge | store_id |

OTel metrics bridge exposes all instruments to Prometheus via `/metrics` endpoint.

### 16.3 Structured logging

`log/slog` with OTel log bridge. Log levels: `DEBUG` (delta application), `INFO` (builds, rotations, promotions), `WARN` (circuit breaker half-open, replication lag), `ERROR` (build failures, split-brain attempts).

---

## 17. Configuration

```yaml
# indexer-server config
role: master          # or "replica"
cluster_id: cluster-us-east-1

fga:
  api_url: http://openfga:8080

database:
  driver: postgres    # or "mysql"
  dsn: postgres://user:pass@host/openfga

redis:
  addr: redis:6379

object_storage:
  backend: s3         # or "gcs" or "minio"
  bucket: leopard-shards
  region: us-east-1

messaging:
  backend: nats       # or "redis_streams"
  nats:
    url: nats://nats:4222

stores:
  - id: "01JSTORE123"
    indexed_types: ["group", "folder"]
    rebuild_on_startup: true

index:
  group_relation: "member"
  group_prefix: "group:"

server:
  grpc_addr: :50051
  http_addr: :8090
  admin_addr: :9090

metrics:
  prometheus_addr: :9091
  otlp_endpoint: http://otel-collector:4317

heartbeat:
  interval: 10s
  ttl: 30s
```

---

## 18. Database Migrations (`migrations/`)

Versioned SQL files, run at service startup via embedded migrator.

```
migrations/
├── postgres/
│   ├── 001_create_leopard_string_table.sql
│   └── 002_create_leopard_epochs.sql
└── mysql/
    ├── 001_create_leopard_string_table.sql
    └── 002_create_leopard_epochs.sql
```

**Epoch counter table** — stores the monotonically increasing epoch number per store, ensuring stable epoch assignment across restarts and replica promotions:

```sql
-- PostgreSQL
CREATE TABLE leopard_epochs (
    store_id   VARCHAR(26) PRIMARY KEY,
    epoch      BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- MySQL
CREATE TABLE leopard_epochs (
    store_id   VARCHAR(26) PRIMARY KEY,
    epoch      BIGINT UNSIGNED NOT NULL DEFAULT 0,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

Epoch increment is atomic: `UPDATE leopard_epochs SET epoch = epoch + 1 WHERE store_id = ? RETURNING epoch` (PostgreSQL) or equivalent transaction in MySQL.

Migration state tracked in `leopard_schema_migrations` table (created on first run).

---

## 19. Testing Strategy

| Layer | Approach |
|---|---|
| `index/` | Unit tests with table-driven cases; `BenchmarkIsMember` for roaring bitmap intersection throughput |
| `stringtable/` | Unit tests with in-memory SQLite (postgres dialect); mock Redis |
| `pipeline/` | Unit tests with `client.MockClient`; integration tests with dockertest (FGA + Postgres) |
| `compute/` | Unit tests with `client.MockClient`; verify delta correctness against known tuples |
| `gateway/` | Unit tests verifying L1→L2→L3 fallback ordering and circuit breaker behaviour |
| `messaging/` | Interface contract tests run against both NATS and Redis Streams implementations |
| `proxy/` | Unit tests verifying model-change detection triggers rebuild signal |
| End-to-end | `//go:build integration` tag; dockertest spins up FGA + Postgres + Redis + NATS |

---

## 20. Open Questions (resolved during implementation)

None — all key decisions have been made.

---

## 21. Decision Log

| Decision | Rationale |
|---|---|
| Separate `write-proxy` binary | Independent horizontal scaling from indexer-server |
| Object storage for shards | Shards are immutable per epoch; object storage is cheaper and more durable than Redis for large blobs |
| Configurable MessageBus (NATS / Redis Streams) | Operators choose based on existing infrastructure |
| PostgreSQL/MySQL in same DB as OpenFGA | Eliminates new persistent dependency; string table is low-write, high-read |
| Per-store string table (shared across types) | Same string always maps to same uint32; cross-type queries need no re-encoding |
| Roaring bitmaps for posting lists | SIMD-accelerated intersection; automatic density adaptation; compact serialization |
| Admin-approval-required promotion | Prevents accidental split-brain; gives operators explicit control during incidents |
| Per-cluster Redis | Failure isolation between clusters; no cross-cluster Redis dependency in the hot read path |
| Auth delegated to infrastructure | Separation of concerns; service mesh / API gateway handles mTLS and token validation |
