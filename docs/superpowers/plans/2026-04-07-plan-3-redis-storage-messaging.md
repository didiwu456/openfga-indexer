# Redis, Object Storage & Messaging Adapters — Implementation Plan 3 of 5

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the three infrastructure adapters the pipeline and service layer depend on: the Redis L2 cache and coordination store, the object storage shard store, and the pluggable MessageBus (NATS JetStream + Redis Streams).

**Architecture:** Each adapter is a small Go package with a single interface and one or more implementations. Tests use in-process fakes (for unit) and dockertest containers (for integration, guarded by `//go:build integration`). The MessageBus interface is verified by a shared contract test suite run against both implementations.

**Tech Stack:** `github.com/redis/go-redis/v9`, `github.com/aws/aws-sdk-go-v2/service/s3`, `github.com/nats-io/nats.go`.

---

## File Map

| Action | File | Responsibility |
|---|---|---|
| Create | `redis/store.go` | `Store` interface |
| Create | `redis/redis.go` | `go-redis/v9` implementation |
| Create | `redis/memory.go` | In-memory fake for unit tests |
| Create | `redis/redis_test.go` | Unit tests (fake) + integration tests |
| Create | `storage/store.go` | `ShardStore` interface |
| Create | `storage/s3.go` | AWS S3 implementation |
| Create | `storage/memory.go` | In-memory fake for unit tests |
| Create | `storage/store_test.go` | Unit tests (fake) |
| Create | `messaging/bus.go` | `MessageBus` interface + all event types |
| Create | `messaging/nats/nats.go` | NATS JetStream implementation |
| Create | `messaging/redisstream/redisstream.go` | Redis Streams implementation |
| Create | `messaging/contract_test.go` | Shared contract suite |
| Create | `messaging/nats/nats_test.go` | Run contract against NATS |
| Create | `messaging/redisstream/redisstream_test.go` | Run contract against Redis Streams |
| Modify | `go.mod` / `go.sum` | Add new dependencies |

---

## Task 1: Add Dependencies

- [ ] **Step 1: Add all adapter dependencies**

```bash
cd d:/GitHub/openfga-indexer
go get github.com/redis/go-redis/v9@latest
go get github.com/aws/aws-sdk-go-v2@latest
go get github.com/aws/aws-sdk-go-v2/config@latest
go get github.com/aws/aws-sdk-go-v2/service/s3@latest
go get github.com/nats-io/nats.go@latest
go get github.com/nats-io/nats.go/jetstream@latest
go mod tidy
```

- [ ] **Step 2: Commit**

```bash
git add go.mod go.sum
git commit -m "chore: add redis, s3, nats adapter dependencies"
```

---

## Task 2: Redis Adapter

**Files:**
- Create: `redis/store.go`
- Create: `redis/redis.go`
- Create: `redis/memory.go`
- Create: `redis/redis_test.go`

- [ ] **Step 1: Write the failing test**

Create `redis/redis_test.go`:

```go
package redis_test

import (
	"context"
	"testing"
	"time"

	leopardredis "github.com/yourorg/openfga-indexer/redis"
)

func runStoreContract(t *testing.T, s leopardredis.Store) {
	t.Helper()
	ctx := context.Background()

	t.Run("cache_and_load_shard", func(t *testing.T) {
		data := []byte("fake-shard-bytes")
		if err := s.CacheShard(ctx, "store1", "group", 1, data); err != nil {
			t.Fatalf("CacheShard: %v", err)
		}
		got, err := s.LoadCachedShard(ctx, "store1", "group", 1)
		if err != nil {
			t.Fatalf("LoadCachedShard: %v", err)
		}
		if string(got) != string(data) {
			t.Fatalf("LoadCachedShard = %q, want %q", got, data)
		}
	})

	t.Run("load_missing_shard_returns_nil", func(t *testing.T) {
		got, err := s.LoadCachedShard(ctx, "store1", "group", 99)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != nil {
			t.Fatal("expected nil for missing shard")
		}
	})

	t.Run("watch_token_roundtrip", func(t *testing.T) {
		if err := s.SetWatchToken(ctx, "store1", "token-abc"); err != nil {
			t.Fatalf("SetWatchToken: %v", err)
		}
		got, err := s.GetWatchToken(ctx, "store1")
		if err != nil {
			t.Fatalf("GetWatchToken: %v", err)
		}
		if got != "token-abc" {
			t.Fatalf("GetWatchToken = %q, want %q", got, "token-abc")
		}
	})

	t.Run("master_roundtrip", func(t *testing.T) {
		if err := s.SetMaster(ctx, "cluster-a", "instance-1"); err != nil {
			t.Fatalf("SetMaster: %v", err)
		}
		clusterID, instanceID, err := s.GetMaster(ctx)
		if err != nil {
			t.Fatalf("GetMaster: %v", err)
		}
		if clusterID != "cluster-a" || instanceID != "instance-1" {
			t.Fatalf("GetMaster = (%q, %q), want (cluster-a, instance-1)", clusterID, instanceID)
		}
	})

	t.Run("heartbeat_refresh", func(t *testing.T) {
		if err := s.RefreshHeartbeat(ctx, "cluster-a", 100*time.Millisecond); err != nil {
			t.Fatalf("RefreshHeartbeat: %v", err)
		}
	})
}

func TestMemoryStore(t *testing.T) {
	s := leopardredis.NewMemoryStore()
	runStoreContract(t, s)
}
```

- [ ] **Step 2: Run to confirm failure**

```bash
go test ./redis/... -run TestMemoryStore -v
```

Expected: `FAIL — no Go files in redis`

- [ ] **Step 3: Create `redis/store.go`**

```go
// Package redis provides the cluster-local L2 cache and coordination adapter
// backed by Redis. It is NOT the authoritative store for any data.
package redis

import (
	"context"
	"time"
)

// Store is the Redis adapter interface.
// All methods must be safe for concurrent use.
type Store interface {
	// L2 shard cache — stores serialized roaring bitmap shards.
	CacheShard(ctx context.Context, storeID, objectType string, epoch uint64, data []byte) error
	LoadCachedShard(ctx context.Context, storeID, objectType string, epoch uint64) ([]byte, error)

	// Watch token — ReadChanges continuation token for the compute tier.
	SetWatchToken(ctx context.Context, storeID, token string) error
	GetWatchToken(ctx context.Context, storeID string) (string, error)

	// Master/replica coordination.
	SetMaster(ctx context.Context, clusterID, instanceID string) error
	GetMaster(ctx context.Context) (clusterID, instanceID string, err error)
	RefreshHeartbeat(ctx context.Context, clusterID string, ttl time.Duration) error
	// WatchHeartbeat returns a channel that is closed when the heartbeat for
	// clusterID expires (key deleted or TTL elapsed). Callers must drain and
	// discard the channel when done.
	WatchHeartbeat(ctx context.Context, clusterID string) (<-chan struct{}, error)
}
```

- [ ] **Step 4: Create `redis/memory.go` (in-memory fake)**

```go
package redis

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type memoryStore struct {
	mu         sync.RWMutex
	shards     map[string][]byte
	tokens     map[string]string
	masterCluster  string
	masterInstance string
}

// NewMemoryStore returns a Store backed entirely by in-memory maps.
// Suitable for unit tests; not safe across process restarts.
func NewMemoryStore() Store {
	return &memoryStore{
		shards: make(map[string][]byte),
		tokens: make(map[string]string),
	}
}

func shardKey(storeID, objectType string, epoch uint64) string {
	return fmt.Sprintf("shard:%s:%s:%d", storeID, objectType, epoch)
}

func (s *memoryStore) CacheShard(_ context.Context, storeID, objectType string, epoch uint64, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	s.shards[shardKey(storeID, objectType, epoch)] = cp
	return nil
}

func (s *memoryStore) LoadCachedShard(_ context.Context, storeID, objectType string, epoch uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, ok := s.shards[shardKey(storeID, objectType, epoch)]
	if !ok {
		return nil, nil
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp, nil
}

func (s *memoryStore) SetWatchToken(_ context.Context, storeID, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokens[storeID] = token
	return nil
}

func (s *memoryStore) GetWatchToken(_ context.Context, storeID string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tokens[storeID], nil
}

func (s *memoryStore) SetMaster(_ context.Context, clusterID, instanceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.masterCluster = clusterID
	s.masterInstance = instanceID
	return nil
}

func (s *memoryStore) GetMaster(_ context.Context) (string, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.masterCluster, s.masterInstance, nil
}

func (s *memoryStore) RefreshHeartbeat(_ context.Context, _ string, _ time.Duration) error {
	return nil
}

func (s *memoryStore) WatchHeartbeat(_ context.Context, _ string) (<-chan struct{}, error) {
	ch := make(chan struct{})
	return ch, nil
}
```

- [ ] **Step 5: Create `redis/redis.go` (go-redis implementation)**

```go
package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config holds Redis connection settings.
type Config struct {
	Addr     string // e.g. "redis:6379"
	Password string
	DB       int
}

type redisStore struct {
	client *redis.Client
}

// New returns a Store connected to Redis using the given Config.
func New(cfg Config) Store {
	return &redisStore{
		client: redis.NewClient(&redis.Options{
			Addr:     cfg.Addr,
			Password: cfg.Password,
			DB:       cfg.DB,
		}),
	}
}

func (s *redisStore) CacheShard(ctx context.Context, storeID, objectType string, epoch uint64, data []byte) error {
	key := fmt.Sprintf("shard:cache:%s:%s:%d", storeID, objectType, epoch)
	if err := s.client.Set(ctx, key, data, 24*time.Hour).Err(); err != nil {
		return fmt.Errorf("redis.CacheShard: %w", err)
	}
	return nil
}

func (s *redisStore) LoadCachedShard(ctx context.Context, storeID, objectType string, epoch uint64) ([]byte, error) {
	key := fmt.Sprintf("shard:cache:%s:%s:%d", storeID, objectType, epoch)
	data, err := s.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("redis.LoadCachedShard: %w", err)
	}
	return data, nil
}

func (s *redisStore) SetWatchToken(ctx context.Context, storeID, token string) error {
	key := fmt.Sprintf("watchtoken:%s", storeID)
	if err := s.client.Set(ctx, key, token, 0).Err(); err != nil {
		return fmt.Errorf("redis.SetWatchToken: %w", err)
	}
	return nil
}

func (s *redisStore) GetWatchToken(ctx context.Context, storeID string) (string, error) {
	key := fmt.Sprintf("watchtoken:%s", storeID)
	val, err := s.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("redis.GetWatchToken: %w", err)
	}
	return val, nil
}

func (s *redisStore) SetMaster(ctx context.Context, clusterID, instanceID string) error {
	key := fmt.Sprintf("master:%s", clusterID)
	if err := s.client.Set(ctx, key, instanceID, 0).Err(); err != nil {
		return fmt.Errorf("redis.SetMaster: %w", err)
	}
	return nil
}

func (s *redisStore) GetMaster(ctx context.Context) (string, string, error) {
	// Scan for master:* keys.
	iter := s.client.Scan(ctx, 0, "master:*", 1).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		instanceID, err := s.client.Get(ctx, key).Result()
		if err != nil {
			continue
		}
		clusterID := key[len("master:"):]
		return clusterID, instanceID, nil
	}
	if err := iter.Err(); err != nil {
		return "", "", fmt.Errorf("redis.GetMaster: %w", err)
	}
	return "", "", nil
}

func (s *redisStore) RefreshHeartbeat(ctx context.Context, clusterID string, ttl time.Duration) error {
	key := fmt.Sprintf("heartbeat:%s", clusterID)
	if err := s.client.Set(ctx, key, "1", ttl).Err(); err != nil {
		return fmt.Errorf("redis.RefreshHeartbeat: %w", err)
	}
	return nil
}

func (s *redisStore) WatchHeartbeat(ctx context.Context, clusterID string) (<-chan struct{}, error) {
	key := fmt.Sprintf("heartbeat:%s", clusterID)
	ch := make(chan struct{}, 1)

	// Use keyspace notifications (KEg) to detect key expiry.
	// The Redis server must have notify-keyspace-events set to at least "KEg".
	expiredChannel := fmt.Sprintf("__keyevent@%s__:expired", strconv.Itoa(s.client.Options().DB))
	sub := s.client.Subscribe(ctx, expiredChannel)

	go func() {
		defer close(ch)
		defer sub.Close()
		msgCh := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgCh:
				if !ok {
					return
				}
				if msg.Payload == key {
					ch <- struct{}{}
					return
				}
			}
		}
	}()
	return ch, nil
}
```

- [ ] **Step 6: Run tests**

```bash
go test ./redis/... -run TestMemoryStore -v
```

Expected: all `TestMemoryStore` subtests PASS.

- [ ] **Step 7: Commit**

```bash
git add redis/
git commit -m "feat(redis): add Store interface, go-redis implementation, and memory fake"
```

---

## Task 3: Object Storage Adapter

**Files:**
- Create: `storage/store.go`
- Create: `storage/s3.go`
- Create: `storage/memory.go`
- Create: `storage/store_test.go`

- [ ] **Step 1: Write the failing test**

Create `storage/store_test.go`:

```go
package storage_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/yourorg/openfga-indexer/storage"
)

func runShardStoreContract(t *testing.T, s storage.ShardStore) {
	t.Helper()
	ctx := context.Background()

	t.Run("write_and_read_shard", func(t *testing.T) {
		data := []byte("roaring-bitmap-bytes")
		if err := s.WriteShard(ctx, "store1", "group", 1, data); err != nil {
			t.Fatalf("WriteShard: %v", err)
		}
		got, err := s.ReadShard(ctx, "store1", "group", 1)
		if err != nil {
			t.Fatalf("ReadShard: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("ReadShard = %q, want %q", got, data)
		}
	})

	t.Run("read_missing_returns_error", func(t *testing.T) {
		_, err := s.ReadShard(ctx, "store1", "group", 99)
		if err == nil {
			t.Fatal("expected error reading missing shard")
		}
	})

	t.Run("list_epochs", func(t *testing.T) {
		_ = s.WriteShard(ctx, "store2", "folder", 10, []byte("a"))
		_ = s.WriteShard(ctx, "store2", "folder", 11, []byte("b"))
		epochs, err := s.ListEpochs(ctx, "store2", "folder")
		if err != nil {
			t.Fatalf("ListEpochs: %v", err)
		}
		if len(epochs) < 2 {
			t.Fatalf("expected at least 2 epochs, got %v", epochs)
		}
	})

	t.Run("delete_shard", func(t *testing.T) {
		_ = s.WriteShard(ctx, "store3", "group", 5, []byte("del"))
		if err := s.DeleteShard(ctx, "store3", "group", 5); err != nil {
			t.Fatalf("DeleteShard: %v", err)
		}
		if _, err := s.ReadShard(ctx, "store3", "group", 5); err == nil {
			t.Fatal("expected error reading deleted shard")
		}
	})
}

func TestMemoryShardStore(t *testing.T) {
	runShardStoreContract(t, storage.NewMemoryStore())
}
```

- [ ] **Step 2: Create `storage/store.go`**

```go
// Package storage provides durable shard storage for the Offline Pipeline.
// Shards are written once per epoch and read by all cluster instances.
package storage

import "context"

// ShardStore stores serialized roaring bitmap index shards identified by
// (storeID, objectType, epoch). Implementations must be safe for concurrent use.
type ShardStore interface {
	WriteShard(ctx context.Context, storeID, objectType string, epoch uint64, data []byte) error
	ReadShard(ctx context.Context, storeID, objectType string, epoch uint64) ([]byte, error)
	DeleteShard(ctx context.Context, storeID, objectType string, epoch uint64) error
	// ListEpochs returns all stored epoch numbers for the given (storeID, objectType),
	// sorted ascending.
	ListEpochs(ctx context.Context, storeID, objectType string) ([]uint64, error)
}
```

- [ ] **Step 3: Create `storage/memory.go`**

```go
package storage

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

type memoryStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewMemoryStore returns a ShardStore backed by an in-memory map.
func NewMemoryStore() ShardStore {
	return &memoryStore{data: make(map[string][]byte)}
}

func objectKey(storeID, objectType string, epoch uint64) string {
	return fmt.Sprintf("leopard/%s/%s/%d.shard", storeID, objectType, epoch)
}

func (s *memoryStore) WriteShard(_ context.Context, storeID, objectType string, epoch uint64, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	s.data[objectKey(storeID, objectType, epoch)] = cp
	return nil
}

func (s *memoryStore) ReadShard(_ context.Context, storeID, objectType string, epoch uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, ok := s.data[objectKey(storeID, objectType, epoch)]
	if !ok {
		return nil, fmt.Errorf("storage.ReadShard: shard not found (%s/%s/%d)", storeID, objectType, epoch)
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp, nil
}

func (s *memoryStore) DeleteShard(_ context.Context, storeID, objectType string, epoch uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, objectKey(storeID, objectType, epoch))
	return nil
}

func (s *memoryStore) ListEpochs(_ context.Context, storeID, objectType string) ([]uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := fmt.Sprintf("leopard/%s/%s/", storeID, objectType)
	var epochs []uint64
	for key := range s.data {
		if len(key) <= len(prefix) || key[:len(prefix)] != prefix {
			continue
		}
		var epoch uint64
		if _, err := fmt.Sscanf(key[len(prefix):], "%d.shard", &epoch); err == nil {
			epochs = append(epochs, epoch)
		}
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })
	return epochs, nil
}
```

- [ ] **Step 4: Create `storage/s3.go`**

```go
package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Config holds settings for the S3-compatible shard store.
type S3Config struct {
	Bucket   string
	Region   string
	Endpoint string // override for MinIO or other S3-compatible stores
}

type s3Store struct {
	client *s3.Client
	bucket string
}

// NewS3Store returns a ShardStore backed by S3 (or an S3-compatible store).
func NewS3Store(cfg S3Config, awsCfg aws.Config) ShardStore {
	opts := []func(*s3.Options){}
	if cfg.Endpoint != "" {
		opts = append(opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true
		})
	}
	return &s3Store{
		client: s3.NewFromConfig(awsCfg, opts...),
		bucket: cfg.Bucket,
	}
}

func (s *s3Store) WriteShard(ctx context.Context, storeID, objectType string, epoch uint64, data []byte) error {
	key := objectKey(storeID, objectType, epoch)
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("storage.WriteShard: %w", err)
	}
	return nil
}

func (s *s3Store) ReadShard(ctx context.Context, storeID, objectType string, epoch uint64) ([]byte, error) {
	key := objectKey(storeID, objectType, epoch)
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("storage.ReadShard: %w", err)
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("storage.ReadShard: read body: %w", err)
	}
	return data, nil
}

func (s *s3Store) DeleteShard(ctx context.Context, storeID, objectType string, epoch uint64) error {
	key := objectKey(storeID, objectType, epoch)
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("storage.DeleteShard: %w", err)
	}
	return nil
}

func (s *s3Store) ListEpochs(ctx context.Context, storeID, objectType string) ([]uint64, error) {
	prefix := fmt.Sprintf("leopard/%s/%s/", storeID, objectType)
	out, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return nil, fmt.Errorf("storage.ListEpochs: %w", err)
	}
	var epochs []uint64
	for _, obj := range out.Contents {
		name := strings.TrimPrefix(*obj.Key, prefix)
		name = strings.TrimSuffix(name, ".shard")
		epoch, err := strconv.ParseUint(name, 10, 64)
		if err == nil {
			epochs = append(epochs, epoch)
		}
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })
	return epochs, nil
}

// DeleteOldEpochs removes all but the newest `keep` epochs for (storeID, objectType).
func DeleteOldEpochs(ctx context.Context, s ShardStore, storeID, objectType string, keep int) error {
	epochs, err := s.ListEpochs(ctx, storeID, objectType)
	if err != nil {
		return err
	}
	if len(epochs) <= keep {
		return nil
	}
	for _, epoch := range epochs[:len(epochs)-keep] {
		if err := s.DeleteShard(ctx, storeID, objectType, epoch); err != nil {
			return err
		}
	}
	return nil
}

// Reuse objectKey from memory.go — same package.
var _ = types.Object{} // ensure s3 types imported
```

- [ ] **Step 5: Run unit tests**

```bash
go test ./storage/... -run TestMemoryShardStore -v
```

Expected: all subtests PASS.

- [ ] **Step 6: Commit**

```bash
git add storage/
git commit -m "feat(storage): add ShardStore interface, S3 and memory implementations"
```

---

## Task 4: MessageBus Interface and Event Types

**Files:**
- Create: `messaging/bus.go`

- [ ] **Step 1: Create `messaging/bus.go`**

```go
// Package messaging provides reliable, durable delivery of control signals
// between Leopard service instances across clusters.
//
// Two implementations are provided: NATS JetStream (messaging/nats) and
// Redis Streams (messaging/redisstream). Select via config.
package messaging

import (
	"context"
	"time"

	"github.com/yourorg/openfga-indexer/client"
)

// MessageBus carries all control signals between components.
// All methods must be safe for concurrent use.
type MessageBus interface {
	// Rotation — published by Offline Pipeline after a successful epoch build,
	// consumed by all Check Gateway instances.
	PublishRotation(ctx context.Context, event RotationEvent) error
	SubscribeRotation(ctx context.Context, storeID string, handler func(RotationEvent)) (Subscription, error)

	// Rebuild — published by Write Proxy on model change,
	// consumed by the Offline Pipeline on the master.
	PublishRebuild(ctx context.Context, event RebuildEvent) error
	SubscribeRebuild(ctx context.Context, storeID string, handler func(RebuildEvent)) (Subscription, error)

	// Delta — published by the master Compute Tier after applying a batch of
	// ReadChanges deltas, consumed by replica Check Gateways.
	PublishDelta(ctx context.Context, event DeltaEvent) error
	SubscribeDelta(ctx context.Context, storeID string, handler func(DeltaEvent)) (Subscription, error)

	// Promotion — published by admin API, consumed by all instances.
	PublishPromotion(ctx context.Context, event PromotionEvent) error
	SubscribePromotion(ctx context.Context, handler func(PromotionEvent)) (Subscription, error)

	Close() error
}

// Subscription represents an active subscription. Call Cancel to unsubscribe.
type Subscription interface {
	Cancel() error
}

// RotationEvent signals that a new epoch's shards are ready in object storage.
type RotationEvent struct {
	StoreID     string    `json:"store_id"`
	Epoch       uint64    `json:"epoch"`
	Types       []string  `json:"types"` // object types in this epoch
	PublishedAt time.Time `json:"published_at"`
}

// RebuildEvent triggers a full index rebuild for a store.
type RebuildEvent struct {
	StoreID     string    `json:"store_id"`
	ModelID     string    `json:"model_id"`
	TriggeredAt time.Time `json:"triggered_at"`
}

// DeltaEvent carries a batch of incremental tuple changes from the master
// Compute Tier to replica Check Gateways.
type DeltaEvent struct {
	StoreID   string              `json:"store_id"`
	Epoch     uint64              `json:"epoch"`
	Changes   []client.TupleChange `json:"changes"`
	AppliedAt time.Time           `json:"applied_at"`
}

// PromotionEvent signals that a replica has been promoted to master.
type PromotionEvent struct {
	ClusterID  string    `json:"cluster_id"`
	InstanceID string    `json:"instance_id"`
	PromotedAt time.Time `json:"promoted_at"`
}
```

- [ ] **Step 2: Commit**

```bash
git add messaging/bus.go
git commit -m "feat(messaging): add MessageBus interface and event types"
```

---

## Task 5: NATS JetStream Implementation

**Files:**
- Create: `messaging/nats/nats.go`
- Create: `messaging/nats/nats_test.go`

- [ ] **Step 1: Create `messaging/nats/nats.go`**

```go
// Package nats implements messaging.MessageBus using NATS JetStream.
// Streams are created automatically if they do not exist.
package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/yourorg/openfga-indexer/messaging"
)

const (
	streamRotations = "LEOPARD_ROTATIONS"
	streamRebuilds  = "LEOPARD_REBUILDS"
	streamDeltas    = "LEOPARD_DELTAS"
	streamPromos    = "LEOPARD_PROMOTIONS"
)

// Config holds NATS connection settings.
type Config struct {
	URL        string // e.g. "nats://nats:4222"
	ConsumerID string // unique per instance, used for durable consumers
}

type natsBus struct {
	conn *nats.Conn
	js   jetstream.JetStream
	cfg  Config
}

// New connects to NATS and returns a MessageBus.
// Creates the required JetStream streams if they don't exist.
func New(cfg Config) (messaging.MessageBus, error) {
	nc, err := nats.Connect(cfg.URL,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("nats.New: connect: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("nats.New: jetstream: %w", err)
	}
	bus := &natsBus{conn: nc, js: js, cfg: cfg}
	if err := bus.ensureStreams(context.Background()); err != nil {
		nc.Close()
		return nil, err
	}
	return bus, nil
}

func (b *natsBus) ensureStreams(ctx context.Context) error {
	streams := []jetstream.StreamConfig{
		{Name: streamRotations, Subjects: []string{"leopard.rotation.>"}, Retention: jetstream.WorkQueuePolicy},
		{Name: streamRebuilds, Subjects: []string{"leopard.rebuild.>"}, Retention: jetstream.WorkQueuePolicy},
		{Name: streamDeltas, Subjects: []string{"leopard.delta.>"}, Retention: jetstream.LimitsPolicy, MaxAge: time.Hour},
		{Name: streamPromos, Subjects: []string{"leopard.promotion"}, Retention: jetstream.WorkQueuePolicy},
	}
	for _, sc := range streams {
		if _, err := b.js.CreateOrUpdateStream(ctx, sc); err != nil {
			return fmt.Errorf("nats.ensureStreams: %s: %w", sc.Name, err)
		}
	}
	return nil
}

func (b *natsBus) publish(ctx context.Context, subject string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("nats.publish: marshal: %w", err)
	}
	if _, err := b.js.Publish(ctx, subject, data); err != nil {
		return fmt.Errorf("nats.publish %s: %w", subject, err)
	}
	return nil
}

func subscribe[T any](ctx context.Context, js jetstream.JetStream, stream, subject, consumerID string, handler func(T)) (messaging.Subscription, error) {
	cons, err := js.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
		Name:          consumerID,
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverNewPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("nats.subscribe: create consumer: %w", err)
	}
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		var event T
		if err := json.Unmarshal(msg.Data(), &event); err == nil {
			handler(event)
		}
		_ = msg.Ack()
	})
	if err != nil {
		return nil, fmt.Errorf("nats.subscribe: consume: %w", err)
	}
	return &natsSub{cc: cc}, nil
}

type natsSub struct{ cc jetstream.ConsumeContext }

func (s *natsSub) Cancel() error { s.cc.Stop(); return nil }

func (b *natsBus) PublishRotation(ctx context.Context, e messaging.RotationEvent) error {
	return b.publish(ctx, fmt.Sprintf("leopard.rotation.%s", e.StoreID), e)
}

func (b *natsBus) SubscribeRotation(ctx context.Context, storeID string, handler func(messaging.RotationEvent)) (messaging.Subscription, error) {
	return subscribe[messaging.RotationEvent](ctx, b.js, streamRotations,
		fmt.Sprintf("leopard.rotation.%s", storeID),
		fmt.Sprintf("%s-rotation-%s", b.cfg.ConsumerID, storeID), handler)
}

func (b *natsBus) PublishRebuild(ctx context.Context, e messaging.RebuildEvent) error {
	return b.publish(ctx, fmt.Sprintf("leopard.rebuild.%s", e.StoreID), e)
}

func (b *natsBus) SubscribeRebuild(ctx context.Context, storeID string, handler func(messaging.RebuildEvent)) (messaging.Subscription, error) {
	return subscribe[messaging.RebuildEvent](ctx, b.js, streamRebuilds,
		fmt.Sprintf("leopard.rebuild.%s", storeID),
		fmt.Sprintf("%s-rebuild-%s", b.cfg.ConsumerID, storeID), handler)
}

func (b *natsBus) PublishDelta(ctx context.Context, e messaging.DeltaEvent) error {
	return b.publish(ctx, fmt.Sprintf("leopard.delta.%s", e.StoreID), e)
}

func (b *natsBus) SubscribeDelta(ctx context.Context, storeID string, handler func(messaging.DeltaEvent)) (messaging.Subscription, error) {
	return subscribe[messaging.DeltaEvent](ctx, b.js, streamDeltas,
		fmt.Sprintf("leopard.delta.%s", storeID),
		fmt.Sprintf("%s-delta-%s", b.cfg.ConsumerID, storeID), handler)
}

func (b *natsBus) PublishPromotion(ctx context.Context, e messaging.PromotionEvent) error {
	return b.publish(ctx, "leopard.promotion", e)
}

func (b *natsBus) SubscribePromotion(ctx context.Context, handler func(messaging.PromotionEvent)) (messaging.Subscription, error) {
	return subscribe[messaging.PromotionEvent](ctx, b.js, streamPromos,
		"leopard.promotion",
		fmt.Sprintf("%s-promotion", b.cfg.ConsumerID), handler)
}

func (b *natsBus) Close() error {
	b.conn.Close()
	return nil
}
```

- [ ] **Step 2: Commit**

```bash
git add messaging/nats/
git commit -m "feat(messaging/nats): add NATS JetStream MessageBus implementation"
```

---

## Task 6: Redis Streams Implementation

**Files:**
- Create: `messaging/redisstream/redisstream.go`

- [ ] **Step 1: Create `messaging/redisstream/redisstream.go`**

```go
// Package redisstream implements messaging.MessageBus using Redis Streams.
// Consumer groups with XACK provide at-least-once delivery.
package redisstream

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yourorg/openfga-indexer/messaging"
)

// Config holds Redis Streams settings.
type Config struct {
	Addr       string // e.g. "redis:6379"
	Password   string
	DB         int
	ConsumerID string // unique per instance
	GroupID    string // consumer group, unique per cluster
}

type rsBus struct {
	client *redis.Client
	cfg    Config
}

// New returns a MessageBus backed by Redis Streams.
func New(cfg Config) (messaging.MessageBus, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("redisstream.New: ping: %w", err)
	}
	return &rsBus{client: client, cfg: cfg}, nil
}

func streamKey(name string) string { return "leopard:" + name }

func (b *rsBus) publish(ctx context.Context, stream string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("redisstream.publish: %w", err)
	}
	if err := b.client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey(stream),
		Values: map[string]any{"data": string(data)},
	}).Err(); err != nil {
		return fmt.Errorf("redisstream.publish %s: %w", stream, err)
	}
	return nil
}

func (b *rsBus) ensureGroup(ctx context.Context, stream, group string) {
	// XGROUP CREATE with MKSTREAM — idempotent.
	b.client.XGroupCreateMkStream(ctx, streamKey(stream), group, "$")
}

func subscribe[T any](ctx context.Context, client *redis.Client, stream, group, consumerID string, handler func(T)) (messaging.Subscription, error) {
	// Ensure the consumer group exists.
	client.XGroupCreateMkStream(ctx, streamKey(stream), group, "0")

	cancelCtx, cancel := context.WithCancel(ctx)
	go func() {
		for {
			if cancelCtx.Err() != nil {
				return
			}
			msgs, err := client.XReadGroup(cancelCtx, &redis.XReadGroupArgs{
				Group:    group,
				Consumer: consumerID,
				Streams:  []string{streamKey(stream), ">"},
				Count:    10,
				Block:    2 * time.Second,
			}).Result()
			if err != nil {
				continue
			}
			for _, s := range msgs {
				for _, msg := range s.Messages {
					raw, ok := msg.Values["data"].(string)
					if !ok {
						continue
					}
					var event T
					if err := json.Unmarshal([]byte(raw), &event); err == nil {
						handler(event)
					}
					client.XAck(cancelCtx, streamKey(stream), group, msg.ID)
				}
			}
		}
	}()
	return &rsSub{cancel: cancel}, nil
}

type rsSub struct{ cancel context.CancelFunc }

func (s *rsSub) Cancel() error { s.cancel(); return nil }

func (b *rsBus) PublishRotation(ctx context.Context, e messaging.RotationEvent) error {
	return b.publish(ctx, fmt.Sprintf("rotations.%s", e.StoreID), e)
}

func (b *rsBus) SubscribeRotation(ctx context.Context, storeID string, handler func(messaging.RotationEvent)) (messaging.Subscription, error) {
	return subscribe[messaging.RotationEvent](ctx, b.client, fmt.Sprintf("rotations.%s", storeID), b.cfg.GroupID, b.cfg.ConsumerID, handler)
}

func (b *rsBus) PublishRebuild(ctx context.Context, e messaging.RebuildEvent) error {
	return b.publish(ctx, fmt.Sprintf("rebuilds.%s", e.StoreID), e)
}

func (b *rsBus) SubscribeRebuild(ctx context.Context, storeID string, handler func(messaging.RebuildEvent)) (messaging.Subscription, error) {
	return subscribe[messaging.RebuildEvent](ctx, b.client, fmt.Sprintf("rebuilds.%s", storeID), b.cfg.GroupID, b.cfg.ConsumerID, handler)
}

func (b *rsBus) PublishDelta(ctx context.Context, e messaging.DeltaEvent) error {
	return b.publish(ctx, fmt.Sprintf("deltas.%s", e.StoreID), e)
}

func (b *rsBus) SubscribeDelta(ctx context.Context, storeID string, handler func(messaging.DeltaEvent)) (messaging.Subscription, error) {
	return subscribe[messaging.DeltaEvent](ctx, b.client, fmt.Sprintf("deltas.%s", storeID), b.cfg.GroupID, b.cfg.ConsumerID, handler)
}

func (b *rsBus) PublishPromotion(ctx context.Context, e messaging.PromotionEvent) error {
	return b.publish(ctx, "promotions", e)
}

func (b *rsBus) SubscribePromotion(ctx context.Context, handler func(messaging.PromotionEvent)) (messaging.Subscription, error) {
	return subscribe[messaging.PromotionEvent](ctx, b.client, "promotions", b.cfg.GroupID, b.cfg.ConsumerID, handler)
}

func (b *rsBus) Close() error {
	return b.client.Close()
}
```

- [ ] **Step 2: Commit**

```bash
git add messaging/redisstream/
git commit -m "feat(messaging/redisstream): add Redis Streams MessageBus implementation"
```

---

## Task 7: MessageBus Contract Tests

**Files:**
- Create: `messaging/contract_test.go`
- Create: `messaging/nats/nats_test.go`
- Create: `messaging/redisstream/redisstream_test.go`

- [ ] **Step 1: Create `messaging/contract_test.go`**

```go
//go:build integration

package messaging_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/messaging"
)

// RunBusContract is the shared contract test suite for any MessageBus implementation.
// Call it from implementation-specific test files.
func RunBusContract(t *testing.T, bus messaging.MessageBus) {
	t.Helper()
	ctx := context.Background()

	t.Run("rotation_pubsub", func(t *testing.T) {
		received := make(chan messaging.RotationEvent, 1)
		sub, err := bus.SubscribeRotation(ctx, "store1", func(e messaging.RotationEvent) {
			received <- e
		})
		if err != nil {
			t.Fatalf("SubscribeRotation: %v", err)
		}
		defer sub.Cancel()

		want := messaging.RotationEvent{StoreID: "store1", Epoch: 42, Types: []string{"group"}, PublishedAt: time.Now()}
		if err := bus.PublishRotation(ctx, want); err != nil {
			t.Fatalf("PublishRotation: %v", err)
		}
		select {
		case got := <-received:
			if got.Epoch != want.Epoch || got.StoreID != want.StoreID {
				t.Fatalf("got %+v, want %+v", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for RotationEvent")
		}
	})

	t.Run("rebuild_pubsub", func(t *testing.T) {
		received := make(chan messaging.RebuildEvent, 1)
		sub, err := bus.SubscribeRebuild(ctx, "store2", func(e messaging.RebuildEvent) {
			received <- e
		})
		if err != nil {
			t.Fatalf("SubscribeRebuild: %v", err)
		}
		defer sub.Cancel()

		want := messaging.RebuildEvent{StoreID: "store2", ModelID: "model-xyz", TriggeredAt: time.Now()}
		if err := bus.PublishRebuild(ctx, want); err != nil {
			t.Fatalf("PublishRebuild: %v", err)
		}
		select {
		case got := <-received:
			if got.ModelID != want.ModelID {
				t.Fatalf("got ModelID %q, want %q", got.ModelID, want.ModelID)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for RebuildEvent")
		}
	})

	t.Run("delta_pubsub", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(1)
		sub, err := bus.SubscribeDelta(ctx, "store3", func(e messaging.DeltaEvent) {
			if e.Epoch == 7 {
				wg.Done()
			}
		})
		if err != nil {
			t.Fatalf("SubscribeDelta: %v", err)
		}
		defer sub.Cancel()

		if err := bus.PublishDelta(ctx, messaging.DeltaEvent{
			StoreID:   "store3",
			Epoch:     7,
			Changes:   []client.TupleChange{{User: "user:alice", Relation: "member", Object: "group:eng", Operation: client.OperationWrite}},
			AppliedAt: time.Now(),
		}); err != nil {
			t.Fatalf("PublishDelta: %v", err)
		}

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for DeltaEvent")
		}
	})
}
```

- [ ] **Step 2: Create `messaging/nats/nats_test.go`**

```go
//go:build integration

package nats_test

import (
	"os"
	"testing"

	leopardnats "github.com/yourorg/openfga-indexer/messaging/nats"
	"github.com/yourorg/openfga-indexer/messaging"
)

func TestNatsBus_Contract(t *testing.T) {
	url := os.Getenv("TEST_NATS_URL")
	if url == "" {
		t.Skip("TEST_NATS_URL not set")
	}
	bus, err := leopardnats.New(leopardnats.Config{
		URL:        url,
		ConsumerID: "test-consumer",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer bus.Close()
	messaging.RunBusContract(t, bus)  // defined in messaging/contract_test.go
}
```

- [ ] **Step 3: Create `messaging/redisstream/redisstream_test.go`**

```go
//go:build integration

package redisstream_test

import (
	"os"
	"testing"

	"github.com/yourorg/openfga-indexer/messaging"
	"github.com/yourorg/openfga-indexer/messaging/redisstream"
)

func TestRedisStreamBus_Contract(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set")
	}
	bus, err := redisstream.New(redisstream.Config{
		Addr:       addr,
		ConsumerID: "test-consumer",
		GroupID:    "test-group",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer bus.Close()
	messaging.RunBusContract(t, bus)
}
```

- [ ] **Step 4: Run unit tests (non-integration)**

```bash
go test ./redis/... ./storage/... ./messaging/... -short -v
```

Expected: `TestMemoryStore`, `TestMemoryShardStore` pass. Integration tests skipped.

- [ ] **Step 5: Commit**

```bash
git add messaging/
git commit -m "feat(messaging): add contract tests for NATS and Redis Streams"
```

---

## Self-Review

- [x] **Spec coverage:** Section 6 (Redis adapter) ✓ Task 2. Section 7 (object storage) ✓ Task 3. Section 8 (messaging) ✓ Tasks 4–7. `DeltaEvent` added to interface per spec Section 10 correction ✓.
- [x] **Placeholder scan:** No TBD/TODO. All code is complete.
- [x] **Type consistency:** `messaging.RotationEvent.Epoch uint64` matches `stringtable.NextEpoch` return type. `client.TupleChange` referenced correctly in `DeltaEvent`. `redis.Store` interface defined here, used in Plans 4 and 5.
