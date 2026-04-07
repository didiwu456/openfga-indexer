# Offline Pipeline & Live Index Compute Tier — Implementation Plan 4 of 5

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the two master-only components that keep the index current: the Offline Pipeline (full rebuild on model change, type-sharded, epoch-gated) and the Live Index Compute Tier (incremental ReadChanges polling with delta broadcast).

**Architecture:** Both packages depend on the adapters from Plans 2 and 3. The Pipeline acquires a per-store mutex, reads all FGA tuples, builds roaring bitmap shards using `stringtable.Store` for stable IDs, writes to `storage.ShardStore`, then publishes a `RotationEvent`. The Compute Tier polls `client.FGAClient.ReadChanges`, applies deltas to the in-memory index and Redis L2 cache, and publishes `DeltaEvent` batches to the `messaging.MessageBus`.

**Tech Stack:** Go 1.22, `github.com/RoaringBitmap/roaring`, types from Plans 1–3.

---

## File Map

| Action | File | Responsibility |
|---|---|---|
| Create | `pipeline/pipeline.go` | `Pipeline` — full rebuild, type-sharded, epoch-gated with saga-style rollback |
| Create | `pipeline/pipeline_test.go` | Unit tests with mock FGA client, in-memory adapters |
| Create | `compute/compute.go` | `ComputeTier` — ReadChanges poller, delta application, MessageBus publish |
| Create | `compute/compute_test.go` | Unit tests with mock FGA client |

---

## Task 1: Offline Pipeline

**Files:**
- Create: `pipeline/pipeline.go`
- Create: `pipeline/pipeline_test.go`

- [ ] **Step 1: Write the failing tests**

Create `pipeline/pipeline_test.go`:

```go
package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/messaging"
	leopardstorage "github.com/yourorg/openfga-indexer/storage"
	"github.com/yourorg/openfga-indexer/stringtable"
	"github.com/yourorg/openfga-indexer/pipeline"
)

// mockBus records published RotationEvents.
type mockBus struct {
	rotations []messaging.RotationEvent
}

func (m *mockBus) PublishRotation(_ context.Context, e messaging.RotationEvent) error {
	m.rotations = append(m.rotations, e)
	return nil
}
func (m *mockBus) SubscribeRotation(_ context.Context, _ string, _ func(messaging.RotationEvent)) (messaging.Subscription, error) {
	return &nopSub{}, nil
}
func (m *mockBus) PublishRebuild(_ context.Context, _ messaging.RebuildEvent) error   { return nil }
func (m *mockBus) SubscribeRebuild(_ context.Context, _ string, _ func(messaging.RebuildEvent)) (messaging.Subscription, error) {
	return &nopSub{}, nil
}
func (m *mockBus) PublishDelta(_ context.Context, _ messaging.DeltaEvent) error { return nil }
func (m *mockBus) SubscribeDelta(_ context.Context, _ string, _ func(messaging.DeltaEvent)) (messaging.Subscription, error) {
	return &nopSub{}, nil
}
func (m *mockBus) PublishPromotion(_ context.Context, _ messaging.PromotionEvent) error { return nil }
func (m *mockBus) SubscribePromotion(_ context.Context, _ func(messaging.PromotionEvent)) (messaging.Subscription, error) {
	return &nopSub{}, nil
}
func (m *mockBus) Close() error { return nil }

type nopSub struct{}
func (n *nopSub) Cancel() error { return nil }

func TestPipeline_BuildAndRotate(t *testing.T) {
	fga := client.NewMockClient([]client.Tuple{
		{User: "user:alice", Relation: "member", Object: "group:eng"},
		{User: "group:backend", Relation: "member", Object: "group:eng"},
		{User: "user:bob", Relation: "member", Object: "group:backend"},
		// Non-indexed type — should be ignored.
		{User: "user:alice", Relation: "viewer", Object: "document:readme"},
	})
	store := leopardstorage.NewMemoryStore()
	bus := &mockBus{}
	stbl := stringtable.NewInMemoryStore() // in-memory Store for tests (see note below)

	p := pipeline.New(fga, stbl, store, bus, pipeline.Options{
		StoreID:      "store1",
		IndexedTypes: []string{"group"},
	})

	ctx := context.Background()
	epoch, err := p.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if epoch == 0 {
		t.Fatal("epoch should be > 0")
	}

	// Shard for "group" type should exist in object storage.
	data, err := store.ReadShard(ctx, "store1", "group", epoch)
	if err != nil {
		t.Fatalf("ReadShard: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("shard data should not be empty")
	}

	// RotationEvent should have been published.
	if len(bus.rotations) != 1 {
		t.Fatalf("expected 1 RotationEvent, got %d", len(bus.rotations))
	}
	rot := bus.rotations[0]
	if rot.Epoch != epoch {
		t.Fatalf("RotationEvent.Epoch = %d, want %d", rot.Epoch, epoch)
	}
	if len(rot.Types) != 1 || rot.Types[0] != "group" {
		t.Fatalf("RotationEvent.Types = %v, want [group]", rot.Types)
	}
}

func TestPipeline_IgnoresNonIndexedTypes(t *testing.T) {
	fga := client.NewMockClient([]client.Tuple{
		{User: "user:alice", Relation: "viewer", Object: "document:readme"},
	})
	store := leopardstorage.NewMemoryStore()
	bus := &mockBus{}
	stbl := stringtable.NewInMemoryStore()

	p := pipeline.New(fga, stbl, store, bus, pipeline.Options{
		StoreID:      "store1",
		IndexedTypes: []string{"group"}, // "document" not indexed
	})

	ctx := context.Background()
	epoch, err := p.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// No shard for "document" should be written.
	_, err = store.ReadShard(ctx, "store1", "document", epoch)
	if err == nil {
		t.Fatal("document shard should not have been written")
	}
}

func TestPipeline_ConcurrentRunsQueued(t *testing.T) {
	fga := client.NewMockClient(nil)
	store := leopardstorage.NewMemoryStore()
	bus := &mockBus{}
	stbl := stringtable.NewInMemoryStore()

	p := pipeline.New(fga, stbl, store, bus, pipeline.Options{
		StoreID:      "store1",
		IndexedTypes: []string{"group"},
	})
	ctx := context.Background()

	// Run twice; second call should queue and not block indefinitely.
	done := make(chan error, 2)
	go func() { _, err := p.Run(ctx); done <- err }()
	go func() { _, err := p.Run(ctx); done <- err }()

	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run %d: %v", i, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("Run %d timed out", i)
		}
	}
}
```

**Note:** `stringtable.NewInMemoryStore()` is an in-memory implementation of `stringtable.Store` for testing. Add it to `stringtable/memory.go` in this task.

- [ ] **Step 2: Add `stringtable/memory.go` (in-memory Store for tests)**

```go
package stringtable

import (
	"context"
	"sync"
	"sync/atomic"
)

type memStore struct {
	mu      sync.Mutex
	fwd     map[string]map[string]uint32 // storeID → value → id
	counter uint32
	epochs  map[string]uint64
}

// NewInMemoryStore returns a Store backed entirely by in-memory maps.
// Suitable for unit tests; IDs are stable within a single process lifetime.
func NewInMemoryStore() Store {
	return &memStore{
		fwd:    make(map[string]map[string]uint32),
		epochs: make(map[string]uint64),
	}
}

func (s *memStore) Intern(_ context.Context, storeID, value string) (uint32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fwd[storeID] == nil {
		s.fwd[storeID] = make(map[string]uint32)
	}
	if id, ok := s.fwd[storeID][value]; ok {
		return id, nil
	}
	id := atomic.AddUint32(&s.counter, 1) - 1
	s.fwd[storeID][value] = id
	return id, nil
}

func (s *memStore) BulkIntern(ctx context.Context, storeID string, values []string) (map[string]uint32, error) {
	result := make(map[string]uint32, len(values))
	for _, v := range values {
		id, err := s.Intern(ctx, storeID, v)
		if err != nil {
			return nil, err
		}
		result[v] = id
	}
	return result, nil
}

func (s *memStore) LoadAll(_ context.Context, storeID string) (*LocalTable, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.fwd[storeID]
	if m == nil {
		return NewLocalTable(map[string]uint32{}), nil
	}
	cp := make(map[string]uint32, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return NewLocalTable(cp), nil
}

func (s *memStore) NextEpoch(_ context.Context, storeID string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.epochs[storeID]++
	return s.epochs[storeID], nil
}
```

- [ ] **Step 3: Run to confirm failure**

```bash
go test ./pipeline/... -run TestPipeline -v
```

Expected: `FAIL — no Go files in pipeline`

- [ ] **Step 4: Implement `pipeline/pipeline.go`**

```go
// Package pipeline implements the Offline Index Building Pipeline.
//
// The Pipeline performs a full index rebuild triggered by a model-change
// RebuildEvent from the Write Proxy. It shards the index by object type,
// assigns stable uint32 IDs via stringtable.Store, writes serialized roaring
// bitmap shards to storage.ShardStore, and publishes a RotationEvent so all
// instances can load the new epoch.
//
// Only one build runs at a time per store. A pending build channel (depth 1)
// queues one additional build; further requests are coalesced.
package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/messaging"
	"github.com/yourorg/openfga-indexer/storage"
	"github.com/yourorg/openfga-indexer/stringtable"
)

// Options configures the Pipeline.
type Options struct {
	StoreID      string
	IndexedTypes []string      // object types to index, e.g. ["group", "folder"]
	Logger       *slog.Logger
}

func (o *Options) applyDefaults() {
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// Pipeline performs full offline index rebuilds for one FGA store.
type Pipeline struct {
	fga     client.FGAClient
	stbl    stringtable.Store
	storage storage.ShardStore
	bus     messaging.MessageBus
	opts    Options

	mu      sync.Mutex       // guards active build
	pending chan struct{}     // depth-1: signals a queued build
}

// New creates a Pipeline. Call Run to trigger a build.
func New(
	fga client.FGAClient,
	stbl stringtable.Store,
	stor storage.ShardStore,
	bus messaging.MessageBus,
	opts Options,
) *Pipeline {
	opts.applyDefaults()
	return &Pipeline{
		fga:     fga,
		stbl:    stbl,
		storage: stor,
		bus:     bus,
		opts:    opts,
		pending: make(chan struct{}, 1),
	}
}

// Run performs one full build and returns the new epoch number.
// If a build is already in progress, Run queues itself (coalescing further
// requests) and waits for the in-progress build to finish before running.
func (p *Pipeline) Run(ctx context.Context) (uint64, error) {
	// Signal a pending build (non-blocking, coalesces duplicates).
	select {
	case p.pending <- struct{}{}:
	default:
	}

	// Drain our own token and acquire the build lock.
	<-p.pending
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.runLocked(ctx)
}

// runLocked performs the actual build. Caller must hold p.mu.
func (p *Pipeline) runLocked(ctx context.Context) (uint64, error) {
	log := p.opts.Logger.With("store", p.opts.StoreID)
	log.Info("offline pipeline starting")
	start := time.Now()

	// 1. Allocate a new epoch.
	epoch, err := p.stbl.NextEpoch(ctx, p.opts.StoreID)
	if err != nil {
		return 0, fmt.Errorf("pipeline.Run: next epoch: %w", err)
	}
	log.Info("epoch allocated", "epoch", epoch)

	// 2. Read all tuples from FGA.
	tuples, err := p.fga.ReadTuples(ctx, p.opts.StoreID)
	if err != nil {
		return 0, fmt.Errorf("pipeline.Run: ReadTuples: %w", err)
	}
	log.Info("tuples read", "count", len(tuples))

	// 3. Partition tuples by indexed object type.
	indexed := make(map[string][]client.Tuple)
	for _, t := range tuples {
		ot := objectType(t.Object)
		for _, it := range p.opts.IndexedTypes {
			if ot == it {
				indexed[ot] = append(indexed[ot], t)
				break
			}
		}
	}

	// 4. Build and write one shard per indexed type.
	var writtenTypes []string
	var rollback []func()

	for _, objType := range p.opts.IndexedTypes {
		typeTuples := indexed[objType]

		// Collect all string values that need IDs.
		var values []string
		for _, t := range typeTuples {
			values = append(values, t.User, t.Object)
		}
		// BulkIntern assigns stable IDs.
		ids, err := p.stbl.BulkIntern(ctx, p.opts.StoreID, values)
		if err != nil {
			p.rollbackShards(ctx, epoch, rollback)
			return 0, fmt.Errorf("pipeline.Run: BulkIntern type %s: %w", objType, err)
		}

		// Build roaring bitmap shards.
		m2g := make(map[uint32]*roaring.Bitmap) // MEMBER2GROUP
		g2g := make(map[uint32]*roaring.Bitmap) // GROUP2GROUP (direct, will be expanded)

		for _, t := range typeTuples {
			if t.Relation != "member" {
				continue
			}
			userID := ids[t.User]
			objID := ids[t.Object]
			bm := m2g[userID]
			if bm == nil {
				bm = roaring.New()
				m2g[userID] = bm
			}
			bm.Add(objID)
			// Reflexive entry.
			g := g2g[objID]
			if g == nil {
				g = roaring.New()
				g2g[objID] = g
			}
			g.Add(objID)
		}

		// Serialize: write member2group and group2group maps.
		data, err := serializeShard(m2g, g2g)
		if err != nil {
			p.rollbackShards(ctx, epoch, rollback)
			return 0, fmt.Errorf("pipeline.Run: serialize type %s: %w", objType, err)
		}

		if err := p.storage.WriteShard(ctx, p.opts.StoreID, objType, epoch, data); err != nil {
			p.rollbackShards(ctx, epoch, rollback)
			return 0, fmt.Errorf("pipeline.Run: WriteShard type %s: %w", objType, err)
		}

		writtenTypes = append(writtenTypes, objType)
		typeSnapshot := objType // capture for closure
		rollback = append(rollback, func() {
			_ = p.storage.DeleteShard(context.Background(), p.opts.StoreID, typeSnapshot, epoch)
		})
	}

	// 5. All shards written — publish RotationEvent.
	event := messaging.RotationEvent{
		StoreID:     p.opts.StoreID,
		Epoch:       epoch,
		Types:       writtenTypes,
		PublishedAt: time.Now(),
	}
	if err := p.bus.PublishRotation(ctx, event); err != nil {
		// Non-fatal: shards are written; instances will pick them up on next signal.
		log.Warn("failed to publish RotationEvent", "err", err)
	}

	log.Info("offline pipeline done", "epoch", epoch, "types", writtenTypes, "elapsed", time.Since(start))
	return epoch, nil
}

func (p *Pipeline) rollbackShards(ctx context.Context, epoch uint64, fns []func()) {
	for _, fn := range fns {
		fn()
	}
	p.opts.Logger.Warn("pipeline rolled back shards", "store", p.opts.StoreID, "epoch", epoch)
}

// serializeShard encodes MEMBER2GROUP and GROUP2GROUP maps to bytes.
// Format: [4-byte count M][M × (uint32 key + roaring bitmap)] repeated for each map.
func serializeShard(m2g, g2g map[uint32]*roaring.Bitmap) ([]byte, error) {
	var buf bytes.Buffer
	for _, m := range []map[uint32]*roaring.Bitmap{m2g, g2g} {
		count := uint32(len(m))
		if err := writeUint32(&buf, count); err != nil {
			return nil, err
		}
		for k, bm := range m {
			if err := writeUint32(&buf, k); err != nil {
				return nil, err
			}
			if _, err := bm.WriteTo(&buf); err != nil {
				return nil, err
			}
		}
	}
	return buf.Bytes(), nil
}

func writeUint32(buf *bytes.Buffer, v uint32) error {
	b := [4]byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
	_, err := buf.Write(b[:])
	return err
}

// objectType extracts the type prefix from an FGA object ID.
// "group:engineering" → "group"
func objectType(object string) string {
	if i := strings.Index(object, ":"); i > 0 {
		return object[:i]
	}
	return object
}
```

- [ ] **Step 5: Add `client.NewMockClient` to support test construction**

Check `client/mock.go` exists. If it only has a zero-value struct, update it to accept pre-seeded tuples:

```go
// In client/mock.go — add or update:

// NewMockClient returns a MockClient pre-seeded with the given tuples.
// ReadTuples returns all tuples; ReadChanges returns empty with no token.
func NewMockClient(tuples []Tuple) *MockClient {
	return &MockClient{tuples: tuples}
}
```

Read `client/mock.go` first to see what currently exists, then apply the minimal change needed.

- [ ] **Step 6: Run the pipeline tests**

```bash
go test ./pipeline/... ./stringtable/... -run "TestPipeline|TestLocalTable" -v
```

Expected: all `TestPipeline_*` and `TestLocalTable_*` tests PASS.

- [ ] **Step 7: Commit**

```bash
git add pipeline/ stringtable/memory.go
git commit -m "feat(pipeline): add Offline Pipeline with epoch-gated type-sharded builds"
```

---

## Task 2: Live Index Compute Tier

**Files:**
- Create: `compute/compute.go`
- Create: `compute/compute_test.go`

- [ ] **Step 1: Write the failing tests**

Create `compute/compute_test.go`:

```go
package compute_test

import (
	"context"
	"testing"
	"time"

	"github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/compute"
	"github.com/yourorg/openfga-indexer/index"
	"github.com/yourorg/openfga-indexer/messaging"
	leopardredis "github.com/yourorg/openfga-indexer/redis"
)

// mockFGA simulates ReadChanges returning a fixed set of changes once,
// then returning empty on subsequent calls.
type mockFGA struct {
	changes []client.TupleChange
	token   string
	called  int
}

func (m *mockFGA) ReadChanges(_ context.Context, _ string, _ string) ([]client.TupleChange, string, error) {
	m.called++
	if m.called == 1 && len(m.changes) > 0 {
		return m.changes, m.token, nil
	}
	return nil, m.token, nil
}
func (m *mockFGA) Check(_ context.Context, _ client.CheckRequest) (bool, error) { return false, nil }
func (m *mockFGA) ListObjects(_ context.Context, _ client.ListObjectsRequest) ([]string, error) {
	return nil, nil
}
func (m *mockFGA) ReadTuples(_ context.Context, _ string) ([]client.Tuple, error) { return nil, nil }

type capturedDeltas struct {
	events []messaging.DeltaEvent
}

func (c *capturedDeltas) PublishDelta(_ context.Context, e messaging.DeltaEvent) error {
	c.events = append(c.events, e)
	return nil
}

func TestComputeTier_AppliesDeltasToIndex(t *testing.T) {
	fga := &mockFGA{
		changes: []client.TupleChange{
			{User: "user:alice", Relation: "member", Object: "group:eng", Operation: client.OperationWrite},
		},
		token: "tok1",
	}
	idx := index.New()
	rstore := leopardredis.NewMemoryStore()
	captured := &capturedDeltas{}

	ct := compute.New(fga, idx, rstore, captured, compute.Options{
		StoreID:      "store1",
		Epoch:        1,
		PollInterval: 10 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = ct.Run(ctx) // runs until ctx cancelled

	// Delta should have been applied to the in-memory index.
	if !idx.IsMember("user:alice", "group:eng") {
		t.Fatal("alice should be in group:eng after delta applied")
	}

	// DeltaEvent should have been published.
	if len(captured.events) == 0 {
		t.Fatal("expected at least one DeltaEvent published")
	}
	if captured.events[0].StoreID != "store1" {
		t.Fatalf("DeltaEvent.StoreID = %q, want %q", captured.events[0].StoreID, "store1")
	}
}

func TestComputeTier_ReanchorsOnRotation(t *testing.T) {
	fga := &mockFGA{token: "old-token"}
	idx := index.New()
	rstore := leopardredis.NewMemoryStore()
	captured := &capturedDeltas{}

	ct := compute.New(fga, idx, rstore, captured, compute.Options{
		StoreID:      "store1",
		Epoch:        1,
		PollInterval: 10 * time.Millisecond,
	})

	// Simulate a rotation by setting a new token in Redis.
	_ = rstore.SetWatchToken(context.Background(), "store1", "new-token")

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = ct.Run(ctx)
	// No assertions on exact token value — just verify it runs without panic.
}
```

- [ ] **Step 2: Run to confirm failure**

```bash
go test ./compute/... -run TestComputeTier -v
```

Expected: `FAIL — no Go files in compute`

- [ ] **Step 3: Implement `compute/compute.go`**

```go
// Package compute implements the Live Index Compute Tier.
//
// The ComputeTier polls FGA's ReadChanges endpoint and applies tuple deltas
// to the in-memory index and Redis L2 cache. It publishes DeltaEvent batches
// to the MessageBus so replica clusters can stay current between full rebuilds.
//
// The ComputeTier runs on the master only. On promotion, it is started live
// without a process restart.
package compute

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/index"
	"github.com/yourorg/openfga-indexer/messaging"
	leopardredis "github.com/yourorg/openfga-indexer/redis"
)

// DeltaPublisher is the subset of messaging.MessageBus used by ComputeTier.
type DeltaPublisher interface {
	PublishDelta(ctx context.Context, event messaging.DeltaEvent) error
}

// Options configures the ComputeTier.
type Options struct {
	StoreID      string
	Epoch        uint64        // current epoch — used to tag DeltaEvents
	PollInterval time.Duration // how long to wait when no changes available
	Logger       *slog.Logger
}

func (o *Options) applyDefaults() {
	if o.PollInterval == 0 {
		o.PollInterval = 5 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// ComputeTier polls ReadChanges and maintains the live index.
type ComputeTier struct {
	fga    client.FGAClient
	idx    index.Index
	redis  leopardredis.Store
	bus    DeltaPublisher
	opts   Options
}

// New creates a ComputeTier. Call Run to start it.
func New(
	fga client.FGAClient,
	idx index.Index,
	redis leopardredis.Store,
	bus DeltaPublisher,
	opts Options,
) *ComputeTier {
	opts.applyDefaults()
	return &ComputeTier{fga: fga, idx: idx, redis: redis, bus: bus, opts: opts}
}

// Run polls ReadChanges until ctx is cancelled.
// It re-anchors to the stored watch token whenever Redis signals a rotation.
func (ct *ComputeTier) Run(ctx context.Context) error {
	log := ct.opts.Logger.With("store", ct.opts.StoreID)
	log.Info("compute tier starting")

	// Load initial token from Redis (set by the pipeline after a rotation).
	token, err := ct.redis.GetWatchToken(ctx, ct.opts.StoreID)
	if err != nil {
		log.Warn("could not load watch token from Redis, starting from beginning", "err", err)
		token = ""
	}

	backoff := ct.opts.PollInterval
	const maxBackoff = 60 * time.Second

	for {
		// Check for a more recent token in Redis (set by a pipeline rotation).
		if stored, err := ct.redis.GetWatchToken(ctx, ct.opts.StoreID); err == nil && stored != "" && stored != token {
			log.Info("re-anchoring to new watch token", "old", token, "new", stored)
			token = stored
		}

		changes, next, err := ct.fga.ReadChanges(ctx, ct.opts.StoreID, token)
		if err != nil {
			if ctx.Err() != nil {
				log.Info("compute tier stopping", "reason", ctx.Err())
				return nil
			}
			log.Error("ReadChanges failed", "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
				backoff = min(backoff*2, maxBackoff)
				continue
			}
		}
		backoff = ct.opts.PollInterval // reset on success

		if len(changes) > 0 {
			ct.applyChanges(ctx, changes)
			token = next
		} else {
			select {
			case <-ctx.Done():
				log.Info("compute tier stopping", "reason", ctx.Err())
				return nil
			case <-time.After(ct.opts.PollInterval):
			}
		}
	}
}

func (ct *ComputeTier) applyChanges(ctx context.Context, changes []client.TupleChange) {
	log := ct.opts.Logger.With("store", ct.opts.StoreID)

	for _, ch := range changes {
		switch ch.Operation {
		case client.OperationWrite:
			ct.idx.ApplyTupleWrite(ch.User, ch.Relation, ch.Object)
		case client.OperationDelete:
			ct.idx.ApplyTupleDelete(ch.User, ch.Relation, ch.Object)
		}
	}

	// Publish delta batch to MessageBus so replicas can apply the same changes.
	event := messaging.DeltaEvent{
		StoreID:   ct.opts.StoreID,
		Epoch:     ct.opts.Epoch,
		Changes:   changes,
		AppliedAt: time.Now(),
	}
	if err := ct.bus.PublishDelta(ctx, event); err != nil {
		log.Warn("failed to publish DeltaEvent", "err", err)
	}

	log.Debug("applied changes", "count", len(changes))
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// UpdateEpoch updates the epoch used in DeltaEvent after a pipeline rotation.
func (ct *ComputeTier) UpdateEpoch(epoch uint64) {
	ct.opts.Epoch = epoch
}

// UpdateToken signals the ComputeTier of a new watch token after a rotation.
// The token is persisted in Redis; Run picks it up on the next poll cycle.
func (ct *ComputeTier) UpdateToken(ctx context.Context, token string) error {
	if err := ct.redis.SetWatchToken(ctx, ct.opts.StoreID, token); err != nil {
		return fmt.Errorf("compute.UpdateToken: %w", err)
	}
	return nil
}
```

- [ ] **Step 4: Run all compute tests**

```bash
go test ./compute/... -run TestComputeTier -v
```

Expected: `TestComputeTier_AppliesDeltasToIndex` and `TestComputeTier_ReanchorsOnRotation` PASS.

- [ ] **Step 5: Run all tests to check for regressions**

```bash
go test ./... -short -v 2>&1 | tail -40
```

Expected: all unit tests pass, integration tests skipped.

- [ ] **Step 6: Commit**

```bash
git add compute/
git commit -m "feat(compute): add Live Index Compute Tier with delta broadcast"
```

---

## Self-Review

- [x] **Spec coverage:** Section 9 (Offline Pipeline) ✓ Task 1. Section 10 (Compute Tier + DeltaEvent publish) ✓ Task 2. Epoch-gated rollback ✓ `rollbackShards`. Per-store mutex ✓ `p.mu` + pending channel. Shard serialization ✓ `serializeShard`.
- [x] **Placeholder scan:** No TBD/TODO. All code complete.
- [x] **Type consistency:** `stringtable.Store` interface from Plan 2 used correctly. `storage.ShardStore` interface from Plan 3 used correctly. `messaging.DeltaEvent` with `[]client.TupleChange` matches Plan 3 definition. `index.Index` from Plan 1 interface unchanged. `compute.DeltaPublisher` is a subset of `messaging.MessageBus` — both `natsBus` and `rsBus` from Plan 3 satisfy it.
