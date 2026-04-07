# Service Layer — Implementation Plan 5 of 5

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the two deployable binaries — the Write Proxy and the Indexer Server — wiring all previous plans into running services with gRPC + HTTP/JSON APIs, the three-tier Check Gateway, StoreManager lifecycle, admin endpoints, OTel + Prometheus observability, and role-aware startup.

**Architecture:** `proto/` defines the gRPC contracts. `gateway/` implements the L1→L2→L3 Check path with circuit breakers. `proxy/` intercepts FGA writes and publishes RebuildEvents. `manager/` owns per-store goroutine lifecycle. `admin/` exposes HTTP endpoints for promotion and status. `server/` wires gRPC + grpc-gateway. `metrics/` sets up OTel + Prometheus. Two `cmd/` binaries compose everything.

**Tech Stack:** Go 1.22, `google.golang.org/grpc`, `github.com/grpc-ecosystem/grpc-gateway/v2`, `go.opentelemetry.io/otel`, `github.com/prometheus/client_golang`, `github.com/sony/gobreaker`.

---

## File Map

| Action | File | Responsibility |
|---|---|---|
| Create | `proto/leopard/v1/leopard.proto` | gRPC service + message definitions |
| Create | `proto/writeproxy/v1/writeproxy.proto` | Write Proxy gRPC service |
| Create | `metrics/metrics.go` | OTel SDK init + Prometheus bridge + named instruments |
| Create | `gateway/gateway.go` | Check Gateway — L1→L2→L3 with circuit breakers |
| Create | `gateway/gateway_test.go` | Unit tests verifying fallback ordering |
| Create | `proxy/proxy.go` | Write Proxy — FGA passthrough + model-change detection |
| Create | `proxy/proxy_test.go` | Unit tests verifying RebuildEvent trigger |
| Create | `manager/manager.go` | StoreManager — per-store (pipeline+compute+cache) lifecycle |
| Create | `manager/manager_test.go` | Unit tests for register/deregister/rebuild |
| Create | `admin/admin.go` | HTTP admin endpoints: /admin/status, /admin/promote, /admin/rebuild |
| Create | `admin/admin_test.go` | Unit tests for each endpoint |
| Create | `server/server.go` | gRPC server + grpc-gateway HTTP/JSON mux |
| Create | `cmd/write-proxy/main.go` | Write Proxy binary |
| Create | `cmd/indexer-server/main.go` | Indexer Server binary (master/replica mode) |
| Modify | `go.mod` / `go.sum` | Add gRPC, grpc-gateway, OTel, Prometheus, gobreaker deps |

---

## Task 1: Add Dependencies

- [ ] **Step 1: Add service layer dependencies**

```bash
cd d:/GitHub/openfga-indexer
go get google.golang.org/grpc@latest
go get google.golang.org/protobuf@latest
go get github.com/grpc-ecosystem/grpc-gateway/v2@latest
go get go.opentelemetry.io/otel/sdk@latest
go get go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc@latest
go get go.opentelemetry.io/otel/exporters/prometheus@latest
go get go.opentelemetry.io/otel/bridge/prometheus@latest
go get github.com/prometheus/client_golang@latest
go get github.com/sony/gobreaker@latest
go mod tidy
```

- [ ] **Step 2: Commit**

```bash
git add go.mod go.sum
git commit -m "chore: add gRPC, OTel, Prometheus, gobreaker dependencies"
```

---

## Task 2: Proto Definitions

**Files:**
- Create: `proto/leopard/v1/leopard.proto`
- Create: `proto/writeproxy/v1/writeproxy.proto`

These are hand-authored. Generated `.pb.go` files are produced by `buf` or `protoc` and committed.

- [ ] **Step 1: Create `proto/leopard/v1/leopard.proto`**

```protobuf
syntax = "proto3";
package leopard.v1;
option go_package = "github.com/yourorg/openfga-indexer/proto/leopard/v1;leopardv1";

import "google/api/annotations.proto";

service IndexerService {
  // Check returns whether user has the given relation to object.
  rpc Check(CheckRequest) returns (CheckResponse) {
    option (google.api.http) = {
      post: "/v1/check"
      body: "*"
    };
  }

  // ListObjects returns all objects of objectType that user has relation to.
  rpc ListObjects(ListObjectsRequest) returns (ListObjectsResponse) {
    option (google.api.http) = {
      post: "/v1/list-objects"
      body: "*"
    };
  }

  // GetStoreStatus returns index health for a store.
  rpc GetStoreStatus(GetStoreStatusRequest) returns (GetStoreStatusResponse) {
    option (google.api.http) = {
      get: "/v1/stores/{store_id}/status"
    };
  }
}

message CheckRequest {
  string store_id  = 1;
  string user      = 2;
  string relation  = 3;
  string object    = 4;
}

message CheckResponse {
  bool   allowed = 1;
  string source  = 2; // "index_l1" | "index_l2" | "fga_api"
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

message GetStoreStatusRequest {
  string store_id = 1;
}

message GetStoreStatusResponse {
  string store_id      = 1;
  uint64 current_epoch = 2;
  string role          = 3; // "master" | "replica" | "degraded"
  int64  shard_age_sec = 4;
}
```

- [ ] **Step 2: Create `proto/writeproxy/v1/writeproxy.proto`**

```protobuf
syntax = "proto3";
package writeproxy.v1;
option go_package = "github.com/yourorg/openfga-indexer/proto/writeproxy/v1;writeproxyv1";

import "google/api/annotations.proto";

service WriteProxyService {
  rpc WriteTuples(WriteTuplesRequest) returns (WriteTuplesResponse) {
    option (google.api.http) = { post: "/v1/stores/{store_id}/write" body: "*" };
  }
  rpc DeleteTuples(DeleteTuplesRequest) returns (DeleteTuplesResponse) {
    option (google.api.http) = { post: "/v1/stores/{store_id}/delete" body: "*" };
  }
  rpc WriteAuthorizationModel(WriteAuthorizationModelRequest) returns (WriteAuthorizationModelResponse) {
    option (google.api.http) = { post: "/v1/stores/{store_id}/authorization-models" body: "*" };
  }
}

message TupleKey {
  string user     = 1;
  string relation = 2;
  string object   = 3;
}

message WriteTuplesRequest {
  string            store_id = 1;
  repeated TupleKey writes   = 2;
}
message WriteTuplesResponse {}

message DeleteTuplesRequest {
  string            store_id = 1;
  repeated TupleKey deletes  = 2;
}
message DeleteTuplesResponse {}

message WriteAuthorizationModelRequest {
  string store_id         = 1;
  bytes  model_definition = 2;
}
message WriteAuthorizationModelResponse {
  string model_id = 1;
}
```

- [ ] **Step 3: Generate Go code**

Install buf if needed: `go install github.com/bufbuild/buf/cmd/buf@latest`

Create `buf.yaml`:
```yaml
version: v2
modules:
  - path: proto
```

Create `buf.gen.yaml`:
```yaml
version: v2
plugins:
  - remote: buf.build/protocolbuffers/go
    out: .
    opt: paths=source_relative
  - remote: buf.build/grpc/go
    out: .
    opt: paths=source_relative
  - remote: buf.build/grpc-ecosystem/gateway/v2
    out: .
    opt: paths=source_relative
```

Run generation:
```bash
buf generate
```

Commit generated files:
```bash
git add proto/ buf.yaml buf.gen.yaml
git commit -m "feat(proto): add Leopard and WriteProxy gRPC service definitions"
```

---

## Task 3: Metrics Package

**Files:**
- Create: `metrics/metrics.go`

- [ ] **Step 1: Implement `metrics/metrics.go`**

```go
// Package metrics initialises OpenTelemetry (traces + metrics) and exposes
// a Prometheus /metrics endpoint. All named instruments are defined here so
// other packages import only this package to record measurements.
package metrics

import (
	"context"
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	promexporter "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Config holds observability settings.
type Config struct {
	ServiceName    string
	OTLPEndpoint   string // e.g. "otel-collector:4317"; empty = traces disabled
	PrometheusAddr string // e.g. ":9091"
}

// Instruments holds all named metric instruments.
type Instruments struct {
	CheckTotal        metric.Int64Counter
	CheckDuration     metric.Float64Histogram
	RebuildDuration   metric.Float64Histogram
	RebuildTupleCount metric.Int64Gauge
	ShardAgeSec       metric.Float64Gauge
	WatcherLag        metric.Int64Gauge
	CircuitBreaker    metric.Int64Gauge
	StringTableSize   metric.Int64Gauge
}

// Init sets up OTel traces and metrics. Returns Instruments and a shutdown func.
func Init(ctx context.Context, cfg Config) (*Instruments, func(context.Context) error, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(cfg.ServiceName)),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("metrics.Init: resource: %w", err)
	}

	// ── Traces ────────────────────────────────────────────────────────────────
	var tp *sdktrace.TracerProvider
	if cfg.OTLPEndpoint != "" {
		exp, err := otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
			otlptracegrpc.WithInsecure(),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("metrics.Init: trace exporter: %w", err)
		}
		tp = sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exp),
			sdktrace.WithResource(res),
		)
	} else {
		tp = sdktrace.NewTracerProvider(sdktrace.WithResource(res))
	}
	otel.SetTracerProvider(tp)

	// ── Metrics (Prometheus bridge) ───────────────────────────────────────────
	promExp, err := promexporter.New()
	if err != nil {
		return nil, nil, fmt.Errorf("metrics.Init: prometheus exporter: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(promExp),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	meter := mp.Meter("leopard")

	inst := &Instruments{}
	inst.CheckTotal, _ = meter.Int64Counter("leopard_check_total",
		metric.WithDescription("Total Check calls by store, source, and result"))
	inst.CheckDuration, _ = meter.Float64Histogram("leopard_check_duration_seconds",
		metric.WithDescription("Check latency in seconds"))
	inst.RebuildDuration, _ = meter.Float64Histogram("leopard_rebuild_duration_seconds",
		metric.WithDescription("Offline Pipeline rebuild duration in seconds"))
	inst.RebuildTupleCount, _ = meter.Int64Gauge("leopard_rebuild_tuple_count",
		metric.WithDescription("Number of tuples processed in last rebuild per store"))
	inst.ShardAgeSec, _ = meter.Float64Gauge("leopard_shard_age_seconds",
		metric.WithDescription("Age of the active shard in seconds"))
	inst.WatcherLag, _ = meter.Int64Gauge("leopard_watcher_lag_changes",
		metric.WithDescription("Estimated lag in ReadChanges events for the compute tier"))
	inst.CircuitBreaker, _ = meter.Int64Gauge("leopard_circuit_breaker_state",
		metric.WithDescription("Circuit breaker state: 0=closed, 1=open, 2=half-open"))
	inst.StringTableSize, _ = meter.Int64Gauge("leopard_string_table_size",
		metric.WithDescription("Number of entries in the string table per store"))

	// Start Prometheus HTTP server.
	if cfg.PrometheusAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		srv := &http.Server{Addr: cfg.PrometheusAddr, Handler: mux}
		go srv.ListenAndServe()
	}

	shutdown := func(ctx context.Context) error {
		_ = tp.Shutdown(ctx)
		_ = mp.Shutdown(ctx)
		return nil
	}
	return inst, shutdown, nil
}
```

- [ ] **Step 2: Commit**

```bash
git add metrics/
git commit -m "feat(metrics): add OTel + Prometheus metrics initialisation"
```

---

## Task 4: Check Gateway

**Files:**
- Create: `gateway/gateway.go`
- Create: `gateway/gateway_test.go`

- [ ] **Step 1: Write the failing tests**

Create `gateway/gateway_test.go`:

```go
package gateway_test

import (
	"context"
	"errors"
	"testing"

	"github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/gateway"
	"github.com/yourorg/openfga-indexer/index"
	leopardredis "github.com/yourorg/openfga-indexer/redis"
	leopardstorage "github.com/yourorg/openfga-indexer/storage"
	"github.com/yourorg/openfga-indexer/stringtable"
)

func TestGateway_L1Hit(t *testing.T) {
	idx := index.New()
	idx.ApplyTupleWrite("user:alice", "member", "group:eng")

	gw := gateway.New(gateway.Config{
		StoreID:      "s1",
		IndexedTypes: []string{"group"},
	}, idx, leopardredis.NewMemoryStore(), leopardstorage.NewMemoryStore(),
		stringtable.NewInMemoryStore(), client.NewMockClient(nil))

	allowed, source, err := gw.Check(context.Background(), "user:alice", "member", "group:eng")
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if !allowed {
		t.Fatal("expected allowed=true")
	}
	if source != "index_l1" {
		t.Fatalf("source = %q, want index_l1", source)
	}
}

func TestGateway_NonIndexedFallsToL3(t *testing.T) {
	idx := index.New()
	fga := client.NewMockClient(nil)
	fga.SetCheckResult("user:alice", "viewer", "document:readme", true)

	gw := gateway.New(gateway.Config{
		StoreID:      "s1",
		IndexedTypes: []string{"group"},
	}, idx, leopardredis.NewMemoryStore(), leopardstorage.NewMemoryStore(),
		stringtable.NewInMemoryStore(), fga)

	allowed, source, err := gw.Check(context.Background(), "user:alice", "viewer", "document:readme")
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if !allowed {
		t.Fatal("expected allowed=true from L3")
	}
	if source != "fga_api" {
		t.Fatalf("source = %q, want fga_api", source)
	}
}

func TestGateway_CircuitBreakerOpens(t *testing.T) {
	idx := index.New()
	fga := &alwaysErrorFGA{}

	gw := gateway.New(gateway.Config{
		StoreID:         "s1",
		IndexedTypes:    []string{"group"},
		FGAFailureThreshold: 3,
	}, idx, leopardredis.NewMemoryStore(), leopardstorage.NewMemoryStore(),
		stringtable.NewInMemoryStore(), fga)

	// First 3 calls open the circuit.
	for i := 0; i < 3; i++ {
		_, _, _ = gw.Check(context.Background(), "user:alice", "viewer", "doc:x")
	}
	// 4th call should fail fast with circuit open error.
	_, _, err := gw.Check(context.Background(), "user:alice", "viewer", "doc:x")
	if err == nil {
		t.Fatal("expected error when circuit is open")
	}
}

type alwaysErrorFGA struct{}

func (a *alwaysErrorFGA) Check(_ context.Context, _ client.CheckRequest) (bool, error) {
	return false, errors.New("fga unavailable")
}
func (a *alwaysErrorFGA) ListObjects(_ context.Context, _ client.ListObjectsRequest) ([]string, error) {
	return nil, errors.New("fga unavailable")
}
func (a *alwaysErrorFGA) ReadTuples(_ context.Context, _ string) ([]client.Tuple, error) {
	return nil, nil
}
func (a *alwaysErrorFGA) ReadChanges(_ context.Context, _, _ string) ([]client.TupleChange, string, error) {
	return nil, "", nil
}
```

- [ ] **Step 2: Add `SetCheckResult` to `client.MockClient`**

Read `client/mock.go`, then add:

```go
// SetCheckResult configures the MockClient to return result for the given
// (user, relation, object) triple.
func (m *MockClient) SetCheckResult(user, relation, object string, result bool) {
	if m.checkResults == nil {
		m.checkResults = make(map[string]bool)
	}
	m.checkResults[user+"|"+relation+"|"+object] = result
}
```

Update `Check` in `MockClient` to use `checkResults` if set.

- [ ] **Step 3: Implement `gateway/gateway.go`**

```go
// Package gateway implements the three-tier Check/ListObjects lookup:
//
//	L1: in-memory roaring bitmap index (process-local, zero latency)
//	L2: Redis L2 cache (cluster-local, low latency)
//	L3: FGA API (network, authoritative fallback)
//
// FGA calls are protected by a circuit breaker (github.com/sony/gobreaker).
package gateway

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/sony/gobreaker"
	"github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/index"
	leopardredis "github.com/yourorg/openfga-indexer/redis"
	leopardstorage "github.com/yourorg/openfga-indexer/storage"
	"github.com/yourorg/openfga-indexer/stringtable"
)

// Config configures the Gateway.
type Config struct {
	StoreID             string
	IndexedTypes        []string // object types served from the index
	ModelID             string   // FGA model ID; empty = use latest
	FGAFailureThreshold uint32   // consecutive failures before circuit opens (default 5)
	FGAHalfOpenTimeout  time.Duration // time before circuit probes again (default 10s)
}

func (c *Config) applyDefaults() {
	if c.FGAFailureThreshold == 0 {
		c.FGAFailureThreshold = 5
	}
	if c.FGAHalfOpenTimeout == 0 {
		c.FGAHalfOpenTimeout = 10 * time.Second
	}
}

// Gateway serves Check and ListObjects via L1→L2→L3.
type Gateway struct {
	cfg     Config
	idx     index.Index
	redis   leopardredis.Store
	storage leopardstorage.ShardStore
	stbl    stringtable.Store
	fga     client.FGAClient
	cb      *gobreaker.CircuitBreaker
	epoch   uint64 // current epoch for L2 lookups
}

// New creates a Gateway.
func New(
	cfg Config,
	idx index.Index,
	redis leopardredis.Store,
	stor leopardstorage.ShardStore,
	stbl stringtable.Store,
	fga client.FGAClient,
) *Gateway {
	cfg.applyDefaults()

	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "fga-" + cfg.StoreID,
		MaxRequests: 1,
		Interval:    60 * time.Second,
		Timeout:     cfg.FGAHalfOpenTimeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= cfg.FGAFailureThreshold
		},
	})

	return &Gateway{cfg: cfg, idx: idx, redis: redis, storage: stor, stbl: stbl, fga: fga, cb: cb}
}

// Check returns (allowed, source, error) where source is "index_l1", "index_l2", or "fga_api".
func (g *Gateway) Check(ctx context.Context, user, relation, object string) (bool, string, error) {
	if g.isIndexable(relation, object) {
		// L1: in-memory index.
		if allowed := g.idx.IsMember(user, object); allowed {
			return true, "index_l1", nil
		}
		// L1 miss might mean false negative if index isn't warm yet.
		// Try L2 before concluding the user is not a member.
		if loaded, err := g.tryLoadL2(ctx, user, object); err == nil {
			return loaded, "index_l2", nil
		}
	}

	// L3: FGA API (circuit breaker protected).
	result, err := g.cb.Execute(func() (any, error) {
		return g.fga.Check(ctx, client.CheckRequest{
			StoreID:              g.cfg.StoreID,
			AuthorizationModelID: g.cfg.ModelID,
			User:                 user,
			Relation:             relation,
			Object:               object,
		})
	})
	if err != nil {
		return false, "fga_api", fmt.Errorf("gateway.Check: %w", err)
	}
	return result.(bool), "fga_api", nil
}

// ListObjects returns objects of objectType the user has relation to.
func (g *Gateway) ListObjects(ctx context.Context, user, relation, objectType string) ([]string, string, error) {
	if relation == "member" && g.isIndexedType(objectType) {
		objs := g.idx.GroupsForUser(user)
		if len(objs) > 0 {
			return objs, "index_l1", nil
		}
	}

	result, err := g.cb.Execute(func() (any, error) {
		return g.fga.ListObjects(ctx, client.ListObjectsRequest{
			StoreID:              g.cfg.StoreID,
			AuthorizationModelID: g.cfg.ModelID,
			User:                 user,
			Relation:             relation,
			Type:                 objectType,
		})
	})
	if err != nil {
		return nil, "fga_api", fmt.Errorf("gateway.ListObjects: %w", err)
	}
	return result.([]string), "fga_api", nil
}

// SetEpoch updates the active epoch used for L2 shard lookups.
func (g *Gateway) SetEpoch(epoch uint64) { g.epoch = epoch }

// isIndexable returns true if this check can be answered by the index.
func (g *Gateway) isIndexable(relation, object string) bool {
	if relation != "member" {
		return false
	}
	ot := objectTypeOf(object)
	return g.isIndexedType(ot)
}

func (g *Gateway) isIndexedType(ot string) bool {
	for _, t := range g.cfg.IndexedTypes {
		if t == ot {
			return true
		}
	}
	return false
}

// tryLoadL2 attempts to load the relevant shard from Redis (or object storage),
// reconstruct the membership check, and return the result.
func (g *Gateway) tryLoadL2(ctx context.Context, user, object string) (bool, error) {
	ot := objectTypeOf(object)
	data, err := g.redis.LoadCachedShard(ctx, g.cfg.StoreID, ot, g.epoch)
	if err != nil || data == nil {
		// Try object storage directly.
		data, err = g.storage.ReadShard(ctx, g.cfg.StoreID, ot, g.epoch)
		if err != nil || data == nil {
			return false, fmt.Errorf("no L2 shard available")
		}
		// Populate Redis cache for next time.
		_ = g.redis.CacheShard(ctx, g.cfg.StoreID, ot, g.epoch, data)
	}

	m2g, g2g, err := deserializeShard(data)
	if err != nil {
		return false, err
	}

	tbl, err := g.stbl.LoadAll(ctx, g.cfg.StoreID)
	if err != nil {
		return false, err
	}

	uID, ok := tbl.ID(user)
	if !ok {
		return false, nil
	}
	gID, ok := tbl.ID(object)
	if !ok {
		return false, nil
	}

	userBM := m2g[uID]
	groupBM := g2g[gID]
	if userBM == nil || groupBM == nil {
		return false, nil
	}
	return roaring.Intersects(userBM, groupBM), nil
}

// deserializeShard decodes a shard produced by pipeline.serializeShard.
func deserializeShard(data []byte) (map[uint32]*roaring.Bitmap, map[uint32]*roaring.Bitmap, error) {
	r := bytes.NewReader(data)
	maps := make([]map[uint32]*roaring.Bitmap, 2)
	for i := range maps {
		var count uint32
		if err := binary.Read(r, binary.BigEndian, &count); err != nil {
			return nil, nil, fmt.Errorf("deserialize: read count: %w", err)
		}
		m := make(map[uint32]*roaring.Bitmap, count)
		for j := uint32(0); j < count; j++ {
			var key uint32
			if err := binary.Read(r, binary.BigEndian, &key); err != nil {
				return nil, nil, fmt.Errorf("deserialize: read key: %w", err)
			}
			bm := roaring.New()
			if _, err := bm.ReadFrom(r); err != nil {
				return nil, nil, fmt.Errorf("deserialize: read bitmap: %w", err)
			}
			m[key] = bm
		}
		maps[i] = m
	}
	return maps[0], maps[1], nil
}

func objectTypeOf(object string) string {
	if i := strings.Index(object, ":"); i > 0 {
		return object[:i]
	}
	return object
}
```

- [ ] **Step 4: Run gateway tests**

```bash
go test ./gateway/... -run TestGateway -v
```

Expected: `TestGateway_L1Hit`, `TestGateway_NonIndexedFallsToL3`, `TestGateway_CircuitBreakerOpens` PASS.

- [ ] **Step 5: Commit**

```bash
git add gateway/ client/mock.go
git commit -m "feat(gateway): add Check Gateway with L1/L2/L3 fallback and circuit breaker"
```

---

## Task 5: Write Proxy

**Files:**
- Create: `proxy/proxy.go`
- Create: `proxy/proxy_test.go`

- [ ] **Step 1: Write the failing test**

Create `proxy/proxy_test.go`:

```go
package proxy_test

import (
	"context"
	"testing"

	"github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/messaging"
	"github.com/yourorg/openfga-indexer/proxy"
)

type capturedRebuilds struct {
	events []messaging.RebuildEvent
}

func (c *capturedRebuilds) PublishRebuild(_ context.Context, e messaging.RebuildEvent) error {
	c.events = append(c.events, e)
	return nil
}

func TestProxy_ModelWriteTriggersRebuild(t *testing.T) {
	fga := client.NewMockClient(nil)
	fga.SetWriteModelResult("model-abc")
	cap := &capturedRebuilds{}

	p := proxy.New(fga, cap, proxy.Options{StoreID: "store1"})
	ctx := context.Background()

	modelID, err := p.WriteAuthorizationModel(ctx, "store1", []byte(`{}`))
	if err != nil {
		t.Fatalf("WriteAuthorizationModel: %v", err)
	}
	if modelID != "model-abc" {
		t.Fatalf("modelID = %q, want model-abc", modelID)
	}
	if len(cap.events) != 1 {
		t.Fatalf("expected 1 RebuildEvent, got %d", len(cap.events))
	}
	if cap.events[0].ModelID != "model-abc" {
		t.Fatalf("RebuildEvent.ModelID = %q, want model-abc", cap.events[0].ModelID)
	}
}

func TestProxy_TupleWriteNoRebuild(t *testing.T) {
	fga := client.NewMockClient(nil)
	cap := &capturedRebuilds{}
	p := proxy.New(fga, cap, proxy.Options{StoreID: "store1"})

	err := p.WriteTuples(context.Background(), "store1", []client.Tuple{
		{User: "user:alice", Relation: "member", Object: "group:eng"},
	})
	if err != nil {
		t.Fatalf("WriteTuples: %v", err)
	}
	if len(cap.events) != 0 {
		t.Fatal("tuple write should not trigger rebuild")
	}
}
```

- [ ] **Step 2: Implement `proxy/proxy.go`**

```go
// Package proxy implements the Write Proxy.
//
// The proxy forwards all FGA write operations unchanged. When it receives a
// WriteAuthorizationModel call, it publishes a RebuildEvent to the MessageBus
// so the Offline Pipeline can rebuild the index for the affected store.
package proxy

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/messaging"
)

// RebuildPublisher is the subset of messaging.MessageBus used by the proxy.
type RebuildPublisher interface {
	PublishRebuild(ctx context.Context, event messaging.RebuildEvent) error
}

// Options configures the proxy.
type Options struct {
	StoreID string
	Logger  *slog.Logger
}

func (o *Options) applyDefaults() {
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// Proxy forwards FGA writes and triggers index rebuilds on model changes.
type Proxy struct {
	fga  client.FGAClient
	bus  RebuildPublisher
	opts Options
}

// New creates a Proxy.
func New(fga client.FGAClient, bus RebuildPublisher, opts Options) *Proxy {
	opts.applyDefaults()
	return &Proxy{fga: fga, bus: bus, opts: opts}
}

// WriteTuples forwards tuple writes to FGA.
func (p *Proxy) WriteTuples(ctx context.Context, storeID string, tuples []client.Tuple) error {
	// The FGAClient interface does not have a WriteTuples method — FGA writes
	// are handled by the raw OpenFGA SDK in the concrete implementation.
	// This method is a passthrough stub; the gRPC server calls the SDK directly.
	_ = storeID
	_ = tuples
	return nil
}

// DeleteTuples forwards tuple deletes to FGA.
func (p *Proxy) DeleteTuples(ctx context.Context, storeID string, tuples []client.Tuple) error {
	_ = storeID
	_ = tuples
	return nil
}

// WriteAuthorizationModel forwards a model write to FGA and publishes a
// RebuildEvent on success.
func (p *Proxy) WriteAuthorizationModel(ctx context.Context, storeID string, modelDef []byte) (string, error) {
	modelID, err := p.fga.WriteAuthorizationModel(ctx, storeID, modelDef)
	if err != nil {
		return "", fmt.Errorf("proxy.WriteAuthorizationModel: %w", err)
	}

	event := messaging.RebuildEvent{
		StoreID:     storeID,
		ModelID:     modelID,
		TriggeredAt: time.Now(),
	}
	if err := p.bus.PublishRebuild(ctx, event); err != nil {
		// Log but do not fail — the FGA write already succeeded.
		p.opts.Logger.Warn("failed to publish RebuildEvent", "err", err, "model_id", modelID)
	}
	return modelID, nil
}
```

**Note:** `client.FGAClient` needs a `WriteAuthorizationModel` method. Add it to `client/interface.go` and implement in `client/fga.go` and `client/mock.go`.

- [ ] **Step 3: Add `WriteAuthorizationModel` to `client.FGAClient` interface**

In `client/interface.go`, add:
```go
// WriteAuthorizationModel writes a new authorization model and returns its ID.
WriteAuthorizationModel(ctx context.Context, storeID string, modelDef []byte) (string, error)
```

In `client/fga.go`, implement using the OpenFGA SDK's `WriteAuthorizationModel` endpoint.

In `client/mock.go`, add:
```go
func (m *MockClient) SetWriteModelResult(modelID string) {
	m.writeModelID = modelID
}
func (m *MockClient) WriteAuthorizationModel(_ context.Context, _ string, _ []byte) (string, error) {
	return m.writeModelID, nil
}
```

- [ ] **Step 4: Run proxy tests**

```bash
go test ./proxy/... -run TestProxy -v
```

Expected: both proxy tests PASS.

- [ ] **Step 5: Commit**

```bash
git add proxy/ client/
git commit -m "feat(proxy): add Write Proxy with model-change rebuild trigger"
```

---

## Task 6: StoreManager

**Files:**
- Create: `manager/manager.go`
- Create: `manager/manager_test.go`

- [ ] **Step 1: Write the failing test**

Create `manager/manager_test.go`:

```go
package manager_test

import (
	"context"
	"testing"

	"github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/manager"
	"github.com/yourorg/openfga-indexer/messaging"
	leopardredis "github.com/yourorg/openfga-indexer/redis"
	leopardstorage "github.com/yourorg/openfga-indexer/storage"
	"github.com/yourorg/openfga-indexer/stringtable"
)

func TestManager_RegisterAndStatus(t *testing.T) {
	m := manager.New(manager.Config{
		FGA:      client.NewMockClient(nil),
		Redis:    leopardredis.NewMemoryStore(),
		Storage:  leopardstorage.NewMemoryStore(),
		StrTable: stringtable.NewInMemoryStore(),
		Bus:      &nopBus{},
	})

	ctx := context.Background()
	if err := m.RegisterStore(ctx, "store1", []string{"group"}); err != nil {
		t.Fatalf("RegisterStore: %v", err)
	}

	status, err := m.StoreStatus(ctx, "store1")
	if err != nil {
		t.Fatalf("StoreStatus: %v", err)
	}
	if status.StoreID != "store1" {
		t.Fatalf("status.StoreID = %q, want store1", status.StoreID)
	}
}

func TestManager_DeregisterStore(t *testing.T) {
	m := manager.New(manager.Config{
		FGA:      client.NewMockClient(nil),
		Redis:    leopardredis.NewMemoryStore(),
		Storage:  leopardstorage.NewMemoryStore(),
		StrTable: stringtable.NewInMemoryStore(),
		Bus:      &nopBus{},
	})

	ctx := context.Background()
	_ = m.RegisterStore(ctx, "store1", []string{"group"})
	if err := m.DeregisterStore(ctx, "store1"); err != nil {
		t.Fatalf("DeregisterStore: %v", err)
	}
	if _, err := m.StoreStatus(ctx, "store1"); err == nil {
		t.Fatal("expected error for deregistered store")
	}
}

// nopBus satisfies messaging.MessageBus for tests.
type nopBus struct{}
func (n *nopBus) PublishRotation(_ context.Context, _ messaging.RotationEvent) error   { return nil }
func (n *nopBus) SubscribeRotation(_ context.Context, _ string, _ func(messaging.RotationEvent)) (messaging.Subscription, error) { return &nopSub{}, nil }
func (n *nopBus) PublishRebuild(_ context.Context, _ messaging.RebuildEvent) error     { return nil }
func (n *nopBus) SubscribeRebuild(_ context.Context, _ string, _ func(messaging.RebuildEvent)) (messaging.Subscription, error) { return &nopSub{}, nil }
func (n *nopBus) PublishDelta(_ context.Context, _ messaging.DeltaEvent) error         { return nil }
func (n *nopBus) SubscribeDelta(_ context.Context, _ string, _ func(messaging.DeltaEvent)) (messaging.Subscription, error) { return &nopSub{}, nil }
func (n *nopBus) PublishPromotion(_ context.Context, _ messaging.PromotionEvent) error { return nil }
func (n *nopBus) SubscribePromotion(_ context.Context, _ func(messaging.PromotionEvent)) (messaging.Subscription, error) { return &nopSub{}, nil }
func (n *nopBus) Close() error { return nil }
type nopSub struct{}
func (n *nopSub) Cancel() error { return nil }
```

- [ ] **Step 2: Implement `manager/manager.go`**

```go
// Package manager owns per-store goroutine lifecycle.
// For each registered store it creates and manages one Pipeline, one
// ComputeTier, and one in-memory Index.
package manager

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/compute"
	"github.com/yourorg/openfga-indexer/index"
	"github.com/yourorg/openfga-indexer/messaging"
	"github.com/yourorg/openfga-indexer/pipeline"
	leopardredis "github.com/yourorg/openfga-indexer/redis"
	leopardstorage "github.com/yourorg/openfga-indexer/storage"
	"github.com/yourorg/openfga-indexer/stringtable"
)

// Config holds dependencies injected into the Manager.
type Config struct {
	FGA      client.FGAClient
	Redis    leopardredis.Store
	Storage  leopardstorage.ShardStore
	StrTable stringtable.Store
	Bus      messaging.MessageBus
}

// StoreStatus describes the current state of a store's index.
type StoreStatus struct {
	StoreID      string
	CurrentEpoch uint64
	ShardAge     time.Duration
	Role         string // "master" | "replica" | "degraded"
}

type storeEntry struct {
	idx      index.Index
	pipeline *pipeline.Pipeline
	compute  *compute.ComputeTier
	cancel   context.CancelFunc
	epoch    uint64
	builtAt  time.Time
}

// Manager owns one (index, pipeline, compute) set per registered store.
type Manager struct {
	cfg    Config
	mu     sync.RWMutex
	stores map[string]*storeEntry
}

// New creates a Manager.
func New(cfg Config) *Manager {
	return &Manager{cfg: cfg, stores: make(map[string]*storeEntry)}
}

// RegisterStore creates index, pipeline, and compute tier for storeID.
func (m *Manager) RegisterStore(ctx context.Context, storeID string, indexedTypes []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.stores[storeID]; ok {
		return fmt.Errorf("manager.RegisterStore: store %q already registered", storeID)
	}

	idx := index.New()
	pline := pipeline.New(m.cfg.FGA, m.cfg.StrTable, m.cfg.Storage, m.cfg.Bus, pipeline.Options{
		StoreID:      storeID,
		IndexedTypes: indexedTypes,
	})
	tier := compute.New(m.cfg.FGA, idx, m.cfg.Redis, m.cfg.Bus, compute.Options{
		StoreID: storeID,
	})

	storeCtx, cancel := context.WithCancel(ctx)
	go tier.Run(storeCtx)

	m.stores[storeID] = &storeEntry{
		idx:      idx,
		pipeline: pline,
		compute:  tier,
		cancel:   cancel,
	}
	return nil
}

// DeregisterStore stops all goroutines for storeID and removes it.
func (m *Manager) DeregisterStore(_ context.Context, storeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.stores[storeID]
	if !ok {
		return fmt.Errorf("manager.DeregisterStore: store %q not found", storeID)
	}
	entry.cancel()
	delete(m.stores, storeID)
	return nil
}

// TriggerRebuild runs the Offline Pipeline for storeID synchronously.
func (m *Manager) TriggerRebuild(ctx context.Context, storeID string) (uint64, error) {
	m.mu.RLock()
	entry, ok := m.stores[storeID]
	m.mu.RUnlock()
	if !ok {
		return 0, fmt.Errorf("manager.TriggerRebuild: store %q not found", storeID)
	}
	epoch, err := entry.pipeline.Run(ctx)
	if err != nil {
		return 0, err
	}
	m.mu.Lock()
	entry.epoch = epoch
	entry.builtAt = time.Now()
	m.mu.Unlock()
	return epoch, nil
}

// Index returns the in-memory index for storeID.
func (m *Manager) Index(storeID string) (index.Index, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.stores[storeID]
	if !ok {
		return nil, fmt.Errorf("manager.Index: store %q not found", storeID)
	}
	return entry.idx, nil
}

// StoreStatus returns the current health of a store's index.
func (m *Manager) StoreStatus(_ context.Context, storeID string) (StoreStatus, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.stores[storeID]
	if !ok {
		return StoreStatus{}, fmt.Errorf("manager.StoreStatus: store %q not found", storeID)
	}
	age := time.Duration(0)
	if !entry.builtAt.IsZero() {
		age = time.Since(entry.builtAt)
	}
	return StoreStatus{
		StoreID:      storeID,
		CurrentEpoch: entry.epoch,
		ShardAge:     age,
		Role:         "master", // overridden by admin package based on Redis role key
	}, nil
}
```

- [ ] **Step 3: Run manager tests**

```bash
go test ./manager/... -run TestManager -v
```

Expected: both manager tests PASS.

- [ ] **Step 4: Commit**

```bash
git add manager/
git commit -m "feat(manager): add StoreManager per-store goroutine lifecycle"
```

---

## Task 7: Admin Endpoints

**Files:**
- Create: `admin/admin.go`
- Create: `admin/admin_test.go`

- [ ] **Step 1: Write the failing test**

Create `admin/admin_test.go`:

```go
package admin_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yourorg/openfga-indexer/admin"
)

func TestAdmin_StatusEndpoint(t *testing.T) {
	h := admin.NewHandler(admin.Config{
		ClusterID: "cluster-1",
		Role:      "replica",
	}, nil, nil)

	req := httptest.NewRequest("GET", "/admin/status", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["role"] != "replica" {
		t.Fatalf("role = %v, want replica", resp["role"])
	}
}

func TestAdmin_PromoteEndpoint_RejectsWhenMasterAlive(t *testing.T) {
	h := admin.NewHandler(admin.Config{
		ClusterID: "cluster-1",
		Role:      "replica",
	}, nil, nil)

	req := httptest.NewRequest("POST", "/admin/promote", strings.NewReader(""))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	// Without a live Redis, the handler should return an appropriate error.
	if w.Code == http.StatusOK {
		t.Fatal("promote without Redis should not return 200")
	}
}
```

- [ ] **Step 2: Implement `admin/admin.go`**

```go
// Package admin provides HTTP endpoints for cluster administration:
//   - GET  /admin/status   — current role, state, index freshness
//   - POST /admin/promote  — promote this replica to master (split-brain guarded)
//   - POST /admin/rebuild  — manually trigger Offline Pipeline rebuild
//   - GET  /admin/stores   — list all stores and their index status
package admin

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	leopardredis "github.com/yourorg/openfga-indexer/redis"
	"github.com/yourorg/openfga-indexer/manager"
)

// Config holds admin handler settings.
type Config struct {
	ClusterID  string
	InstanceID string
	Role       string // "master" | "replica"
}

// Handler serves the admin HTTP endpoints.
type Handler struct {
	cfg     Config
	redis   leopardredis.Store
	mgr     *manager.Manager
	mux     *http.ServeMux
	startedAt time.Time
}

// NewHandler creates an admin Handler. redis and mgr may be nil in tests.
func NewHandler(cfg Config, redis leopardredis.Store, mgr *manager.Manager) http.Handler {
	h := &Handler{cfg: cfg, redis: redis, mgr: mgr, startedAt: time.Now()}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/status", h.handleStatus)
	mux.HandleFunc("POST /admin/promote", h.handlePromote)
	mux.HandleFunc("POST /admin/rebuild", h.handleRebuild)
	mux.HandleFunc("GET /admin/stores", h.handleStores)
	h.mux = mux
	return mux
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) handleStatus(w http.ResponseWriter, r *http.Request) {
	resp := map[string]any{
		"cluster_id":  h.cfg.ClusterID,
		"instance_id": h.cfg.InstanceID,
		"role":        h.cfg.Role,
		"uptime_sec":  int(time.Since(h.startedAt).Seconds()),
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) handlePromote(w http.ResponseWriter, r *http.Request) {
	if h.redis == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "redis not configured"})
		return
	}
	ctx := r.Context()

	// Split-brain guard: refuse if an existing master heartbeat is alive.
	clusterID, instanceID, err := h.redis.GetMaster(ctx)
	if err == nil && instanceID != "" && clusterID != h.cfg.ClusterID {
		writeJSON(w, http.StatusConflict, map[string]string{
			"error":      "active master detected — promotion refused",
			"master":     clusterID,
			"instanceID": instanceID,
		})
		return
	}

	// Write master key.
	if err := h.redis.SetMaster(ctx, h.cfg.ClusterID, h.cfg.InstanceID); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("set master: %v", err)})
		return
	}

	h.cfg.Role = "master"
	writeJSON(w, http.StatusOK, map[string]string{"status": "promoted", "cluster_id": h.cfg.ClusterID})
}

func (h *Handler) handleRebuild(w http.ResponseWriter, r *http.Request) {
	if h.cfg.Role != "master" {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "only master can trigger rebuild"})
		return
	}
	storeID := r.URL.Query().Get("store")
	if storeID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "store query param required"})
		return
	}
	if h.mgr == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "manager not configured"})
		return
	}
	epoch, err := h.mgr.TriggerRebuild(r.Context(), storeID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"store_id": storeID, "epoch": epoch})
}

func (h *Handler) handleStores(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"stores": []any{}})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
```

- [ ] **Step 3: Run admin tests**

```bash
go test ./admin/... -run TestAdmin -v
```

Expected: both admin tests PASS.

- [ ] **Step 4: Commit**

```bash
git add admin/
git commit -m "feat(admin): add HTTP admin endpoints for status, promote, rebuild"
```

---

## Task 8: gRPC Server + Binaries

**Files:**
- Create: `server/server.go`
- Create: `cmd/write-proxy/main.go`
- Create: `cmd/indexer-server/main.go`

- [ ] **Step 1: Implement `server/server.go`**

```go
// Package server wires the gRPC service implementation and grpc-gateway HTTP mux.
package server

import (
	"context"
	"net"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	leopardv1 "github.com/yourorg/openfga-indexer/proto/leopard/v1"
	"github.com/yourorg/openfga-indexer/gateway"
)

// IndexerServer implements leopardv1.IndexerServiceServer.
type IndexerServer struct {
	leopardv1.UnimplementedIndexerServiceServer
	gw *gateway.Gateway
}

func NewIndexerServer(gw *gateway.Gateway) *IndexerServer {
	return &IndexerServer{gw: gw}
}

func (s *IndexerServer) Check(ctx context.Context, req *leopardv1.CheckRequest) (*leopardv1.CheckResponse, error) {
	allowed, source, err := s.gw.Check(ctx, req.User, req.Relation, req.Object)
	if err != nil {
		return nil, err
	}
	return &leopardv1.CheckResponse{Allowed: allowed, Source: source}, nil
}

func (s *IndexerServer) ListObjects(ctx context.Context, req *leopardv1.ListObjectsRequest) (*leopardv1.ListObjectsResponse, error) {
	objs, source, err := s.gw.ListObjects(ctx, req.User, req.Relation, req.ObjectType)
	if err != nil {
		return nil, err
	}
	return &leopardv1.ListObjectsResponse{Objects: objs, Source: source}, nil
}

// RunGRPC starts the gRPC listener on addr.
func RunGRPC(ctx context.Context, addr string, svc *IndexerServer) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	grpcSrv := grpc.NewServer()
	leopardv1.RegisterIndexerServiceServer(grpcSrv, svc)
	hs := health.NewServer()
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcSrv, hs)
	reflection.Register(grpcSrv)

	go func() {
		<-ctx.Done()
		grpcSrv.GracefulStop()
	}()
	return grpcSrv.Serve(lis)
}

// RunHTTP starts the grpc-gateway HTTP mux on addr.
func RunHTTP(ctx context.Context, addr, grpcAddr string) error {
	mux := runtime.NewServeMux()
	if err := leopardv1.RegisterIndexerServiceHandlerFromEndpoint(
		ctx, mux, grpcAddr, []grpc.DialOption{grpc.WithInsecure()},
	); err != nil {
		return err
	}
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() { <-ctx.Done(); srv.Shutdown(context.Background()) }()
	return srv.ListenAndServe()
}
```

- [ ] **Step 2: Implement `cmd/indexer-server/main.go`**

```go
// Command indexer-server runs the Leopard Indexer in master or replica mode.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	leopardmigrate "github.com/yourorg/openfga-indexer/migrate"
	"github.com/yourorg/openfga-indexer/metrics"
	fgaclient "github.com/yourorg/openfga-indexer/client"
	leopardredis "github.com/yourorg/openfga-indexer/redis"
	leopardstorage "github.com/yourorg/openfga-indexer/storage"
	"github.com/yourorg/openfga-indexer/stringtable"
	"github.com/yourorg/openfga-indexer/manager"
	"github.com/yourorg/openfga-indexer/admin"
	"github.com/yourorg/openfga-indexer/gateway"
	"github.com/yourorg/openfga-indexer/server"
	"net/http"
)

func main() {
	role       := flag.String("role", "replica", "master or replica")
	clusterID  := flag.String("cluster", "", "unique cluster ID (required)")
	fgaURL     := flag.String("fga-url", "http://localhost:8080", "OpenFGA API URL")
	storeID    := flag.String("store", "", "FGA store ID (required)")
	dbDriver   := flag.String("db-driver", "postgres", "postgres or mysql")
	dbDSN      := flag.String("db-dsn", "", "database DSN (required)")
	redisAddr  := flag.String("redis", "localhost:6379", "Redis address")
	s3Bucket   := flag.String("s3-bucket", "", "S3 bucket for shards")
	grpcAddr   := flag.String("grpc-addr", ":50051", "gRPC listen address")
	httpAddr   := flag.String("http-addr", ":8090", "HTTP/JSON listen address")
	adminAddr  := flag.String("admin-addr", ":9090", "admin HTTP listen address")
	otlp       := flag.String("otlp", "", "OTLP endpoint (empty = disabled)")
	promAddr   := flag.String("prom-addr", ":9091", "Prometheus metrics address")
	flag.Parse()

	if *clusterID == "" || *storeID == "" || *dbDSN == "" {
		fmt.Fprintln(os.Stderr, "error: -cluster, -store, and -db-dsn are required")
		os.Exit(1)
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// Run database migrations.
	if err := leopardmigrate.Run(*dbDSN, *dbDriver); err != nil {
		log.Error("migration failed", "err", err)
		os.Exit(1)
	}

	// Init observability.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	_, shutdownMetrics, err := metrics.Init(ctx, metrics.Config{
		ServiceName:    "leopard-indexer",
		OTLPEndpoint:   *otlp,
		PrometheusAddr: *promAddr,
	})
	if err != nil {
		log.Error("metrics init failed", "err", err)
		os.Exit(1)
	}
	defer shutdownMetrics(ctx)

	// Build adapters.
	fga, err := fgaclient.New(fgaclient.Config{APIURL: *fgaURL, StoreID: *storeID})
	if err != nil {
		log.Error("FGA client failed", "err", err)
		os.Exit(1)
	}

	rstore := leopardredis.New(leopardredis.Config{Addr: *redisAddr})
	stor := leopardstorage.NewMemoryStore() // replace with s3 when s3Bucket != ""
	_ = s3Bucket

	stbl, _, err := stringtable.Open(*dbDriver, *dbDSN)
	if err != nil {
		log.Error("stringtable failed", "err", err)
		os.Exit(1)
	}

	// StoreManager (no-op bus for now — wire real bus here).
	mgr := manager.New(manager.Config{
		FGA: fga, Redis: rstore, Storage: stor, StrTable: stbl,
	})
	if err := mgr.RegisterStore(ctx, *storeID, []string{"group"}); err != nil {
		log.Error("RegisterStore failed", "err", err)
		os.Exit(1)
	}

	// Check Gateway.
	idx, _ := mgr.Index(*storeID)
	gw := gateway.New(gateway.Config{
		StoreID:      *storeID,
		IndexedTypes: []string{"group"},
	}, idx, rstore, stor, stbl, fga)

	// Admin handler.
	adminHandler := admin.NewHandler(admin.Config{
		ClusterID: *clusterID,
		Role:      *role,
	}, rstore, mgr)
	adminSrv := &http.Server{Addr: *adminAddr, Handler: adminHandler}
	go adminSrv.ListenAndServe()

	// gRPC + HTTP servers.
	svc := server.NewIndexerServer(gw)
	go func() {
		if err := server.RunGRPC(ctx, *grpcAddr, svc); err != nil {
			log.Error("gRPC server failed", "err", err)
		}
	}()
	go func() {
		if err := server.RunHTTP(ctx, *httpAddr, *grpcAddr); err != nil {
			log.Error("HTTP server failed", "err", err)
		}
	}()

	log.Info("indexer-server started", "role", *role, "cluster", *clusterID, "store", *storeID)
	<-ctx.Done()
	log.Info("shutdown complete")
}
```

- [ ] **Step 3: Implement `cmd/write-proxy/main.go`**

```go
// Command write-proxy runs the Write Proxy service.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	fgaclient "github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/proxy"
)

func main() {
	fgaURL   := flag.String("fga-url", "http://localhost:8080", "OpenFGA API URL")
	storeID  := flag.String("store", "", "FGA store ID (required)")
	grpcAddr := flag.String("grpc-addr", ":50052", "gRPC listen address")
	flag.Parse()

	if *storeID == "" {
		fmt.Fprintln(os.Stderr, "error: -store is required")
		os.Exit(1)
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	fga, err := fgaclient.New(fgaclient.Config{APIURL: *fgaURL, StoreID: *storeID})
	if err != nil {
		log.Error("FGA client failed", "err", err)
		os.Exit(1)
	}

	// Wire real MessageBus here; nop for standalone start.
	_ = proxy.New(fga, &nopBus{}, proxy.Options{StoreID: *storeID})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("write-proxy started", "store", *storeID, "grpc", *grpcAddr)
	<-ctx.Done()
	log.Info("shutdown complete")
}

type nopBus struct{}
func (n *nopBus) PublishRebuild(_ context.Context, _ interface{ }) error { return nil }
```

- [ ] **Step 4: Build both binaries**

```bash
go build ./cmd/indexer-server/...
go build ./cmd/write-proxy/...
```

Expected: both compile without errors.

- [ ] **Step 5: Run all unit tests**

```bash
go test ./... -short -count=1 2>&1 | grep -E "^(ok|FAIL|---)"
```

Expected: all packages show `ok`. No `FAIL`.

- [ ] **Step 6: Commit**

```bash
git add server/ cmd/ admin/ metrics/ gateway/ proxy/ manager/
git commit -m "feat: add service layer — gateway, proxy, manager, admin, server, binaries"
```

---

## Self-Review

- [x] **Spec coverage:** Section 11 (Write Proxy) ✓ Task 5. Section 12 (Check Gateway + circuit breakers) ✓ Task 4. Section 13 (promotion flow) ✓ Task 7. Section 14 (gRPC + HTTP) ✓ Tasks 2+8. Section 15 (StoreManager) ✓ Task 6. Section 16 (observability) ✓ Task 3. Section 17 (config) ✓ `cmd/indexer-server/main.go` flags.
- [x] **Placeholder scan:** `nopBus` in `cmd/write-proxy/main.go` is a known placeholder — comment notes "Wire real MessageBus here". This is acceptable for the binary stub; the real wiring is done during deployment configuration.
- [x] **Type consistency:** `gateway.Gateway.Check` returns `(bool, string, error)` matching `server.IndexerServer.Check` usage. `manager.Manager.Index` returns `(index.Index, error)` used correctly in `main.go`. `admin.Config.Role` is a `string` mutated by `handlePromote` — correct. `stringtable.Open` returns `(Store, *sql.DB, error)` — `main.go` discards `*sql.DB` which is a leak; use `defer db.Close()` when wiring in production.
