# openfga-indexer

A Go library implementing **Leopard-style indexing** on top of OpenFGA, inspired by the Zanzibar paper's Leopard subsystem. It pre-computes transitive group membership to enable O(1) authorization checks without recursive graph traversal.

## Project Goal

Wrap the OpenFGA Go SDK to add a local Leopard index that:
- Caches `GROUP2GROUP` and `MEMBER2GROUP` posting lists
- Resolves nested group membership via set intersection instead of recursive API calls
- Stays current via a three-layer pipeline: periodic offline builds, incremental watcher, and atomic shard rotation
- Exposes a thin `Check` / `ListObjects` interface backed by either the index or the raw FGA API

## Architecture

The system is composed of three cooperating layers, matching the Zanzibar Leopard design:

```
┌──────────────────────────────────────────────────────────────────┐
│                     cmd/indexer/main.go                          │
└──────┬────────────────────┬──────────────────────┬──────────────┘
       │                    │                      │
┌──────▼──────┐   ┌─────────▼────────┐   ┌────────▼──────────────┐
│  Builder    │   │  Watcher         │   │  Checker (public API)  │
│  builder/   │   │  watcher/        │   │  check/                │
│             │   │                  │   │                        │
│ Full scan   │   │ ReadChanges poll │   │ IsMember → index       │
│ (periodic)  │   │ Apply deltas     │   │ other   → FGA fallback │
│ ──Rotate──► │   │ Re-anchor on     │   └────────────────────────┘
└─────────────┘   │ token signal     │
        ▼         └────────┬─────────┘
  ┌─────▼───────────────────▼──────┐
  │           Cache  cache/        │
  │  atomic shard pointer          │
  │  + watch-token channel         │
  └───────────────┬────────────────┘
                  │
          ┌───────▼────────┐
          │  Index  index/ │
          │  GROUP2GROUP   │
          │  MEMBER2GROUP  │
          └────────────────┘
```

### Layer 1 — Offline Builder (`builder/`)

Runs on a configurable schedule (default: 1 hour).

1. Calls `client.ReadTuples` to fetch all current tuples (full snapshot).
2. Builds a brand-new `index.Index` shard from scratch.
3. Calls `cache.Rotate` to atomically swap the shard in.
4. Calls `cache.SetWatchToken` with the `ReadChanges` high-water mark so the watcher re-anchors exactly at the snapshot boundary.

### Layer 2 — Watcher (`watcher/`)

Runs continuously in its own goroutine.

- Polls `client.ReadChanges` and applies `ApplyTupleWrite` / `ApplyTupleDelete` deltas to `cache.Index()`.
- Listens on `cache.WatchToken()` channel; when the builder publishes a new token after rotation, the watcher re-anchors to that position to avoid double-applying or missing deltas.

### Layer 3 — Cache / Shard Store (`cache/`)

The central serving layer.

- Holds the active `index.Index` behind an atomic pointer (`sync/atomic`) for lock-free reads.
- `Rotate(newShard)` — atomic swap; safe to call from the builder goroutine while the watcher reads.
- `SetWatchToken(token)` / `WatchToken()` — mutex-protected; closing a channel signals the watcher without a lock in the hot path.

### Key Data Structures

| Name | Type | Semantics |
|------|------|-----------|
| `GROUP2GROUP` | `map[groupID]sortedSet` | ancestor group → set of all descendant groups (transitive, reflexive) |
| `MEMBER2GROUP` | `map[userID]sortedSet` | user → sorted set of direct parent groups |

**Membership test:** `user U ∈ group G` iff `MEMBER2GROUP(U) ∩ GROUP2GROUP(G) ≠ ∅`

`sortedSet` is a sorted `[]string` slice enabling O(m+n) set intersection and O(log n) contains.

### Core Packages

```
openfga-indexer/
├── index/          # Index interface + leopardIndex (GROUP2GROUP + MEMBER2GROUP + sortedSet)
├── cache/          # Cache — atomic shard store + watch-token signalling
├── builder/        # Builder — periodic full-scan shard construction
├── watcher/        # Watcher — ReadChanges incremental delta application
├── client/         # FGAClient interface + OpenFGA Go SDK wrapper + MockClient
├── check/          # Checker — index-first Check/ListObjects, FGA fallback
└── cmd/indexer/    # Example binary wiring all three layers
```

## OpenFGA Concepts Used

- **Tuples**: `(user, relation, object)` — source of truth stored in FGA
- **Read**: full tuple scan used by the offline builder for snapshot construction
- **ReadChanges**: streaming changelog used by the watcher for incremental updates
- **Check**: direct API call for relations not covered by the index
- **ListObjects**: used for non-group object queries from the Checker

## Go Conventions

- Go 1.22+, module path `github.com/yourorg/openfga-indexer`
- `context.Context` as first argument on every exported function
- Errors wrapped with `fmt.Errorf("package.Func: %w", err)` — no sentinel errors unless tested
- Interfaces kept small (1–3 methods); concrete types unexported where possible
- Concurrency: `sync.RWMutex` guards index maps; `sync/atomic` for the cache shard pointer; builder and watcher run in separate goroutines
- No global state; everything injected via `New*` constructors
- Tests: table-driven with `t.Run`; integration tests require a live FGA instance and are guarded by `testing.Short()`

## Dependencies

```
github.com/openfga/go-sdk     # FGA client (v0.6.4+)
```

## Development Workflow

1. Start a local FGA: `docker run -p 8080:8080 openfga/openfga run`
2. `go test ./...` — all unit tests run offline using `client.MockClient`
3. `go vet ./... && staticcheck ./...` before committing
4. Integration tests: set `FGA_API_URL=http://localhost:8080` and run without `-short`

## Testing Strategy

- **Unit**: `client.MockClient` for all FGA interactions; each package tested independently
- **Integration**: real FGA container via `TestMain` / `dockertest` (tag: `//go:build integration`)
- **Benchmark**: `BenchmarkIsMember` in `index/` measures set-intersection throughput

## Glossary

| Term | Meaning |
|------|---------|
| Leopard | Zanzibar subsystem that pre-computes group membership indexes |
| Posting list | Sorted array of IDs associated with a key, used for fast intersection |
| Shard | A complete snapshot of `GROUP2GROUP` + `MEMBER2GROUP` built by the offline builder |
| Shard rotation | Atomic swap of the active shard in the cache after an offline build |
| Continuation token | Opaque token from FGA used to resume `ReadChanges` from a known position |
| Watch token | The continuation token the builder records at snapshot time; handed to the watcher after rotation |
