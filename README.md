# openfga-indexer

A Go library that adds **Leopard-style group membership indexing** on top of [OpenFGA](https://openfga.dev), inspired by the [Zanzibar paper](https://research.google/pubs/zanzibar-googles-consistent-global-authorization-system/). It pre-computes transitive group membership so authorization checks avoid recursive API traversal and run in O(1) time.

## Why

OpenFGA resolves group membership by recursively walking relationship tuples. For deeply nested groups this means multiple serial API round-trips per `Check` call. Leopard eliminates this by maintaining two pre-computed maps:

| Map | Semantics |
|-----|-----------|
| `MEMBER2GROUP[u]` | sorted set of groups that directly contain user `u` |
| `GROUP2GROUP[g]` | sorted set of all groups transitively descended from group `g` (including `g` itself) |

**Membership test:** user `U` belongs to group `G` iff

```
MEMBER2GROUP[U] ∩ GROUP2GROUP[G] ≠ ∅
```

This is a single in-memory sorted-slice intersection — no network, no recursion.

## Architecture

The system runs as three cooperating layers:

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│  Builder        │    │  Watcher         │    │  Checker            │
│  (periodic)     │    │  (continuous)    │    │  (per-request)      │
│                 │    │                  │    │                     │
│ Full tuple scan │    │ ReadChanges poll │    │ member + group:*    │
│ → new shard     │    │ → apply deltas   │    │   → index (no I/O)  │
│ → Rotate()      │    │ → re-anchor on   │    │ everything else     │
│ → SetWatchToken │    │   token signal   │    │   → FGA API         │
└────────┬────────┘    └────────┬─────────┘    └──────────┬──────────┘
         │                      │                          │
         └──────────────────────▼──────────────────────────┘
                           Cache (shard store)
                      atomic pointer swap on Rotate
                      watch-token channel for re-anchor
                                 │
                          Index (in-memory)
                       GROUP2GROUP + MEMBER2GROUP
```

### Layer 1 — Offline Builder

Runs on a configurable schedule (default 1 hour). Reads **all** tuples from FGA via `Read`, builds a fresh `Index` shard, atomically swaps it into the cache via `Rotate`, then publishes the `ReadChanges` high-water mark so the watcher re-anchors to the exact snapshot boundary.

### Layer 2 — Watcher

Runs continuously. Tails `ReadChanges` and applies `ApplyTupleWrite` / `ApplyTupleDelete` deltas to the live shard. After a builder rotation, the watcher detects the new watch token on a closed channel and resumes from that position — no deltas are skipped or double-applied.

### Layer 3 — Cache

Holds the active shard behind a `sync/atomic` pointer for lock-free concurrent reads. The builder swaps shards atomically; the watcher mutates the shard in place between rotations.

## Package Layout

```
openfga-indexer/
├── index/          # Index interface + leopardIndex (sortedSet, GROUP2GROUP, MEMBER2GROUP)
├── cache/          # Cache — atomic shard pointer + watch-token signalling
├── builder/        # Builder — periodic full-scan shard construction
├── watcher/        # Watcher — ReadChanges incremental delta application
├── client/         # FGAClient interface, OpenFGA Go SDK wrapper, MockClient
├── check/          # Checker — index-first Check/ListObjects, FGA fallback
└── cmd/indexer/    # Example binary wiring all three layers
```

## Quick Start

### Prerequisites

- Go 1.22+
- A running OpenFGA instance: `docker run -p 8080:8080 openfga/openfga run`

### Run the example binary

```bash
go run ./cmd/indexer -api http://localhost:8080 -store <your-store-id>
```

Flags:

| Flag | Default | Description |
|------|---------|-------------|
| `-api` | `http://localhost:8080` | OpenFGA API URL |
| `-store` | *(required)* | OpenFGA store ID |
| `-rebuild` | `1h` | Offline builder interval |
| `-poll` | `5s` | Watcher poll interval |

### Use as a library

```go
import (
    "github.com/yourorg/openfga-indexer/builder"
    "github.com/yourorg/openfga-indexer/cache"
    "github.com/yourorg/openfga-indexer/check"
    fgaclient "github.com/yourorg/openfga-indexer/client"
    "github.com/yourorg/openfga-indexer/watcher"
)

// 1. Create FGA client.
c, _ := fgaclient.New(fgaclient.Config{APIURL: "http://localhost:8080", StoreID: storeID})

// 2. Create cache.
ca := cache.New()

// 3. Initial offline build.
b := builder.New(c, ca, builder.Options{StoreID: storeID})
token, _ := b.RunOnce(ctx)
ca.SetWatchToken(token)

// 4. Start watcher for incremental updates.
w := watcher.New(c, ca, watcher.Options{StoreID: storeID})
go w.Run(ctx)

// 5. Start periodic rebuild.
go b.Run(ctx)

// 6. Answer authorization checks.
checker := check.New(ca, c, storeID, "")
allowed, _ := checker.Check(ctx, "user:alice", "member", "group:engineering")
```

## Testing

Unit tests use `client.MockClient` — no FGA instance required:

```bash
go test ./...
```

Integration tests (require `FGA_API_URL`):

```bash
FGA_API_URL=http://localhost:8080 go test -tags integration ./...
```

Benchmark (set-intersection throughput):

```bash
go test ./index/... -bench=. -benchmem
```

## How the Leopard Algorithm Works

```
Write:  user:alice  member  group:backend
         → MEMBER2GROUP["user:alice"].add("group:backend")
         → GROUP2GROUP["group:backend"].add("group:backend")  // reflexive

Write:  group:backend  member  group:engineering
         → GROUP2GROUP["group:engineering"].add("group:backend")
         → transitive closure propagated to all ancestors of group:engineering

Check:  IsMember("user:alice", "group:engineering")
         → MEMBER2GROUP["user:alice"]  = {"group:backend"}
         → GROUP2GROUP["group:engineering"] = {"group:backend", "group:engineering"}
         → intersection non-empty → true  ✓  (zero FGA API calls)
```

## References

- [Google Zanzibar paper (USENIX ATC 2019)](https://research.google/pubs/zanzibar-googles-consistent-global-authorization-system/)
- [OpenFGA documentation](https://openfga.dev/docs)
- [OpenFGA Go SDK](https://github.com/openfga/go-sdk)
