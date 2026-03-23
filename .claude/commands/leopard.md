# /leopard — Leopard Indexer Development Skill

Use this skill to scaffold, extend, or debug components of the openfga-indexer project.

## What this skill does

When invoked, review the current state of the codebase and take the next logical development step based on the argument provided:

- `/leopard index`   — implement or extend `index/` (GROUP2GROUP / MEMBER2GROUP maps, sortedSet, set intersection)
- `/leopard cache`   — implement or extend `cache/` (atomic shard store, watch-token channel)
- `/leopard builder` — implement or extend `builder/` (periodic offline full-scan shard construction)
- `/leopard watcher` — implement or extend `watcher/` (ReadChanges polling, delta application, re-anchor on rotation)
- `/leopard client`  — implement or extend `client/` (FGA SDK wrapper, ReadTuples, ReadChanges, MockClient)
- `/leopard check`   — implement or extend `check/` (index-first Check + ListObjects with FGA fallback)
- `/leopard cmd`     — implement or extend `cmd/` (main.go wiring all three layers)
- `/leopard test`    — write or fix tests for the package named in the argument
- `/leopard status`  — summarize what exists vs what is missing; suggest next step
- `/leopard wire`    — verify all three layers wire together correctly end-to-end

If no argument is given, run `/leopard status` automatically.

## Three-layer architecture

```
Builder (builder/)          Watcher (watcher/)        Checker (check/)
  │                           │                          │
  │ periodic full scan        │ ReadChanges deltas        │ index-first reads
  │ → new shard               │ → ApplyTupleWrite/Delete  │ → FGA fallback
  └──── Rotate() ────────────►│                          │
                    Cache (cache/)                        │
                    ─────────────────────────────────────►│
                    atomic shard pointer
                    + watch-token channel
                              ▼
                    Index (index/)
                    GROUP2GROUP + MEMBER2GROUP
```

**Invariant:** `IsMember(U, G)` = `MEMBER2GROUP[U] ∩ GROUP2GROUP[G] ≠ ∅`

## Development rules to follow

1. **Read before writing**: always read existing files in the target package before creating or editing.
2. **Interface first**: define the interface in `interface.go` before implementing it.
3. **Index package is pure**: `index/` has zero knowledge of OpenFGA — only string IDs and sets.
4. **Cache is the only mutable shared state**: builder calls `Rotate`, watcher calls `cache.Index()` and mutates the returned `index.Index`. No other package holds a direct `index.Index` reference.
5. **Builder sets the watch token**: after every `Rotate`, call `cache.SetWatchToken(token)` with the `ReadChanges` high-water mark at snapshot time.
6. **Watcher re-anchors on token signal**: watch `cache.WatchToken()` channel; on close, call `cache.WatchToken()` again to get the new token and resume from there.
7. **Check routes by relation + object type**: `member` + `group:*` → index; everything else → FGA API.
8. **No global state**: all types constructed via `New*` constructors with explicit dependencies.
9. **Error wrapping**: `fmt.Errorf("packagename.FuncName: %w", err)` — never discard errors.
10. **Context propagation**: every exported function takes `context.Context` as the first argument.
11. **Test alongside implementation**: for every `foo.go`, create `foo_test.go` with at least one test.
12. **Integration tests**: tag with `//go:build integration` and guard with `testing.Short()` skip.

## Key types (current implementation)

```go
// index package
type Index interface {
    ApplyTupleWrite(user, relation, object string)
    ApplyTupleDelete(user, relation, object string)
    IsMember(userID, groupID string) bool
    GroupsForUser(userID string) []string
    DescendantGroups(groupID string) []string
    Snapshot() Snapshot
}

// cache package
type Cache struct { /* atomic shard pointer + watch-token mutex */ }
func (c *Cache) Rotate(newShard index.Index)
func (c *Cache) Index() index.Index
func (c *Cache) SetWatchToken(token string)
func (c *Cache) WatchToken() (token string, ready <-chan struct{})

// builder package
type Builder struct { /* fga, cache, opts */ }
func (b *Builder) RunOnce(ctx context.Context) (continuationToken string, err error)
func (b *Builder) Run(ctx context.Context) error   // periodic; blocks until ctx cancel

// watcher package
type Watcher struct { /* client, cache, opts */ }
func (w *Watcher) Run(ctx context.Context) error   // blocking; re-anchors on rotation

// client package
type FGAClient interface {
    Check(ctx context.Context, req CheckRequest) (bool, error)
    ListObjects(ctx context.Context, req ListObjectsRequest) ([]string, error)
    ReadTuples(ctx context.Context, storeID string) ([]Tuple, error)
    ReadChanges(ctx context.Context, storeID, continuationToken string) ([]TupleChange, string, error)
}

// check package
type Checker struct { /* cache, fga, storeID, modelID */ }
func (c *Checker) Check(ctx context.Context, user, relation, object string) (bool, error)
func (c *Checker) ListObjects(ctx context.Context, user, relation, objectType string) ([]string, error)
```

## Leopard algorithm reference

```go
// Membership test — O(1) amortized (two map lookups + sorted-slice intersection)
func (idx *leopardIndex) IsMember(userID, groupID string) bool {
    memberGroups  := idx.memberToGroup[userID]   // MEMBER2GROUP(U)
    descendants   := idx.groupToGroup[groupID]   // GROUP2GROUP(G) — includes G itself
    return intersects(memberGroups, descendants)
}

// Sorted-slice set intersection — O(m+n)
func intersects(a, b sortedSet) bool {
    i, j := 0, 0
    for i < len(a) && j < len(b) {
        switch { case a[i] == b[j]: return true
                 case a[i] < b[j]:  i++
                 default:           j++ }
    }
    return false
}
```

## Go module setup

If `go.mod` does not exist yet:
```
go mod init github.com/yourorg/openfga-indexer
go get github.com/openfga/go-sdk@latest
```

## Execution instructions

1. Read `CLAUDE.md` for the full architecture and layer descriptions.
2. Run `Glob` to understand what already exists in the target package.
3. Identify the gap for the requested component.
4. Implement: interface file → implementation → tests.
5. Run `go build ./...` then `go test ./... -timeout 15s` to verify.
6. Report what was done and suggest the logical next step.
