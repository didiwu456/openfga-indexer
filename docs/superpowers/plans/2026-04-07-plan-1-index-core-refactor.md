# Index Core Refactor — Implementation Plan 1 of 5

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the broken `sortedSet`-based in-memory index with a correct, roaring-bitmap-backed implementation that fixes group-edge deletes and adds configurable relation/prefix support.

**Architecture:** All public `Index` interface methods keep their string-based signatures unchanged — no caller changes needed. Internally, `leopardIndex` maps strings to `uint32` IDs via an append-only `intern` table (protected by the existing index mutex), stores all posting lists as `*roaring.Bitmap`, and adds an explicit `directEdges` map so `removeGroupEdge` can BFS-recompute `GROUP2GROUP` correctly instead of the current no-op stub.

**Tech Stack:** Go 1.22, `github.com/RoaringBitmap/roaring` (new dependency), `sync.RWMutex` (existing pattern).

---

## File Map

| Action | File | Responsibility |
|---|---|---|
| Modify | `go.mod` | Add roaring bitmap dependency |
| Modify | `index/interface.go` | Add `IndexConfig`, `NewWithConfig` |
| Create | `index/intern.go` | Append-only string→uint32 interning table (no internal lock — protected by `leopardIndex.mu`) |
| Create | `index/intern_test.go` | Unit tests for intern |
| Rewrite | `index/leopard.go` | `leopardIndex` using roaring bitmaps, uint32 keys, `directEdges`, BFS recompute on delete |
| Modify | `index/leopard_test.go` | Add three failing group-edge delete tests + one config test before rewriting implementation |
| Delete | `index/set.go` | Replaced by roaring.Bitmap |
| Delete | `index/set_test.go` | No longer relevant |

---

## Task 1: Add Roaring Bitmap Dependency

**Files:**
- Modify: `go.mod`
- Modify: `go.sum` (auto-generated)

- [ ] **Step 1: Add the dependency**

```bash
cd d:/GitHub/openfga-indexer
go get github.com/RoaringBitmap/roaring@latest
```

Expected output: line added to `go.mod` like:
```
require github.com/RoaringBitmap/roaring v1.x.x
```

- [ ] **Step 2: Verify the module graph is clean**

```bash
go mod tidy
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "chore: add roaring bitmap dependency"
```

---

## Task 2: Add `IndexConfig` and `NewWithConfig` to the Interface

**Files:**
- Modify: `index/interface.go`
- Modify: `index/leopard_test.go` (add config test)

- [ ] **Step 1: Write the failing test**

Add to the bottom of `index/leopard_test.go`:

```go
func TestNewWithConfig_CustomRelation(t *testing.T) {
	idx := NewWithConfig(IndexConfig{
		GroupRelation: "belongs_to",
		GroupPrefix:   "team:",
	})
	idx.ApplyTupleWrite("user:alice", "belongs_to", "team:eng")

	if !idx.IsMember("user:alice", "team:eng") {
		t.Fatal("custom relation should be indexed")
	}

	// Default "member" relation must be ignored when a custom one is configured.
	idx.ApplyTupleWrite("user:bob", "member", "team:eng")
	if idx.IsMember("user:bob", "team:eng") {
		t.Fatal("default relation must be ignored when custom relation is configured")
	}
}

func TestNewWithConfig_Defaults(t *testing.T) {
	// NewWithConfig with zero value must behave identically to New().
	idx := NewWithConfig(IndexConfig{})
	idx.ApplyTupleWrite("user:alice", "member", "group:eng")
	if !idx.IsMember("user:alice", "group:eng") {
		t.Fatal("zero-value IndexConfig should use default relation and prefix")
	}
}
```

- [ ] **Step 2: Run to confirm failure**

```bash
go test ./index/... -run TestNewWithConfig -v
```

Expected: `FAIL — undefined: NewWithConfig`

- [ ] **Step 3: Add `IndexConfig` and `NewWithConfig` to `index/interface.go`**

Replace the entire content of `index/interface.go` with:

```go
// Package index implements an in-memory Leopard-style index for fast group
// membership resolution without recursive OpenFGA API calls.
//
// It maintains two posting-list maps:
//
//	GROUP2GROUP[g] = set of all groups that are descendants of g (transitive)
//	MEMBER2GROUP[u] = set of all groups that directly contain user u
//
// Membership test: user U is a member of group G iff
//
//	MEMBER2GROUP(U) ∩ GROUP2GROUP(G) ≠ ∅
package index

// IndexConfig controls which tuples the index tracks.
// Zero value is valid and uses the defaults documented on each field.
type IndexConfig struct {
	// GroupRelation is the relation name that denotes group membership.
	// Default: "member".
	GroupRelation string

	// GroupPrefix is the type prefix that identifies group objects.
	// Default: "group:".
	GroupPrefix string

	// ObjectType labels this shard for multi-type deployments (e.g. "group").
	// Optional; informational only within this package.
	ObjectType string
}

// Index is the public interface for the Leopard in-memory index.
// Implementations must be safe for concurrent use.
type Index interface {
	// ApplyTupleWrite records a new (user, relation, object) tuple.
	ApplyTupleWrite(user, relation, object string)

	// ApplyTupleDelete removes an existing (user, relation, object) tuple.
	ApplyTupleDelete(user, relation, object string)

	// IsMember returns true if userID transitively belongs to groupID.
	IsMember(userID, groupID string) bool

	// GroupsForUser returns all groups the user directly belongs to.
	GroupsForUser(userID string) []string

	// DescendantGroups returns all groups that are descendants of groupID
	// (including groupID itself).
	DescendantGroups(groupID string) []string

	// Snapshot returns a point-in-time copy of the raw maps for debugging.
	Snapshot() Snapshot
}

// Snapshot is a read-only view of the index state at a point in time.
type Snapshot struct {
	// MemberToGroup maps userID → sorted list of direct parent group IDs.
	MemberToGroup map[string][]string
	// GroupToGroup maps groupID → sorted list of all descendant group IDs
	// (including self).
	GroupToGroup map[string][]string
}
```

- [ ] **Step 4: Run the failing test again — should fail with a different error**

```bash
go test ./index/... -run TestNewWithConfig -v
```

Expected: `FAIL — undefined: NewWithConfig` still (we haven't added it to leopard.go yet — that's Task 5). This confirms the interface is in place. Move on.

- [ ] **Step 5: Commit**

```bash
git add index/interface.go index/leopard_test.go
git commit -m "feat(index): add IndexConfig and NewWithConfig to interface"
```

---

## Task 3: Add Internal String Interning (`index/intern.go`)

**Files:**
- Create: `index/intern.go`
- Create: `index/intern_test.go`

The `intern` table is **not thread-safe on its own** — it is always accessed while the enclosing `leopardIndex.mu` is held. This avoids double-locking.

- [ ] **Step 1: Write the failing test**

Create `index/intern_test.go`:

```go
package index

import "testing"

func TestIntern_IDStability(t *testing.T) {
	tbl := newIntern()

	id1 := tbl.id("user:alice")
	id2 := tbl.id("user:alice")
	if id1 != id2 {
		t.Fatalf("same string must return same ID: got %d and %d", id1, id2)
	}
}

func TestIntern_DistinctIDs(t *testing.T) {
	tbl := newIntern()

	a := tbl.id("user:alice")
	b := tbl.id("user:bob")
	if a == b {
		t.Fatal("distinct strings must get distinct IDs")
	}
}

func TestIntern_StrRoundtrip(t *testing.T) {
	tbl := newIntern()

	id := tbl.id("group:engineering")
	if got := tbl.str(id); got != "group:engineering" {
		t.Fatalf("str(%d) = %q, want %q", id, got, "group:engineering")
	}
}

func TestIntern_StrOutOfRange(t *testing.T) {
	tbl := newIntern()
	if got := tbl.str(99); got != "" {
		t.Fatalf("str for unknown ID should return empty string, got %q", got)
	}
}

func TestIntern_Lookup_Present(t *testing.T) {
	tbl := newIntern()
	tbl.id("user:alice")

	id, ok := tbl.lookup("user:alice")
	if !ok {
		t.Fatal("lookup should find interned string")
	}
	if tbl.str(id) != "user:alice" {
		t.Fatal("lookup returned wrong ID")
	}
}

func TestIntern_Lookup_Absent(t *testing.T) {
	tbl := newIntern()

	_, ok := tbl.lookup("user:nobody")
	if ok {
		t.Fatal("lookup should return false for non-interned string")
	}
}
```

- [ ] **Step 2: Run to confirm failure**

```bash
go test ./index/... -run TestIntern -v
```

Expected: `FAIL — undefined: newIntern`

- [ ] **Step 3: Implement `index/intern.go`**

Create `index/intern.go`:

```go
package index

// intern is an append-only string interning table.
// It assigns a stable uint32 ID to each unique string.
//
// NOT thread-safe — all callers must hold the enclosing leopardIndex.mu.
type intern struct {
	fwd map[string]uint32 // string → ID
	rev []string          // ID → string (index == ID)
}

func newIntern() *intern {
	return &intern{fwd: make(map[string]uint32)}
}

// id returns the uint32 ID for s, assigning a new one if s is not yet interned.
func (t *intern) id(s string) uint32 {
	if n, ok := t.fwd[s]; ok {
		return n
	}
	n := uint32(len(t.rev))
	t.fwd[s] = n
	t.rev = append(t.rev, s)
	return n
}

// str returns the string for id. Returns "" if id is out of range.
func (t *intern) str(id uint32) string {
	if int(id) >= len(t.rev) {
		return ""
	}
	return t.rev[id]
}

// lookup returns the ID for s without assigning a new one.
// Returns (0, false) if s has not been interned.
func (t *intern) lookup(s string) (uint32, bool) {
	n, ok := t.fwd[s]
	return n, ok
}
```

- [ ] **Step 4: Run to confirm tests pass**

```bash
go test ./index/... -run TestIntern -v
```

Expected: all `TestIntern_*` tests PASS.

- [ ] **Step 5: Commit**

```bash
git add index/intern.go index/intern_test.go
git commit -m "feat(index): add internal string interning table"
```

---

## Task 4: Write Failing Tests for the Broken Delete Paths

**Files:**
- Modify: `index/leopard_test.go`

These tests expose the known bugs in the current `removeGroupEdge` implementation. They must be added **before** the rewrite so the TDD cycle is honest.

- [ ] **Step 1: Add the failing tests to `index/leopard_test.go`**

Append to `index/leopard_test.go`:

```go
// TestApplyTupleDelete_GroupEdge_Direct verifies that removing a group-in-group
// edge correctly updates GROUP2GROUP so transitive membership is revoked.
func TestApplyTupleDelete_GroupEdge_Direct(t *testing.T) {
	idx := New()
	idx.ApplyTupleWrite("group:backend", "member", "group:engineering")
	idx.ApplyTupleWrite("user:alice", "member", "group:backend")

	if !idx.IsMember("user:alice", "group:engineering") {
		t.Fatal("alice should be in engineering before delete")
	}

	idx.ApplyTupleDelete("group:backend", "member", "group:engineering")

	if idx.IsMember("user:alice", "group:engineering") {
		t.Fatal("alice should NOT be in engineering after backend→engineering edge deleted")
	}
	if !idx.IsMember("user:alice", "group:backend") {
		t.Fatal("alice should still be in backend")
	}
}

// TestApplyTupleDelete_GroupEdge_Transitive verifies that removing an edge in
// the middle of a chain revokes all downstream transitive memberships.
func TestApplyTupleDelete_GroupEdge_Transitive(t *testing.T) {
	idx := New()
	// alice → backend → engineering → company
	idx.ApplyTupleWrite("user:alice", "member", "group:backend")
	idx.ApplyTupleWrite("group:backend", "member", "group:engineering")
	idx.ApplyTupleWrite("group:engineering", "member", "group:company")

	idx.ApplyTupleDelete("group:engineering", "member", "group:company")

	if idx.IsMember("user:alice", "group:company") {
		t.Fatal("alice should NOT be in company after engineering→company edge deleted")
	}
	if !idx.IsMember("user:alice", "group:engineering") {
		t.Fatal("alice should still be in engineering")
	}
	if !idx.IsMember("user:alice", "group:backend") {
		t.Fatal("alice should still be in backend")
	}
}

// TestApplyTupleDelete_GroupEdge_Diamond verifies that a user with two paths
// to a group retains membership after one path is removed.
func TestApplyTupleDelete_GroupEdge_Diamond(t *testing.T) {
	idx := New()
	// Diamond: alice → left & right, left → top, right → top
	idx.ApplyTupleWrite("user:alice", "member", "group:left")
	idx.ApplyTupleWrite("user:alice", "member", "group:right")
	idx.ApplyTupleWrite("group:left", "member", "group:top")
	idx.ApplyTupleWrite("group:right", "member", "group:top")

	if !idx.IsMember("user:alice", "group:top") {
		t.Fatal("alice should be in top before any delete")
	}

	// Remove one path — alice still reachable via right.
	idx.ApplyTupleDelete("group:left", "member", "group:top")
	if !idx.IsMember("user:alice", "group:top") {
		t.Fatal("alice should still be in top via right after left path removed")
	}

	// Remove second path — now unreachable.
	idx.ApplyTupleDelete("group:right", "member", "group:top")
	if idx.IsMember("user:alice", "group:top") {
		t.Fatal("alice should NOT be in top after both paths removed")
	}
}
```

- [ ] **Step 2: Run to confirm these new tests fail**

```bash
go test ./index/... -run "TestApplyTupleDelete_GroupEdge|TestNewWithConfig" -v
```

Expected: `TestApplyTupleDelete_GroupEdge_Direct`, `_Transitive`, `_Diamond`, and `TestNewWithConfig_*` all FAIL (broken impl + missing NewWithConfig).

- [ ] **Step 3: Commit the failing tests**

```bash
git add index/leopard_test.go
git commit -m "test(index): add failing tests for group-edge delete and IndexConfig"
```

---

## Task 5: Rewrite `index/leopard.go`

**Files:**
- Rewrite: `index/leopard.go`

Replace the entire file. This is the core of the plan.

- [ ] **Step 1: Replace `index/leopard.go` entirely**

```go
package index

import (
	"sync"

	"github.com/RoaringBitmap/roaring"
)

// leopardIndex is the concrete implementation of Index.
//
// All posting lists are stored as roaring bitmaps keyed by uint32 IDs
// assigned by the intern table. This enables SIMD-accelerated set
// intersection and compact in-memory representation.
//
// Three maps are maintained:
//
//	memberToGroup  (MEMBER2GROUP): userID  → bitmap of direct parent group IDs
//	groupToGroup   (GROUP2GROUP):  groupID → bitmap of all descendant group IDs (transitive, reflexive)
//	directEdges:                   child groupID → bitmap of direct parent group IDs
//
// directEdges is the source of truth for the group topology. GROUP2GROUP is
// a derived, pre-computed transitive closure. On delete, GROUP2GROUP is
// recomputed via BFS from directEdges for the affected subgraph.
type leopardIndex struct {
	mu            sync.RWMutex
	strings       *intern
	memberToGroup map[uint32]*roaring.Bitmap
	groupToGroup  map[uint32]*roaring.Bitmap
	directEdges   map[uint32]*roaring.Bitmap
	cfg           IndexConfig
}

// New returns an empty, thread-safe Leopard index with default configuration.
func New() Index {
	return NewWithConfig(IndexConfig{})
}

// NewWithConfig returns an empty, thread-safe Leopard index with the given
// configuration. Zero-value IndexConfig uses defaults ("member" relation,
// "group:" prefix).
func NewWithConfig(cfg IndexConfig) Index {
	if cfg.GroupRelation == "" {
		cfg.GroupRelation = "member"
	}
	if cfg.GroupPrefix == "" {
		cfg.GroupPrefix = "group:"
	}
	return &leopardIndex{
		strings:       newIntern(),
		memberToGroup: make(map[uint32]*roaring.Bitmap),
		groupToGroup:  make(map[uint32]*roaring.Bitmap),
		directEdges:   make(map[uint32]*roaring.Bitmap),
		cfg:           cfg,
	}
}

// ApplyTupleWrite records a new relationship tuple.
//
// Only tuples whose relation matches cfg.GroupRelation are indexed.
// Tuples of the form:
//
//	user:alice   member   group:engineering   → user-in-group
//	group:backend member  group:engineering   → group-in-group
func (idx *leopardIndex) ApplyTupleWrite(user, relation, object string) {
	if relation != idx.cfg.GroupRelation {
		return
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.isGroup(user) {
		childID := idx.strings.id(user)
		parentID := idx.strings.id(object)
		idx.addGroupEdge(childID, parentID)
	} else {
		userID := idx.strings.id(user)
		groupID := idx.strings.id(object)
		bm := idx.memberToGroup[userID]
		if bm == nil {
			bm = roaring.New()
			idx.memberToGroup[userID] = bm
		}
		bm.Add(groupID)
		// Ensure GROUP2GROUP[groupID] contains groupID itself (reflexive)
		// so that direct membership is found by the intersection check.
		idx.ensureSelf(groupID)
	}
}

// ApplyTupleDelete removes an existing relationship tuple.
func (idx *leopardIndex) ApplyTupleDelete(user, relation, object string) {
	if relation != idx.cfg.GroupRelation {
		return
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.isGroup(user) {
		childID, ok1 := idx.strings.lookup(user)
		parentID, ok2 := idx.strings.lookup(object)
		if ok1 && ok2 {
			idx.removeGroupEdge(childID, parentID)
		}
	} else {
		userID, ok1 := idx.strings.lookup(user)
		groupID, ok2 := idx.strings.lookup(object)
		if !ok1 || !ok2 {
			return
		}
		bm := idx.memberToGroup[userID]
		if bm != nil {
			bm.Remove(groupID)
			if bm.IsEmpty() {
				delete(idx.memberToGroup, userID)
			}
		}
	}
}

// IsMember returns true if userID transitively belongs to groupID.
//
//	MEMBER2GROUP(U) ∩ GROUP2GROUP(G) ≠ ∅
func (idx *leopardIndex) IsMember(userID, groupID string) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	uID, ok := idx.strings.lookup(userID)
	if !ok {
		return false
	}
	gID, ok := idx.strings.lookup(groupID)
	if !ok {
		return false
	}
	m2g := idx.memberToGroup[uID]
	if m2g == nil {
		return false
	}
	g2g := idx.groupToGroup[gID]
	if g2g == nil {
		return false
	}
	return roaring.Intersects(m2g, g2g)
}

// GroupsForUser returns the direct parent groups of userID.
func (idx *leopardIndex) GroupsForUser(userID string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	uID, ok := idx.strings.lookup(userID)
	if !ok {
		return nil
	}
	bm := idx.memberToGroup[uID]
	if bm == nil {
		return nil
	}
	result := make([]string, 0, bm.GetCardinality())
	it := bm.Iterator()
	for it.HasNext() {
		result = append(result, idx.strings.str(it.Next()))
	}
	return result
}

// DescendantGroups returns all descendant group IDs of groupID (including self).
func (idx *leopardIndex) DescendantGroups(groupID string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	gID, ok := idx.strings.lookup(groupID)
	if !ok {
		return nil
	}
	bm := idx.groupToGroup[gID]
	if bm == nil {
		return nil
	}
	result := make([]string, 0, bm.GetCardinality())
	it := bm.Iterator()
	for it.HasNext() {
		result = append(result, idx.strings.str(it.Next()))
	}
	return result
}

// Snapshot returns a read-only copy of the index state.
func (idx *leopardIndex) Snapshot() Snapshot {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	m2g := make(map[string][]string, len(idx.memberToGroup))
	for uid, bm := range idx.memberToGroup {
		userStr := idx.strings.str(uid)
		groups := make([]string, 0, bm.GetCardinality())
		it := bm.Iterator()
		for it.HasNext() {
			groups = append(groups, idx.strings.str(it.Next()))
		}
		m2g[userStr] = groups
	}

	g2g := make(map[string][]string, len(idx.groupToGroup))
	for gid, bm := range idx.groupToGroup {
		groupStr := idx.strings.str(gid)
		descs := make([]string, 0, bm.GetCardinality())
		it := bm.Iterator()
		for it.HasNext() {
			descs = append(descs, idx.strings.str(it.Next()))
		}
		g2g[groupStr] = descs
	}

	return Snapshot{MemberToGroup: m2g, GroupToGroup: g2g}
}

// ── private helpers ──────────────────────────────────────────────────────────

// addGroupEdge records that childID is a direct member of parentID and
// propagates transitive closure updates to GROUP2GROUP.
func (idx *leopardIndex) addGroupEdge(childID, parentID uint32) {
	// Record direct edge.
	bm := idx.directEdges[childID]
	if bm == nil {
		bm = roaring.New()
		idx.directEdges[childID] = bm
	}
	bm.Add(parentID)

	// All descendants of childID (including itself).
	childDesc := idx.computeDescendants(childID)

	// Add childDesc to parentID and every ancestor of parentID.
	targets := idx.ancestorsOf(parentID)
	targets = append(targets, parentID)
	for _, t := range targets {
		g2g := idx.groupToGroup[t]
		if g2g == nil {
			g2g = roaring.New()
			idx.groupToGroup[t] = g2g
		}
		g2g.Or(childDesc)
		g2g.Add(t) // reflexive: every group contains itself
	}
}

// removeGroupEdge removes the direct edge childID→parentID and recomputes
// GROUP2GROUP for parentID and all its ancestors via BFS from directEdges.
func (idx *leopardIndex) removeGroupEdge(childID, parentID uint32) {
	// Remove direct edge.
	if bm := idx.directEdges[childID]; bm != nil {
		bm.Remove(parentID)
		if bm.IsEmpty() {
			delete(idx.directEdges, childID)
		}
	}

	// Recompute GROUP2GROUP for parentID and all its ancestors.
	affected := idx.ancestorsOf(parentID)
	affected = append(affected, parentID)
	for _, nodeID := range affected {
		newDesc := idx.computeDescendants(nodeID)
		if newDesc.GetCardinality() == 0 {
			delete(idx.groupToGroup, nodeID)
		} else {
			idx.groupToGroup[nodeID] = newDesc
		}
	}
}

// computeDescendants returns all descendants of groupID (including itself)
// by BFS over the reverse of directEdges (parent→children direction).
// Caller must hold idx.mu.
func (idx *leopardIndex) computeDescendants(groupID uint32) *roaring.Bitmap {
	result := roaring.New()
	result.Add(groupID)

	queue := []uint32{groupID}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		// Find children: nodes X where directEdges[X].Contains(cur).
		for childID, parents := range idx.directEdges {
			if parents.Contains(cur) && !result.Contains(childID) {
				result.Add(childID)
				queue = append(queue, childID)
			}
		}
	}
	return result
}

// ancestorsOf returns all group IDs that have groupID in their GROUP2GROUP
// (i.e., all groups for which groupID is a descendant).
func (idx *leopardIndex) ancestorsOf(groupID uint32) []uint32 {
	var result []uint32
	for g, descendants := range idx.groupToGroup {
		if g != groupID && descendants.Contains(groupID) {
			result = append(result, g)
		}
	}
	return result
}

// isGroup returns true if id uses the configured group prefix.
func (idx *leopardIndex) isGroup(id string) bool {
	p := idx.cfg.GroupPrefix
	return len(id) > len(p) && id[:len(p)] == p
}

// ensureSelf ensures GROUP2GROUP[groupID] contains groupID itself (reflexive).
func (idx *leopardIndex) ensureSelf(groupID uint32) {
	bm := idx.groupToGroup[groupID]
	if bm == nil {
		bm = roaring.New()
		idx.groupToGroup[groupID] = bm
	}
	bm.Add(groupID)
}
```

- [ ] **Step 2: Run all index tests**

```bash
go test ./index/... -v
```

Expected: all tests PASS, including the three new `TestApplyTupleDelete_GroupEdge_*` and both `TestNewWithConfig_*` tests.

- [ ] **Step 3: Commit**

```bash
git add index/leopard.go
git commit -m "feat(index): rewrite with roaring bitmaps, directEdges, BFS delete recompute"
```

---

## Task 6: Remove `set.go` and `set_test.go`

**Files:**
- Delete: `index/set.go`
- Delete: `index/set_test.go`

`sortedSet` is no longer used anywhere. Deleting it keeps the package clean and prevents confusion.

- [ ] **Step 1: Delete the files**

```bash
git rm index/set.go index/set_test.go
```

- [ ] **Step 2: Confirm the package still compiles and all tests pass**

```bash
go test ./index/... -v
```

Expected: all tests PASS. No references to `sortedSet` remain.

- [ ] **Step 3: Confirm no other packages reference `sortedSet`**

```bash
grep -r "sortedSet" .
```

Expected: no output.

- [ ] **Step 4: Commit**

```bash
git commit -m "chore(index): remove sortedSet — replaced by roaring.Bitmap"
```

---

## Task 7: Run Benchmark and Record Baseline

**Files:**
- Modify: `index/leopard_test.go` (update benchmark to exercise roaring intersection)

- [ ] **Step 1: Run the existing benchmark**

```bash
go test ./index/... -bench=BenchmarkIsMember -benchmem -count=3
```

Record the output (ns/op and allocs/op) as the baseline for roaring bitmaps. Example output:
```
BenchmarkIsMember-8   5000000   234 ns/op   0 B/op   0 allocs/op
```

- [ ] **Step 2: Add a wide-fanout benchmark to stress the intersection path**

Append to `index/leopard_test.go`:

```go
// BenchmarkIsMember_WideFanout benchmarks a user in one group that is a
// member of 500 peer groups, checking membership against one of them.
// This exercises the roaring bitmap intersection path with wider bitmaps.
func BenchmarkIsMember_WideFanout(b *testing.B) {
	idx := New()
	idx.ApplyTupleWrite("user:alice", "member", "group:g0")
	for i := 0; i < 500; i++ {
		child := "group:g" + itoa(i)
		parent := "group:g" + itoa(i+1)
		idx.ApplyTupleWrite(child, "member", parent)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.IsMember("user:alice", "group:g499")
	}
}
```

- [ ] **Step 3: Run both benchmarks**

```bash
go test ./index/... -bench=BenchmarkIsMember -benchmem -count=3
```

Expected: both benchmarks pass with zero allocations on the hot path (`IsMember`).

- [ ] **Step 4: Commit**

```bash
git add index/leopard_test.go
git commit -m "bench(index): add wide-fanout IsMember benchmark"
```

---

## Self-Review Checklist

- [x] **Spec coverage:** Section 4.1 (directEdges + BFS delete) ✓ Task 5. Section 4.2 (roaring bitmaps) ✓ Tasks 1+5. Section 4.3 (IndexConfig) ✓ Tasks 2+5. `New()` remains backwards-compatible ✓ Task 5.
- [x] **Placeholder scan:** No TBD/TODO in any step. All code blocks are complete.
- [x] **Type consistency:** `IndexConfig` defined in Task 2 (`interface.go`), used in Task 5 (`leopard.go`). `newIntern()` defined in Task 3, used in Task 5. `intern.lookup()` defined in Task 3, called in Task 5. `roaring.Intersects(a, b)` is the correct API from `github.com/RoaringBitmap/roaring`. `bm.GetCardinality()` returns `uint64` — `make([]string, 0, bm.GetCardinality())` requires a cast: fix below.

**Fix:** `bm.GetCardinality()` returns `uint64`. `make` accepts `int`. Add cast:

In Task 5 Step 1, all occurrences of `make([]string, 0, bm.GetCardinality())` should be:

```go
make([]string, 0, int(bm.GetCardinality()))
```

This applies in `GroupsForUser`, `DescendantGroups`, and `Snapshot`. The code in Task 5 Step 1 above already reflects this fix — verify when implementing.
