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
	return m2g.Intersects(g2g)
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
	result := make([]string, 0, int(bm.GetCardinality()))
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
	result := make([]string, 0, int(bm.GetCardinality()))
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
		groups := make([]string, 0, int(bm.GetCardinality()))
		it := bm.Iterator()
		for it.HasNext() {
			groups = append(groups, idx.strings.str(it.Next()))
		}
		m2g[userStr] = groups
	}

	g2g := make(map[string][]string, len(idx.groupToGroup))
	for gid, bm := range idx.groupToGroup {
		groupStr := idx.strings.str(gid)
		descs := make([]string, 0, int(bm.GetCardinality()))
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
