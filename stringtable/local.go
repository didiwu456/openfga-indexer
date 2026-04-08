// Package stringtable manages the persistent string↔uint32 ID mapping
// used to key roaring bitmap posting lists in the Leopard index.
//
// LocalTable is a read-only in-memory view for use during index builds.
// Store is the durable interface backed by PostgreSQL or MySQL.
package stringtable

// LocalTable is a read-only in-memory string table loaded from the DB.
// Not thread-safe — use within a single goroutine or under external lock.
type LocalTable struct {
	fwd map[string]uint32
	rev []string // index == uint32 ID
}

// NewLocalTable builds a LocalTable from a fwd map.
// The rev slice is derived automatically.
func NewLocalTable(fwd map[string]uint32) *LocalTable {
	if len(fwd) == 0 {
		return &LocalTable{fwd: make(map[string]uint32)}
	}
	// Find max ID to size rev correctly.
	var maxID uint32
	for _, id := range fwd {
		if id > maxID {
			maxID = id
		}
	}
	rev := make([]string, int(maxID)+1)
	for s, id := range fwd {
		rev[id] = s
	}
	return &LocalTable{fwd: fwd, rev: rev}
}

// ID returns the uint32 ID for s. Returns (0, false) if not found.
func (t *LocalTable) ID(s string) (uint32, bool) {
	id, ok := t.fwd[s]
	return id, ok
}

// Str returns the string for id. Returns "" if id is out of range.
func (t *LocalTable) Str(id uint32) string {
	if int(id) >= len(t.rev) {
		return ""
	}
	return t.rev[id]
}

// Fwd returns the forward map (string → ID). Do not modify.
func (t *LocalTable) Fwd() map[string]uint32 {
	return t.fwd
}

// Len returns the number of entries in the table.
func (t *LocalTable) Len() int {
	return len(t.fwd)
}
