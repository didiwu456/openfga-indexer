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
