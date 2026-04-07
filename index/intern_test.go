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
