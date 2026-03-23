package index

import "testing"

func TestSortedSet_AddRemove(t *testing.T) {
	s := newSortedSet("c", "a", "b", "a")
	if len(s) != 3 {
		t.Fatalf("expected len 3 after dedup, got %d", len(s))
	}
	if s[0] != "a" || s[1] != "b" || s[2] != "c" {
		t.Fatalf("unexpected order: %v", s)
	}

	s = s.add("b") // duplicate
	if len(s) != 3 {
		t.Fatal("add of duplicate should not grow set")
	}

	s = s.remove("b")
	if s.contains("b") {
		t.Fatal("b should be removed")
	}
	if len(s) != 2 {
		t.Fatalf("expected len 2 after remove, got %d", len(s))
	}
}

func TestIntersects(t *testing.T) {
	tests := []struct {
		a, b sortedSet
		want bool
	}{
		{newSortedSet("a", "b"), newSortedSet("b", "c"), true},
		{newSortedSet("a", "b"), newSortedSet("c", "d"), false},
		{nil, newSortedSet("a"), false},
		{newSortedSet("x"), nil, false},
	}
	for _, tt := range tests {
		got := intersects(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("intersects(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
		}
	}
}
