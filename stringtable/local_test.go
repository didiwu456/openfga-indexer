package stringtable_test

import (
	"testing"

	"github.com/yourorg/openfga-indexer/stringtable"
)

func TestLocalTable_RoundTrip(t *testing.T) {
	fwd := map[string]uint32{
		"user:alice":    0,
		"group:eng":     1,
		"group:backend": 2,
	}
	tbl := stringtable.NewLocalTable(fwd)

	tests := []struct {
		input  string
		wantID uint32
	}{
		{"user:alice", 0},
		{"group:eng", 1},
		{"group:backend", 2},
	}
	for _, tt := range tests {
		id, ok := tbl.ID(tt.input)
		if !ok {
			t.Fatalf("ID(%q): not found", tt.input)
		}
		if id != tt.wantID {
			t.Fatalf("ID(%q) = %d, want %d", tt.input, id, tt.wantID)
		}
		if got := tbl.Str(id); got != tt.input {
			t.Fatalf("Str(%d) = %q, want %q", id, got, tt.input)
		}
	}
}

func TestLocalTable_Missing(t *testing.T) {
	tbl := stringtable.NewLocalTable(map[string]uint32{})

	if _, ok := tbl.ID("user:nobody"); ok {
		t.Fatal("ID for unknown string should return false")
	}
	if got := tbl.Str(99); got != "" {
		t.Fatalf("Str for out-of-range ID should be empty, got %q", got)
	}
}

func TestLocalTable_Fwd(t *testing.T) {
	fwd := map[string]uint32{"user:alice": 0}
	tbl := stringtable.NewLocalTable(fwd)
	if len(tbl.Fwd()) != 1 {
		t.Fatal("Fwd should return full map")
	}
}
