package index

import (
	"testing"
)

func TestIsMember_DirectMembership(t *testing.T) {
	idx := New()
	idx.ApplyTupleWrite("user:alice", "member", "group:engineering")

	if !idx.IsMember("user:alice", "group:engineering") {
		t.Fatal("alice should be a direct member of engineering")
	}
	if idx.IsMember("user:bob", "group:engineering") {
		t.Fatal("bob should not be a member of engineering")
	}
}

func TestIsMember_NestedGroups(t *testing.T) {
	idx := New()
	// backend ⊂ engineering ⊂ company
	idx.ApplyTupleWrite("group:backend", "member", "group:engineering")
	idx.ApplyTupleWrite("group:engineering", "member", "group:company")
	idx.ApplyTupleWrite("user:alice", "member", "group:backend")

	tests := []struct {
		user, group string
		want        bool
	}{
		{"user:alice", "group:backend", true},
		{"user:alice", "group:engineering", true},
		{"user:alice", "group:company", true},
		{"user:alice", "group:other", false},
	}
	for _, tt := range tests {
		got := idx.IsMember(tt.user, tt.group)
		if got != tt.want {
			t.Errorf("IsMember(%q, %q) = %v, want %v", tt.user, tt.group, got, tt.want)
		}
	}
}

func TestApplyTupleDelete_User(t *testing.T) {
	idx := New()
	idx.ApplyTupleWrite("user:alice", "member", "group:engineering")
	idx.ApplyTupleDelete("user:alice", "member", "group:engineering")

	if idx.IsMember("user:alice", "group:engineering") {
		t.Fatal("alice should no longer be a member after delete")
	}
}

func TestGroupsForUser(t *testing.T) {
	idx := New()
	idx.ApplyTupleWrite("user:alice", "member", "group:backend")
	idx.ApplyTupleWrite("user:alice", "member", "group:infra")

	groups := idx.GroupsForUser("user:alice")
	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d: %v", len(groups), groups)
	}
}

func TestNonMemberRelationIgnored(t *testing.T) {
	idx := New()
	idx.ApplyTupleWrite("user:alice", "viewer", "doc:readme")

	// Should not panic and should not affect group index.
	if idx.IsMember("user:alice", "doc:readme") {
		t.Fatal("non-member relations should not be indexed")
	}
}

func TestDescendantGroups(t *testing.T) {
	idx := New()
	idx.ApplyTupleWrite("group:backend", "member", "group:engineering")

	desc := idx.DescendantGroups("group:engineering")
	found := false
	for _, d := range desc {
		if d == "group:backend" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected group:backend in descendants of group:engineering, got %v", desc)
	}
}

// BenchmarkIsMember measures the set-intersection path.
func BenchmarkIsMember(b *testing.B) {
	idx := New()
	// Build a chain: user → g0 → g1 → ... → g99
	idx.ApplyTupleWrite("user:alice", "member", "group:g0")
	for i := 0; i < 99; i++ {
		child := "group:g" + itoa(i)
		parent := "group:g" + itoa(i+1)
		idx.ApplyTupleWrite(child, "member", parent)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.IsMember("user:alice", "group:g99")
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	b := make([]byte, 0, 10)
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
