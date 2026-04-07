package index

import (
	"fmt"
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

// BenchmarkIsMember_WideFanout benchmarks a user in one group that is a
// member of 500 peer groups, checking membership against one of them.
// This exercises the roaring bitmap intersection path with wider bitmaps.
func BenchmarkIsMember_WideFanout(b *testing.B) {
	idx := New()
	idx.ApplyTupleWrite("user:alice", "member", "group:g0")
	for i := 0; i < 500; i++ {
		child := fmt.Sprintf("group:g%d", i)
		parent := fmt.Sprintf("group:g%d", i+1)
		idx.ApplyTupleWrite(child, "member", parent)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.IsMember("user:alice", "group:g499")
	}
}
