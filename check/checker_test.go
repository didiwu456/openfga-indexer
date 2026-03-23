package check

import (
	"context"
	"testing"

	"github.com/yourorg/openfga-indexer/cache"
	"github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/index"
)

func newCache(idx index.Index) *cache.Cache {
	ca := cache.New()
	ca.Rotate(idx)
	return ca
}

func TestChecker_MemberFromIndex(t *testing.T) {
	idx := index.New()
	idx.ApplyTupleWrite("user:alice", "member", "group:eng")

	mock := client.NewMock()
	mock.AllowCheck("user:alice", "member", "group:eng", false) // would fail if hit

	c := New(newCache(idx), mock, "store1", "")
	ok, err := c.Check(context.Background(), "user:alice", "member", "group:eng")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("alice should be a member (from index)")
	}
}

func TestChecker_NonMemberFallsToFGA(t *testing.T) {
	mock := client.NewMock()
	mock.AllowCheck("user:alice", "viewer", "doc:readme", true)

	c := New(cache.New(), mock, "store1", "")
	ok, err := c.Check(context.Background(), "user:alice", "viewer", "doc:readme")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("alice should be a viewer (from FGA)")
	}
}

func TestChecker_ListObjectsFromIndex(t *testing.T) {
	idx := index.New()
	idx.ApplyTupleWrite("user:alice", "member", "group:eng")
	idx.ApplyTupleWrite("user:alice", "member", "group:infra")

	c := New(newCache(idx), client.NewMock(), "store1", "")
	groups, err := c.ListObjects(context.Background(), "user:alice", "member", "group")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d: %v", len(groups), groups)
	}
}

func TestChecker_ListObjectsFallsToFGA(t *testing.T) {
	mock := client.NewMock()
	mock.AddObjects("user:alice", "viewer", "document", []string{"doc:readme", "doc:spec"})

	c := New(cache.New(), mock, "store1", "")
	docs, err := c.ListObjects(context.Background(), "user:alice", "viewer", "document")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("expected 2 docs, got %d: %v", len(docs), docs)
	}
}
