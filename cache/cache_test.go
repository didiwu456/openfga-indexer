package cache

import (
	"testing"

	"github.com/yourorg/openfga-indexer/index"
)

func TestCache_RotateSwapsShard(t *testing.T) {
	c := New()

	shard1 := index.New()
	shard1.ApplyTupleWrite("user:alice", "member", "group:eng")
	c.Rotate(shard1)

	if !c.Index().IsMember("user:alice", "group:eng") {
		t.Fatal("alice should be in eng after first rotate")
	}

	shard2 := index.New()
	shard2.ApplyTupleWrite("user:bob", "member", "group:infra")
	c.Rotate(shard2)

	if c.Index().IsMember("user:alice", "group:eng") {
		t.Fatal("alice should not be in eng after shard2 rotated in")
	}
	if !c.Index().IsMember("user:bob", "group:infra") {
		t.Fatal("bob should be in infra after second rotate")
	}
}

func TestCache_SetWatchToken(t *testing.T) {
	c := New()
	tok, ch := c.WatchToken()
	if tok != "" {
		t.Fatalf("expected empty initial token, got %q", tok)
	}

	// SetWatchToken should close the ready channel and update the token.
	c.SetWatchToken("tok-abc")
	select {
	case <-ch:
		// expected
	default:
		t.Fatal("channel should be closed after SetWatchToken")
	}

	tok, _ = c.WatchToken()
	if tok != "tok-abc" {
		t.Fatalf("expected tok-abc, got %q", tok)
	}
}

func TestCache_EmptyShardNeverNil(t *testing.T) {
	c := New()
	idx := c.Index()
	if idx == nil {
		t.Fatal("Index() should never return nil")
	}
	// Should not panic.
	idx.IsMember("user:x", "group:y")
}
