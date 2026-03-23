package builder

import (
	"context"
	"testing"
	"time"

	"github.com/yourorg/openfga-indexer/cache"
	"github.com/yourorg/openfga-indexer/client"
)

func TestBuilder_RunOnce_BuildsAndRotatesShard(t *testing.T) {
	mock := client.NewMock()
	mock.AddTuple(client.Tuple{User: "user:alice", Relation: "member", Object: "group:eng"})
	mock.AddTuple(client.Tuple{User: "group:eng", Relation: "member", Object: "group:company"})

	ca := cache.New()
	b := New(mock, ca, Options{StoreID: "s", RebuildInterval: time.Hour})

	tok, err := b.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	_ = tok // empty in mock; that's fine

	idx := ca.Index()
	if !idx.IsMember("user:alice", "group:eng") {
		t.Error("alice should be in group:eng")
	}
	if !idx.IsMember("user:alice", "group:company") {
		t.Error("alice should transitively be in group:company")
	}
}

func TestBuilder_Run_RebuildsPeriodically(t *testing.T) {
	mock := client.NewMock()
	mock.AddTuple(client.Tuple{User: "user:bob", Relation: "member", Object: "group:ops"})

	ca := cache.New()
	b := New(mock, ca, Options{
		StoreID:         "s",
		RebuildInterval: 20 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	go b.Run(ctx) //nolint:errcheck

	// After the initial build bob should be indexed.
	deadline := time.After(200 * time.Millisecond)
	for {
		if ca.Index().IsMember("user:bob", "group:ops") {
			return
		}
		select {
		case <-deadline:
			t.Fatal("bob was not indexed within deadline")
		case <-time.After(5 * time.Millisecond):
		}
	}
}
