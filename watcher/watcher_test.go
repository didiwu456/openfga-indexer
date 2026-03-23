package watcher

import (
	"context"
	"testing"
	"time"

	"github.com/yourorg/openfga-indexer/cache"
	"github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/index"
)

func newTestCache(idx index.Index) *cache.Cache {
	c := cache.New()
	c.Rotate(idx)
	return c
}

func TestWatcher_AppliesWriteChange(t *testing.T) {
	mock := client.NewMock()
	mock.PushChange(client.TupleChange{
		User: "user:alice", Relation: "member", Object: "group:engineering",
		Operation: client.OperationWrite,
	})

	idx := index.New()
	ca := newTestCache(idx)
	w := New(mock, ca, Options{StoreID: "s", PollInterval: 5 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Poll until the change is applied or timeout.
	deadline := time.After(500 * time.Millisecond)
	for {
		if ca.Index().IsMember("user:alice", "group:engineering") {
			break
		}
		select {
		case <-deadline:
			t.Fatal("alice was not indexed within deadline")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	<-done
}

func TestWatcher_AppliesDeleteChange(t *testing.T) {
	mock := client.NewMock()
	mock.PushChange(client.TupleChange{
		User: "user:bob", Relation: "member", Object: "group:infra",
		Operation: client.OperationWrite,
	})
	mock.PushChange(client.TupleChange{
		User: "user:bob", Relation: "member", Object: "group:infra",
		Operation: client.OperationDelete,
	})

	idx := index.New()
	ca := newTestCache(idx)
	w := New(mock, ca, Options{StoreID: "s", PollInterval: 5 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	deadline := time.After(500 * time.Millisecond)
	for {
		if !ca.Index().IsMember("user:bob", "group:infra") {
			// Confirm the queue is drained (changes were applied).
			time.Sleep(20 * time.Millisecond)
			if !ca.Index().IsMember("user:bob", "group:infra") {
				break
			}
		}
		select {
		case <-deadline:
			t.Fatal("delete was not applied within deadline")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	<-done
}

func TestWatcher_ReanchorsOnRotation(t *testing.T) {
	mock := client.NewMock()

	idx := index.New()
	ca := newTestCache(idx)
	w := New(mock, ca, Options{StoreID: "s", PollInterval: 5 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Simulate a builder rotation: new shard + new token.
	newShard := index.New()
	newShard.ApplyTupleWrite("user:carol", "member", "group:ops")
	ca.Rotate(newShard)
	ca.SetWatchToken("tok-after-rotation")

	deadline := time.After(500 * time.Millisecond)
	for {
		if ca.Index().IsMember("user:carol", "group:ops") {
			break
		}
		select {
		case <-deadline:
			t.Fatal("rotation was not picked up within deadline")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	<-done
}

func TestWatcher_StopsOnContextCancel(t *testing.T) {
	mock := client.NewMock()
	ca := cache.New()
	w := New(mock, ca, Options{StoreID: "s", PollInterval: 5 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run did not stop after context cancel")
	}
}
