package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/messaging"
	leopardstorage "github.com/yourorg/openfga-indexer/storage"
	"github.com/yourorg/openfga-indexer/stringtable"
	"github.com/yourorg/openfga-indexer/pipeline"
)

type mockBus struct {
	rotations []messaging.RotationEvent
}

func (m *mockBus) PublishRotation(_ context.Context, e messaging.RotationEvent) error {
	m.rotations = append(m.rotations, e)
	return nil
}
func (m *mockBus) SubscribeRotation(_ context.Context, _ string, _ func(messaging.RotationEvent)) (messaging.Subscription, error) {
	return &nopSub{}, nil
}
func (m *mockBus) PublishRebuild(_ context.Context, _ messaging.RebuildEvent) error { return nil }
func (m *mockBus) SubscribeRebuild(_ context.Context, _ string, _ func(messaging.RebuildEvent)) (messaging.Subscription, error) {
	return &nopSub{}, nil
}
func (m *mockBus) PublishDelta(_ context.Context, _ messaging.DeltaEvent) error { return nil }
func (m *mockBus) SubscribeDelta(_ context.Context, _ string, _ func(messaging.DeltaEvent)) (messaging.Subscription, error) {
	return &nopSub{}, nil
}
func (m *mockBus) PublishPromotion(_ context.Context, _ messaging.PromotionEvent) error { return nil }
func (m *mockBus) SubscribePromotion(_ context.Context, _ func(messaging.PromotionEvent)) (messaging.Subscription, error) {
	return &nopSub{}, nil
}
func (m *mockBus) Close() error { return nil }

type nopSub struct{}

func (n *nopSub) Cancel() error { return nil }

func TestPipeline_BuildAndRotate(t *testing.T) {
	fga := client.NewMockClient([]client.Tuple{
		{User: "user:alice", Relation: "member", Object: "group:eng"},
		{User: "group:backend", Relation: "member", Object: "group:eng"},
		{User: "user:bob", Relation: "member", Object: "group:backend"},
		{User: "user:alice", Relation: "viewer", Object: "document:readme"},
	})
	store := leopardstorage.NewMemoryStore()
	bus := &mockBus{}
	stbl := stringtable.NewInMemoryStore()

	p := pipeline.New(fga, stbl, store, bus, pipeline.Options{
		StoreID:      "store1",
		IndexedTypes: []string{"group"},
	})

	ctx := context.Background()
	epoch, err := p.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if epoch == 0 {
		t.Fatal("epoch should be > 0")
	}

	data, err := store.ReadShard(ctx, "store1", "group", epoch)
	if err != nil {
		t.Fatalf("ReadShard: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("shard data should not be empty")
	}

	if len(bus.rotations) != 1 {
		t.Fatalf("expected 1 RotationEvent, got %d", len(bus.rotations))
	}
	rot := bus.rotations[0]
	if rot.Epoch != epoch {
		t.Fatalf("RotationEvent.Epoch = %d, want %d", rot.Epoch, epoch)
	}
	if len(rot.Types) != 1 || rot.Types[0] != "group" {
		t.Fatalf("RotationEvent.Types = %v, want [group]", rot.Types)
	}
}

func TestPipeline_IgnoresNonIndexedTypes(t *testing.T) {
	fga := client.NewMockClient([]client.Tuple{
		{User: "user:alice", Relation: "viewer", Object: "document:readme"},
	})
	store := leopardstorage.NewMemoryStore()
	bus := &mockBus{}
	stbl := stringtable.NewInMemoryStore()

	p := pipeline.New(fga, stbl, store, bus, pipeline.Options{
		StoreID:      "store1",
		IndexedTypes: []string{"group"},
	})

	ctx := context.Background()
	epoch, err := p.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	_, err = store.ReadShard(ctx, "store1", "document", epoch)
	if err == nil {
		t.Fatal("document shard should not have been written")
	}
}

func TestPipeline_ConcurrentRunsQueued(t *testing.T) {
	fga := client.NewMockClient(nil)
	store := leopardstorage.NewMemoryStore()
	bus := &mockBus{}
	stbl := stringtable.NewInMemoryStore()

	p := pipeline.New(fga, stbl, store, bus, pipeline.Options{
		StoreID:      "store1",
		IndexedTypes: []string{"group"},
	})
	ctx := context.Background()

	done := make(chan error, 2)
	go func() { _, err := p.Run(ctx); done <- err }()
	go func() { _, err := p.Run(ctx); done <- err }()

	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run %d: %v", i, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("Run %d timed out", i)
		}
	}
}
