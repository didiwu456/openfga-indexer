//go:build integration

package messaging_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/messaging"
)

// RunBusContract is the shared contract test suite for any MessageBus implementation.
// Call it from implementation-specific test files.
func RunBusContract(t *testing.T, bus messaging.MessageBus) {
	t.Helper()
	ctx := context.Background()

	t.Run("rotation_pubsub", func(t *testing.T) {
		received := make(chan messaging.RotationEvent, 1)
		sub, err := bus.SubscribeRotation(ctx, "store1", func(e messaging.RotationEvent) {
			received <- e
		})
		if err != nil {
			t.Fatalf("SubscribeRotation: %v", err)
		}
		defer sub.Cancel()

		want := messaging.RotationEvent{StoreID: "store1", Epoch: 42, Types: []string{"group"}, PublishedAt: time.Now()}
		if err := bus.PublishRotation(ctx, want); err != nil {
			t.Fatalf("PublishRotation: %v", err)
		}
		select {
		case got := <-received:
			if got.Epoch != want.Epoch || got.StoreID != want.StoreID {
				t.Fatalf("got %+v, want %+v", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for RotationEvent")
		}
	})

	t.Run("rebuild_pubsub", func(t *testing.T) {
		received := make(chan messaging.RebuildEvent, 1)
		sub, err := bus.SubscribeRebuild(ctx, "store2", func(e messaging.RebuildEvent) {
			received <- e
		})
		if err != nil {
			t.Fatalf("SubscribeRebuild: %v", err)
		}
		defer sub.Cancel()

		want := messaging.RebuildEvent{StoreID: "store2", ModelID: "model-xyz", TriggeredAt: time.Now()}
		if err := bus.PublishRebuild(ctx, want); err != nil {
			t.Fatalf("PublishRebuild: %v", err)
		}
		select {
		case got := <-received:
			if got.ModelID != want.ModelID {
				t.Fatalf("got ModelID %q, want %q", got.ModelID, want.ModelID)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for RebuildEvent")
		}
	})

	t.Run("delta_pubsub", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(1)
		sub, err := bus.SubscribeDelta(ctx, "store3", func(e messaging.DeltaEvent) {
			if e.Epoch == 7 {
				wg.Done()
			}
		})
		if err != nil {
			t.Fatalf("SubscribeDelta: %v", err)
		}
		defer sub.Cancel()

		if err := bus.PublishDelta(ctx, messaging.DeltaEvent{
			StoreID:   "store3",
			Epoch:     7,
			Changes:   []client.TupleChange{{User: "user:alice", Relation: "member", Object: "group:eng", Operation: client.OperationWrite}},
			AppliedAt: time.Now(),
		}); err != nil {
			t.Fatalf("PublishDelta: %v", err)
		}

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for DeltaEvent")
		}
	})

	t.Run("promotion_pubsub", func(t *testing.T) {
		received := make(chan messaging.PromotionEvent, 1)
		sub, err := bus.SubscribePromotion(ctx, func(e messaging.PromotionEvent) {
			received <- e
		})
		if err != nil {
			t.Fatalf("SubscribePromotion: %v", err)
		}
		defer sub.Cancel()

		want := messaging.PromotionEvent{ClusterID: "cluster-a", InstanceID: "inst-1", PromotedAt: time.Now()}
		if err := bus.PublishPromotion(ctx, want); err != nil {
			t.Fatalf("PublishPromotion: %v", err)
		}
		select {
		case got := <-received:
			if got.ClusterID != want.ClusterID || got.InstanceID != want.InstanceID {
				t.Fatalf("got %+v, want %+v", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for PromotionEvent")
		}
	})
}
