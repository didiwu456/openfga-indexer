package manager_test

import (
	"context"
	"testing"

	"github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/manager"
	"github.com/yourorg/openfga-indexer/messaging"
	leopardredis "github.com/yourorg/openfga-indexer/redis"
	leopardstorage "github.com/yourorg/openfga-indexer/storage"
	"github.com/yourorg/openfga-indexer/stringtable"
)

func TestManager_RegisterAndStatus(t *testing.T) {
	m := manager.New(manager.Config{
		FGA:      client.NewMockClient(nil),
		Redis:    leopardredis.NewMemoryStore(),
		Storage:  leopardstorage.NewMemoryStore(),
		StrTable: stringtable.NewInMemoryStore(),
		Bus:      &nopBus{},
	})

	ctx := context.Background()
	if err := m.RegisterStore(ctx, "store1", []string{"group"}); err != nil {
		t.Fatalf("RegisterStore: %v", err)
	}

	status, err := m.StoreStatus(ctx, "store1")
	if err != nil {
		t.Fatalf("StoreStatus: %v", err)
	}
	if status.StoreID != "store1" {
		t.Fatalf("status.StoreID = %q, want store1", status.StoreID)
	}
}

func TestManager_DeregisterStore(t *testing.T) {
	m := manager.New(manager.Config{
		FGA:      client.NewMockClient(nil),
		Redis:    leopardredis.NewMemoryStore(),
		Storage:  leopardstorage.NewMemoryStore(),
		StrTable: stringtable.NewInMemoryStore(),
		Bus:      &nopBus{},
	})

	ctx := context.Background()
	_ = m.RegisterStore(ctx, "store1", []string{"group"})
	if err := m.DeregisterStore(ctx, "store1"); err != nil {
		t.Fatalf("DeregisterStore: %v", err)
	}
	if _, err := m.StoreStatus(ctx, "store1"); err == nil {
		t.Fatal("expected error for deregistered store")
	}
}

// nopBus satisfies messaging.MessageBus for tests.
type nopBus struct{}

func (n *nopBus) PublishRotation(_ context.Context, _ messaging.RotationEvent) error { return nil }
func (n *nopBus) SubscribeRotation(_ context.Context, _ string, _ func(messaging.RotationEvent)) (messaging.Subscription, error) {
	return &nopSub{}, nil
}
func (n *nopBus) PublishRebuild(_ context.Context, _ messaging.RebuildEvent) error { return nil }
func (n *nopBus) SubscribeRebuild(_ context.Context, _ string, _ func(messaging.RebuildEvent)) (messaging.Subscription, error) {
	return &nopSub{}, nil
}
func (n *nopBus) PublishDelta(_ context.Context, _ messaging.DeltaEvent) error { return nil }
func (n *nopBus) SubscribeDelta(_ context.Context, _ string, _ func(messaging.DeltaEvent)) (messaging.Subscription, error) {
	return &nopSub{}, nil
}
func (n *nopBus) PublishPromotion(_ context.Context, _ messaging.PromotionEvent) error { return nil }
func (n *nopBus) SubscribePromotion(_ context.Context, _ func(messaging.PromotionEvent)) (messaging.Subscription, error) {
	return &nopSub{}, nil
}
func (n *nopBus) Close() error { return nil }

type nopSub struct{}

func (n *nopSub) Cancel() error { return nil }
