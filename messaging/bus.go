// Package messaging provides reliable, durable delivery of control signals
// between Leopard service instances across clusters.
//
// Two implementations are provided: NATS JetStream (messaging/nats) and
// Redis Streams (messaging/redisstream). Select via config.
package messaging

import (
	"context"
	"time"

	"github.com/yourorg/openfga-indexer/client"
)

// MessageBus carries all control signals between components.
// All methods must be safe for concurrent use.
type MessageBus interface {
	// Rotation — published by Offline Pipeline after a successful epoch build,
	// consumed by all Check Gateway instances.
	PublishRotation(ctx context.Context, event RotationEvent) error
	SubscribeRotation(ctx context.Context, storeID string, handler func(RotationEvent)) (Subscription, error)

	// Rebuild — published by Write Proxy on model change,
	// consumed by the Offline Pipeline on the master.
	PublishRebuild(ctx context.Context, event RebuildEvent) error
	SubscribeRebuild(ctx context.Context, storeID string, handler func(RebuildEvent)) (Subscription, error)

	// Delta — published by the master Compute Tier after applying a batch of
	// ReadChanges deltas, consumed by replica Check Gateways.
	PublishDelta(ctx context.Context, event DeltaEvent) error
	SubscribeDelta(ctx context.Context, storeID string, handler func(DeltaEvent)) (Subscription, error)

	// Promotion — published by admin API, consumed by all instances.
	PublishPromotion(ctx context.Context, event PromotionEvent) error
	SubscribePromotion(ctx context.Context, handler func(PromotionEvent)) (Subscription, error)

	Close() error
}

// Subscription represents an active subscription. Call Cancel to unsubscribe.
type Subscription interface {
	Cancel() error
}

// RotationEvent signals that a new epoch's shards are ready in object storage.
type RotationEvent struct {
	StoreID     string    `json:"store_id"`
	Epoch       uint64    `json:"epoch"`
	Types       []string  `json:"types"` // object types in this epoch
	PublishedAt time.Time `json:"published_at"`
}

// RebuildEvent triggers a full index rebuild for a store.
type RebuildEvent struct {
	StoreID     string    `json:"store_id"`
	ModelID     string    `json:"model_id"`
	TriggeredAt time.Time `json:"triggered_at"`
}

// DeltaEvent carries a batch of incremental tuple changes from the master
// Compute Tier to replica Check Gateways.
type DeltaEvent struct {
	StoreID   string               `json:"store_id"`
	Epoch     uint64               `json:"epoch"`
	Changes   []client.TupleChange `json:"changes"`
	AppliedAt time.Time            `json:"applied_at"`
}

// PromotionEvent signals that a replica has been promoted to master.
type PromotionEvent struct {
	ClusterID  string    `json:"cluster_id"`
	InstanceID string    `json:"instance_id"`
	PromotedAt time.Time `json:"promoted_at"`
}
