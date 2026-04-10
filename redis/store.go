// Package redis provides the cluster-local L2 cache and coordination adapter
// backed by Redis. It is NOT the authoritative store for any data.
package redis

import (
	"context"
	"time"
)

// Store is the Redis adapter interface.
// All methods must be safe for concurrent use.
type Store interface {
	// L2 shard cache — stores serialized roaring bitmap shards.
	CacheShard(ctx context.Context, storeID, objectType string, epoch uint64, data []byte) error
	LoadCachedShard(ctx context.Context, storeID, objectType string, epoch uint64) ([]byte, error)

	// Watch token — ReadChanges continuation token for the compute tier.
	SetWatchToken(ctx context.Context, storeID, token string) error
	GetWatchToken(ctx context.Context, storeID string) (string, error)

	// Master/replica coordination.
	SetMaster(ctx context.Context, clusterID, instanceID string) error
	GetMaster(ctx context.Context) (clusterID, instanceID string, err error)
	RefreshHeartbeat(ctx context.Context, clusterID string, ttl time.Duration) error
	// WatchHeartbeat returns a channel that is closed when the heartbeat for
	// clusterID expires (key deleted or TTL elapsed). Callers must drain and
	// discard the channel when done.
	WatchHeartbeat(ctx context.Context, clusterID string) (<-chan struct{}, error)
}
