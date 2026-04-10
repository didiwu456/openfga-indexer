// Package storage provides durable shard storage for the Offline Pipeline.
// Shards are written once per epoch and read by all cluster instances.
package storage

import "context"

// ShardStore stores serialized roaring bitmap index shards identified by
// (storeID, objectType, epoch). Implementations must be safe for concurrent use.
type ShardStore interface {
	WriteShard(ctx context.Context, storeID, objectType string, epoch uint64, data []byte) error
	ReadShard(ctx context.Context, storeID, objectType string, epoch uint64) ([]byte, error)
	DeleteShard(ctx context.Context, storeID, objectType string, epoch uint64) error
	// ListEpochs returns all stored epoch numbers for the given (storeID, objectType),
	// sorted ascending.
	ListEpochs(ctx context.Context, storeID, objectType string) ([]uint64, error)
}
