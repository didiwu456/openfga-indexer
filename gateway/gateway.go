// Package gateway implements the three-tier Check/ListObjects lookup:
//
//	L1: in-memory roaring bitmap index (process-local, zero latency)
//	L2: Redis L2 cache (cluster-local, low latency)
//	L3: FGA API (network, authoritative fallback)
//
// FGA calls are protected by a circuit breaker (github.com/sony/gobreaker).
package gateway

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/sony/gobreaker"
	"github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/index"
	leopardredis "github.com/yourorg/openfga-indexer/redis"
	leopardstorage "github.com/yourorg/openfga-indexer/storage"
	"github.com/yourorg/openfga-indexer/stringtable"
)

// Config configures the Gateway.
type Config struct {
	StoreID             string
	IndexedTypes        []string      // object types served from the index
	ModelID             string        // FGA model ID; empty = use latest
	FGAFailureThreshold uint32        // consecutive failures before circuit opens (default 5)
	FGAHalfOpenTimeout  time.Duration // time before circuit probes again (default 10s)
}

func (c *Config) applyDefaults() {
	if c.FGAFailureThreshold == 0 {
		c.FGAFailureThreshold = 5
	}
	if c.FGAHalfOpenTimeout == 0 {
		c.FGAHalfOpenTimeout = 10 * time.Second
	}
}

// Gateway serves Check and ListObjects via L1→L2→L3.
type Gateway struct {
	cfg     Config
	idx     index.Index
	redis   leopardredis.Store
	storage leopardstorage.ShardStore
	stbl    stringtable.Store
	fga     client.FGAClient
	cb      *gobreaker.CircuitBreaker
	epoch   uint64 // current epoch for L2 lookups
}

// New creates a Gateway.
func New(
	cfg Config,
	idx index.Index,
	redis leopardredis.Store,
	stor leopardstorage.ShardStore,
	stbl stringtable.Store,
	fga client.FGAClient,
) *Gateway {
	cfg.applyDefaults()

	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "fga-" + cfg.StoreID,
		MaxRequests: 1,
		Interval:    60 * time.Second,
		Timeout:     cfg.FGAHalfOpenTimeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= cfg.FGAFailureThreshold
		},
	})

	return &Gateway{cfg: cfg, idx: idx, redis: redis, storage: stor, stbl: stbl, fga: fga, cb: cb}
}

// Check returns (allowed, source, error) where source is "index_l1", "index_l2", or "fga_api".
func (g *Gateway) Check(ctx context.Context, user, relation, object string) (bool, string, error) {
	if g.isIndexable(relation, object) {
		// L1: in-memory index.
		if allowed := g.idx.IsMember(user, object); allowed {
			return true, "index_l1", nil
		}
		// L1 miss — try L2 before concluding the user is not a member.
		if loaded, err := g.tryLoadL2(ctx, user, object); err == nil {
			return loaded, "index_l2", nil
		}
	}

	// L3: FGA API (circuit breaker protected).
	result, err := g.cb.Execute(func() (any, error) {
		return g.fga.Check(ctx, client.CheckRequest{
			StoreID:              g.cfg.StoreID,
			AuthorizationModelID: g.cfg.ModelID,
			User:                 user,
			Relation:             relation,
			Object:               object,
		})
	})
	if err != nil {
		return false, "fga_api", fmt.Errorf("gateway.Check: %w", err)
	}
	return result.(bool), "fga_api", nil
}

// ListObjects returns objects of objectType the user has relation to.
func (g *Gateway) ListObjects(ctx context.Context, user, relation, objectType string) ([]string, string, error) {
	if relation == "member" && g.isIndexedType(objectType) {
		objs := g.idx.GroupsForUser(user)
		if len(objs) > 0 {
			return objs, "index_l1", nil
		}
	}

	result, err := g.cb.Execute(func() (any, error) {
		return g.fga.ListObjects(ctx, client.ListObjectsRequest{
			StoreID:              g.cfg.StoreID,
			AuthorizationModelID: g.cfg.ModelID,
			User:                 user,
			Relation:             relation,
			Type:                 objectType,
		})
	})
	if err != nil {
		return nil, "fga_api", fmt.Errorf("gateway.ListObjects: %w", err)
	}
	return result.([]string), "fga_api", nil
}

// SetEpoch updates the active epoch used for L2 shard lookups.
func (g *Gateway) SetEpoch(epoch uint64) { g.epoch = epoch }

// isIndexable returns true if this check can be answered by the index.
func (g *Gateway) isIndexable(relation, object string) bool {
	if relation != "member" {
		return false
	}
	return g.isIndexedType(objectTypeOf(object))
}

func (g *Gateway) isIndexedType(ot string) bool {
	for _, t := range g.cfg.IndexedTypes {
		if t == ot {
			return true
		}
	}
	return false
}

// tryLoadL2 attempts to load the relevant shard from Redis (or object storage),
// reconstruct the membership check, and return the result.
func (g *Gateway) tryLoadL2(ctx context.Context, user, object string) (bool, error) {
	ot := objectTypeOf(object)
	data, err := g.redis.LoadCachedShard(ctx, g.cfg.StoreID, ot, g.epoch)
	if err != nil || data == nil {
		// Try object storage directly.
		data, err = g.storage.ReadShard(ctx, g.cfg.StoreID, ot, g.epoch)
		if err != nil || data == nil {
			return false, fmt.Errorf("no L2 shard available")
		}
		// Populate Redis cache for next time.
		_ = g.redis.CacheShard(ctx, g.cfg.StoreID, ot, g.epoch, data)
	}

	m2g, g2g, err := deserializeShard(data)
	if err != nil {
		return false, err
	}

	tbl, err := g.stbl.LoadAll(ctx, g.cfg.StoreID)
	if err != nil {
		return false, err
	}

	uID, ok := tbl.ID(user)
	if !ok {
		return false, nil
	}
	gID, ok := tbl.ID(object)
	if !ok {
		return false, nil
	}

	userBM := m2g[uID]
	groupBM := g2g[gID]
	if userBM == nil || groupBM == nil {
		return false, nil
	}
	return userBM.Intersects(groupBM), nil
}

// deserializeShard decodes a shard produced by pipeline.serializeShard.
func deserializeShard(data []byte) (map[uint32]*roaring.Bitmap, map[uint32]*roaring.Bitmap, error) {
	r := bytes.NewReader(data)
	maps := make([]map[uint32]*roaring.Bitmap, 2)
	for i := range maps {
		var count uint32
		if err := binary.Read(r, binary.BigEndian, &count); err != nil {
			return nil, nil, fmt.Errorf("deserialize: read count: %w", err)
		}
		m := make(map[uint32]*roaring.Bitmap, count)
		for j := uint32(0); j < count; j++ {
			var key uint32
			if err := binary.Read(r, binary.BigEndian, &key); err != nil {
				return nil, nil, fmt.Errorf("deserialize: read key: %w", err)
			}
			bm := roaring.New()
			if _, err := bm.ReadFrom(r); err != nil {
				return nil, nil, fmt.Errorf("deserialize: read bitmap: %w", err)
			}
			m[key] = bm
		}
		maps[i] = m
	}
	return maps[0], maps[1], nil
}

func objectTypeOf(object string) string {
	if i := strings.Index(object, ":"); i > 0 {
		return object[:i]
	}
	return object
}
