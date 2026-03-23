// Package builder implements the offline index-building component of the
// Leopard system.
//
// On a configurable schedule the Builder reads all tuples from FGA (or a
// backup snapshot), constructs a brand-new in-memory Index shard, and hands
// it to a cache.Cache via cache.Rotate.  The watcher then resumes applying
// incremental ReadChanges deltas on top of the fresh shard.
//
// Lifecycle:
//
//	builder ──(full scan)──► new shard ──► cache.Rotate ──► watcher resumes
package builder

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/yourorg/openfga-indexer/cache"
	"github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/index"
)

// Options configures Builder behaviour.
type Options struct {
	// StoreID is the OpenFGA store to read tuples from.
	StoreID string

	// RebuildInterval controls how often a fresh shard is built.
	// Defaults to 1 hour.
	RebuildInterval time.Duration

	// Logger receives structured log output. Defaults to slog.Default().
	Logger *slog.Logger
}

func (o *Options) applyDefaults() {
	if o.RebuildInterval == 0 {
		o.RebuildInterval = time.Hour
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// Builder periodically builds a full index shard from FGA tuples and rotates
// it into the cache.
type Builder struct {
	fga   client.FGAClient
	cache *cache.Cache
	opts  Options
}

// New creates a Builder. Call RunOnce to build immediately, or Run to loop.
func New(fga client.FGAClient, c *cache.Cache, opts Options) *Builder {
	opts.applyDefaults()
	return &Builder{fga: fga, cache: c, opts: opts}
}

// RunOnce performs a single full index build and rotates the result into the
// cache. It returns the continuation token from the last ReadChanges page so
// the watcher can resume from exactly this point.
func (b *Builder) RunOnce(ctx context.Context) (continuationToken string, err error) {
	log := b.opts.Logger.With("store", b.opts.StoreID)
	log.Info("offline build starting")
	start := time.Now()

	// 1. Read all current tuples (full snapshot).
	tuples, err := b.fga.ReadTuples(ctx, b.opts.StoreID)
	if err != nil {
		return "", fmt.Errorf("builder.RunOnce: ReadTuples: %w", err)
	}
	log.Info("tuples read", "count", len(tuples))

	// 2. Build a fresh shard.
	shard := index.New()
	for _, t := range tuples {
		shard.ApplyTupleWrite(t.User, t.Relation, t.Object)
	}

	// 3. Record the current ReadChanges high-water mark so the watcher knows
	//    where to resume after the shard is rotated in.
	_, token, err := b.fga.ReadChanges(ctx, b.opts.StoreID, "")
	if err != nil {
		// Non-fatal: watcher will start from the beginning.
		log.Warn("could not fetch ReadChanges token", "err", err)
		token = ""
	}

	// 4. Atomically swap the new shard into the cache.
	b.cache.Rotate(shard)
	log.Info("shard rotated", "elapsed", time.Since(start), "continuation_token", token)

	return token, nil
}

// Run calls RunOnce immediately and then repeats on opts.RebuildInterval until
// ctx is cancelled.  After each successful build it signals the cache so the
// watcher can re-anchor its continuation token via the returned channel.
func (b *Builder) Run(ctx context.Context) error {
	log := b.opts.Logger.With("store", b.opts.StoreID)

	ticker := time.NewTicker(b.opts.RebuildInterval)
	defer ticker.Stop()

	// Initial build.
	if token, err := b.RunOnce(ctx); err != nil {
		log.Error("initial build failed", "err", err)
	} else {
		b.cache.SetWatchToken(token)
	}

	for {
		select {
		case <-ctx.Done():
			log.Info("builder stopping", "reason", ctx.Err())
			return nil
		case <-ticker.C:
			token, err := b.RunOnce(ctx)
			if err != nil {
				log.Error("rebuild failed", "err", err)
				continue
			}
			b.cache.SetWatchToken(token)
		}
	}
}
