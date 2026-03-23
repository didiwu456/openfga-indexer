// Package watcher polls OpenFGA's ReadChanges endpoint and applies tuple
// deltas to the active index shard held in a cache.Cache.
//
// When the offline builder rotates a new shard in, it also calls
// cache.SetWatchToken with the ReadChanges high-water mark at the time of the
// snapshot.  The watcher detects the token update and re-anchors its poll
// loop to that position so deltas are never double-applied or skipped.
package watcher

import (
	"context"
	"log/slog"
	"time"

	"github.com/yourorg/openfga-indexer/cache"
	"github.com/yourorg/openfga-indexer/client"
)

// Options configures Watcher behaviour.
type Options struct {
	// StoreID is the OpenFGA store to watch.
	StoreID string

	// PollInterval is how long to wait between ReadChanges calls when
	// there are no new changes. Defaults to 5 seconds.
	PollInterval time.Duration

	// Logger receives structured log output. Defaults to slog.Default().
	Logger *slog.Logger
}

func (o *Options) applyDefaults() {
	if o.PollInterval == 0 {
		o.PollInterval = 5 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// Watcher polls ReadChanges and feeds deltas into the cache's active shard.
type Watcher struct {
	client client.FGAClient
	cache  *cache.Cache
	opts   Options
}

// New creates a Watcher. Call Run to start it.
func New(c client.FGAClient, ca *cache.Cache, opts Options) *Watcher {
	opts.applyDefaults()
	return &Watcher{client: c, cache: ca, opts: opts}
}

// Run polls ReadChanges in a loop until ctx is cancelled.
// It watches for token updates from the builder and re-anchors automatically.
//
//	go watcher.Run(ctx)
func (w *Watcher) Run(ctx context.Context) error {
	log := w.opts.Logger.With("store", w.opts.StoreID)
	log.Info("watcher starting")

	token, tokenReady := w.cache.WatchToken()

	for {
		// Check if the builder has published a new continuation token
		// (i.e., a shard rotation just happened).
		select {
		case <-tokenReady:
			token, tokenReady = w.cache.WatchToken()
			log.Info("watcher re-anchored after shard rotation", "token", token)
		default:
		}

		changes, next, err := w.client.ReadChanges(ctx, w.opts.StoreID, token)
		if err != nil {
			if ctx.Err() != nil {
				log.Info("watcher stopping", "reason", ctx.Err())
				return nil
			}
			log.Error("ReadChanges failed", "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(w.opts.PollInterval):
				continue
			}
		}

		if len(changes) > 0 {
			log.Debug("applying changes", "count", len(changes))
			idx := w.cache.Index()
			for _, ch := range changes {
				switch ch.Operation {
				case client.OperationWrite:
					idx.ApplyTupleWrite(ch.User, ch.Relation, ch.Object)
				case client.OperationDelete:
					idx.ApplyTupleDelete(ch.User, ch.Relation, ch.Object)
				}
			}
			token = next
		} else {
			select {
			case <-ctx.Done():
				log.Info("watcher stopping", "reason", ctx.Err())
				return nil
			case <-tokenReady:
				token, tokenReady = w.cache.WatchToken()
				log.Info("watcher re-anchored after shard rotation", "token", token)
			case <-time.After(w.opts.PollInterval):
			}
		}
	}
}
