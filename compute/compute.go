// Package compute implements the Live Index Compute Tier.
package compute

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/index"
	"github.com/yourorg/openfga-indexer/messaging"
	leopardredis "github.com/yourorg/openfga-indexer/redis"
)

// DeltaPublisher is the subset of messaging.MessageBus used by ComputeTier.
type DeltaPublisher interface {
	PublishDelta(ctx context.Context, event messaging.DeltaEvent) error
}

// Options configures the ComputeTier.
type Options struct {
	StoreID      string
	Epoch        uint64
	PollInterval time.Duration
	Logger       *slog.Logger
}

func (o *Options) applyDefaults() {
	if o.PollInterval == 0 {
		o.PollInterval = 5 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// ComputeTier polls ReadChanges and maintains the live index.
type ComputeTier struct {
	fga   client.FGAClient
	idx   index.Index
	redis leopardredis.Store
	bus   DeltaPublisher
	opts  Options
}

// New creates a ComputeTier.
func New(
	fga client.FGAClient,
	idx index.Index,
	redis leopardredis.Store,
	bus DeltaPublisher,
	opts Options,
) *ComputeTier {
	opts.applyDefaults()
	return &ComputeTier{fga: fga, idx: idx, redis: redis, bus: bus, opts: opts}
}

// Run polls ReadChanges until ctx is cancelled.
func (ct *ComputeTier) Run(ctx context.Context) error {
	log := ct.opts.Logger.With("store", ct.opts.StoreID)
	log.Info("compute tier starting")

	token, err := ct.redis.GetWatchToken(ctx, ct.opts.StoreID)
	if err != nil {
		log.Warn("could not load watch token from Redis, starting from beginning", "err", err)
		token = ""
	}

	backoff := ct.opts.PollInterval
	const maxBackoff = 60 * time.Second

	for {
		if stored, err := ct.redis.GetWatchToken(ctx, ct.opts.StoreID); err == nil && stored != "" && stored != token {
			log.Info("re-anchoring to new watch token", "old", token, "new", stored)
			token = stored
		}

		changes, next, err := ct.fga.ReadChanges(ctx, ct.opts.StoreID, token)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Error("ReadChanges failed", "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
				backoff = minDuration(backoff*2, maxBackoff)
				continue
			}
		}
		backoff = ct.opts.PollInterval

		if len(changes) > 0 {
			ct.applyChanges(ctx, changes)
			token = next
		} else {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(ct.opts.PollInterval):
			}
		}
	}
}

func (ct *ComputeTier) applyChanges(ctx context.Context, changes []client.TupleChange) {
	log := ct.opts.Logger.With("store", ct.opts.StoreID)

	for _, ch := range changes {
		switch ch.Operation {
		case client.OperationWrite:
			ct.idx.ApplyTupleWrite(ch.User, ch.Relation, ch.Object)
		case client.OperationDelete:
			ct.idx.ApplyTupleDelete(ch.User, ch.Relation, ch.Object)
		}
	}

	event := messaging.DeltaEvent{
		StoreID:   ct.opts.StoreID,
		Epoch:     ct.opts.Epoch,
		Changes:   changes,
		AppliedAt: time.Now(),
	}
	if err := ct.bus.PublishDelta(ctx, event); err != nil {
		log.Warn("failed to publish DeltaEvent", "err", err)
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// UpdateEpoch updates the epoch used in DeltaEvent after a pipeline rotation.
func (ct *ComputeTier) UpdateEpoch(epoch uint64) {
	ct.opts.Epoch = epoch
}

// UpdateToken persists a new watch token in Redis after a pipeline rotation.
func (ct *ComputeTier) UpdateToken(ctx context.Context, token string) error {
	if err := ct.redis.SetWatchToken(ctx, ct.opts.StoreID, token); err != nil {
		return fmt.Errorf("compute.UpdateToken: %w", err)
	}
	return nil
}
