// Package pipeline implements the Offline Index Building Pipeline.
package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/messaging"
	"github.com/yourorg/openfga-indexer/storage"
	"github.com/yourorg/openfga-indexer/stringtable"
)

// Options configures the Pipeline.
type Options struct {
	StoreID      string
	IndexedTypes []string
	Logger       *slog.Logger
}

func (o *Options) applyDefaults() {
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// Pipeline performs full offline index rebuilds for one FGA store.
type Pipeline struct {
	fga     client.FGAClient
	stbl    stringtable.Store
	storage storage.ShardStore
	bus     messaging.MessageBus
	opts    Options
	mu      sync.Mutex
	pending chan struct{}
}

// New creates a Pipeline.
func New(
	fga client.FGAClient,
	stbl stringtable.Store,
	stor storage.ShardStore,
	bus messaging.MessageBus,
	opts Options,
) *Pipeline {
	opts.applyDefaults()
	return &Pipeline{
		fga:     fga,
		stbl:    stbl,
		storage: stor,
		bus:     bus,
		opts:    opts,
		pending: make(chan struct{}, 1),
	}
}

// Run performs one full build and returns the new epoch number.
func (p *Pipeline) Run(ctx context.Context) (uint64, error) {
	select {
	case p.pending <- struct{}{}:
	default:
	}
	<-p.pending
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.runLocked(ctx)
}

func (p *Pipeline) runLocked(ctx context.Context) (uint64, error) {
	log := p.opts.Logger.With("store", p.opts.StoreID)
	log.Info("offline pipeline starting")
	start := time.Now()

	epoch, err := p.stbl.NextEpoch(ctx, p.opts.StoreID)
	if err != nil {
		return 0, fmt.Errorf("pipeline.Run: next epoch: %w", err)
	}

	tuples, err := p.fga.ReadTuples(ctx, p.opts.StoreID)
	if err != nil {
		return 0, fmt.Errorf("pipeline.Run: ReadTuples: %w", err)
	}

	indexed := make(map[string][]client.Tuple)
	for _, t := range tuples {
		ot := objectType(t.Object)
		for _, it := range p.opts.IndexedTypes {
			if ot == it {
				indexed[ot] = append(indexed[ot], t)
				break
			}
		}
	}

	var writtenTypes []string
	var rollback []func()

	for _, objType := range p.opts.IndexedTypes {
		typeTuples := indexed[objType]

		var values []string
		for _, t := range typeTuples {
			values = append(values, t.User, t.Object)
		}
		ids, err := p.stbl.BulkIntern(ctx, p.opts.StoreID, values)
		if err != nil {
			p.rollbackShards(ctx, epoch, rollback)
			return 0, fmt.Errorf("pipeline.Run: BulkIntern type %s: %w", objType, err)
		}

		m2g := make(map[uint32]*roaring.Bitmap)
		g2g := make(map[uint32]*roaring.Bitmap)

		for _, t := range typeTuples {
			if t.Relation != "member" {
				continue
			}
			userID := ids[t.User]
			objID := ids[t.Object]
			bm := m2g[userID]
			if bm == nil {
				bm = roaring.New()
				m2g[userID] = bm
			}
			bm.Add(objID)
			g := g2g[objID]
			if g == nil {
				g = roaring.New()
				g2g[objID] = g
			}
			g.Add(objID)
		}

		data, err := serializeShard(m2g, g2g)
		if err != nil {
			p.rollbackShards(ctx, epoch, rollback)
			return 0, fmt.Errorf("pipeline.Run: serialize type %s: %w", objType, err)
		}

		if err := p.storage.WriteShard(ctx, p.opts.StoreID, objType, epoch, data); err != nil {
			p.rollbackShards(ctx, epoch, rollback)
			return 0, fmt.Errorf("pipeline.Run: WriteShard type %s: %w", objType, err)
		}

		writtenTypes = append(writtenTypes, objType)
		typeSnapshot := objType
		rollback = append(rollback, func() {
			_ = p.storage.DeleteShard(context.Background(), p.opts.StoreID, typeSnapshot, epoch)
		})
	}

	event := messaging.RotationEvent{
		StoreID:     p.opts.StoreID,
		Epoch:       epoch,
		Types:       writtenTypes,
		PublishedAt: time.Now(),
	}
	if err := p.bus.PublishRotation(ctx, event); err != nil {
		log.Warn("failed to publish RotationEvent", "err", err)
	}

	log.Info("offline pipeline done", "epoch", epoch, "types", writtenTypes, "elapsed", time.Since(start))
	return epoch, nil
}

func (p *Pipeline) rollbackShards(ctx context.Context, epoch uint64, fns []func()) {
	for _, fn := range fns {
		fn()
	}
	p.opts.Logger.Warn("pipeline rolled back shards", "store", p.opts.StoreID, "epoch", epoch)
}

func serializeShard(m2g, g2g map[uint32]*roaring.Bitmap) ([]byte, error) {
	var buf bytes.Buffer
	for _, m := range []map[uint32]*roaring.Bitmap{m2g, g2g} {
		count := uint32(len(m))
		if err := writeUint32(&buf, count); err != nil {
			return nil, err
		}
		for k, bm := range m {
			if err := writeUint32(&buf, k); err != nil {
				return nil, err
			}
			if _, err := bm.WriteTo(&buf); err != nil {
				return nil, err
			}
		}
	}
	return buf.Bytes(), nil
}

func writeUint32(buf *bytes.Buffer, v uint32) error {
	b := [4]byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
	_, err := buf.Write(b[:])
	return err
}

func objectType(object string) string {
	if i := strings.Index(object, ":"); i > 0 {
		return object[:i]
	}
	return object
}
