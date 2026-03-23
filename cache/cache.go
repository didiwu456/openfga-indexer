// Package cache implements the serving layer of the Leopard system.
//
// A Cache holds the currently active index shard and exposes it for concurrent
// reads.  The offline builder calls Rotate to atomically swap in a new shard;
// the watcher calls Index to obtain the live shard it should apply deltas to.
//
// Thread-safety contract:
//
//   - Rotate and Index are safe for concurrent use via an atomic pointer swap.
//   - SetWatchToken / WatchToken are protected by a mutex; they carry the
//     ReadChanges continuation token the watcher should resume from after a
//     shard rotation.
package cache

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/yourorg/openfga-indexer/index"
)

// Cache is the central shard store.
type Cache struct {
	// shard holds a *index.Index as an unsafe pointer for atomic swap.
	shard unsafe.Pointer // *index.Index

	mu         sync.Mutex
	watchToken string // continuation token for the watcher post-rotation
	tokenReady chan struct{} // closed & replaced each time a new token is set
}

// New returns an empty Cache. The first call to Rotate populates the shard.
func New() *Cache {
	c := &Cache{
		tokenReady: make(chan struct{}),
	}
	// Seed with an empty index so callers never get a nil shard.
	empty := index.New()
	atomic.StorePointer(&c.shard, unsafe.Pointer(&empty))
	return c
}

// Index returns the currently active shard.  Callers may read from it freely;
// it will not be mutated under them (deltas are applied to the same pointer).
func (c *Cache) Index() index.Index {
	p := atomic.LoadPointer(&c.shard)
	return *(*index.Index)(p)
}

// Rotate atomically replaces the active shard with newShard.
// The watcher detects this and re-anchors to the new token via WatchTokenCh.
func (c *Cache) Rotate(newShard index.Index) {
	atomic.StorePointer(&c.shard, unsafe.Pointer(&newShard))
}

// SetWatchToken stores the ReadChanges continuation token that the watcher
// should use after the next shard rotation, and signals WatchTokenCh.
func (c *Cache) SetWatchToken(token string) {
	c.mu.Lock()
	c.watchToken = token
	old := c.tokenReady
	c.tokenReady = make(chan struct{})
	c.mu.Unlock()
	close(old) // wake anyone waiting on the previous channel
}

// WatchToken returns the latest continuation token set by the builder, plus
// the channel that will be closed when the next token arrives.
func (c *Cache) WatchToken() (token string, ready <-chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.watchToken, c.tokenReady
}

// Snapshot returns a point-in-time debug snapshot of the active shard.
func (c *Cache) Snapshot() index.Snapshot {
	return c.Index().Snapshot()
}
