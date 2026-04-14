// Package manager owns per-store goroutine lifecycle.
// For each registered store it creates and manages one Pipeline, one
// ComputeTier, and one in-memory Index.
package manager

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/compute"
	"github.com/yourorg/openfga-indexer/index"
	"github.com/yourorg/openfga-indexer/messaging"
	"github.com/yourorg/openfga-indexer/pipeline"
	leopardredis "github.com/yourorg/openfga-indexer/redis"
	leopardstorage "github.com/yourorg/openfga-indexer/storage"
	"github.com/yourorg/openfga-indexer/stringtable"
)

// Config holds dependencies injected into the Manager.
type Config struct {
	FGA      client.FGAClient
	Redis    leopardredis.Store
	Storage  leopardstorage.ShardStore
	StrTable stringtable.Store
	Bus      messaging.MessageBus
}

// StoreStatus describes the current state of a store's index.
type StoreStatus struct {
	StoreID      string
	CurrentEpoch uint64
	ShardAge     time.Duration
	Role         string // "master" | "replica" | "degraded"
}

type storeEntry struct {
	idx      index.Index
	pipeline *pipeline.Pipeline
	compute  *compute.ComputeTier
	cancel   context.CancelFunc
	epoch    uint64
	builtAt  time.Time
}

// Manager owns one (index, pipeline, compute) set per registered store.
type Manager struct {
	cfg    Config
	mu     sync.RWMutex
	stores map[string]*storeEntry
}

// New creates a Manager.
func New(cfg Config) *Manager {
	return &Manager{cfg: cfg, stores: make(map[string]*storeEntry)}
}

// RegisterStore creates index, pipeline, and compute tier for storeID.
func (m *Manager) RegisterStore(ctx context.Context, storeID string, indexedTypes []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.stores[storeID]; ok {
		return fmt.Errorf("manager.RegisterStore: store %q already registered", storeID)
	}

	idx := index.New()
	pline := pipeline.New(m.cfg.FGA, m.cfg.StrTable, m.cfg.Storage, m.cfg.Bus, pipeline.Options{
		StoreID:      storeID,
		IndexedTypes: indexedTypes,
	})
	tier := compute.New(m.cfg.FGA, idx, m.cfg.Redis, m.cfg.Bus, compute.Options{
		StoreID: storeID,
	})

	storeCtx, cancel := context.WithCancel(ctx)
	go tier.Run(storeCtx) //nolint:errcheck

	m.stores[storeID] = &storeEntry{
		idx:      idx,
		pipeline: pline,
		compute:  tier,
		cancel:   cancel,
	}
	return nil
}

// DeregisterStore stops all goroutines for storeID and removes it.
func (m *Manager) DeregisterStore(_ context.Context, storeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.stores[storeID]
	if !ok {
		return fmt.Errorf("manager.DeregisterStore: store %q not found", storeID)
	}
	entry.cancel()
	delete(m.stores, storeID)
	return nil
}

// TriggerRebuild runs the Offline Pipeline for storeID synchronously.
func (m *Manager) TriggerRebuild(ctx context.Context, storeID string) (uint64, error) {
	m.mu.RLock()
	entry, ok := m.stores[storeID]
	m.mu.RUnlock()
	if !ok {
		return 0, fmt.Errorf("manager.TriggerRebuild: store %q not found", storeID)
	}
	epoch, err := entry.pipeline.Run(ctx)
	if err != nil {
		return 0, err
	}
	m.mu.Lock()
	entry.epoch = epoch
	entry.builtAt = time.Now()
	m.mu.Unlock()
	return epoch, nil
}

// Index returns the in-memory index for storeID.
func (m *Manager) Index(storeID string) (index.Index, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.stores[storeID]
	if !ok {
		return nil, fmt.Errorf("manager.Index: store %q not found", storeID)
	}
	return entry.idx, nil
}

// StoreStatus returns the current health of a store's index.
func (m *Manager) StoreStatus(_ context.Context, storeID string) (StoreStatus, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.stores[storeID]
	if !ok {
		return StoreStatus{}, fmt.Errorf("manager.StoreStatus: store %q not found", storeID)
	}
	age := time.Duration(0)
	if !entry.builtAt.IsZero() {
		age = time.Since(entry.builtAt)
	}
	return StoreStatus{
		StoreID:      storeID,
		CurrentEpoch: entry.epoch,
		ShardAge:     age,
		Role:         "master", // overridden by admin package based on Redis role key
	}, nil
}
