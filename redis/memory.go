package redis

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type memoryStore struct {
	mu             sync.RWMutex
	shards         map[string][]byte
	tokens         map[string]string
	masterCluster  string
	masterInstance string
}

// NewMemoryStore returns a Store backed entirely by in-memory maps.
// Suitable for unit tests; not safe across process restarts.
func NewMemoryStore() Store {
	return &memoryStore{
		shards: make(map[string][]byte),
		tokens: make(map[string]string),
	}
}

func shardKey(storeID, objectType string, epoch uint64) string {
	return fmt.Sprintf("shard:%s:%s:%d", storeID, objectType, epoch)
}

func (s *memoryStore) CacheShard(_ context.Context, storeID, objectType string, epoch uint64, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	s.shards[shardKey(storeID, objectType, epoch)] = cp
	return nil
}

func (s *memoryStore) LoadCachedShard(_ context.Context, storeID, objectType string, epoch uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, ok := s.shards[shardKey(storeID, objectType, epoch)]
	if !ok {
		return nil, nil
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp, nil
}

func (s *memoryStore) SetWatchToken(_ context.Context, storeID, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokens[storeID] = token
	return nil
}

func (s *memoryStore) GetWatchToken(_ context.Context, storeID string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tokens[storeID], nil
}

func (s *memoryStore) SetMaster(_ context.Context, clusterID, instanceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.masterCluster = clusterID
	s.masterInstance = instanceID
	return nil
}

func (s *memoryStore) GetMaster(_ context.Context) (string, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.masterCluster, s.masterInstance, nil
}

func (s *memoryStore) RefreshHeartbeat(_ context.Context, _ string, _ time.Duration) error {
	return nil
}

func (s *memoryStore) WatchHeartbeat(_ context.Context, _ string) (<-chan struct{}, error) {
	ch := make(chan struct{})
	return ch, nil
}
