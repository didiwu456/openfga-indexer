package storage

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

type memoryStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewMemoryStore returns a ShardStore backed by an in-memory map.
func NewMemoryStore() ShardStore {
	return &memoryStore{data: make(map[string][]byte)}
}

func objectKey(storeID, objectType string, epoch uint64) string {
	return fmt.Sprintf("leopard/%s/%s/%d.shard", storeID, objectType, epoch)
}

func (s *memoryStore) WriteShard(_ context.Context, storeID, objectType string, epoch uint64, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	s.data[objectKey(storeID, objectType, epoch)] = cp
	return nil
}

func (s *memoryStore) ReadShard(_ context.Context, storeID, objectType string, epoch uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, ok := s.data[objectKey(storeID, objectType, epoch)]
	if !ok {
		return nil, fmt.Errorf("storage.ReadShard: shard not found (%s/%s/%d)", storeID, objectType, epoch)
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp, nil
}

func (s *memoryStore) DeleteShard(_ context.Context, storeID, objectType string, epoch uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, objectKey(storeID, objectType, epoch))
	return nil
}

func (s *memoryStore) ListEpochs(_ context.Context, storeID, objectType string) ([]uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := fmt.Sprintf("leopard/%s/%s/", storeID, objectType)
	var epochs []uint64
	for key := range s.data {
		if len(key) <= len(prefix) || key[:len(prefix)] != prefix {
			continue
		}
		var epoch uint64
		if _, err := fmt.Sscanf(key[len(prefix):], "%d.shard", &epoch); err == nil {
			epochs = append(epochs, epoch)
		}
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })
	return epochs, nil
}
