package stringtable

import (
	"context"
	"sync"
	"sync/atomic"
)

type memStore struct {
	mu      sync.Mutex
	fwd     map[string]map[string]uint32 // storeID → value → id
	counter uint32
	epochs  map[string]uint64
}

// NewInMemoryStore returns a Store backed entirely by in-memory maps.
// Suitable for unit tests; IDs are stable within a single process lifetime.
func NewInMemoryStore() Store {
	return &memStore{
		fwd:    make(map[string]map[string]uint32),
		epochs: make(map[string]uint64),
	}
}

func (s *memStore) Intern(_ context.Context, storeID, value string) (uint32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fwd[storeID] == nil {
		s.fwd[storeID] = make(map[string]uint32)
	}
	if id, ok := s.fwd[storeID][value]; ok {
		return id, nil
	}
	id := atomic.AddUint32(&s.counter, 1) - 1
	s.fwd[storeID][value] = id
	return id, nil
}

func (s *memStore) BulkIntern(ctx context.Context, storeID string, values []string) (map[string]uint32, error) {
	result := make(map[string]uint32, len(values))
	for _, v := range values {
		id, err := s.Intern(ctx, storeID, v)
		if err != nil {
			return nil, err
		}
		result[v] = id
	}
	return result, nil
}

func (s *memStore) LoadAll(_ context.Context, storeID string) (*LocalTable, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.fwd[storeID]
	if m == nil {
		return NewLocalTable(map[string]uint32{}), nil
	}
	cp := make(map[string]uint32, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return NewLocalTable(cp), nil
}

func (s *memStore) NextEpoch(_ context.Context, storeID string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.epochs[storeID]++
	return s.epochs[storeID], nil
}
