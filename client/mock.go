package client

import (
	"context"
	"fmt"
)

// MockClient is an in-memory FGAClient for use in unit tests.
// It stores tuples as a simple slice and supports manual injection of
// ReadChanges pages.
type MockClient struct {
	tuples      []TupleChange
	allTuples   []Tuple             // pre-loaded for ReadTuples (offline build)
	allowed     map[string]bool     // "user|relation|object" → allowed
	objects     map[string][]string // "user|relation|type" → objects
}

// NewMock creates an empty MockClient.
func NewMock() *MockClient {
	return &MockClient{
		allowed: make(map[string]bool),
		objects: make(map[string][]string),
	}
}

// AllowCheck pre-programs a Check response.
func (m *MockClient) AllowCheck(user, relation, object string, allowed bool) {
	m.allowed[checkKey(user, relation, object)] = allowed
}

// AddObjects pre-programs a ListObjects response.
func (m *MockClient) AddObjects(user, relation, objType string, objs []string) {
	m.objects[listKey(user, relation, objType)] = objs
}

// PushChange appends a TupleChange to the ReadChanges queue.
func (m *MockClient) PushChange(tc TupleChange) {
	m.tuples = append(m.tuples, tc)
}

// AddTuple pre-loads a Tuple for ReadTuples (simulates a full snapshot).
func (m *MockClient) AddTuple(t Tuple) {
	m.allTuples = append(m.allTuples, t)
}

func (m *MockClient) ReadTuples(_ context.Context, _ string) ([]Tuple, error) {
	return append([]Tuple(nil), m.allTuples...), nil
}

func (m *MockClient) Check(_ context.Context, req CheckRequest) (bool, error) {
	v, ok := m.allowed[checkKey(req.User, req.Relation, req.Object)]
	if !ok {
		return false, nil
	}
	return v, nil
}

func (m *MockClient) ListObjects(_ context.Context, req ListObjectsRequest) ([]string, error) {
	return m.objects[listKey(req.User, req.Relation, req.Type)], nil
}

func (m *MockClient) ReadChanges(_ context.Context, _, continuationToken string) ([]TupleChange, string, error) {
	if len(m.tuples) == 0 {
		// No more changes; signal end-of-stream with an empty token.
		return nil, "", nil
	}
	changes := append([]TupleChange(nil), m.tuples...)
	m.tuples = m.tuples[:0] // consume
	return changes, "", nil
}

func checkKey(user, relation, object string) string {
	return fmt.Sprintf("%s|%s|%s", user, relation, object)
}

func listKey(user, relation, objType string) string {
	return fmt.Sprintf("%s|%s|%s", user, relation, objType)
}
