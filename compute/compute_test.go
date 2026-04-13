package compute_test

import (
	"context"
	"testing"
	"time"

	"github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/compute"
	"github.com/yourorg/openfga-indexer/index"
	"github.com/yourorg/openfga-indexer/messaging"
	leopardredis "github.com/yourorg/openfga-indexer/redis"
)

type mockFGA struct {
	changes []client.TupleChange
	token   string
	called  int
}

func (m *mockFGA) ReadChanges(_ context.Context, _, _ string) ([]client.TupleChange, string, error) {
	m.called++
	if m.called == 1 && len(m.changes) > 0 {
		return m.changes, m.token, nil
	}
	return nil, m.token, nil
}
func (m *mockFGA) Check(_ context.Context, _ client.CheckRequest) (bool, error) { return false, nil }
func (m *mockFGA) ListObjects(_ context.Context, _ client.ListObjectsRequest) ([]string, error) {
	return nil, nil
}
func (m *mockFGA) ReadTuples(_ context.Context, _ string) ([]client.Tuple, error) { return nil, nil }

type capturedDeltas struct {
	events []messaging.DeltaEvent
}

func (c *capturedDeltas) PublishDelta(_ context.Context, e messaging.DeltaEvent) error {
	c.events = append(c.events, e)
	return nil
}

func TestComputeTier_AppliesDeltasToIndex(t *testing.T) {
	fga := &mockFGA{
		changes: []client.TupleChange{
			{User: "user:alice", Relation: "member", Object: "group:eng", Operation: client.OperationWrite},
		},
		token: "tok1",
	}
	idx := index.New()
	rstore := leopardredis.NewMemoryStore()
	captured := &capturedDeltas{}

	ct := compute.New(fga, idx, rstore, captured, compute.Options{
		StoreID:      "store1",
		Epoch:        1,
		PollInterval: 10 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = ct.Run(ctx)

	if !idx.IsMember("user:alice", "group:eng") {
		t.Fatal("alice should be in group:eng after delta applied")
	}

	if len(captured.events) == 0 {
		t.Fatal("expected at least one DeltaEvent published")
	}
	if captured.events[0].StoreID != "store1" {
		t.Fatalf("DeltaEvent.StoreID = %q, want %q", captured.events[0].StoreID, "store1")
	}
}

func TestComputeTier_ReanchorsOnRotation(t *testing.T) {
	fga := &mockFGA{token: "old-token"}
	idx := index.New()
	rstore := leopardredis.NewMemoryStore()
	captured := &capturedDeltas{}

	ct := compute.New(fga, idx, rstore, captured, compute.Options{
		StoreID:      "store1",
		Epoch:        1,
		PollInterval: 10 * time.Millisecond,
	})

	_ = rstore.SetWatchToken(context.Background(), "store1", "new-token")

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = ct.Run(ctx)
}
