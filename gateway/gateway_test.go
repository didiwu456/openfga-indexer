package gateway_test

import (
	"context"
	"errors"
	"testing"

	"github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/gateway"
	"github.com/yourorg/openfga-indexer/index"
	leopardredis "github.com/yourorg/openfga-indexer/redis"
	leopardstorage "github.com/yourorg/openfga-indexer/storage"
	"github.com/yourorg/openfga-indexer/stringtable"
)

func TestGateway_L1Hit(t *testing.T) {
	idx := index.New()
	idx.ApplyTupleWrite("user:alice", "member", "group:eng")

	gw := gateway.New(gateway.Config{
		StoreID:      "s1",
		IndexedTypes: []string{"group"},
	}, idx, leopardredis.NewMemoryStore(), leopardstorage.NewMemoryStore(),
		stringtable.NewInMemoryStore(), client.NewMockClient(nil))

	allowed, source, err := gw.Check(context.Background(), "user:alice", "member", "group:eng")
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if !allowed {
		t.Fatal("expected allowed=true")
	}
	if source != "index_l1" {
		t.Fatalf("source = %q, want index_l1", source)
	}
}

func TestGateway_NonIndexedFallsToL3(t *testing.T) {
	idx := index.New()
	fga := client.NewMockClient(nil)
	fga.SetCheckResult("user:alice", "viewer", "document:readme", true)

	gw := gateway.New(gateway.Config{
		StoreID:      "s1",
		IndexedTypes: []string{"group"},
	}, idx, leopardredis.NewMemoryStore(), leopardstorage.NewMemoryStore(),
		stringtable.NewInMemoryStore(), fga)

	allowed, source, err := gw.Check(context.Background(), "user:alice", "viewer", "document:readme")
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if !allowed {
		t.Fatal("expected allowed=true from L3")
	}
	if source != "fga_api" {
		t.Fatalf("source = %q, want fga_api", source)
	}
}

func TestGateway_CircuitBreakerOpens(t *testing.T) {
	idx := index.New()
	fga := &alwaysErrorFGA{}

	gw := gateway.New(gateway.Config{
		StoreID:             "s1",
		IndexedTypes:        []string{"group"},
		FGAFailureThreshold: 3,
	}, idx, leopardredis.NewMemoryStore(), leopardstorage.NewMemoryStore(),
		stringtable.NewInMemoryStore(), fga)

	// First 3 calls open the circuit.
	for i := 0; i < 3; i++ {
		_, _, _ = gw.Check(context.Background(), "user:alice", "viewer", "doc:x")
	}
	// 4th call should fail fast with circuit open error.
	_, _, err := gw.Check(context.Background(), "user:alice", "viewer", "doc:x")
	if err == nil {
		t.Fatal("expected error when circuit is open")
	}
}

type alwaysErrorFGA struct{}

func (a *alwaysErrorFGA) Check(_ context.Context, _ client.CheckRequest) (bool, error) {
	return false, errors.New("fga unavailable")
}
func (a *alwaysErrorFGA) ListObjects(_ context.Context, _ client.ListObjectsRequest) ([]string, error) {
	return nil, errors.New("fga unavailable")
}
func (a *alwaysErrorFGA) ReadTuples(_ context.Context, _ string) ([]client.Tuple, error) {
	return nil, nil
}
func (a *alwaysErrorFGA) ReadChanges(_ context.Context, _, _ string) ([]client.TupleChange, string, error) {
	return nil, "", nil
}
func (a *alwaysErrorFGA) WriteAuthorizationModel(_ context.Context, _ string, _ []byte) (string, error) {
	return "", errors.New("fga unavailable")
}
