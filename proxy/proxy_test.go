package proxy_test

import (
	"context"
	"testing"

	"github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/messaging"
	"github.com/yourorg/openfga-indexer/proxy"
)

type capturedRebuilds struct {
	events []messaging.RebuildEvent
}

func (c *capturedRebuilds) PublishRebuild(_ context.Context, e messaging.RebuildEvent) error {
	c.events = append(c.events, e)
	return nil
}

func TestProxy_ModelWriteTriggersRebuild(t *testing.T) {
	fga := client.NewMockClient(nil)
	fga.SetWriteModelResult("model-abc")
	cap := &capturedRebuilds{}

	p := proxy.New(fga, cap, proxy.Options{StoreID: "store1"})
	ctx := context.Background()

	modelID, err := p.WriteAuthorizationModel(ctx, "store1", []byte(`{}`))
	if err != nil {
		t.Fatalf("WriteAuthorizationModel: %v", err)
	}
	if modelID != "model-abc" {
		t.Fatalf("modelID = %q, want model-abc", modelID)
	}
	if len(cap.events) != 1 {
		t.Fatalf("expected 1 RebuildEvent, got %d", len(cap.events))
	}
	if cap.events[0].ModelID != "model-abc" {
		t.Fatalf("RebuildEvent.ModelID = %q, want model-abc", cap.events[0].ModelID)
	}
}

func TestProxy_TupleWriteNoRebuild(t *testing.T) {
	fga := client.NewMockClient(nil)
	cap := &capturedRebuilds{}
	p := proxy.New(fga, cap, proxy.Options{StoreID: "store1"})

	err := p.WriteTuples(context.Background(), "store1", []client.Tuple{
		{User: "user:alice", Relation: "member", Object: "group:eng"},
	})
	if err != nil {
		t.Fatalf("WriteTuples: %v", err)
	}
	if len(cap.events) != 0 {
		t.Fatal("tuple write should not trigger rebuild")
	}
}
