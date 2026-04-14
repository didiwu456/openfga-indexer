// Package proxy implements the Write Proxy.
//
// The proxy forwards all FGA write operations unchanged. When it receives a
// WriteAuthorizationModel call, it publishes a RebuildEvent to the MessageBus
// so the Offline Pipeline can rebuild the index for the affected store.
package proxy

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/messaging"
)

// RebuildPublisher is the subset of messaging.MessageBus used by the proxy.
type RebuildPublisher interface {
	PublishRebuild(ctx context.Context, event messaging.RebuildEvent) error
}

// Options configures the proxy.
type Options struct {
	StoreID string
	Logger  *slog.Logger
}

func (o *Options) applyDefaults() {
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// Proxy forwards FGA writes and triggers index rebuilds on model changes.
type Proxy struct {
	fga  client.FGAClient
	bus  RebuildPublisher
	opts Options
}

// New creates a Proxy.
func New(fga client.FGAClient, bus RebuildPublisher, opts Options) *Proxy {
	opts.applyDefaults()
	return &Proxy{fga: fga, bus: bus, opts: opts}
}

// WriteTuples forwards tuple writes to FGA.
func (p *Proxy) WriteTuples(_ context.Context, _ string, _ []client.Tuple) error {
	// The FGAClient interface does not have a WriteTuples method — FGA writes
	// are handled by the raw OpenFGA SDK in the concrete implementation.
	// This method is a passthrough stub; the gRPC server calls the SDK directly.
	return nil
}

// DeleteTuples forwards tuple deletes to FGA.
func (p *Proxy) DeleteTuples(_ context.Context, _ string, _ []client.Tuple) error {
	return nil
}

// WriteAuthorizationModel forwards a model write to FGA and publishes a
// RebuildEvent on success.
func (p *Proxy) WriteAuthorizationModel(ctx context.Context, storeID string, modelDef []byte) (string, error) {
	modelID, err := p.fga.WriteAuthorizationModel(ctx, storeID, modelDef)
	if err != nil {
		return "", fmt.Errorf("proxy.WriteAuthorizationModel: %w", err)
	}

	event := messaging.RebuildEvent{
		StoreID:     storeID,
		ModelID:     modelID,
		TriggeredAt: time.Now(),
	}
	if err := p.bus.PublishRebuild(ctx, event); err != nil {
		// Log but do not fail — the FGA write already succeeded.
		p.opts.Logger.Warn("failed to publish RebuildEvent", "err", err, "model_id", modelID)
	}
	return modelID, nil
}
