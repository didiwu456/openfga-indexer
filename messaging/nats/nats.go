// Package nats implements messaging.MessageBus using NATS JetStream.
// Streams are created automatically if they do not exist.
package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/yourorg/openfga-indexer/messaging"
)

const (
	streamRotations = "LEOPARD_ROTATIONS"
	streamRebuilds  = "LEOPARD_REBUILDS"
	streamDeltas    = "LEOPARD_DELTAS"
	streamPromos    = "LEOPARD_PROMOTIONS"
)

// Config holds NATS connection settings.
type Config struct {
	URL        string // e.g. "nats://nats:4222"
	ConsumerID string // unique per instance, used for durable consumers
}

type natsBus struct {
	conn *gonats.Conn
	js   jetstream.JetStream
	cfg  Config
}

// New connects to NATS and returns a MessageBus.
// Creates the required JetStream streams if they don't exist.
func New(cfg Config) (messaging.MessageBus, error) {
	nc, err := gonats.Connect(cfg.URL,
		gonats.RetryOnFailedConnect(true),
		gonats.MaxReconnects(-1),
		gonats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("nats.New: connect: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("nats.New: jetstream: %w", err)
	}
	bus := &natsBus{conn: nc, js: js, cfg: cfg}
	if err := bus.ensureStreams(context.Background()); err != nil {
		nc.Close()
		return nil, err
	}
	return bus, nil
}

func (b *natsBus) ensureStreams(ctx context.Context) error {
	streams := []jetstream.StreamConfig{
		{Name: streamRotations, Subjects: []string{"leopard.rotation.>"}, Retention: jetstream.WorkQueuePolicy},
		{Name: streamRebuilds, Subjects: []string{"leopard.rebuild.>"}, Retention: jetstream.WorkQueuePolicy},
		{Name: streamDeltas, Subjects: []string{"leopard.delta.>"}, Retention: jetstream.LimitsPolicy, MaxAge: time.Hour},
		{Name: streamPromos, Subjects: []string{"leopard.promotion"}, Retention: jetstream.WorkQueuePolicy},
	}
	for _, sc := range streams {
		if _, err := b.js.CreateOrUpdateStream(ctx, sc); err != nil {
			return fmt.Errorf("nats.ensureStreams: %s: %w", sc.Name, err)
		}
	}
	return nil
}

func (b *natsBus) publish(ctx context.Context, subject string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("nats.publish: marshal: %w", err)
	}
	if _, err := b.js.Publish(ctx, subject, data); err != nil {
		return fmt.Errorf("nats.publish %s: %w", subject, err)
	}
	return nil
}

func subscribe[T any](ctx context.Context, js jetstream.JetStream, stream, subject, consumerID string, handler func(T)) (messaging.Subscription, error) {
	cons, err := js.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
		Name:          consumerID,
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverNewPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("nats.subscribe: create consumer: %w", err)
	}
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		var event T
		if err := json.Unmarshal(msg.Data(), &event); err == nil {
			handler(event)
		}
		_ = msg.Ack()
	})
	if err != nil {
		return nil, fmt.Errorf("nats.subscribe: consume: %w", err)
	}
	return &natsSub{cc: cc}, nil
}

type natsSub struct{ cc jetstream.ConsumeContext }

func (s *natsSub) Cancel() error { s.cc.Stop(); return nil }

func (b *natsBus) PublishRotation(ctx context.Context, e messaging.RotationEvent) error {
	return b.publish(ctx, fmt.Sprintf("leopard.rotation.%s", e.StoreID), e)
}

func (b *natsBus) SubscribeRotation(ctx context.Context, storeID string, handler func(messaging.RotationEvent)) (messaging.Subscription, error) {
	return subscribe[messaging.RotationEvent](ctx, b.js, streamRotations,
		fmt.Sprintf("leopard.rotation.%s", storeID),
		fmt.Sprintf("%s-rotation-%s", b.cfg.ConsumerID, storeID), handler)
}

func (b *natsBus) PublishRebuild(ctx context.Context, e messaging.RebuildEvent) error {
	return b.publish(ctx, fmt.Sprintf("leopard.rebuild.%s", e.StoreID), e)
}

func (b *natsBus) SubscribeRebuild(ctx context.Context, storeID string, handler func(messaging.RebuildEvent)) (messaging.Subscription, error) {
	return subscribe[messaging.RebuildEvent](ctx, b.js, streamRebuilds,
		fmt.Sprintf("leopard.rebuild.%s", storeID),
		fmt.Sprintf("%s-rebuild-%s", b.cfg.ConsumerID, storeID), handler)
}

func (b *natsBus) PublishDelta(ctx context.Context, e messaging.DeltaEvent) error {
	return b.publish(ctx, fmt.Sprintf("leopard.delta.%s", e.StoreID), e)
}

func (b *natsBus) SubscribeDelta(ctx context.Context, storeID string, handler func(messaging.DeltaEvent)) (messaging.Subscription, error) {
	return subscribe[messaging.DeltaEvent](ctx, b.js, streamDeltas,
		fmt.Sprintf("leopard.delta.%s", storeID),
		fmt.Sprintf("%s-delta-%s", b.cfg.ConsumerID, storeID), handler)
}

func (b *natsBus) PublishPromotion(ctx context.Context, e messaging.PromotionEvent) error {
	return b.publish(ctx, "leopard.promotion", e)
}

func (b *natsBus) SubscribePromotion(ctx context.Context, handler func(messaging.PromotionEvent)) (messaging.Subscription, error) {
	return subscribe[messaging.PromotionEvent](ctx, b.js, streamPromos,
		"leopard.promotion",
		fmt.Sprintf("%s-promotion", b.cfg.ConsumerID), handler)
}

func (b *natsBus) Close() error {
	b.conn.Close()
	return nil
}
