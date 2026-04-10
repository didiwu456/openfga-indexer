// Package redisstream implements messaging.MessageBus using Redis Streams.
// Consumer groups with XACK provide at-least-once delivery.
package redisstream

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yourorg/openfga-indexer/messaging"
)

// Config holds Redis Streams settings.
type Config struct {
	Addr       string // e.g. "redis:6379"
	Password   string
	DB         int
	ConsumerID string // unique per instance
	GroupID    string // consumer group, unique per cluster
}

type rsBus struct {
	client *redis.Client
	cfg    Config
}

// New returns a MessageBus backed by Redis Streams.
func New(cfg Config) (messaging.MessageBus, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("redisstream.New: ping: %w", err)
	}
	return &rsBus{client: client, cfg: cfg}, nil
}

func streamKey(name string) string { return "leopard:" + name }

func (b *rsBus) publish(ctx context.Context, stream string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("redisstream.publish: %w", err)
	}
	if err := b.client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey(stream),
		Values: map[string]any{"data": string(data)},
	}).Err(); err != nil {
		return fmt.Errorf("redisstream.publish %s: %w", stream, err)
	}
	return nil
}

func subscribe[T any](ctx context.Context, client *redis.Client, stream, group, consumerID string, handler func(T)) (messaging.Subscription, error) {
	// Ensure the consumer group exists.
	client.XGroupCreateMkStream(ctx, streamKey(stream), group, "0")

	cancelCtx, cancel := context.WithCancel(ctx)
	go func() {
		for {
			if cancelCtx.Err() != nil {
				return
			}
			msgs, err := client.XReadGroup(cancelCtx, &redis.XReadGroupArgs{
				Group:    group,
				Consumer: consumerID,
				Streams:  []string{streamKey(stream), ">"},
				Count:    10,
				Block:    2 * time.Second,
			}).Result()
			if err != nil {
				continue
			}
			for _, s := range msgs {
				for _, msg := range s.Messages {
					raw, ok := msg.Values["data"].(string)
					if !ok {
						continue
					}
					var event T
					if err := json.Unmarshal([]byte(raw), &event); err == nil {
						handler(event)
					}
					client.XAck(cancelCtx, streamKey(stream), group, msg.ID)
				}
			}
		}
	}()
	return &rsSub{cancel: cancel}, nil
}

type rsSub struct{ cancel context.CancelFunc }

func (s *rsSub) Cancel() error { s.cancel(); return nil }

func (b *rsBus) PublishRotation(ctx context.Context, e messaging.RotationEvent) error {
	return b.publish(ctx, fmt.Sprintf("rotations.%s", e.StoreID), e)
}

func (b *rsBus) SubscribeRotation(ctx context.Context, storeID string, handler func(messaging.RotationEvent)) (messaging.Subscription, error) {
	return subscribe[messaging.RotationEvent](ctx, b.client, fmt.Sprintf("rotations.%s", storeID), b.cfg.GroupID, b.cfg.ConsumerID, handler)
}

func (b *rsBus) PublishRebuild(ctx context.Context, e messaging.RebuildEvent) error {
	return b.publish(ctx, fmt.Sprintf("rebuilds.%s", e.StoreID), e)
}

func (b *rsBus) SubscribeRebuild(ctx context.Context, storeID string, handler func(messaging.RebuildEvent)) (messaging.Subscription, error) {
	return subscribe[messaging.RebuildEvent](ctx, b.client, fmt.Sprintf("rebuilds.%s", storeID), b.cfg.GroupID, b.cfg.ConsumerID, handler)
}

func (b *rsBus) PublishDelta(ctx context.Context, e messaging.DeltaEvent) error {
	return b.publish(ctx, fmt.Sprintf("deltas.%s", e.StoreID), e)
}

func (b *rsBus) SubscribeDelta(ctx context.Context, storeID string, handler func(messaging.DeltaEvent)) (messaging.Subscription, error) {
	return subscribe[messaging.DeltaEvent](ctx, b.client, fmt.Sprintf("deltas.%s", storeID), b.cfg.GroupID, b.cfg.ConsumerID, handler)
}

func (b *rsBus) PublishPromotion(ctx context.Context, e messaging.PromotionEvent) error {
	return b.publish(ctx, "promotions", e)
}

func (b *rsBus) SubscribePromotion(ctx context.Context, handler func(messaging.PromotionEvent)) (messaging.Subscription, error) {
	return subscribe[messaging.PromotionEvent](ctx, b.client, "promotions", b.cfg.GroupID, b.cfg.ConsumerID, handler)
}

func (b *rsBus) Close() error {
	return b.client.Close()
}
