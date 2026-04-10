package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config holds Redis connection settings.
type Config struct {
	Addr     string // e.g. "redis:6379"
	Password string
	DB       int
}

type redisStore struct {
	client *redis.Client
}

// New returns a Store connected to Redis using the given Config.
func New(cfg Config) Store {
	return &redisStore{
		client: redis.NewClient(&redis.Options{
			Addr:     cfg.Addr,
			Password: cfg.Password,
			DB:       cfg.DB,
		}),
	}
}

func (s *redisStore) CacheShard(ctx context.Context, storeID, objectType string, epoch uint64, data []byte) error {
	key := fmt.Sprintf("shard:cache:%s:%s:%d", storeID, objectType, epoch)
	if err := s.client.Set(ctx, key, data, 24*time.Hour).Err(); err != nil {
		return fmt.Errorf("redis.CacheShard: %w", err)
	}
	return nil
}

func (s *redisStore) LoadCachedShard(ctx context.Context, storeID, objectType string, epoch uint64) ([]byte, error) {
	key := fmt.Sprintf("shard:cache:%s:%s:%d", storeID, objectType, epoch)
	data, err := s.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("redis.LoadCachedShard: %w", err)
	}
	return data, nil
}

func (s *redisStore) SetWatchToken(ctx context.Context, storeID, token string) error {
	key := fmt.Sprintf("watchtoken:%s", storeID)
	if err := s.client.Set(ctx, key, token, 0).Err(); err != nil {
		return fmt.Errorf("redis.SetWatchToken: %w", err)
	}
	return nil
}

func (s *redisStore) GetWatchToken(ctx context.Context, storeID string) (string, error) {
	key := fmt.Sprintf("watchtoken:%s", storeID)
	val, err := s.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("redis.GetWatchToken: %w", err)
	}
	return val, nil
}

func (s *redisStore) SetMaster(ctx context.Context, clusterID, instanceID string) error {
	key := fmt.Sprintf("master:%s", clusterID)
	if err := s.client.Set(ctx, key, instanceID, 0).Err(); err != nil {
		return fmt.Errorf("redis.SetMaster: %w", err)
	}
	return nil
}

func (s *redisStore) GetMaster(ctx context.Context) (string, string, error) {
	iter := s.client.Scan(ctx, 0, "master:*", 1).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		instanceID, err := s.client.Get(ctx, key).Result()
		if err != nil {
			continue
		}
		clusterID := key[len("master:"):]
		return clusterID, instanceID, nil
	}
	if err := iter.Err(); err != nil {
		return "", "", fmt.Errorf("redis.GetMaster: %w", err)
	}
	return "", "", nil
}

func (s *redisStore) RefreshHeartbeat(ctx context.Context, clusterID string, ttl time.Duration) error {
	key := fmt.Sprintf("heartbeat:%s", clusterID)
	if err := s.client.Set(ctx, key, "1", ttl).Err(); err != nil {
		return fmt.Errorf("redis.RefreshHeartbeat: %w", err)
	}
	return nil
}

func (s *redisStore) WatchHeartbeat(ctx context.Context, clusterID string) (<-chan struct{}, error) {
	key := fmt.Sprintf("heartbeat:%s", clusterID)
	ch := make(chan struct{}, 1)

	expiredChannel := fmt.Sprintf("__keyevent@%s__:expired", strconv.Itoa(s.client.Options().DB))
	sub := s.client.Subscribe(ctx, expiredChannel)

	go func() {
		defer close(ch)
		defer sub.Close()
		msgCh := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgCh:
				if !ok {
					return
				}
				if msg.Payload == key {
					ch <- struct{}{}
					return
				}
			}
		}
	}()
	return ch, nil
}
