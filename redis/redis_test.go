package redis_test

import (
	"context"
	"testing"
	"time"

	leopardredis "github.com/yourorg/openfga-indexer/redis"
)

func runStoreContract(t *testing.T, s leopardredis.Store) {
	t.Helper()
	ctx := context.Background()

	t.Run("cache_and_load_shard", func(t *testing.T) {
		data := []byte("fake-shard-bytes")
		if err := s.CacheShard(ctx, "store1", "group", 1, data); err != nil {
			t.Fatalf("CacheShard: %v", err)
		}
		got, err := s.LoadCachedShard(ctx, "store1", "group", 1)
		if err != nil {
			t.Fatalf("LoadCachedShard: %v", err)
		}
		if string(got) != string(data) {
			t.Fatalf("LoadCachedShard = %q, want %q", got, data)
		}
	})

	t.Run("load_missing_shard_returns_nil", func(t *testing.T) {
		got, err := s.LoadCachedShard(ctx, "store1", "group", 99)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != nil {
			t.Fatal("expected nil for missing shard")
		}
	})

	t.Run("watch_token_roundtrip", func(t *testing.T) {
		if err := s.SetWatchToken(ctx, "store1", "token-abc"); err != nil {
			t.Fatalf("SetWatchToken: %v", err)
		}
		got, err := s.GetWatchToken(ctx, "store1")
		if err != nil {
			t.Fatalf("GetWatchToken: %v", err)
		}
		if got != "token-abc" {
			t.Fatalf("GetWatchToken = %q, want %q", got, "token-abc")
		}
	})

	t.Run("master_roundtrip", func(t *testing.T) {
		if err := s.SetMaster(ctx, "cluster-a", "instance-1"); err != nil {
			t.Fatalf("SetMaster: %v", err)
		}
		clusterID, instanceID, err := s.GetMaster(ctx)
		if err != nil {
			t.Fatalf("GetMaster: %v", err)
		}
		if clusterID != "cluster-a" || instanceID != "instance-1" {
			t.Fatalf("GetMaster = (%q, %q), want (cluster-a, instance-1)", clusterID, instanceID)
		}
	})

	t.Run("heartbeat_refresh", func(t *testing.T) {
		if err := s.RefreshHeartbeat(ctx, "cluster-a", 100*time.Millisecond); err != nil {
			t.Fatalf("RefreshHeartbeat: %v", err)
		}
	})
}

func TestMemoryStore(t *testing.T) {
	s := leopardredis.NewMemoryStore()
	runStoreContract(t, s)
}
