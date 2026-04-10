package storage_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/yourorg/openfga-indexer/storage"
)

func runShardStoreContract(t *testing.T, s storage.ShardStore) {
	t.Helper()
	ctx := context.Background()

	t.Run("write_and_read_shard", func(t *testing.T) {
		data := []byte("roaring-bitmap-bytes")
		if err := s.WriteShard(ctx, "store1", "group", 1, data); err != nil {
			t.Fatalf("WriteShard: %v", err)
		}
		got, err := s.ReadShard(ctx, "store1", "group", 1)
		if err != nil {
			t.Fatalf("ReadShard: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("ReadShard = %q, want %q", got, data)
		}
	})

	t.Run("read_missing_returns_error", func(t *testing.T) {
		_, err := s.ReadShard(ctx, "store1", "group", 99)
		if err == nil {
			t.Fatal("expected error reading missing shard")
		}
	})

	t.Run("list_epochs", func(t *testing.T) {
		_ = s.WriteShard(ctx, "store2", "folder", 10, []byte("a"))
		_ = s.WriteShard(ctx, "store2", "folder", 11, []byte("b"))
		epochs, err := s.ListEpochs(ctx, "store2", "folder")
		if err != nil {
			t.Fatalf("ListEpochs: %v", err)
		}
		if len(epochs) < 2 {
			t.Fatalf("expected at least 2 epochs, got %v", epochs)
		}
	})

	t.Run("delete_shard", func(t *testing.T) {
		_ = s.WriteShard(ctx, "store3", "group", 5, []byte("del"))
		if err := s.DeleteShard(ctx, "store3", "group", 5); err != nil {
			t.Fatalf("DeleteShard: %v", err)
		}
		if _, err := s.ReadShard(ctx, "store3", "group", 5); err == nil {
			t.Fatal("expected error reading deleted shard")
		}
	})
}

func TestMemoryShardStore(t *testing.T) {
	runShardStoreContract(t, storage.NewMemoryStore())
}
