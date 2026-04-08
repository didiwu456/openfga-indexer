//go:build integration

package stringtable_test

import (
	"context"
	"os"
	"testing"

	_ "github.com/lib/pq"
	"github.com/yourorg/openfga-indexer/stringtable"
)

func openTestStore(t *testing.T) (stringtable.Store, func()) {
	t.Helper()
	dsn := os.Getenv("TEST_DB_DSN")
	driver := os.Getenv("TEST_DB_DRIVER")
	if dsn == "" || driver == "" {
		t.Skip("TEST_DB_DSN and TEST_DB_DRIVER not set")
	}
	store, db, err := stringtable.Open(driver, dsn)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return store, func() { db.Close() }
}

func runStoreContract(t *testing.T, store stringtable.Store) {
	ctx := context.Background()
	storeID := "01TESTSTORE0001"

	t.Run("intern_idempotent", func(t *testing.T) {
		id1, err := store.Intern(ctx, storeID, "user:alice")
		if err != nil {
			t.Fatalf("first Intern: %v", err)
		}
		id2, err := store.Intern(ctx, storeID, "user:alice")
		if err != nil {
			t.Fatalf("second Intern: %v", err)
		}
		if id1 != id2 {
			t.Fatalf("Intern must be idempotent: got %d then %d", id1, id2)
		}
	})

	t.Run("bulk_intern_and_load_all", func(t *testing.T) {
		values := []string{"group:eng", "group:backend", "user:bob"}
		ids, err := store.BulkIntern(ctx, storeID, values)
		if err != nil {
			t.Fatalf("BulkIntern: %v", err)
		}
		if len(ids) != len(values) {
			t.Fatalf("BulkIntern returned %d ids, want %d", len(ids), len(values))
		}

		tbl, err := store.LoadAll(ctx, storeID)
		if err != nil {
			t.Fatalf("LoadAll: %v", err)
		}
		for _, v := range values {
			id, ok := tbl.ID(v)
			if !ok {
				t.Fatalf("LoadAll missing %q", v)
			}
			if tbl.Str(id) != v {
				t.Fatalf("Str(%d) = %q, want %q", id, tbl.Str(id), v)
			}
		}
	})

	t.Run("next_epoch_monotonic", func(t *testing.T) {
		e1, err := store.NextEpoch(ctx, storeID)
		if err != nil {
			t.Fatalf("NextEpoch: %v", err)
		}
		e2, err := store.NextEpoch(ctx, storeID)
		if err != nil {
			t.Fatalf("NextEpoch: %v", err)
		}
		if e2 != e1+1 {
			t.Fatalf("NextEpoch not monotonic: %d then %d", e1, e2)
		}
	})
}

func TestPostgresStore(t *testing.T) {
	store, cleanup := openTestStore(t)
	defer cleanup()
	runStoreContract(t, store)
}
