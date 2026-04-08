package migrate_test

import (
	"database/sql"
	"testing"

	_ "github.com/lib/pq"
	leopardmigrate "github.com/yourorg/openfga-indexer/migrate"
)

// TestRun_Idempotent verifies that running migrations twice is idempotent.
// This test requires a running PostgreSQL instance; skip under -short.
func TestRun_Idempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: requires PostgreSQL")
	}
	dsn := "postgres://postgres:postgres@localhost:5432/testdb?sslmode=disable"
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}

	// First run — applies migrations.
	if err := leopardmigrate.Run(dsn, "postgres"); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	// Second run — must be idempotent (ErrNoChange treated as success).
	if err := leopardmigrate.Run(dsn, "postgres"); err != nil {
		t.Fatalf("second Run: %v", err)
	}
}

func TestRun_UnsupportedDriver(t *testing.T) {
	err := leopardmigrate.Run("dsn", "sqlite3")
	if err == nil {
		t.Fatal("expected error for unsupported driver")
	}
}
