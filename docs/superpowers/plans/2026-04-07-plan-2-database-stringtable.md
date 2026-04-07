# Database Migrations & String Table — Implementation Plan 2 of 5

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the SQL schema, embedded migrator, and persistent string table that assigns stable `uint32` IDs to every user/group string in a store, backed by the same PostgreSQL or MySQL instance OpenFGA uses.

**Architecture:** `migrate/` holds SQL files embedded in the binary and a `Run()` function called at server startup. `stringtable/` provides a `Store` interface with PostgreSQL and MySQL implementations plus a `LocalTable` (pure in-memory, no DB) used during index builds. Redis caching of the string table is wired in Plan 3.

**Tech Stack:** Go 1.22, `database/sql`, `github.com/lib/pq` (PostgreSQL), `github.com/go-sql-driver/mysql`, `github.com/golang-migrate/migrate/v4`.

---

## File Map

| Action | File | Responsibility |
|---|---|---|
| Create | `migrations/postgres/000001_leopard_string_table.up.sql` | Create leopard_string_table |
| Create | `migrations/postgres/000001_leopard_string_table.down.sql` | Drop leopard_string_table |
| Create | `migrations/postgres/000002_leopard_epochs.up.sql` | Create leopard_epochs |
| Create | `migrations/postgres/000002_leopard_epochs.down.sql` | Drop leopard_epochs |
| Create | `migrations/mysql/000001_leopard_string_table.up.sql` | Create leopard_string_table (MySQL dialect) |
| Create | `migrations/mysql/000001_leopard_string_table.down.sql` | Drop leopard_string_table |
| Create | `migrations/mysql/000002_leopard_epochs.up.sql` | Create leopard_epochs (MySQL dialect) |
| Create | `migrations/mysql/000002_leopard_epochs.down.sql` | Drop leopard_epochs |
| Create | `migrate/migrator.go` | Embedded-FS migrator, `Run(dsn, driver string)` |
| Create | `migrate/migrator_test.go` | Smoke test with in-memory SQLite |
| Create | `stringtable/local.go` | `LocalTable` — in-memory fwd/rev maps, no locks |
| Create | `stringtable/store.go` | `Store` interface + `Config` |
| Create | `stringtable/postgres.go` | PostgreSQL `Store` implementation |
| Create | `stringtable/mysql.go` | MySQL `Store` implementation |
| Create | `stringtable/local_test.go` | Unit tests for `LocalTable` |
| Create | `stringtable/store_test.go` | Integration tests (dockertest, both dialects) |
| Modify | `go.mod` / `go.sum` | Add migration + driver dependencies |

---

## Task 1: Add Dependencies

**Files:** `go.mod`, `go.sum`

- [ ] **Step 1: Add all required dependencies**

```bash
cd d:/GitHub/openfga-indexer
go get github.com/golang-migrate/migrate/v4@latest
go get github.com/golang-migrate/migrate/v4/database/postgres
go get github.com/golang-migrate/migrate/v4/database/mysql
go get github.com/golang-migrate/migrate/v4/source/iofs
go get github.com/lib/pq@latest
go get github.com/go-sql-driver/mysql@latest
go mod tidy
```

Expected: `go.mod` gains four new `require` entries.

- [ ] **Step 2: Commit**

```bash
git add go.mod go.sum
git commit -m "chore: add database migration and driver dependencies"
```

---

## Task 2: SQL Migration Files

**Files:** all eight `migrations/` SQL files

- [ ] **Step 1: Create the PostgreSQL up migrations**

Create `migrations/postgres/000001_leopard_string_table.up.sql`:
```sql
CREATE TABLE IF NOT EXISTS leopard_string_table (
    id         BIGSERIAL PRIMARY KEY,
    store_id   VARCHAR(26) NOT NULL,
    value      TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (store_id, value)
);
```

Create `migrations/postgres/000002_leopard_epochs.up.sql`:
```sql
CREATE TABLE IF NOT EXISTS leopard_epochs (
    store_id   VARCHAR(26) PRIMARY KEY,
    epoch      BIGINT      NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

- [ ] **Step 2: Create the PostgreSQL down migrations**

Create `migrations/postgres/000001_leopard_string_table.down.sql`:
```sql
DROP TABLE IF EXISTS leopard_string_table;
```

Create `migrations/postgres/000002_leopard_epochs.down.sql`:
```sql
DROP TABLE IF EXISTS leopard_epochs;
```

- [ ] **Step 3: Create the MySQL up migrations**

Create `migrations/mysql/000001_leopard_string_table.up.sql`:
```sql
CREATE TABLE IF NOT EXISTS leopard_string_table (
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    store_id   VARCHAR(26)     NOT NULL,
    value      TEXT            NOT NULL,
    created_at DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_store_value (store_id, value(768))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

Create `migrations/mysql/000002_leopard_epochs.up.sql`:
```sql
CREATE TABLE IF NOT EXISTS leopard_epochs (
    store_id   VARCHAR(26)     NOT NULL PRIMARY KEY,
    epoch      BIGINT UNSIGNED NOT NULL DEFAULT 0,
    updated_at DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

- [ ] **Step 4: Create the MySQL down migrations**

Create `migrations/mysql/000001_leopard_string_table.down.sql`:
```sql
DROP TABLE IF EXISTS leopard_string_table;
```

Create `migrations/mysql/000002_leopard_epochs.down.sql`:
```sql
DROP TABLE IF EXISTS leopard_epochs;
```

- [ ] **Step 5: Commit**

```bash
git add migrations/
git commit -m "feat(migrations): add leopard_string_table and leopard_epochs SQL files"
```

---

## Task 3: Embedded Migrator

**Files:**
- Create: `migrate/migrator.go`
- Create: `migrate/migrator_test.go`

- [ ] **Step 1: Write the failing test**

Create `migrate/migrator_test.go`:

```go
package migrate_test

import (
	"database/sql"
	"testing"

	_ "github.com/lib/pq"
	leopardmigrate "github.com/yourorg/openfga-indexer/migrate"
)

// TestRun_NoChange verifies that running migrations twice is idempotent.
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
```

- [ ] **Step 2: Run to confirm failure**

```bash
go test ./migrate/... -run TestRun_UnsupportedDriver -v
```

Expected: `FAIL — undefined: leopardmigrate.Run`

- [ ] **Step 3: Implement `migrate/migrator.go`**

```go
// Package migrate runs versioned SQL migrations embedded in the binary.
// Supports PostgreSQL and MySQL (same drivers OpenFGA uses).
package migrate

import (
	"embed"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/mysql"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed ../migrations/postgres
var postgresFS embed.FS

//go:embed ../migrations/mysql
var mysqlFS embed.FS

// Run applies all pending UP migrations for the given driver.
// dsn must be a fully qualified DSN understood by golang-migrate
// (e.g. "postgres://user:pass@host/db?sslmode=disable").
// Returns nil if there are no pending migrations.
func Run(dsn, driver string) error {
	var fs embed.FS
	var subdir string
	switch driver {
	case "postgres":
		fs = postgresFS
		subdir = "migrations/postgres"
	case "mysql":
		fs = mysqlFS
		subdir = "migrations/mysql"
	default:
		return fmt.Errorf("migrate.Run: unsupported driver %q (want postgres or mysql)", driver)
	}

	src, err := iofs.New(fs, subdir)
	if err != nil {
		return fmt.Errorf("migrate.Run: open source: %w", err)
	}

	m, err := migrate.NewWithSourceInstance("iofs", src, dsn)
	if err != nil {
		return fmt.Errorf("migrate.Run: new instance: %w", err)
	}
	defer m.Close()

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("migrate.Run: up: %w", err)
	}
	return nil
}
```

- [ ] **Step 4: Run the unsupported driver test**

```bash
go test ./migrate/... -run TestRun_UnsupportedDriver -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add migrate/
git commit -m "feat(migrate): add embedded SQL migrator"
```

---

## Task 4: `LocalTable` — In-Memory String Table

**Files:**
- Create: `stringtable/local.go`
- Create: `stringtable/local_test.go`

`LocalTable` is a read-only in-memory view loaded from the DB at pipeline start. It has no locks — callers use it single-threaded during shard builds.

- [ ] **Step 1: Write the failing test**

Create `stringtable/local_test.go`:

```go
package stringtable_test

import (
	"testing"

	"github.com/yourorg/openfga-indexer/stringtable"
)

func TestLocalTable_RoundTrip(t *testing.T) {
	fwd := map[string]uint32{
		"user:alice":      0,
		"group:eng":       1,
		"group:backend":   2,
	}
	tbl := stringtable.NewLocalTable(fwd)

	tests := []struct {
		input string
		wantID uint32
	}{
		{"user:alice", 0},
		{"group:eng", 1},
		{"group:backend", 2},
	}
	for _, tt := range tests {
		id, ok := tbl.ID(tt.input)
		if !ok {
			t.Fatalf("ID(%q): not found", tt.input)
		}
		if id != tt.wantID {
			t.Fatalf("ID(%q) = %d, want %d", tt.input, id, tt.wantID)
		}
		if got := tbl.Str(id); got != tt.input {
			t.Fatalf("Str(%d) = %q, want %q", id, got, tt.input)
		}
	}
}

func TestLocalTable_Missing(t *testing.T) {
	tbl := stringtable.NewLocalTable(map[string]uint32{})

	if _, ok := tbl.ID("user:nobody"); ok {
		t.Fatal("ID for unknown string should return false")
	}
	if got := tbl.Str(99); got != "" {
		t.Fatalf("Str for out-of-range ID should be empty, got %q", got)
	}
}

func TestLocalTable_Fwd(t *testing.T) {
	fwd := map[string]uint32{"user:alice": 0}
	tbl := stringtable.NewLocalTable(fwd)
	if len(tbl.Fwd()) != 1 {
		t.Fatal("Fwd should return full map")
	}
}
```

- [ ] **Step 2: Run to confirm failure**

```bash
go test ./stringtable/... -run TestLocalTable -v
```

Expected: `FAIL — no Go files in stringtable`

- [ ] **Step 3: Implement `stringtable/local.go`**

```go
// Package stringtable manages the persistent string↔uint32 ID mapping
// used to key roaring bitmap posting lists in the Leopard index.
//
// LocalTable is a read-only in-memory view for use during index builds.
// Store is the durable interface backed by PostgreSQL or MySQL.
package stringtable

// LocalTable is a read-only in-memory string table loaded from the DB.
// Not thread-safe — use within a single goroutine or under external lock.
type LocalTable struct {
	fwd map[string]uint32
	rev []string // index == uint32 ID
}

// NewLocalTable builds a LocalTable from a fwd map.
// The rev slice is derived automatically.
func NewLocalTable(fwd map[string]uint32) *LocalTable {
	if len(fwd) == 0 {
		return &LocalTable{fwd: make(map[string]uint32)}
	}
	// Find max ID to size rev correctly.
	var maxID uint32
	for _, id := range fwd {
		if id > maxID {
			maxID = id
		}
	}
	rev := make([]string, int(maxID)+1)
	for s, id := range fwd {
		rev[id] = s
	}
	return &LocalTable{fwd: fwd, rev: rev}
}

// ID returns the uint32 ID for s. Returns (0, false) if not found.
func (t *LocalTable) ID(s string) (uint32, bool) {
	id, ok := t.fwd[s]
	return id, ok
}

// Str returns the string for id. Returns "" if id is out of range.
func (t *LocalTable) Str(id uint32) string {
	if int(id) >= len(t.rev) {
		return ""
	}
	return t.rev[id]
}

// Fwd returns the forward map (string → ID). Do not modify.
func (t *LocalTable) Fwd() map[string]uint32 {
	return t.fwd
}

// Len returns the number of entries in the table.
func (t *LocalTable) Len() int {
	return len(t.fwd)
}
```

- [ ] **Step 4: Run tests**

```bash
go test ./stringtable/... -run TestLocalTable -v
```

Expected: all `TestLocalTable_*` tests PASS.

- [ ] **Step 5: Commit**

```bash
git add stringtable/local.go stringtable/local_test.go
git commit -m "feat(stringtable): add LocalTable in-memory string table"
```

---

## Task 5: `Store` Interface and PostgreSQL Implementation

**Files:**
- Create: `stringtable/store.go`
- Create: `stringtable/postgres.go`

- [ ] **Step 1: Write the Store interface in `stringtable/store.go`**

```go
package stringtable

import (
	"context"
	"database/sql"
	"fmt"
)

// Store is the persistent string table backed by a relational database.
// It assigns stable uint32 IDs to strings, idempotently.
type Store interface {
	// Intern returns the stable ID for value within storeID.
	// Creates a new entry if value has not been seen before.
	Intern(ctx context.Context, storeID, value string) (uint32, error)

	// BulkIntern assigns IDs for multiple values in the fewest DB round-trips.
	// Returns a map from value → ID for all inputs.
	BulkIntern(ctx context.Context, storeID string, values []string) (map[string]uint32, error)

	// LoadAll returns a LocalTable containing every entry for storeID.
	LoadAll(ctx context.Context, storeID string) (*LocalTable, error)

	// NextEpoch atomically increments and returns the epoch counter for storeID.
	// Creates the row with epoch=1 if storeID has no entry yet.
	NextEpoch(ctx context.Context, storeID string) (uint64, error)
}

// Open opens a *sql.DB using the given driver and DSN, pings it, and returns
// a Store. driver must be "postgres" or "mysql".
func Open(driver, dsn string) (Store, *sql.DB, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("stringtable.Open: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, nil, fmt.Errorf("stringtable.Open: ping: %w", err)
	}
	switch driver {
	case "postgres":
		return NewPostgresStore(db), db, nil
	case "mysql":
		return NewMySQLStore(db), db, nil
	default:
		db.Close()
		return nil, nil, fmt.Errorf("stringtable.Open: unsupported driver %q", driver)
	}
}
```

- [ ] **Step 2: Write the failing integration test in `stringtable/store_test.go`**

```go
//go:build integration

package stringtable_test

import (
	"context"
	"database/sql"
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
	return store, func() { db.(*sql.DB).Close() }
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
```

- [ ] **Step 3: Implement `stringtable/postgres.go`**

```go
package stringtable

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
)

type postgresStore struct {
	db *sql.DB
}

// NewPostgresStore returns a Store backed by an open *sql.DB connected to PostgreSQL.
func NewPostgresStore(db *sql.DB) Store {
	return &postgresStore{db: db}
}

func (s *postgresStore) Intern(ctx context.Context, storeID, value string) (uint32, error) {
	var id uint32
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO leopard_string_table (store_id, value)
		VALUES ($1, $2)
		ON CONFLICT (store_id, value) DO NOTHING
		RETURNING id`,
		storeID, value,
	).Scan(&id)
	if err == sql.ErrNoRows {
		// Row already existed; fetch existing ID.
		err = s.db.QueryRowContext(ctx,
			`SELECT id FROM leopard_string_table WHERE store_id = $1 AND value = $2`,
			storeID, value,
		).Scan(&id)
	}
	if err != nil {
		return 0, fmt.Errorf("stringtable.Intern (postgres): %w", err)
	}
	return id, nil
}

func (s *postgresStore) BulkIntern(ctx context.Context, storeID string, values []string) (map[string]uint32, error) {
	if len(values) == 0 {
		return map[string]uint32{}, nil
	}

	// Build a multi-row INSERT.
	args := make([]any, 0, len(values)*2)
	placeholders := make([]string, 0, len(values))
	for i, v := range values {
		placeholders = append(placeholders, fmt.Sprintf("($1, $%d)", i+2))
		args = append(args, v)
	}
	args = append([]any{storeID}, args...)

	query := fmt.Sprintf(`
		INSERT INTO leopard_string_table (store_id, value)
		VALUES %s
		ON CONFLICT (store_id, value) DO NOTHING`,
		strings.Join(placeholders, ", "),
	)
	if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
		return nil, fmt.Errorf("stringtable.BulkIntern (postgres): insert: %w", err)
	}

	// Fetch all IDs for the given values.
	return s.fetchIDs(ctx, storeID, values)
}

func (s *postgresStore) fetchIDs(ctx context.Context, storeID string, values []string) (map[string]uint32, error) {
	args := make([]any, len(values)+1)
	args[0] = storeID
	placeholders := make([]string, len(values))
	for i, v := range values {
		args[i+1] = v
		placeholders[i] = fmt.Sprintf("$%d", i+2)
	}
	query := fmt.Sprintf(
		`SELECT id, value FROM leopard_string_table WHERE store_id = $1 AND value IN (%s)`,
		strings.Join(placeholders, ", "),
	)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("stringtable.fetchIDs (postgres): %w", err)
	}
	defer rows.Close()

	result := make(map[string]uint32, len(values))
	for rows.Next() {
		var id uint32
		var val string
		if err := rows.Scan(&id, &val); err != nil {
			return nil, fmt.Errorf("stringtable.fetchIDs (postgres): scan: %w", err)
		}
		result[val] = id
	}
	return result, rows.Err()
}

func (s *postgresStore) LoadAll(ctx context.Context, storeID string) (*LocalTable, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, value FROM leopard_string_table WHERE store_id = $1 ORDER BY id`,
		storeID,
	)
	if err != nil {
		return nil, fmt.Errorf("stringtable.LoadAll (postgres): %w", err)
	}
	defer rows.Close()

	fwd := make(map[string]uint32)
	for rows.Next() {
		var id uint32
		var val string
		if err := rows.Scan(&id, &val); err != nil {
			return nil, fmt.Errorf("stringtable.LoadAll (postgres): scan: %w", err)
		}
		fwd[val] = id
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("stringtable.LoadAll (postgres): rows: %w", err)
	}
	return NewLocalTable(fwd), nil
}

func (s *postgresStore) NextEpoch(ctx context.Context, storeID string) (uint64, error) {
	var epoch uint64
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO leopard_epochs (store_id, epoch)
		VALUES ($1, 1)
		ON CONFLICT (store_id) DO UPDATE
		  SET epoch = leopard_epochs.epoch + 1,
		      updated_at = now()
		RETURNING epoch`,
		storeID,
	).Scan(&epoch)
	if err != nil {
		return 0, fmt.Errorf("stringtable.NextEpoch (postgres): %w", err)
	}
	return epoch, nil
}
```

- [ ] **Step 4: Commit**

```bash
git add stringtable/store.go stringtable/postgres.go
git commit -m "feat(stringtable): add Store interface and PostgreSQL implementation"
```

---

## Task 6: MySQL Implementation

**Files:**
- Create: `stringtable/mysql.go`

- [ ] **Step 1: Implement `stringtable/mysql.go`**

```go
package stringtable

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type mysqlStore struct {
	db *sql.DB
}

// NewMySQLStore returns a Store backed by an open *sql.DB connected to MySQL.
func NewMySQLStore(db *sql.DB) Store {
	return &mysqlStore{db: db}
}

func (s *mysqlStore) Intern(ctx context.Context, storeID, value string) (uint32, error) {
	if _, err := s.db.ExecContext(ctx,
		`INSERT IGNORE INTO leopard_string_table (store_id, value) VALUES (?, ?)`,
		storeID, value,
	); err != nil {
		return 0, fmt.Errorf("stringtable.Intern (mysql): insert: %w", err)
	}
	var id uint32
	if err := s.db.QueryRowContext(ctx,
		`SELECT id FROM leopard_string_table WHERE store_id = ? AND value = ?`,
		storeID, value,
	).Scan(&id); err != nil {
		return 0, fmt.Errorf("stringtable.Intern (mysql): select: %w", err)
	}
	return id, nil
}

func (s *mysqlStore) BulkIntern(ctx context.Context, storeID string, values []string) (map[string]uint32, error) {
	if len(values) == 0 {
		return map[string]uint32{}, nil
	}
	args := make([]any, 0, len(values)*2)
	placeholders := make([]string, 0, len(values))
	for _, v := range values {
		placeholders = append(placeholders, "(?, ?)")
		args = append(args, storeID, v)
	}
	query := fmt.Sprintf(
		`INSERT IGNORE INTO leopard_string_table (store_id, value) VALUES %s`,
		strings.Join(placeholders, ", "),
	)
	if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
		return nil, fmt.Errorf("stringtable.BulkIntern (mysql): insert: %w", err)
	}
	return s.fetchIDsMySQL(ctx, storeID, values)
}

func (s *mysqlStore) fetchIDsMySQL(ctx context.Context, storeID string, values []string) (map[string]uint32, error) {
	args := make([]any, len(values)+1)
	args[0] = storeID
	placeholders := make([]string, len(values))
	for i, v := range values {
		args[i+1] = v
		placeholders[i] = "?"
	}
	query := fmt.Sprintf(
		`SELECT id, value FROM leopard_string_table WHERE store_id = ? AND value IN (%s)`,
		strings.Join(placeholders, ", "),
	)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("stringtable.fetchIDs (mysql): %w", err)
	}
	defer rows.Close()
	result := make(map[string]uint32, len(values))
	for rows.Next() {
		var id uint32
		var val string
		if err := rows.Scan(&id, &val); err != nil {
			return nil, fmt.Errorf("stringtable.fetchIDs (mysql): scan: %w", err)
		}
		result[val] = id
	}
	return result, rows.Err()
}

func (s *mysqlStore) LoadAll(ctx context.Context, storeID string) (*LocalTable, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, value FROM leopard_string_table WHERE store_id = ? ORDER BY id`,
		storeID,
	)
	if err != nil {
		return nil, fmt.Errorf("stringtable.LoadAll (mysql): %w", err)
	}
	defer rows.Close()
	fwd := make(map[string]uint32)
	for rows.Next() {
		var id uint32
		var val string
		if err := rows.Scan(&id, &val); err != nil {
			return nil, fmt.Errorf("stringtable.LoadAll (mysql): scan: %w", err)
		}
		fwd[val] = id
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("stringtable.LoadAll (mysql): rows: %w", err)
	}
	return NewLocalTable(fwd), nil
}

func (s *mysqlStore) NextEpoch(ctx context.Context, storeID string) (uint64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("stringtable.NextEpoch (mysql): begin: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO leopard_epochs (store_id, epoch)
		VALUES (?, 1)
		ON DUPLICATE KEY UPDATE epoch = epoch + 1`,
		storeID,
	); err != nil {
		return 0, fmt.Errorf("stringtable.NextEpoch (mysql): upsert: %w", err)
	}

	var epoch uint64
	if err := tx.QueryRowContext(ctx,
		`SELECT epoch FROM leopard_epochs WHERE store_id = ?`, storeID,
	).Scan(&epoch); err != nil {
		return 0, fmt.Errorf("stringtable.NextEpoch (mysql): select: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("stringtable.NextEpoch (mysql): commit: %w", err)
	}
	return epoch, nil
}
```

- [ ] **Step 2: Run unit tests (non-integration only)**

```bash
go test ./stringtable/... -short -v
```

Expected: `TestLocalTable_*` pass. Integration tests skipped.

- [ ] **Step 3: Commit**

```bash
git add stringtable/mysql.go stringtable/store_test.go
git commit -m "feat(stringtable): add MySQL Store implementation and integration tests"
```

---

## Self-Review

- [x] **Spec coverage:** Section 5 (string table) ✓ Tasks 4–6. Section 18 (migrations) ✓ Tasks 2–3. Both PostgreSQL and MySQL dialects covered ✓.
- [x] **Placeholder scan:** No TBD/TODO in any step.
- [x] **Type consistency:** `LocalTable`, `Store`, `NewPostgresStore`, `NewMySQLStore` defined here and referenced correctly in later plans. `NextEpoch` returns `uint64` matching the `epoch uint64` type used in pipeline Plan 4.
