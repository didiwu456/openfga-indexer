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
