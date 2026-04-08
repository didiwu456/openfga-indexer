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
