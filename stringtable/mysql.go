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
	_, err := s.db.ExecContext(ctx, `
		INSERT IGNORE INTO leopard_string_table (store_id, value)
		VALUES (?, ?)`,
		storeID, value,
	)
	if err != nil {
		return 0, fmt.Errorf("stringtable.Intern (mysql): %w", err)
	}
	var id uint32
	err = s.db.QueryRowContext(ctx,
		`SELECT id FROM leopard_string_table WHERE store_id = ? AND value = ?`,
		storeID, value,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("stringtable.Intern (mysql): %w", err)
	}
	return id, nil
}

func (s *mysqlStore) BulkIntern(ctx context.Context, storeID string, values []string) (map[string]uint32, error) {
	if len(values) == 0 {
		return map[string]uint32{}, nil
	}

	placeholders := make([]string, len(values))
	args := make([]any, 0, len(values)*2)
	for i, v := range values {
		placeholders[i] = "(?, ?)"
		args = append(args, storeID, v)
	}
	query := fmt.Sprintf(
		`INSERT IGNORE INTO leopard_string_table (store_id, value) VALUES %s`,
		strings.Join(placeholders, ", "),
	)
	if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
		return nil, fmt.Errorf("stringtable.BulkIntern (mysql): insert: %w", err)
	}

	return s.fetchIDs(ctx, storeID, values)
}

func (s *mysqlStore) fetchIDs(ctx context.Context, storeID string, values []string) (map[string]uint32, error) {
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
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO leopard_epochs (store_id, epoch)
		VALUES (?, 1)
		ON DUPLICATE KEY UPDATE epoch = epoch + 1, updated_at = now()`,
		storeID,
	)
	if err != nil {
		return 0, fmt.Errorf("stringtable.NextEpoch (mysql): %w", err)
	}
	var epoch uint64
	err = s.db.QueryRowContext(ctx,
		`SELECT epoch FROM leopard_epochs WHERE store_id = ?`,
		storeID,
	).Scan(&epoch)
	if err != nil {
		return 0, fmt.Errorf("stringtable.NextEpoch (mysql): %w", err)
	}
	return epoch, nil
}
