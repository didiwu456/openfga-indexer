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

//go:embed migrations/postgres
var postgresFS embed.FS

//go:embed migrations/mysql
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
