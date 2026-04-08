// Package db provides database migration and driver support for openfga-indexer.
package db

import (
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/lib/pq"
)
