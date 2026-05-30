package models

import (
	"context"
	"database/sql"
)

type Check interface {
	Name() string
	Severity() Severity
	Run(ctx context.Context, db *sql.DB) ([]Anomaly, error)
}
