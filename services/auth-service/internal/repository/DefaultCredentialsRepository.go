package repository

import (
	"context"
	"database/sql"
)

type DefaultCredentialsRepository struct {
	db *sql.DB
}

func NewDefaultCredentialsRepository(db *sql.DB) *DefaultCredentialsRepository {
	return &DefaultCredentialsRepository{db: db}
}

func (r *DefaultCredentialsRepository) ExistsByAccountID(ctx context.Context, accountID string) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM defaultcredentials WHERE account_id = $1)`,
		accountID,
	).Scan(&exists)
	return exists, err
}

// ExistsByAccountIDAsAccount checks that the account_id is in defaultcredentials and has no users row.
func (r *DefaultCredentialsRepository) ExistsByAccountIDAsAccount(ctx context.Context, accountID string) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx,
		`SELECT EXISTS(
			SELECT 1 FROM defaultcredentials dc
			WHERE dc.account_id = $1
			AND NOT EXISTS (SELECT 1 FROM users u WHERE u.account_id = $1)
		)`,
		accountID,
	).Scan(&exists)
	return exists, err
}

// ExistsByAccountIDAsUser checks that the account_id is in defaultcredentials and has a users row.
func (r *DefaultCredentialsRepository) ExistsByAccountIDAsUser(ctx context.Context, accountID string) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx,
		`SELECT EXISTS(
			SELECT 1 FROM defaultcredentials dc
			WHERE dc.account_id = $1
			AND EXISTS (SELECT 1 FROM users u WHERE u.account_id = $1)
		)`,
		accountID,
	).Scan(&exists)
	return exists, err
}
