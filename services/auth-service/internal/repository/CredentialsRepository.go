package repository

import (
	"context"
	"database/sql"
	"flowpay/auth-service/internal/domain"
)

type CredentialsRepository struct {
	db *sql.DB
}

func NewCredentialsRepository(db *sql.DB) *CredentialsRepository {
	return &CredentialsRepository{db: db}
}

func (r *CredentialsRepository) Create(ctx context.Context, tx *sql.Tx, creds domain.Credentials) error {
	query := `
		INSERT INTO credentials (account_id, email, password_hash, password_updated_at, created_at, updated_at)
		VALUES ($1, $2, $3, NOW(), NOW(), NOW())
	`
	_, err := tx.ExecContext(ctx, query, creds.AccountID, creds.Email, creds.PasswordHash)
	return err
}

func (r *CredentialsRepository) FindByEmail(ctx context.Context, email string) (*domain.Credentials, error) {
	query := `
		SELECT account_id, email, password_hash, password_updated_at, created_at, updated_at
		FROM credentials
		WHERE email = $1
	`
	row := r.db.QueryRowContext(ctx, query, email)
	var creds domain.Credentials
	err := row.Scan(
		&creds.AccountID,
		&creds.Email,
		&creds.PasswordHash,
		&creds.PasswordUpdatedAt,
		&creds.CreatedAt,
		&creds.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &creds, nil
}
