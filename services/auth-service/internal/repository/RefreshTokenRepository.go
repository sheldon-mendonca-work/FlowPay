package repository

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"

	"flowpay/auth-service/internal/domain"
)

type RefreshTokenRepository struct {
	db *sql.DB
}

func NewRefreshTokenRepository(db *sql.DB) *RefreshTokenRepository {
	return &RefreshTokenRepository{db: db}
}

func HashRefreshToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return fmt.Sprintf("%x", sum)
}

// Upsert replaces any existing session for the account with the new token (single session).
func (r *RefreshTokenRepository) Upsert(ctx context.Context, tx *sql.Tx, token domain.RefreshToken) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM refresh_tokens WHERE account_id = $1`, token.AccountID); err != nil {
		return err
	}
	query := `
		INSERT INTO refresh_tokens (id, account_id, token_hash, expires_at, created_at)
		VALUES ($1, $2, $3, $4, NOW())
	`
	_, err := tx.ExecContext(ctx, query, token.ID, token.AccountID, token.TokenHash, token.ExpiresAt)
	return err
}

func (r *RefreshTokenRepository) FindByHash(ctx context.Context, tokenHash string) (*domain.RefreshToken, error) {
	query := `
		SELECT id, account_id, token_hash, expires_at, created_at
		FROM refresh_tokens
		WHERE token_hash = $1
	`
	row := r.db.QueryRowContext(ctx, query, tokenHash)
	var t domain.RefreshToken
	err := row.Scan(&t.ID, &t.AccountID, &t.TokenHash, &t.ExpiresAt, &t.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func (r *RefreshTokenRepository) DeleteByHash(ctx context.Context, tokenHash string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM refresh_tokens WHERE token_hash = $1`, tokenHash)
	return err
}

func (r *RefreshTokenRepository) DeleteByAccountID(ctx context.Context, accountID string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM refresh_tokens WHERE account_id = $1`, accountID)
	return err
}
