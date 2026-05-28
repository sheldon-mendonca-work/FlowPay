package repo

import (
	"context"
	"database/sql"
	"fmt"
)

type PaymentIdempotencyRepository struct {
	db *sql.DB
}

func NewPaymentIdempotencyRepository(db *sql.DB) *PaymentIdempotencyRepository {
	return &PaymentIdempotencyRepository{db: db}
}

func (r *PaymentIdempotencyRepository) MarkFailed(
	ctx context.Context,
	idempotencyKey string,
	errorCode string,
	errorMessage string,
) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	committed := false

	defer func() {
		if !committed {
			tx.Rollback()
		}
	}()

	query := `
		UPDATE idempotency_keys
		SET
			status = 'FAILED',
			error_code = $2,
			error_message = $3,
			response_body = NULL,
			owner_token = NULL,
			locked_until = NOW(),
			updated_at = NOW()
		WHERE idempotency_key = $1;
	`

	res, err := tx.ExecContext(ctx, query, idempotencyKey, errorCode, errorMessage)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("mark failed failed: expected 1 row affected, got %d", rowsAffected)
	}
	committed = true
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}
