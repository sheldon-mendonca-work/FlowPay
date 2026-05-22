package repository

import (
	"context"
	"database/sql"
	"flowpay/payment-executor/internal/domain"
	"fmt"
)

type PaymentIdempotencyRepository struct {
	db *sql.DB
}

func NewPaymentIdempotencyRepository(db *sql.DB) *PaymentIdempotencyRepository {
	return &PaymentIdempotencyRepository{db: db}
}

func (r *PaymentIdempotencyRepository) GetByKey(
	ctx context.Context,
	key string,
) (domain.PaymentIdempotencyKey, error) {
	query := `
		SELECT
			idempotency_key,
			request_hash,
			COALESCE(response_body::text, ''),
			status,
			COALESCE(error_code, ''),
			COALESCE(error_message, ''),
			COALESCE(payment_id::text, ''),
			COALESCE(owner_token, ''),
			locked_until,
			created_at,
			updated_at
		FROM idempotency_keys
		WHERE idempotency_key = $1;
	`

	var p domain.PaymentIdempotencyKey
	err := r.db.QueryRowContext(ctx, query, key).Scan(
		&p.IdempotencyKey,
		&p.RequestHash,
		&p.ResponseBody,
		&p.Status,
		&p.ErrorCode,
		&p.ErrorMessage,
		&p.PaymentID,
		&p.OwnerToken,
		&p.LockedUntil,
		&p.CreatedAt,
		&p.UpdatedAt,
	)

	return p, err
}

func (r *PaymentIdempotencyRepository) MarkCompleted(
	tx *sql.Tx,
	ctx context.Context,
	idempotencyKey string,
	responseBody string,
	paymentID string,
	ownerToken string,
) error {
	query := `
		UPDATE idempotency_keys
		SET
			response_body = $2::jsonb,
			status = 'COMPLETED',
			payment_id = $3::uuid,
			error_code = NULL,
			error_message = NULL,
			owner_token = NULL,
			locked_until = NOW(),
			updated_at = NOW()
		WHERE idempotency_key = $1
		  AND owner_token = $4;
	`

	res, err := tx.ExecContext(ctx, query, idempotencyKey, responseBody, paymentID, ownerToken)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("mark completed failed: expected 1 row affected, got %d", rowsAffected)
	}
	return nil
}

func (r *PaymentIdempotencyRepository) MarkFailed(
	tx *sql.Tx,
	ctx context.Context,
	idempotencyKey string,
	errorCode string,
	errorMessage string,
	ownerToken string,
) error {
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
		WHERE idempotency_key = $1
		  AND owner_token = $4;
	`

	res, err := tx.ExecContext(ctx, query, idempotencyKey, errorCode, errorMessage, ownerToken)
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
	return nil
}
