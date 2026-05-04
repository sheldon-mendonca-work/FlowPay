package repository

import (
	"context"
	"database/sql"
	"flowpay/payment-service/internal/domain"
	"fmt"
)

type PaymentIdempotencyRepository struct {
	db *sql.DB
}

func NewPaymentIdempotencyRepository(db *sql.DB) *PaymentIdempotencyRepository {
	return &PaymentIdempotencyRepository{db: db}
}

func (r *PaymentIdempotencyRepository) TryCreateOrGet(ctx context.Context, idempotency domain.PaymentIdempotencyKey) (domain.PaymentIdempotencyKey, bool, error) {
	query := `
		INSERT INTO idempotency_keys (
			idempotency_key,
			request_hash,
			status,
			created_at,
			updated_at
		) VALUES ($1, $2, $3, NOW(), NOW())
		ON CONFLICT (idempotency_key) DO NOTHING;
	`

	res, err := r.db.ExecContext(ctx,
		query,
		idempotency.IdempotencyKey,
		idempotency.RequestHash,
		idempotency.Status,
	)

	if err != nil {
		return domain.PaymentIdempotencyKey{}, false, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return domain.PaymentIdempotencyKey{}, false, err
	}
	if rowsAffected == 1 {
		return idempotency, true, nil
	}

	if rowsAffected == 0 {
		existing, err := r.GetByKey(ctx, idempotency.IdempotencyKey)
		return existing, false, err
	}

	return domain.PaymentIdempotencyKey{}, false, fmt.Errorf("unexpected rowsAffected: %d", rowsAffected)
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
			created_at
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
		&p.CreatedAt,
	)

	return p, err
}

func (r *PaymentIdempotencyRepository) MarkCompleted(ctx context.Context, idempotencyKey string, responseBody string) error {
	query := `
		UPDATE idempotency_keys
		SET
			response_body = $2::jsonb,
			status = 'COMPLETED',
			error_code = NULL,
			error_message = NULL,
			updated_at = NOW()
		WHERE idempotency_key = $1;
	`

	res, err := r.db.ExecContext(ctx, query, idempotencyKey, responseBody)
	if err != nil {
		return err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("Mark Completed failed: expected 1 row affected, but got %d", rowsAffected)
	}
	return nil
}

func (r *PaymentIdempotencyRepository) MarkFailed(
	ctx context.Context,
	idempotencyKey string,
	errorCode string,
	errorMessage string,
) error {
	query := `
		UPDATE idempotency_keys
		SET
			status = 'FAILED',
			error_code = $2,
			error_message = $3,
			response_body = NULL,
			updated_at = NOW()
		WHERE idempotency_key = $1;
	`

	res, err := r.db.ExecContext(ctx, query, idempotencyKey, errorCode, errorMessage)
	if err != nil {
		return err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("Mark Failed failed: expected 1 row affected, but got %d", rowsAffected)
	}

	return nil
}
