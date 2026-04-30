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

func (r *PaymentIdempotencyRepository) TryCreateOrGet(tx *sql.Tx, ctx context.Context, idempotency domain.PaymentIdempotencyKey) (domain.PaymentIdempotencyKey, bool, error) {
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

	res, err := tx.ExecContext(ctx,
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
		existing, err := r.GetByKeyForUpdate(ctx, tx, idempotency.IdempotencyKey)
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
		&p.CreatedAt,
	)

	return p, err
}

func (r *PaymentIdempotencyRepository) GetByKeyForUpdate(
	ctx context.Context,
	tx *sql.Tx,
	key string,
) (domain.PaymentIdempotencyKey, error) {

	query := `
		SELECT
			idempotency_key,
			request_hash,
			COALESCE(response_body::text, ''),
			status,
			created_at
		FROM idempotency_keys
		WHERE idempotency_key = $1
		FOR UPDATE;
	`

	var p domain.PaymentIdempotencyKey
	err := tx.QueryRowContext(ctx, query, key).Scan(
		&p.IdempotencyKey,
		&p.RequestHash,
		&p.ResponseBody,
		&p.Status,
		&p.CreatedAt,
	)

	return p, err
}

func (r *PaymentIdempotencyRepository) MarkCompleted(
	tx *sql.Tx,
	ctx context.Context,
	idempotencyKey string,
	responseBody string,
) error {
	query := `
		UPDATE idempotency_keys
		SET
			response_body = $2::jsonb,
			status = 'COMPLETED',
			updated_at = NOW()
		WHERE idempotency_key = $1;
	`

	res, err := tx.ExecContext(ctx, query, idempotencyKey, responseBody)
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
