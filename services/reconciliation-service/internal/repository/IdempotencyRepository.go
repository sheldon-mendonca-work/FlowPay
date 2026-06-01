package repository

import (
	"context"
	"database/sql"
	"flowpay/reconciliation-service/internal/domain"
)

type PaymentIdempotencyRepository struct {
	db *sql.DB
}

func NewPaymentIdempotencyRepository(db *sql.DB) *PaymentIdempotencyRepository {
	return &PaymentIdempotencyRepository{db: db}
}

const IdempotencyMissingPaymentIDCheckName = "idempotency_missing_payment_id"
const IdempotencyDuplicatePaymentIDCheckName = "idempotency_duplicate_payment_id"
const IdempotencyExpiredInProgressCheckName = "idempotency_expired_in_progress"
const IdempotencyInProgressWithoutOutboxCheckName = "idempotency_in_progress_without_outbox"
const IdempotencyOutboxPaymentIDMismatchCheckName = "idempotency_outbox_payment_id_mismatch"
const IdempotencyMultipleOutboxPaymentIDsCheckName = "idempotency_multiple_outbox_payment_ids"

func (r *PaymentIdempotencyRepository) GetIdempotencyCompletedWithoutPayment(ctx context.Context, tx *sql.Tx) ([]domain.PaymentIdempotencyKey, error) {
	query := `
		SELECT
			i.idempotency_key,
			i.payment_id,
			i.status,
			i.updated_at
		FROM idempotency_keys i
		WHERE i.status = 'COMPLETED'
		AND NOT EXISTS (
			SELECT 1
			FROM payments p
			WHERE p.id = i.payment_id
		);
	`
	rows, err := tx.QueryContext(ctx, query)

	var idempotencyList []domain.PaymentIdempotencyKey

	if err != nil {
		return idempotencyList, err
	}

	defer rows.Close()
	for rows.Next() {
		var idempotency domain.PaymentIdempotencyKey
		err := rows.Scan(
			&idempotency.IdempotencyKey,
			&idempotency.PaymentID,
			&idempotency.Status,
			&idempotency.UpdatedAt,
		)

		if err != nil {
			return nil, err
		}

		idempotencyList = append(idempotencyList, idempotency)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return idempotencyList, err
}
