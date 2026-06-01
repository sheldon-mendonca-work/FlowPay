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

func (r *PaymentIdempotencyRepository) GetIdempotencyCompletedWithoutPayment(ctx context.Context, tx *sql.Tx) ([]domain.PaymentIdempotencyKey, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT i.idempotency_key, COALESCE(i.payment_id::text, ''), i.status, i.updated_at
		FROM idempotency_keys i
		WHERE i.status = 'COMPLETED'
		AND NOT EXISTS (
			SELECT 1
			FROM payments p
			WHERE p.id = i.payment_id
		);
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var idempotencies []domain.PaymentIdempotencyKey
	for rows.Next() {
		var idempotency domain.PaymentIdempotencyKey
		if err := rows.Scan(&idempotency.IdempotencyKey, &idempotency.PaymentID, &idempotency.Status, &idempotency.UpdatedAt); err != nil {
			return nil, err
		}
		idempotencies = append(idempotencies, idempotency)
	}
	return idempotencies, rows.Err()
}

func (r *PaymentIdempotencyRepository) GetIdempotencyMissingPaymentID(ctx context.Context, tx *sql.Tx) ([]domain.PaymentIdempotencyKey, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT i.idempotency_key, i.status, COALESCE(i.owner_token, '')
		FROM idempotency_keys i
		WHERE i.payment_id IS NULL;
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var idempotencies []domain.PaymentIdempotencyKey
	for rows.Next() {
		var idempotency domain.PaymentIdempotencyKey
		if err := rows.Scan(&idempotency.IdempotencyKey, &idempotency.Status, &idempotency.OwnerToken); err != nil {
			return nil, err
		}
		idempotencies = append(idempotencies, idempotency)
	}
	return idempotencies, rows.Err()
}

func (r *PaymentIdempotencyRepository) GetIdempotencyDuplicatePaymentID(ctx context.Context, tx *sql.Tx) ([]domain.IdempotencyDuplicatePaymentID, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT i.payment_id::text, COUNT(*) AS idempotency_row_count
		FROM idempotency_keys i
		WHERE i.payment_id IS NOT NULL
		GROUP BY i.payment_id
		HAVING COUNT(*) > 1
		ORDER BY idempotency_row_count DESC, i.payment_id;
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var duplicates []domain.IdempotencyDuplicatePaymentID
	for rows.Next() {
		var duplicate domain.IdempotencyDuplicatePaymentID
		if err := rows.Scan(&duplicate.PaymentID, &duplicate.RowCount); err != nil {
			return nil, err
		}
		duplicates = append(duplicates, duplicate)
	}
	return duplicates, rows.Err()
}

func (r *PaymentIdempotencyRepository) GetIdempotencyExpiredInProgress(ctx context.Context, tx *sql.Tx) ([]domain.IdempotencyExpiredInProgress, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT i.idempotency_key, COALESCE(i.payment_id::text, ''), COALESCE(i.owner_token, ''), NOW() - i.locked_until AS expired_for
		FROM idempotency_keys i
		WHERE i.status = 'IN_PROGRESS'
		AND i.locked_until < NOW()
		ORDER BY i.locked_until ASC;
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var expiredRows []domain.IdempotencyExpiredInProgress
	for rows.Next() {
		var expired domain.IdempotencyExpiredInProgress
		if err := rows.Scan(&expired.IdempotencyKey, &expired.PaymentID, &expired.OwnerToken, &expired.ExpiredFor); err != nil {
			return nil, err
		}
		expiredRows = append(expiredRows, expired)
	}
	return expiredRows, rows.Err()
}

func (r *PaymentIdempotencyRepository) GetIdempotencyInProgressWithoutOutbox(ctx context.Context, tx *sql.Tx) ([]domain.PaymentIdempotencyKey, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT i.idempotency_key, COALESCE(i.payment_id::text, ''), COALESCE(i.owner_token, '')
		FROM idempotency_keys i
		WHERE i.status = 'IN_PROGRESS'
		AND NOT EXISTS (
			SELECT 1
			FROM outbox_events o
			WHERE o.idempotency_key = i.idempotency_key
		);
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var idempotencies []domain.PaymentIdempotencyKey
	for rows.Next() {
		var idempotency domain.PaymentIdempotencyKey
		if err := rows.Scan(&idempotency.IdempotencyKey, &idempotency.PaymentID, &idempotency.OwnerToken); err != nil {
			return nil, err
		}
		idempotencies = append(idempotencies, idempotency)
	}
	return idempotencies, rows.Err()
}

func (r *PaymentIdempotencyRepository) GetIdempotencyOutboxPaymentIDMismatch(ctx context.Context, tx *sql.Tx) ([]domain.IdempotencyOutboxPaymentIDMismatch, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT i.idempotency_key, i.payment_id::text, o.aggregate_id, o.id::text
		FROM idempotency_keys i
		JOIN outbox_events o
			ON o.idempotency_key = i.idempotency_key
		WHERE i.payment_id IS NOT NULL
		AND o.aggregate_id IS DISTINCT FROM i.payment_id::text;
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var mismatches []domain.IdempotencyOutboxPaymentIDMismatch
	for rows.Next() {
		var mismatch domain.IdempotencyOutboxPaymentIDMismatch
		if err := rows.Scan(&mismatch.IdempotencyKey, &mismatch.IdempotencyPaymentID, &mismatch.OutboxPaymentID, &mismatch.OutboxEventID); err != nil {
			return nil, err
		}
		mismatches = append(mismatches, mismatch)
	}
	return mismatches, rows.Err()
}

func (r *PaymentIdempotencyRepository) GetIdempotencyMultipleOutboxPaymentIDs(ctx context.Context, tx *sql.Tx) ([]domain.IdempotencyMultipleOutboxPaymentIDs, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT o.idempotency_key, COUNT(DISTINCT o.aggregate_id) AS outbox_payment_id_count
		FROM outbox_events o
		GROUP BY o.idempotency_key
		HAVING COUNT(DISTINCT o.aggregate_id) > 1
		ORDER BY outbox_payment_id_count DESC, o.idempotency_key;
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rowsWithMultipleIDs []domain.IdempotencyMultipleOutboxPaymentIDs
	for rows.Next() {
		var row domain.IdempotencyMultipleOutboxPaymentIDs
		if err := rows.Scan(&row.IdempotencyKey, &row.PaymentIDCount); err != nil {
			return nil, err
		}
		rowsWithMultipleIDs = append(rowsWithMultipleIDs, row)
	}
	return rowsWithMultipleIDs, rows.Err()
}
