package repository

import (
	"context"
	"database/sql"
	"flowpay/reconciliation-service/internal/domain"
)

type PaymentRepository struct {
	db *sql.DB
}

func NewPaymentRepository(db *sql.DB) *PaymentRepository {
	return &PaymentRepository{db: db}
}

func (r *PaymentRepository) GetPaymentsWithoutTransactions(ctx context.Context, tx *sql.Tx) ([]domain.Payment, error) {
	query := `
		SELECT
			p.id AS payment_id,
			p.idempotency_key,
			p.status,
			p.amount,
			p.currency,
			p.created_at
		FROM payments p
		WHERE NOT EXISTS (
			SELECT 1
			FROM transactions t
			WHERE t.payment_id = p.id
		);
	`
	rows, err := tx.QueryContext(ctx, query)

	var paymentsList []domain.Payment

	if err != nil {
		return paymentsList, err
	}

	defer rows.Close()
	for rows.Next() {
		var payment domain.Payment
		err := rows.Scan(
			&payment.ID,
			&payment.IdempotencyKey,
			&payment.Status,
			&payment.Amount,
			&payment.Currency,
			&payment.CreatedAt,
		)

		if err != nil {
			return nil, err
		}

		paymentsList = append(paymentsList, payment)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return paymentsList, err
}

func (r *PaymentRepository) GetPaymentsWithInvalidTransactionPair(ctx context.Context, tx *sql.Tx) ([]domain.PaymentWithTransactionCounts, error) {
	query := `
		SELECT
			p.id AS payment_id,
			p.idempotency_key,
			p.status,
			COUNT(t.id) AS transaction_count,
			COUNT(*) FILTER (WHERE t.type = 'DEBIT') AS debit_count,
			COUNT(*) FILTER (WHERE t.type = 'CREDIT') AS credit_count
		FROM payments p
		LEFT JOIN transactions t
			ON t.payment_id = p.id
		WHERE p.status = 'SUCCESS'
		GROUP BY p.id, p.idempotency_key, p.status
		HAVING COUNT(*) FILTER (WHERE t.type = 'DEBIT') != 1
		OR COUNT(*) FILTER (WHERE t.type = 'CREDIT') != 1;;
	`
	rows, err := tx.QueryContext(ctx, query)

	var paymentsList []domain.PaymentWithTransactionCounts

	if err != nil {
		return paymentsList, err
	}

	defer rows.Close()
	for rows.Next() {
		var payment domain.PaymentWithTransactionCounts
		err := rows.Scan(
			&payment.ID,
			&payment.IdempotencyKey,
			&payment.Status,
			&payment.TransactionCount,
			&payment.DebitCount,
			&payment.CreditCount,
		)

		if err != nil {
			return nil, err
		}

		paymentsList = append(paymentsList, payment)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return paymentsList, err
}

func (r *PaymentRepository) GetPaymentsWithoutIdempotencyRows(ctx context.Context, tx *sql.Tx) ([]domain.Payment, error) {
	query := `
		SELECT
		p.id AS payment_id,
		p.idempotency_key,
		p.status,
		p.created_at
	FROM payments p
	WHERE NOT EXISTS (
		SELECT 1
		FROM idempotency_keys i
		WHERE i.idempotency_key = p.idempotency_key
	);
	`
	rows, err := tx.QueryContext(ctx, query)

	var paymentsList []domain.Payment

	if err != nil {
		return paymentsList, err
	}

	defer rows.Close()
	for rows.Next() {
		var payment domain.Payment
		err := rows.Scan(
			&payment.ID,
			&payment.IdempotencyKey,
			&payment.Status,
			&payment.CreatedAt,
		)

		if err != nil {
			return nil, err
		}

		paymentsList = append(paymentsList, payment)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return paymentsList, err
}

func (r *PaymentRepository) GetPaymentsWithIdempotencyPaymentIDMismatch(ctx context.Context, tx *sql.Tx) ([]domain.PaymentWithPaymentIdempotency, error) {
	query := `
		SELECT
			p.id AS payment_id,
			p.idempotency_key,
			i.payment_id AS idempotency_payment_id,
			i.status AS idempotency_status,
			p.created_at
		FROM payments p
		JOIN idempotency_keys i
			ON i.idempotency_key = p.idempotency_key
		WHERE i.payment_id IS DISTINCT FROM p.id;
	`
	rows, err := tx.QueryContext(ctx, query)

	var paymentsList []domain.PaymentWithPaymentIdempotency

	if err != nil {
		return paymentsList, err
	}

	defer rows.Close()
	for rows.Next() {
		var payment domain.PaymentWithPaymentIdempotency
		err := rows.Scan(
			&payment.ID,
			&payment.IdempotencyKey,
			&payment.PaymentIdempotencyKey,
			&payment.IdempotencyStatus,
			&payment.CreatedAt,
		)

		if err != nil {
			return nil, err
		}

		paymentsList = append(paymentsList, payment)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return paymentsList, err
}

func (r *PaymentRepository) GetPaymentsMissingCompletedIdempotency(ctx context.Context, tx *sql.Tx) ([]domain.PaymentWithIdempotencyPayment, error) {
	query := `
		SELECT
			p.id AS payment_id,
			p.idempotency_key,
			p.status AS payment_status,
			i.status AS idempotency_status,
			i.payment_id AS idempotency_payment_id
		FROM payments p
		LEFT JOIN idempotency_keys i
			ON i.idempotency_key = p.idempotency_key
		WHERE i.idempotency_key IS NULL
		OR i.status != 'COMPLETED'
		OR i.payment_id IS DISTINCT FROM p.id;
	`
	rows, err := tx.QueryContext(ctx, query)

	var paymentsList []domain.PaymentWithIdempotencyPayment

	if err != nil {
		return paymentsList, err
	}

	defer rows.Close()
	for rows.Next() {
		var payment domain.PaymentWithIdempotencyPayment
		err := rows.Scan(
			&payment.ID,
			&payment.IdempotencyKey,
			&payment.IdempotencyStatus,
			&payment.IdempotencyPaymentKey,
		)

		if err != nil {
			return nil, err
		}

		paymentsList = append(paymentsList, payment)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return paymentsList, err
}
