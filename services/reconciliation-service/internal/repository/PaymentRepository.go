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
