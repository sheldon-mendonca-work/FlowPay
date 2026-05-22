package repository

import (
	"context"
	"database/sql"
	"flowpay/payment-executor/internal/domain"
	"fmt"
)

type PaymentRepository struct {
	db *sql.DB
}

func NewPaymentRepository(db *sql.DB) *PaymentRepository {
	return &PaymentRepository{db: db}
}

func (r *PaymentRepository) CreatePayment(tx *sql.Tx, ctx context.Context, payment domain.Payment) error {
	query := `
		INSERT INTO payments (
			id,
			idempotency_key,
			sender_id,
			receiver_id,
			amount,
			currency,
			status,
			created_at,
			updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
		 ON CONFLICT (id) DO NOTHING;
	`

	res, err := tx.ExecContext(ctx,
		query,
		payment.ID,
		payment.IdempotencyKey,
		payment.SenderID,
		payment.ReceiverID,
		payment.Amount,
		payment.Currency,
		payment.Status,
	)

	if err != nil {
		return err
	}

	rowsAffected, _ := res.RowsAffected()
	if rowsAffected != 1 {
		return fmt.Errorf("Payment Creation failed: expected 1 row affected, but got %d", rowsAffected)
	}
	return nil
}

func (r *PaymentRepository) GetByPaymentIdAndIdempotencyKey(ctx context.Context, paymentID string, idempotencyKey string) (domain.Payment, error) {
	query := `
		SELECT id, idempotency_key, status
		FROM payments
		WHERE id=$1 AND idempotency_key = $2;
	`
	var payment domain.Payment

	err := r.db.QueryRowContext(ctx, query, paymentID, idempotencyKey).Scan(
		&payment.ID,
		&payment.IdempotencyKey,
		&payment.Status,
	)

	return payment, err
}
