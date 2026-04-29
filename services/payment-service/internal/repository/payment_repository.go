package repository

import (
	"context"
	"database/sql"
	"flowpay/payment-service/internal/domain"
)

type PaymentRepository struct {
	db *sql.DB
}

func NewPaymentRepository(db *sql.DB) *PaymentRepository {
	return &PaymentRepository{db: db}
}

func (r *PaymentRepository) CreatePayment(ctx context.Context, payment domain.Payment) error {
	query := `
		INSERT INTO payments (
			payment_id,
			user_id,
			amount,
			currency,
			status,
			idempotency_key,
			created_at,
			updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW());
	`

	_, err := r.db.ExecContext(ctx,
		query,
		payment.PaymentID,
		payment.UserID,
		payment.Amount,
		payment.Currency,
		payment.Status,
		payment.IdempotencyKey,
	)

	return err
}

func (r *PaymentRepository) GetPaymentByIdempotencyKey(ctx context.Context, key string) (domain.Payment, error) {
	query := `
		SELECT 
			payment_id,
			user_id,
			amount,
			currency,
			status,
			idempotency_key,
			created_at,
			updated_at
		FROM payments WHERE idempotency_key = $1;
	`

	var p domain.Payment
	err := r.db.QueryRowContext(ctx, query, key).Scan(
		&p.PaymentID,
		&p.UserID,
		&p.Amount,
		&p.Currency,
		&p.Status,
		&p.IdempotencyKey,
		&p.CreatedAt,
		&p.UpdatedAt,
	)

	return p, err
}
