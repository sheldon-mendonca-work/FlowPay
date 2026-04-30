package repository

import (
	"context"
	"database/sql"
	"flowpay/payment-service/internal/domain"
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
			sender_id,
			receiver_id,
			amount,
			currency,
			status,
			created_at,
			updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
		 ON CONFLICT (id) DO NOTHING;
	`

	res, err := tx.ExecContext(ctx,
		query,
		payment.ID,
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

// func (r *PaymentRepository) GetPaymentByIdempotencyKey(ctx context.Context, key string) (domain.PaymentIdempotencyKey, error) {
// 	query := `
// 		SELECT
// 			idempotency_key,
// 			request_hash,
// 			response_body,
// 			status,
// 			created_at,
// 		FROM payments WHERE idempotency_key = $1;
// 	`

// 	var p domain.PaymentIdempotencyKey
// 	err := r.db.QueryRowContext(ctx, query, key).Scan(
// 		&p.IdempotencyKey,
// 		&p.RequestHash,
// 		&p.ResponseBody,
// 		&p.Status,
// 		&p.CreatedAt,
// 	)

// 	return p, err
// }
