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

func (r *PaymentRepository) GetPaymentByIdempotencyKey(ctx context.Context, key string) (domain.Payment, error) {
	query := `
		SELECT
			id,
			idempotency_key,
			sender_id,
			receiver_id,
			amount,
			currency,
			status,
			created_at,
			updated_at
		FROM payments
		WHERE idempotency_key = $1;
	`

	var payment domain.Payment
	err := r.db.QueryRowContext(ctx, query, key).Scan(
		&payment.ID,
		&payment.IdempotencyKey,
		&payment.SenderID,
		&payment.ReceiverID,
		&payment.Amount,
		&payment.Currency,
		&payment.Status,
		&payment.CreatedAt,
		&payment.UpdatedAt,
	)

	return payment, err
}

func (r *PaymentRepository) GetPaymentByID(ctx context.Context, paymentID string) (domain.Payment, error) {
	query := `
		SELECT
			id,
			idempotency_key,
			sender_id,
			receiver_id,
			amount,
			currency,
			offer_id,
			offer_benefit_amount,
			offer_type,
			offer_code,
			payment_method,
			status,
			created_at,
			updated_at,
			completed_at
		FROM payments
		WHERE id = $1;
	`

	var payment domain.Payment
	var offerID, offerType, offerCode sql.NullString
	var offerBenefitAmount sql.NullInt64
	var completedAt sql.NullTime

	err := r.db.QueryRowContext(ctx, query, paymentID).Scan(
		&payment.ID,
		&payment.IdempotencyKey,
		&payment.SenderID,
		&payment.ReceiverID,
		&payment.Amount,
		&payment.Currency,
		&offerID,
		&offerBenefitAmount,
		&offerType,
		&offerCode,
		&payment.PaymentMethod,
		&payment.Status,
		&payment.CreatedAt,
		&payment.UpdatedAt,
		&completedAt,
	)
	if err != nil {
		return domain.Payment{}, err
	}

	payment.OfferID = offerID.String
	payment.OfferType = offerType.String
	payment.OfferCode = offerCode.String
	payment.OfferBenefitAmount = offerBenefitAmount.Int64
	if completedAt.Valid {
		completedAtValue := completedAt.Time
		payment.CompletedAt = &completedAtValue
	}

	return payment, nil
}
