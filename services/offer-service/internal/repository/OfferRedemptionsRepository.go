package repository

import (
	"context"
	"database/sql"
	"flowpay/offer-service/internal/domain"
	"fmt"
)

type OfferRedemptionsRepository struct {
	db *sql.DB
}

func NewOfferRedemptionsRepository(db *sql.DB) *OfferRedemptionsRepository {
	return &OfferRedemptionsRepository{
		db: db,
	}
}

func (r *OfferRedemptionsRepository) CreateRedemption(ctx context.Context, tx *sql.Tx, redemption domain.OfferRedemptionEntity) error {
	query := `
		INSERT INTO offer_redemptions (
			id,
			offer_id,
			account_id,
			payment_id,
			reservation_id,
			idempotency_key,
			status,
			created_at,
			redeemed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
		 ON CONFLICT (id) DO NOTHING;
	`
	res, err := tx.ExecContext(ctx, query,
		redemption.ID,
		redemption.OfferID,
		redemption.AccountID,
		redemption.PaymentID,
		redemption.ReservationID,
		redemption.IdempotencyKey,
		redemption.Status,
	)

	if err != nil {
		return err
	}

	rowsAffected, _ := res.RowsAffected()
	if rowsAffected != 1 {
		return fmt.Errorf("Offer Reservation Creation failed: expected 1 row affected, but got %d", rowsAffected)
	}
	return nil
}
