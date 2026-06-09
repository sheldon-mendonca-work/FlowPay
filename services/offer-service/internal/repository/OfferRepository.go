package repository

import (
	"context"
	"database/sql"
	"flowpay/offer-service/internal/domain"
	"fmt"
)

type OfferRepository struct {
	db *sql.DB
}

func NewOfferRepository(db *sql.DB) *OfferRepository {
	return &OfferRepository{
		db: db,
	}
}

func (r *OfferRepository) CreateOffer(ctx context.Context, tx *sql.Tx, offerItem domain.OfferEntity) error {
	query := `
		INSERT INTO offers (
			id,
			offer_code,
			offer_type,
			offer_amount,
			offer_percentage,
			max_benefit_amount,
			minimum_payment_amount,
			maximum_payment_amount,
			max_redemptions,
			redeemed_count,
			max_redemptions_per_user,
			idempotency_key,
			status,
			version,
			start_time,
			end_time,
			created_by,
			created_at,
			updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, NOW(), NOW())
		 ON CONFLICT (id) DO NOTHING;
	`
	res, err := tx.ExecContext(ctx, query,
		offerItem.ID,
		offerItem.OfferCode,
		offerItem.OfferType,
		offerItem.OfferAmount,
		offerItem.OfferPercentage,
		offerItem.MaxBenefitAmount,
		offerItem.MinimumPaymentAmount,
		offerItem.MaximumPaymentAmount,
		offerItem.MaxRedemptions,
		offerItem.RedeemedCount,
		offerItem.MaxRedemptionsPerUser,
		offerItem.IdempotencyKey,
		offerItem.Status,
		offerItem.Version,
		offerItem.StartTime,
		offerItem.EndTime,
		offerItem.CreatedBy,
	)

	if err != nil {
		return err
	}

	rowsAffected, _ := res.RowsAffected()
	if rowsAffected != 1 {
		return fmt.Errorf("Offer Creation failed: expected 1 row affected, but got %d", rowsAffected)
	}
	return nil
}
