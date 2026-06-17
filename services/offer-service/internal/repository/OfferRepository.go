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
			reserved_count,
			max_redemptions_per_user,
			idempotency_key,
			status,
			version,
			start_time,
			end_time,
			created_by,
			promotion_pool_account_id,
			budget_amount,
			created_at,
			updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, NOW(), NOW())
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
		offerItem.ReservedCount,
		offerItem.MaxRedemptionsPerUser,
		offerItem.IdempotencyKey,
		offerItem.Status,
		offerItem.Version,
		offerItem.StartTime,
		offerItem.EndTime,
		offerItem.CreatedBy,
		offerItem.PromotionPoolAccountId,
		offerItem.BudgetAmount,
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

func (r *OfferRepository) GetOfferByIDForUpdate(ctx context.Context, tx *sql.Tx, offerID string) (domain.OfferEntity, error) {
	query := `
		SELECT
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
			reserved_count,
			max_redemptions_per_user,
			idempotency_key,
			status,
			version,
			start_time,
			end_time,
			created_by,
			promotion_pool_account_id,
			budget_amount,
			created_at,
			updated_at
		FROM offers
		WHERE id = $1
		ORDER BY id
		FOR UPDATE;
	`

	var offer domain.OfferEntity
	err := tx.QueryRowContext(ctx, query, offerID).Scan(
		&offer.ID,
		&offer.OfferCode,
		&offer.OfferType,
		&offer.OfferAmount,
		&offer.OfferPercentage,
		&offer.MaxBenefitAmount,
		&offer.MinimumPaymentAmount,
		&offer.MaximumPaymentAmount,
		&offer.MaxRedemptions,
		&offer.RedeemedCount,
		&offer.ReservedCount,
		&offer.MaxRedemptionsPerUser,
		&offer.IdempotencyKey,
		&offer.Status,
		&offer.Version,
		&offer.StartTime,
		&offer.EndTime,
		&offer.CreatedBy,
		&offer.PromotionPoolAccountId,
		&offer.BudgetAmount,
		&offer.CreatedAt,
		&offer.UpdatedAt,
	)

	return offer, err
}

func (r *OfferRepository) IncrementReservedCount(
	ctx context.Context,
	tx *sql.Tx,
	offerID string,
) error {
	query := `
		UPDATE offers
		SET
			reserved_count = reserved_count+1,
			updated_at = NOW()
		WHERE id = $1
		  AND (reserved_count + redeemed_count) < max_redemptions;
	`

	res, err := tx.ExecContext(ctx, query, offerID)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("offer deduction failed: expected 1 row affected, got %d", rowsAffected)
	}
	return nil
}

func (r *OfferRepository) ConvertReservedToRedeemed(
	ctx context.Context,
	tx *sql.Tx,
	offerID string,
) error {
	query := `
		UPDATE offers
		SET
			reserved_count = reserved_count-1,
			redeemed_count = redeemed_count+1,
			updated_at = NOW()
		WHERE id = $1
		  AND reserved_count > 0;
	`

	res, err := tx.ExecContext(ctx, query, offerID)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("offer conversion to redeem failed: expected 1 row affected, got %d", rowsAffected)
	}
	return nil
}
