package repository

import (
	"context"
	"database/sql"
	"flowpay/offer-service/internal/domain"
	"fmt"
)

func (r *OfferRepository) GetOffersByCompanyID(ctx context.Context, companyID string) ([]domain.CompanyOfferRow, error) {
	query := `
		SELECT
			o.id,
			o.offer_code,
			o.offer_type,
			o.offer_amount,
			o.offer_percentage,
			o.max_benefit_amount,
			o.minimum_payment_amount,
			o.maximum_payment_amount,
			o.max_redemptions,
			o.max_redemptions_per_user,
			o.redeemed_count,
			o.status,
			o.budget_amount,
			o.start_time,
			o.end_time,
			o.created_at,
			COALESCE(a.account_name, '') AS promotion_pool_name,
			a.balance AS remaining_budget
		FROM offers o
		JOIN users u ON u.id = o.created_by
		LEFT JOIN accounts a ON a.id = o.promotion_pool_account_id
		WHERE u.company_id = $1
		ORDER BY o.created_at DESC
	`

	rows, err := r.db.QueryContext(ctx, query, companyID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var offers []domain.CompanyOfferRow
	for rows.Next() {
		var row domain.CompanyOfferRow
		var maxPaymentAmount sql.NullInt64
		var budgetAmount sql.NullInt64
		var remainingBudget sql.NullInt64
		var offerAmount sql.NullInt64
		var offerPercentage sql.NullInt64

		err := rows.Scan(
			&row.ID,
			&row.OfferCode,
			&row.OfferType,
			&offerAmount,
			&offerPercentage,
			&row.MaxBenefitAmount,
			&row.MinimumPaymentAmount,
			&maxPaymentAmount,
			&row.MaxRedemptions,
			&row.MaxRedemptionsPerUser,
			&row.RedeemedCount,
			&row.Status,
			&budgetAmount,
			&row.StartTime,
			&row.EndTime,
			&row.CreatedAt,
			&row.PromotionPoolName,
			&remainingBudget,
		)
		if err != nil {
			return nil, err
		}

		if offerAmount.Valid {
			row.OfferAmount = &offerAmount.Int64
		}
		if offerPercentage.Valid {
			row.OfferPercentage = &offerPercentage.Int64
		}
		if maxPaymentAmount.Valid {
			row.MaximumPaymentAmount = &maxPaymentAmount.Int64
		}
		if budgetAmount.Valid {
			row.BudgetAmount = &budgetAmount.Int64
		}
		if remainingBudget.Valid {
			row.RemainingBudget = &remainingBudget.Int64
		}

		offers = append(offers, row)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return offers, nil
}

func (r *OfferRepository) GetAllOffers(ctx context.Context) ([]domain.CompanyOfferRow, error) {
	query := `
		SELECT
			o.id,
			o.offer_code,
			o.offer_type,
			o.offer_amount,
			o.offer_percentage,
			o.max_benefit_amount,
			o.minimum_payment_amount,
			o.maximum_payment_amount,
			o.max_redemptions,
			o.max_redemptions_per_user,
			o.redeemed_count,
			o.status,
			o.budget_amount,
			o.start_time,
			o.end_time,
			o.created_at,
			COALESCE(a.account_name, '') AS promotion_pool_name,
			a.balance AS remaining_budget
		FROM offers o
		LEFT JOIN accounts a ON a.id = o.promotion_pool_account_id
		ORDER BY o.created_at DESC
	`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var offers []domain.CompanyOfferRow
	for rows.Next() {
		var row domain.CompanyOfferRow
		var maxPaymentAmount sql.NullInt64
		var budgetAmount sql.NullInt64
		var remainingBudget sql.NullInt64
		var offerAmount sql.NullInt64
		var offerPercentage sql.NullInt64

		err := rows.Scan(
			&row.ID,
			&row.OfferCode,
			&row.OfferType,
			&offerAmount,
			&offerPercentage,
			&row.MaxBenefitAmount,
			&row.MinimumPaymentAmount,
			&maxPaymentAmount,
			&row.MaxRedemptions,
			&row.MaxRedemptionsPerUser,
			&row.RedeemedCount,
			&row.Status,
			&budgetAmount,
			&row.StartTime,
			&row.EndTime,
			&row.CreatedAt,
			&row.PromotionPoolName,
			&remainingBudget,
		)
		if err != nil {
			return nil, err
		}

		if offerAmount.Valid {
			row.OfferAmount = &offerAmount.Int64
		}
		if offerPercentage.Valid {
			row.OfferPercentage = &offerPercentage.Int64
		}
		if maxPaymentAmount.Valid {
			row.MaximumPaymentAmount = &maxPaymentAmount.Int64
		}
		if budgetAmount.Valid {
			row.BudgetAmount = &budgetAmount.Int64
		}
		if remainingBudget.Valid {
			row.RemainingBudget = &remainingBudget.Int64
		}

		offers = append(offers, row)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return offers, nil
}

func (r *OfferRepository) GetCompanyOffersSummary(ctx context.Context, companyID string) (domain.CompanyOffersSummaryRow, error) {
	query := `
		SELECT
			COUNT(*) FILTER (WHERE o.status = 'ACTIVE') AS active_offers,
			COUNT(*) AS total_offers,
			COALESCE(SUM(o.redeemed_count), 0) AS total_redemptions,
			COALESCE(SUM(a.balance), 0) AS budget_remaining,
			COALESCE(SUM(o.budget_amount), 0) AS initial_budget,
			COALESCE(
				AVG(
					CASE WHEN o.max_redemptions > 0
					THEN o.redeemed_count * 100.0 / o.max_redemptions
					ELSE 0 END
				),
				0
			) AS avg_conversion_rate
		FROM offers o
		JOIN users u ON u.id = o.created_by
		LEFT JOIN accounts a ON a.id = o.promotion_pool_account_id
		WHERE u.company_id = $1
	`

	var summary domain.CompanyOffersSummaryRow
	err := r.db.QueryRowContext(ctx, query, companyID).Scan(
		&summary.ActiveOffers,
		&summary.TotalOffers,
		&summary.TotalRedemptions,
		&summary.BudgetRemaining,
		&summary.InitialBudget,
		&summary.AvgConversionRate,
	)
	if err != nil {
		return domain.CompanyOffersSummaryRow{}, err
	}

	return summary, nil
}

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

func (r *OfferRepository) UpdateOfferStatus(
	ctx context.Context,
	tx *sql.Tx,
	offerID string,
	status string,
) error {
	query := `
		UPDATE offers
		SET
			status = $2,
			updated_at = NOW()
		WHERE id = $1;
	`

	res, err := tx.ExecContext(ctx, query, offerID, status)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("offer status update failed: expected 1 row affected, got %d", rowsAffected)
	}
	return nil
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
