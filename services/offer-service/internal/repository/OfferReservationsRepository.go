package repository

import (
	"context"
	"database/sql"
	"flowpay/offer-service/internal/domain"
	"fmt"
)

type OfferReservationsRepository struct {
	db *sql.DB
}

func NewOfferReservationsRepository(db *sql.DB) *OfferReservationsRepository {
	return &OfferReservationsRepository{
		db: db,
	}
}

func (r *OfferReservationsRepository) CreateReservation(ctx context.Context, tx *sql.Tx, reservation domain.OfferReservationEntity) error {
	query := `
		INSERT INTO offer_reservations (
			id,
			offer_id,
			account_id,
			payment_id,
			idempotency_key,
			status,
			expires_at,
			created_at,
			updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
		 ON CONFLICT (id) DO NOTHING;
	`
	res, err := tx.ExecContext(ctx, query,
		reservation.ID,
		reservation.OfferID,
		reservation.AccountID,
		reservation.PaymentID,
		reservation.IdempotencyKey,
		reservation.Status,
		reservation.ExpiresAt,
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

func (r *OfferReservationsRepository) CountActiveReservationsForAccount(
	ctx context.Context,
	tx *sql.Tx,
	offerID string,
	accountID string,
) (int, error) {

	query := `
		SELECT COUNT(*)
		FROM offer_reservations
		WHERE offer_id = $1::uuid
		AND account_id = $2::uuid
		AND (
				status = 'REDEEMED'
				OR
				(
					status = 'RESERVED'
					AND expires_at > NOW()
				)
		);
	`

	var count int

	err := tx.QueryRowContext(
		ctx,
		query,
		offerID,
		accountID,
	).Scan(&count)

	return count, err
}
