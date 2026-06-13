package repository

import (
	"context"
	"database/sql"
	"flowpay/offer-expiry-worker/internal/domain"
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

func (r *OfferReservationsRepository) ClaimBatch(
	ctx context.Context,
	batchSize int,
) ([]domain.OfferReservationEntity, error) {

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	committed := false

	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	query := `
		WITH claimed AS (
			SELECT id
			FROM offer_reservations
			WHERE status = 'RESERVED'
			  AND expires_at < NOW()
			ORDER BY created_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE offer_reservations r
		SET
			status = 'EXPIRED',
			updated_at = NOW()
		FROM claimed
		WHERE r.id = claimed.id
		RETURNING
			r.id,
			r.offer_id,
			r.payment_id,
			r.account_id,
			r.idempotency_key,
			r.status;
	`

	rows, err := tx.QueryContext(ctx, query, batchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reservations []domain.OfferReservationEntity

	offerCounts := make(map[string]int)

	for rows.Next() {
		var e domain.OfferReservationEntity

		if err := rows.Scan(
			&e.ID,
			&e.OfferID,
			&e.PaymentID,
			&e.AccountID,
			&e.IdempotencyKey,
			&e.Status,
		); err != nil {
			return nil, err
		}

		offerCounts[e.OfferID]++
		reservations = append(reservations, e)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(reservations) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		committed = true
		return reservations, nil
	}

	for offerID, count := range offerCounts {

		updateOfferQuery := `
			UPDATE offers
			SET
				reserved_count = GREATEST(reserved_count - $1, 0),
				updated_at = NOW()
			WHERE offer_id = $2;
		`

		res, err := tx.ExecContext(
			ctx,
			updateOfferQuery,
			count,
			offerID,
		)
		if err != nil {
			return nil, err
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}

		if rowsAffected != 1 {
			return nil, fmt.Errorf(
				"offer update failed: offer_id=%s affected=%d",
				offerID,
				rowsAffected,
			)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	committed = true

	return reservations, nil
}
