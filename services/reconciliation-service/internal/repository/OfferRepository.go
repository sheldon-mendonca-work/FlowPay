package repository

import (
	"context"
	"database/sql"

	"flowpay/reconciliation-service/internal/domain"
)

type OfferRepository struct {
	db *sql.DB
}

func NewOfferRepository(db *sql.DB) *OfferRepository {
	return &OfferRepository{db: db}
}

// GetSuccessfulPaymentsWithUnredeemedReservation finds payments that completed
// successfully whose linked offer reservation was never marked REDEEMED.
//
// payments.offer_id/reservation_id/redemption_id are write-only columns from
// payment-service's perspective (never populated by any INSERT/UPDATE), so the
// reliable link between a payment and its offer is offer_reservations.payment_id,
// which offer-service populates when it consumes the PaymentInitiated event.
func (r *OfferRepository) GetSuccessfulPaymentsWithUnredeemedReservation(ctx context.Context, tx *sql.Tx) ([]domain.SuccessfulPaymentWithUnredeemedReservation, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT p.id::text, res.id::text, res.offer_id::text, res.status, p.status
		FROM payments p
		JOIN offer_reservations res
			ON res.payment_id = p.id
		WHERE p.status = 'SUCCESS'
		AND res.status != 'REDEEMED';
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var payments []domain.SuccessfulPaymentWithUnredeemedReservation
	for rows.Next() {
		var payment domain.SuccessfulPaymentWithUnredeemedReservation
		if err := rows.Scan(&payment.PaymentID, &payment.ReservationID, &payment.OfferID, &payment.ReservationStatus, &payment.PaymentStatus); err != nil {
			return nil, err
		}
		payments = append(payments, payment)
	}
	return payments, rows.Err()
}

// GetRedeemedReservationsWithoutSuccessfulPayment is the converse of
// GetSuccessfulPaymentsWithUnredeemedReservation: reservations marked REDEEMED
// whose linked payment did not succeed (customer received the offer benefit
// despite the underlying payment failing or still being pending).
func (r *OfferRepository) GetRedeemedReservationsWithoutSuccessfulPayment(ctx context.Context, tx *sql.Tx) ([]domain.RedeemedReservationWithoutSuccessfulPayment, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT res.id::text, res.offer_id::text, p.id::text, res.status, p.status
		FROM offer_reservations res
		JOIN payments p
			ON p.id = res.payment_id
		WHERE res.status = 'REDEEMED'
		AND p.status != 'SUCCESS';
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reservations []domain.RedeemedReservationWithoutSuccessfulPayment
	for rows.Next() {
		var reservation domain.RedeemedReservationWithoutSuccessfulPayment
		if err := rows.Scan(&reservation.ReservationID, &reservation.OfferID, &reservation.PaymentID, &reservation.ReservationStatus, &reservation.PaymentStatus); err != nil {
			return nil, err
		}
		reservations = append(reservations, reservation)
	}
	return reservations, rows.Err()
}

func (r *OfferRepository) GetReservationsWithoutPayment(ctx context.Context, tx *sql.Tx) ([]domain.OfferReservationWithoutPayment, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT res.id::text, res.offer_id::text, res.payment_id::text, res.status
		FROM offer_reservations res
		WHERE res.payment_id IS NOT NULL
		AND NOT EXISTS (
			SELECT 1 FROM payments p WHERE p.id = res.payment_id
		);
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reservations []domain.OfferReservationWithoutPayment
	for rows.Next() {
		var reservation domain.OfferReservationWithoutPayment
		if err := rows.Scan(&reservation.ID, &reservation.OfferID, &reservation.PaymentID, &reservation.Status); err != nil {
			return nil, err
		}
		reservations = append(reservations, reservation)
	}
	return reservations, rows.Err()
}

func (r *OfferRepository) GetRedemptionsWithoutPayment(ctx context.Context, tx *sql.Tx) ([]domain.OfferRedemptionWithoutPayment, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT red.id::text, red.offer_id::text, red.reservation_id::text, red.payment_id::text, red.status
		FROM offer_redemptions red
		WHERE red.payment_id IS NOT NULL
		AND NOT EXISTS (
			SELECT 1 FROM payments p WHERE p.id = red.payment_id
		);
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var redemptions []domain.OfferRedemptionWithoutPayment
	for rows.Next() {
		var redemption domain.OfferRedemptionWithoutPayment
		if err := rows.Scan(&redemption.ID, &redemption.OfferID, &redemption.ReservationID, &redemption.PaymentID, &redemption.Status); err != nil {
			return nil, err
		}
		redemptions = append(redemptions, redemption)
	}
	return redemptions, rows.Err()
}

func (r *OfferRepository) GetExpiredReservationsStuckReserved(ctx context.Context, tx *sql.Tx) ([]domain.ExpiredOfferReservation, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT res.id::text, res.offer_id::text, res.payment_id::text, res.expires_at
		FROM offer_reservations res
		WHERE res.status = 'RESERVED'
		AND res.expires_at < NOW();
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reservations []domain.ExpiredOfferReservation
	for rows.Next() {
		var reservation domain.ExpiredOfferReservation
		if err := rows.Scan(&reservation.ID, &reservation.OfferID, &reservation.PaymentID, &reservation.ExpiresAt); err != nil {
			return nil, err
		}
		reservations = append(reservations, reservation)
	}
	return reservations, rows.Err()
}

func (r *OfferRepository) GetReservationRedemptionStatusMismatch(ctx context.Context, tx *sql.Tx) ([]domain.OfferReservationRedemptionMismatch, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT res.id::text, res.offer_id::text, res.status, red.id::text, red.status
		FROM offer_reservations res
		LEFT JOIN offer_redemptions red
			ON red.reservation_id = res.id
		WHERE (res.status = 'REDEEMED' AND (red.id IS NULL OR red.status != 'SUCCESS'))
		OR (red.status = 'SUCCESS' AND res.status != 'REDEEMED');
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var mismatches []domain.OfferReservationRedemptionMismatch
	for rows.Next() {
		var mismatch domain.OfferReservationRedemptionMismatch
		if err := rows.Scan(&mismatch.ReservationID, &mismatch.OfferID, &mismatch.ReservationStatus, &mismatch.RedemptionID, &mismatch.RedemptionStatus); err != nil {
			return nil, err
		}
		mismatches = append(mismatches, mismatch)
	}
	return mismatches, rows.Err()
}

func (r *OfferRepository) GetOfferRedeemedCountDrift(ctx context.Context, tx *sql.Tx) ([]domain.OfferRedeemedCountDrift, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT o.id::text, o.offer_code, o.redeemed_count,
			COUNT(red.id) FILTER (WHERE red.status = 'SUCCESS') AS actual_redeemed_count
		FROM offers o
		LEFT JOIN offer_redemptions red
			ON red.offer_id = o.id
		GROUP BY o.id, o.offer_code, o.redeemed_count
		HAVING o.redeemed_count != COUNT(red.id) FILTER (WHERE red.status = 'SUCCESS');
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var drifts []domain.OfferRedeemedCountDrift
	for rows.Next() {
		var drift domain.OfferRedeemedCountDrift
		if err := rows.Scan(&drift.OfferID, &drift.OfferCode, &drift.RedeemedCount, &drift.ActualRedeemedCount); err != nil {
			return nil, err
		}
		drifts = append(drifts, drift)
	}
	return drifts, rows.Err()
}
