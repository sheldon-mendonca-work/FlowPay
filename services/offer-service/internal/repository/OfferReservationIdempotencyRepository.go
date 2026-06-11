package repository

import (
	"context"
	"database/sql"
	"errors"
	"flowpay/offer-service/internal/domain"
	"flowpay/offer-service/internal/types"
	"fmt"
	"time"
)

type OfferReservationIdempotencyRepository struct {
	db *sql.DB
}

func NewOfferReservationIdempotencyRepository(db *sql.DB) *OfferReservationIdempotencyRepository {
	return &OfferReservationIdempotencyRepository{
		db: db,
	}
}

func (r *OfferReservationIdempotencyRepository) ClaimOrGet(ctx context.Context, idempotency domain.OfferReservationIdempotencyKeyEntity) (domain.OfferReservationIdempotencyKeyEntity, bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.OfferReservationIdempotencyKeyEntity{}, false, err
	}

	committed := false
	defer func() {
		if !committed {
			tx.Rollback()
		}
	}()

	existing, err := r.getByKeyForUpdate(ctx, tx, idempotency.IdempotencyKey)
	switch {
	case err == nil:
	case errors.Is(err, sql.ErrNoRows):
		{
			if err := r.insertClaim(ctx, tx, idempotency); err != nil {
				return domain.OfferReservationIdempotencyKeyEntity{}, false, err
			}

			if err := tx.Commit(); err != nil {
				return domain.OfferReservationIdempotencyKeyEntity{}, false, err
			}
			committed = true
			return idempotency, true, nil

		}
	default:
		return domain.OfferReservationIdempotencyKeyEntity{}, false, err
	}

	if existing.RequestHash != idempotency.RequestHash {
		if err := tx.Commit(); err != nil {
			return domain.OfferReservationIdempotencyKeyEntity{}, false, err
		}
		committed = true
		return existing, false, nil
	}

	if existing.Status == "COMPLETED" || existing.Status == "FAILED" {
		if err := tx.Commit(); err != nil {
			return domain.OfferReservationIdempotencyKeyEntity{}, false, err
		}
		committed = true
		return existing, false, nil
	}

	now := time.Now().UTC()
	if existing.LockedUntil.After(now) {
		if err := tx.Commit(); err != nil {
			return domain.OfferReservationIdempotencyKeyEntity{}, false, err
		}
		committed = true
		return existing, false, nil
	}

	existingReservation, err := r.takeOverClaim(ctx, tx, idempotency)
	if err != nil {
		return domain.OfferReservationIdempotencyKeyEntity{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return domain.OfferReservationIdempotencyKeyEntity{}, false, err
	}
	if existingReservation.ReservationID != "" {
		idempotency.ReservationID = existingReservation.ReservationID
	}
	committed = true
	return idempotency, true, nil
}

func (r *OfferReservationIdempotencyRepository) getByKeyForUpdate(ctx context.Context, tx *sql.Tx, idempotencyKey string) (domain.OfferReservationIdempotencyKeyEntity, error) {
	query := `
		SELECT
			idempotency_key,
			request_hash,
			COALESCE(response_body::text, ''),
			status,
			COALESCE(error_code, ''),
			COALESCE(error_message, ''),
			COALESCE(offer_id::text, ''),
			COALESCE(payment_id::text, ''),
			COALESCE(reservation_id::text, ''),
			COALESCE(owner_token, ''),
			locked_until,
			created_at,
			updated_at
		FROM offer_reservation_idempotency_keys
		WHERE idempotency_key = $1
		FOR UPDATE;
	`

	var p domain.OfferReservationIdempotencyKeyEntity
	err := tx.QueryRowContext(ctx, query, idempotencyKey).Scan(
		&p.IdempotencyKey,
		&p.RequestHash,
		&p.ResponseBody,
		&p.Status,
		&p.ErrorCode,
		&p.ErrorMessage,
		&p.OfferID,
		&p.PaymentID,
		&p.ReservationID,
		&p.OwnerToken,
		&p.LockedUntil,
		&p.CreatedAt,
		&p.UpdatedAt,
	)
	return p, err
}

func (r *OfferReservationIdempotencyRepository) insertClaim(
	ctx context.Context,
	tx *sql.Tx,
	idempotency domain.OfferReservationIdempotencyKeyEntity,
) error {
	query := `
		INSERT INTO offer_reservation_idempotency_keys (
			idempotency_key,
			request_hash,
			status,
			owner_token,
			locked_until,
			offer_id,
			payment_id,
			reservation_id,
			created_at,
			updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW());
	`
	res, err := tx.ExecContext(
		ctx,
		query,
		idempotency.IdempotencyKey,
		idempotency.RequestHash,
		idempotency.Status,
		idempotency.OwnerToken,
		idempotency.LockedUntil,
		idempotency.OfferID,
		idempotency.PaymentID,
		idempotency.ReservationID,
	)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("insert claim failed: expected 1 row affected, got %d", rowsAffected)
	}
	return nil
}

func (r *OfferReservationIdempotencyRepository) takeOverClaim(
	ctx context.Context,
	tx *sql.Tx,
	idempotency domain.OfferReservationIdempotencyKeyEntity,
) (types.ExistingReservation, error) {
	query := `
		WITH updated AS (
			UPDATE offer_reservation_idempotency_keys
			SET
				owner_token = $2,
				locked_until = $3,
				updated_at = NOW()
			WHERE idempotency_key = $1
			RETURNING reservation_id, payment_id, offer_id
		)
		SELECT
			COUNT(*) AS affected_rows,
			MAX(reservation_id) AS reservation_id,
			MAX(payment_id) AS payment_id,
			MAX(offer_id) AS offer_id
		FROM updated;
	`

	var (
		affectedRows        int
		existingReservation types.ExistingReservation
	)

	err := tx.QueryRowContext(
		ctx,
		query,
		idempotency.IdempotencyKey,
		idempotency.OwnerToken,
		idempotency.LockedUntil,
	).Scan(&affectedRows, &existingReservation.ReservationID, &existingReservation.PaymentID, &existingReservation.OfferID)

	if err != nil {
		return types.ExistingReservation{}, err
	}

	if affectedRows != 1 {
		return types.ExistingReservation{}, fmt.Errorf(
			"take over claim failed: expected 1 row affected, got %d",
			affectedRows,
		)
	}

	return existingReservation, nil
}

func (r *OfferReservationIdempotencyRepository) GetByKey(
	ctx context.Context,
	key string,
) (domain.OfferReservationIdempotencyKeyEntity, error) {
	query := `
		SELECT
			idempotency_key,
			request_hash,
			COALESCE(response_body::text, ''),
			status,
			COALESCE(error_code, ''),
			COALESCE(error_message, ''),
			COALESCE(offer_id::text, ''),
			COALESCE(payment_id::text, ''),
			COALESCE(reservation_id::text, ''),
			COALESCE(owner_token, ''),
			locked_until,
			created_at,
			updated_at
		FROM offer_reservation_idempotency_keys
		WHERE idempotency_key = $1;
	`

	var p domain.OfferReservationIdempotencyKeyEntity
	err := r.db.QueryRowContext(ctx, query, key).Scan(
		&p.IdempotencyKey,
		&p.RequestHash,
		&p.ResponseBody,
		&p.Status,
		&p.ErrorCode,
		&p.ErrorMessage,
		&p.OfferID,
		&p.PaymentID,
		&p.ReservationID,
		&p.OwnerToken,
		&p.LockedUntil,
		&p.CreatedAt,
		&p.UpdatedAt,
	)

	return p, err
}

func (r *OfferReservationIdempotencyRepository) GetByReservationID(
	ctx context.Context,
	reservationID string,
) (domain.OfferReservationIdempotencyKeyEntity, error) {
	query := `
		SELECT
			idempotency_key,
			request_hash,
			COALESCE(response_body::text, ''),
			status,
			COALESCE(error_code, ''),
			COALESCE(error_message, ''),
			COALESCE(offer_id::text, ''),
			COALESCE(payment_id::text, ''),
			COALESCE(reservation_id::text, ''),
			COALESCE(owner_token, ''),
			locked_until,
			created_at,
			updated_at
		FROM offer_reservation_idempotency_keys
		WHERE reservation_id = $1::uuid;
	`

	var p domain.OfferReservationIdempotencyKeyEntity
	err := r.db.QueryRowContext(ctx, query, reservationID).Scan(
		&p.IdempotencyKey,
		&p.RequestHash,
		&p.ResponseBody,
		&p.Status,
		&p.ErrorCode,
		&p.ErrorMessage,
		&p.OfferID,
		&p.PaymentID,
		&p.ReservationID,
		&p.OwnerToken,
		&p.LockedUntil,
		&p.CreatedAt,
		&p.UpdatedAt,
	)

	return p, err
}

func (r *OfferReservationIdempotencyRepository) MarkCompleted(
	tx *sql.Tx,
	ctx context.Context,
	idempotencyKey string,
	responseBody string,
	reservationID string,
	paymentID string,
	ownerToken string,
) error {
	query := `
		UPDATE offer_reservation_idempotency_keys
		SET
			response_body = $2::jsonb,
			status = 'COMPLETED',
			reservation_id = $3::uuid,
			payment_id = $5::uuid,
			error_code = NULL,
			error_message = NULL,
			owner_token = NULL,
			locked_until = NOW(),
			updated_at = NOW()
		WHERE idempotency_key = $1
		  AND owner_token = $4;
	`

	res, err := tx.ExecContext(ctx, query, idempotencyKey, responseBody, reservationID, ownerToken, paymentID)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("mark completed failed: expected 1 row affected, got %d", rowsAffected)
	}
	return nil
}

func (r *OfferReservationIdempotencyRepository) MarkFailed(
	tx *sql.Tx,
	ctx context.Context,
	idempotencyKey string,
	errorCode string,
	errorMessage string,
	ownerToken string,
) error {
	query := `
		UPDATE offer_reservation_idempotency_keys
		SET
			status = 'FAILED',
			error_code = $2,
			error_message = $3,
			response_body = NULL,
			owner_token = NULL,
			locked_until = NOW(),
			updated_at = NOW()
		WHERE idempotency_key = $1
		  AND owner_token = $4;
	`

	res, err := tx.ExecContext(ctx, query, idempotencyKey, errorCode, errorMessage, ownerToken)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("mark failed failed: expected 1 row affected, got %d", rowsAffected)
	}
	return nil
}
