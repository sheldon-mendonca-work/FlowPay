package repository

import (
	"context"
	"database/sql"
	"errors"
	"flowpay/offer-service/internal/domain"
	"fmt"
	"time"
)

type OfferRedemptionIdempotencyRepository struct {
	db *sql.DB
}

func NewOfferRedemptionIdempotencyRepository(db *sql.DB) *OfferRedemptionIdempotencyRepository {
	return &OfferRedemptionIdempotencyRepository{
		db: db,
	}
}

func (r *OfferRedemptionIdempotencyRepository) ClaimOrGet(ctx context.Context, idempotency domain.OfferIdempotencyKey) (domain.OfferIdempotencyKey, bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.OfferIdempotencyKey{}, false, err
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
				return domain.OfferIdempotencyKey{}, false, err
			}

			if err := tx.Commit(); err != nil {
				return domain.OfferIdempotencyKey{}, false, err
			}
			committed = true
			return idempotency, true, nil

		}
	default:
		return domain.OfferIdempotencyKey{}, false, err
	}

	if existing.RequestHash != idempotency.RequestHash {
		if err := tx.Commit(); err != nil {
			return domain.OfferIdempotencyKey{}, false, err
		}
		committed = true
		return existing, false, nil
	}

	if existing.Status == "COMPLETED" || existing.Status == "FAILED" {
		if err := tx.Commit(); err != nil {
			return domain.OfferIdempotencyKey{}, false, err
		}
		committed = true
		return existing, false, nil
	}

	now := time.Now().UTC()
	if existing.LockedUntil.After(now) {
		if err := tx.Commit(); err != nil {
			return domain.OfferIdempotencyKey{}, false, err
		}
		committed = true
		return existing, false, nil
	}

	existingOfferId, err := r.takeOverClaim(ctx, tx, idempotency)
	if err != nil {
		return domain.OfferIdempotencyKey{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return domain.OfferIdempotencyKey{}, false, err
	}
	if existingOfferId != "" {
		idempotency.OfferID = existingOfferId
	}
	committed = true
	return idempotency, true, nil
}

func (r *OfferRedemptionIdempotencyRepository) getByKeyForUpdate(ctx context.Context, tx *sql.Tx, idempotencyKey string) (domain.OfferIdempotencyKey, error) {
	query := `
		SELECT
			idempotency_key,
			request_hash,
			COALESCE(response_body::text, ''),
			status,
			COALESCE(error_code, ''),
			COALESCE(error_message, ''),
			COALESCE(offer_id::text, ''),
			COALESCE(owner_token, ''),
			locked_until,
			created_at,
			updated_at
		FROM offer_idempotency_keys
		WHERE idempotency_key = $1
		FOR UPDATE;
	`

	var p domain.OfferIdempotencyKey
	err := tx.QueryRowContext(ctx, query, idempotencyKey).Scan(
		&p.IdempotencyKey,
		&p.RequestHash,
		&p.ResponseBody,
		&p.Status,
		&p.ErrorCode,
		&p.ErrorMessage,
		&p.OfferID,
		&p.OwnerToken,
		&p.LockedUntil,
		&p.CreatedAt,
		&p.UpdatedAt,
	)
	return p, err
}

func (r *OfferRedemptionIdempotencyRepository) insertClaim(
	ctx context.Context,
	tx *sql.Tx,
	idempotency domain.OfferIdempotencyKey,
) error {
	query := `
		INSERT INTO offer_idempotency_keys (
			idempotency_key,
			request_hash,
			status,
			owner_token,
			locked_until,
			offer_id,
			redemption_id,
			created_at,
			updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW());
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

func (r *OfferRedemptionIdempotencyRepository) takeOverClaim(
	ctx context.Context,
	tx *sql.Tx,
	idempotency domain.OfferIdempotencyKey,
) (string, error) {
	query := `
		WITH updated AS (
			UPDATE offer_idempotency_keys
			SET
				owner_token = $2,
				locked_until = $3,
				updated_at = NOW()
			WHERE idempotency_key = $1
			RETURNING offer_id
		)
		SELECT
			COUNT(*) AS affected_rows,
			MAX(offer_id) AS offer_id
		FROM updated;
	`

	var (
		affectedRows  int
		existingOffer string
	)

	err := tx.QueryRowContext(
		ctx,
		query,
		idempotency.IdempotencyKey,
		idempotency.OwnerToken,
		idempotency.LockedUntil,
	).Scan(&affectedRows, &existingOffer)

	if err != nil {
		return "", err
	}

	if affectedRows != 1 {
		return "", fmt.Errorf(
			"take over claim failed: expected 1 row affected, got %d",
			affectedRows,
		)
	}

	return existingOffer, nil
}

func (r *OfferRedemptionIdempotencyRepository) GetByKey(
	ctx context.Context,
	key string,
) (domain.OfferIdempotencyKey, error) {
	query := `
		SELECT
			idempotency_key,
			request_hash,
			COALESCE(response_body::text, ''),
			status,
			COALESCE(error_code, ''),
			COALESCE(error_message, ''),
			COALESCE(offer_id::text, ''),
			COALESCE(owner_token, ''),
			locked_until,
			created_at,
			updated_at
		FROM offer_idempotency_keys
		WHERE idempotency_key = $1;
	`

	var p domain.OfferIdempotencyKey
	err := r.db.QueryRowContext(ctx, query, key).Scan(
		&p.IdempotencyKey,
		&p.RequestHash,
		&p.ResponseBody,
		&p.Status,
		&p.ErrorCode,
		&p.ErrorMessage,
		&p.OfferID,
		&p.OwnerToken,
		&p.LockedUntil,
		&p.CreatedAt,
		&p.UpdatedAt,
	)

	return p, err
}

func (r *OfferRedemptionIdempotencyRepository) GetByOfferId(
	ctx context.Context,
	offerId string,
) (domain.OfferIdempotencyKey, error) {
	query := `
		SELECT
			idempotency_key,
			request_hash,
			COALESCE(response_body::text, ''),
			status,
			COALESCE(error_code, ''),
			COALESCE(error_message, ''),
			COALESCE(offer_id::text, ''),
			COALESCE(owner_token, ''),
			locked_until,
			created_at,
			updated_at
		FROM offer_idempotency_keys
		WHERE offer_id = $1::uuid;
	`

	var p domain.OfferIdempotencyKey
	err := r.db.QueryRowContext(ctx, query, offerId).Scan(
		&p.IdempotencyKey,
		&p.RequestHash,
		&p.ResponseBody,
		&p.Status,
		&p.ErrorCode,
		&p.ErrorMessage,
		&p.OfferID,
		&p.OwnerToken,
		&p.LockedUntil,
		&p.CreatedAt,
		&p.UpdatedAt,
	)

	return p, err
}

func (r *OfferRedemptionIdempotencyRepository) MarkCompleted(
	tx *sql.Tx,
	ctx context.Context,
	idempotencyKey string,
	responseBody string,
	OfferId string,
	ownerToken string,
) error {
	query := `
		UPDATE offer_idempotency_keys
		SET
			response_body = $2::jsonb,
			status = 'COMPLETED',
			offer_id = $3::uuid,
			error_code = NULL,
			error_message = NULL,
			owner_token = NULL,
			locked_until = NOW(),
			updated_at = NOW()
		WHERE idempotency_key = $1
		  AND owner_token = $4;
	`

	res, err := tx.ExecContext(ctx, query, idempotencyKey, responseBody, OfferId, ownerToken)
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

func (r *OfferRedemptionIdempotencyRepository) MarkFailed(
	tx *sql.Tx,
	ctx context.Context,
	idempotencyKey string,
	errorCode string,
	errorMessage string,
	ownerToken string,
) error {
	query := `
		UPDATE offer_idempotency_keys
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
