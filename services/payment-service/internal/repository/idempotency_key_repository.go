package repository

import (
	"context"
	"database/sql"
	"errors"
	"flowpay/payment-service/internal/domain"
	"fmt"
	"time"
)

type PaymentIdempotencyRepository struct {
	db *sql.DB
}

func NewPaymentIdempotencyRepository(db *sql.DB) *PaymentIdempotencyRepository {
	return &PaymentIdempotencyRepository{db: db}
}

func (r *PaymentIdempotencyRepository) ClaimOrGet(
	ctx context.Context,
	idempotency domain.PaymentIdempotencyKey,
) (domain.PaymentIdempotencyKey, bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.PaymentIdempotencyKey{}, false, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	existing, err := r.getByKeyForUpdate(ctx, tx, idempotency.IdempotencyKey)
	switch {
	case err == nil:
	case errors.Is(err, sql.ErrNoRows):
		if err := r.insertClaim(ctx, tx, idempotency); err != nil {
			return domain.PaymentIdempotencyKey{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return domain.PaymentIdempotencyKey{}, false, err
		}
		committed = true
		return idempotency, true, nil
	default:
		return domain.PaymentIdempotencyKey{}, false, err
	}

	if existing.RequestHash != idempotency.RequestHash {
		if err := tx.Commit(); err != nil {
			return domain.PaymentIdempotencyKey{}, false, err
		}
		committed = true
		return existing, false, nil
	}

	if existing.Status == "COMPLETED" || existing.Status == "FAILED" {
		if err := tx.Commit(); err != nil {
			return domain.PaymentIdempotencyKey{}, false, err
		}
		committed = true
		return existing, false, nil
	}

	now := time.Now().UTC()
	if existing.LockedUntil.After(now) {
		if err := tx.Commit(); err != nil {
			return domain.PaymentIdempotencyKey{}, false, err
		}
		committed = true
		return existing, false, nil
	}

	if err := r.takeOverClaim(ctx, tx, idempotency); err != nil {
		return domain.PaymentIdempotencyKey{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return domain.PaymentIdempotencyKey{}, false, err
	}
	committed = true
	return idempotency, true, nil
}

func (r *PaymentIdempotencyRepository) getByKeyForUpdate(
	ctx context.Context,
	tx *sql.Tx,
	key string,
) (domain.PaymentIdempotencyKey, error) {
	query := `
		SELECT
			idempotency_key,
			request_hash,
			COALESCE(response_body::text, ''),
			status,
			COALESCE(error_code, ''),
			COALESCE(error_message, ''),
			COALESCE(payment_id::text, ''),
			COALESCE(owner_token, ''),
			locked_until,
			created_at,
			updated_at
		FROM idempotency_keys
		WHERE idempotency_key = $1
		FOR UPDATE;
	`

	var p domain.PaymentIdempotencyKey
	err := tx.QueryRowContext(ctx, query, key).Scan(
		&p.IdempotencyKey,
		&p.RequestHash,
		&p.ResponseBody,
		&p.Status,
		&p.ErrorCode,
		&p.ErrorMessage,
		&p.PaymentID,
		&p.OwnerToken,
		&p.LockedUntil,
		&p.CreatedAt,
		&p.UpdatedAt,
	)
	return p, err
}

func (r *PaymentIdempotencyRepository) insertClaim(
	ctx context.Context,
	tx *sql.Tx,
	idempotency domain.PaymentIdempotencyKey,
) error {
	query := `
		INSERT INTO idempotency_keys (
			idempotency_key,
			request_hash,
			status,
			owner_token,
			locked_until,
			created_at,
			updated_at
		) VALUES ($1, $2, $3, $4, $5, NOW(), NOW());
	`
	res, err := tx.ExecContext(
		ctx,
		query,
		idempotency.IdempotencyKey,
		idempotency.RequestHash,
		idempotency.Status,
		idempotency.OwnerToken,
		idempotency.LockedUntil,
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

func (r *PaymentIdempotencyRepository) takeOverClaim(
	ctx context.Context,
	tx *sql.Tx,
	idempotency domain.PaymentIdempotencyKey,
) error {
	query := `
		UPDATE idempotency_keys
		SET
			owner_token = $2,
			locked_until = $3,
			updated_at = NOW()
		WHERE idempotency_key = $1;
	`
	res, err := tx.ExecContext(ctx, query, idempotency.IdempotencyKey, idempotency.OwnerToken, idempotency.LockedUntil)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("take over claim failed: expected 1 row affected, got %d", rowsAffected)
	}
	return nil
}

func (r *PaymentIdempotencyRepository) GetByKey(
	ctx context.Context,
	key string,
) (domain.PaymentIdempotencyKey, error) {
	query := `
		SELECT
			idempotency_key,
			request_hash,
			COALESCE(response_body::text, ''),
			status,
			COALESCE(error_code, ''),
			COALESCE(error_message, ''),
			COALESCE(payment_id::text, ''),
			COALESCE(owner_token, ''),
			locked_until,
			created_at,
			updated_at
		FROM idempotency_keys
		WHERE idempotency_key = $1;
	`

	var p domain.PaymentIdempotencyKey
	err := r.db.QueryRowContext(ctx, query, key).Scan(
		&p.IdempotencyKey,
		&p.RequestHash,
		&p.ResponseBody,
		&p.Status,
		&p.ErrorCode,
		&p.ErrorMessage,
		&p.PaymentID,
		&p.OwnerToken,
		&p.LockedUntil,
		&p.CreatedAt,
		&p.UpdatedAt,
	)

	return p, err
}

func (r *PaymentIdempotencyRepository) MarkCompleted(
	tx *sql.Tx,
	ctx context.Context,
	idempotencyKey string,
	responseBody string,
	paymentID string,
	ownerToken string,
) error {
	query := `
		UPDATE idempotency_keys
		SET
			response_body = $2::jsonb,
			status = 'COMPLETED',
			payment_id = $3::uuid,
			error_code = NULL,
			error_message = NULL,
			owner_token = NULL,
			locked_until = NOW(),
			updated_at = NOW()
		WHERE idempotency_key = $1
		  AND owner_token = $4;
	`

	res, err := tx.ExecContext(ctx, query, idempotencyKey, responseBody, paymentID, ownerToken)
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

func (r *PaymentIdempotencyRepository) MarkFailed(
	tx *sql.Tx,
	ctx context.Context,
	idempotencyKey string,
	errorCode string,
	errorMessage string,
	ownerToken string,
) error {
	query := `
		UPDATE idempotency_keys
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
