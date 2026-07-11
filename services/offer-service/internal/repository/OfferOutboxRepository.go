package repository

import (
	"context"
	"database/sql"
	"flowpay/offer-service/internal/domain"
	"fmt"
)

type OfferOutboxRepository struct {
	db *sql.DB
}

func NewOfferOutboxRepository(db *sql.DB) *OfferOutboxRepository {
	return &OfferOutboxRepository{
		db: db,
	}
}

func (r *OfferOutboxRepository) InsertOfferOutboxEvent(ctx context.Context, tx *sql.Tx, offerOutboxEvent domain.OfferOutboxEvent) error {
	query := `
		INSERT INTO offer_outbox_events(
			id,
			aggregate_type,
			aggregate_id,
			event_type,
			event_version,
			idempotency_key,
			payload,
			status,
			locked_until,
			retry_count,
			trace_id,
			request_id,
			error_code,
			error_message,
			created_at,
			updated_at
		)
		VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW(), NOW()
		);
	`

	rows, err := tx.ExecContext(ctx, query,
		offerOutboxEvent.ID,
		offerOutboxEvent.AggregateType,
		offerOutboxEvent.AggregateID,
		offerOutboxEvent.EventType,
		offerOutboxEvent.EventVersion,
		offerOutboxEvent.IdempotencyKey,
		offerOutboxEvent.Payload,
		offerOutboxEvent.Status,
		offerOutboxEvent.LockedUntil,
		offerOutboxEvent.RetryCount,
		offerOutboxEvent.TraceID,
		offerOutboxEvent.RequestID,
		offerOutboxEvent.ErrorCode,
		offerOutboxEvent.ErrorMessage,
	)
	if err != nil {
		return err
	}

	rowsAffected, err := rows.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected != 1 {
		return fmt.Errorf("Transaction mark publish failed: Expected 1 row updated, got %d", rowsAffected)
	}

	return nil
}

func (r *OfferOutboxRepository) MarkPublished(ctx context.Context, tx *sql.Tx, eventID string, eventType string) error {
	query := `
		UPDATE offer_outbox_events
		SET 
			status = 'PUBLISHED',
			event_type = $2,
			published_at = NOW(),
			updated_at = NOW(),
			locked_until = NULL
		WHERE id=$1;
	`

	rows, err := tx.ExecContext(ctx, query, eventID, eventType)
	if err != nil {
		return err
	}

	rowsAffected, err := rows.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected != 1 {
		return fmt.Errorf("Transaction mark publish failed: Expected 1 row updated, got %d", rowsAffected)
	}

	return nil
}

func (r *OfferOutboxRepository) MarkFailed(ctx context.Context, eventID string, errorCode string, errorText string, eventType string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	committed := false

	defer func() {
		if !committed {
			tx.Rollback()
		}
	}()
	query := `
		UPDATE offer_outbox_events
		SET status = 'FAILED', 
			error_code = $2,
			error_message = $3,
			event_type = $4,
			locked_until = NULL,
			updated_at = NOW()
		WHERE id=$1;
	`

	rows, err := tx.ExecContext(ctx, query, eventID, errorCode, errorText, eventType)
	if err != nil {
		return err
	}

	rowsAffected, err := rows.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected != 1 {
		return fmt.Errorf("Transaction mark publish failure failed: Expected 1 row updated, got %d", rowsAffected)
	}

	committed = true
	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (r *OfferOutboxRepository) MarkRetryableFailure(ctx context.Context, eventID string, errorCode string, errorText string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	committed := false

	defer func() {
		if !committed {
			tx.Rollback()
		}
	}()
	query := `
		UPDATE offer_outbox_events
		SET status = 'PENDING',
			retry_count = retry_count + 1, 
			error_code = $2,
			error_message = $3,
			locked_until = NULL,
			updated_at = NOW()
		WHERE id=$1;
	`

	rows, err := tx.ExecContext(ctx, query, eventID, errorCode, errorText)
	if err != nil {
		return err
	}

	rowsAffected, err := rows.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected != 1 {
		return fmt.Errorf("Transaction mark publish tryablefailure failed: Expected 1 row updated, got %d", rowsAffected)
	}

	committed = true
	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}
