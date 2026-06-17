package repository

import (
	"context"
	"database/sql"
	"flowpay/payment-executor/internal/domain"
	"fmt"
)

type OutboxEventsRepository struct {
	db *sql.DB
}

func NewOutboxEventsRepository(db *sql.DB) *OutboxEventsRepository {
	return &OutboxEventsRepository{db: db}
}

func (r *OutboxEventsRepository) InsertOutboxEvent(tx *sql.Tx, ctx context.Context, payload domain.OutboxEventType) error {
	query := `
		INSERT INTO outbox_events (
			id,
			aggregate_type,
			aggregate_id,
			event_type,
			event_version,
			payload,
			status,
			trace_id,
			request_id,
			retry_count,
			idempotency_key,
			locked_until,
			created_at,
			published_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW(), NOW(), NULL);
	`

	res, err := tx.ExecContext(ctx,
		query,
		payload.ID,
		payload.AggregateType,
		payload.AggregateID,
		payload.EventType,
		payload.EventVersion,
		payload.Payload,
		payload.Status,
		payload.TraceID,
		payload.RequestID,
		payload.RetryCount,
		payload.IdempotencyKey,
	)

	if err != nil {
		return err
	}

	rowsAffected, _ := res.RowsAffected()
	if rowsAffected != 1 {
		return fmt.Errorf("Insertion of outbox event in db failed: expected 1 row affected but got %d.", rowsAffected)

	}
	return nil
}
