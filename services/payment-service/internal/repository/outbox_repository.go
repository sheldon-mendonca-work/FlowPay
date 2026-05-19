package repository

import (
	"context"
	"database/sql"
	"flowpay/payment-service/internal/domain"
	"fmt"
)

type OutboxEventRepository struct {
	db *sql.DB
}

func NewOutboxEventRepository(db *sql.DB) *OutboxEventRepository {
	return &OutboxEventRepository{db: db}
}

func (r *OutboxEventRepository) InsertOutboxEvent(tx *sql.Tx, ctx context.Context, payload domain.OutboxEventType) error {
	query := `
		INSERT INTO outbox_events (
			id,
			aggregate_type,
			aggregate_id,
			event_type,
			event_version,
			payload,
			status,
			created_at,
			published_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW());
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
