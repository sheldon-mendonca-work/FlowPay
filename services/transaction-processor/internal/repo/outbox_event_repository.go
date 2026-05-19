package repo

import (
	"context"
	"database/sql"
	"flowpay/transaction-processor/internal/domain"
	"fmt"
)

type OutboxEventRepository struct {
	db *sql.DB
}

func NewOutboxEventRepository(db *sql.DB) *OutboxEventRepository {
	return &OutboxEventRepository{db: db}
}

func (r *OutboxEventRepository) FetchUnpublished(tx *sql.Tx, ctx context.Context, limit int) ([]domain.OutboxEventType, error) {
	query := `
		SELECT id,
			aggregate_type,
			aggregate_id,
			event_type,
			event_version,
			payload,
			status
		FROM outbox_events
		WHERE status = 'PENDING'
		ORDER BY created_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED;
	`

	rows, err := tx.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	var events []domain.OutboxEventType

	for rows.Next() {
		var e domain.OutboxEventType
		err := rows.Scan(
			&e.ID,
			&e.AggregateType,
			&e.AggregateID,
			&e.EventType,
			&e.EventVersion,
			&e.Payload,
			&e.Status,
		)

		if err != nil {
			return nil, err
		}

		events = append(events, e)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil

}

func (r *OutboxEventRepository) MarkPublished(ctx context.Context, tx *sql.Tx, eventID string) error {
	query := `
		UPDATE outbox_events
		SET status = 'PUBLISHED', published_at = NOW()
		WHERE id=$1;
	`

	rows, err := tx.ExecContext(ctx, query, eventID)
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
