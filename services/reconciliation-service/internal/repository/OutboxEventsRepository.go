package repository

import (
	"context"
	"database/sql"

	"flowpay/reconciliation-service/internal/domain"
)

type OutboxEventsRepository struct {
	db *sql.DB
}

func NewOutboxEventsRepository(db *sql.DB) *OutboxEventsRepository {
	return &OutboxEventsRepository{db: db}
}

func (r *OutboxEventsRepository) GetOutboxStuckProcessing(ctx context.Context, tx *sql.Tx) ([]domain.OutboxEventReconciliation, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id::text, aggregate_id, event_type, idempotency_key, retry_count, NOW() - locked_until AS stuck_for
		FROM outbox_events
		WHERE status = 'PROCESSING'
		AND locked_until < NOW()
		ORDER BY locked_until ASC;
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []domain.OutboxEventReconciliation
	for rows.Next() {
		var event domain.OutboxEventReconciliation
		if err := rows.Scan(&event.ID, &event.AggregateID, &event.EventType, &event.IdempotencyKey, &event.RetryCount, &event.StuckFor); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (r *OutboxEventsRepository) GetOutboxFailedRetryExhausted(ctx context.Context, tx *sql.Tx) ([]domain.OutboxEventReconciliation, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id::text, aggregate_id, event_type, idempotency_key, status, retry_count, COALESCE(error_code, ''), COALESCE(error_message, '')
		FROM outbox_events
		WHERE status = 'FAILED'
		OR (status != 'PUBLISHED' AND retry_count >= 5)
		ORDER BY updated_at ASC;
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []domain.OutboxEventReconciliation
	for rows.Next() {
		var event domain.OutboxEventReconciliation
		if err := rows.Scan(&event.ID, &event.AggregateID, &event.EventType, &event.IdempotencyKey, &event.Status, &event.RetryCount, &event.ErrorCode, &event.ErrorMessage); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (r *OutboxEventsRepository) GetOutboxEligibleBacklog(ctx context.Context, tx *sql.Tx) ([]domain.OutboxEventReconciliation, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id::text, aggregate_id, event_type, idempotency_key, status, retry_count, NOW() - created_at AS age
		FROM outbox_events
		WHERE (
			status = 'PENDING'
			OR (status = 'PROCESSING' AND locked_until < NOW())
		)
		AND retry_count < 5
		ORDER BY created_at ASC;
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []domain.OutboxEventReconciliation
	for rows.Next() {
		var event domain.OutboxEventReconciliation
		if err := rows.Scan(&event.ID, &event.AggregateID, &event.EventType, &event.IdempotencyKey, &event.Status, &event.RetryCount, &event.Age); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (r *OutboxEventsRepository) GetOutboxBacklogSummary(ctx context.Context, tx *sql.Tx) ([]domain.OutboxBacklogSummary, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT status, retry_count, COUNT(*) AS event_count
		FROM outbox_events
		WHERE status IN ('PENDING', 'PROCESSING', 'FAILED')
		GROUP BY status, retry_count
		ORDER BY status, retry_count;
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var summaries []domain.OutboxBacklogSummary
	for rows.Next() {
		var summary domain.OutboxBacklogSummary
		if err := rows.Scan(&summary.Status, &summary.RetryCount, &summary.EventCount); err != nil {
			return nil, err
		}
		summaries = append(summaries, summary)
	}
	return summaries, rows.Err()
}

func (r *OutboxEventsRepository) GetOutboxWithoutIdempotencyRows(ctx context.Context, tx *sql.Tx) ([]domain.OutboxEventReconciliation, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT o.id::text, o.aggregate_id, o.event_type, o.idempotency_key, o.status, o.retry_count
		FROM outbox_events o
		WHERE NOT EXISTS (
			SELECT 1
			FROM idempotency_keys i
			WHERE i.idempotency_key = o.idempotency_key
		);
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []domain.OutboxEventReconciliation
	for rows.Next() {
		var event domain.OutboxEventReconciliation
		if err := rows.Scan(&event.ID, &event.AggregateID, &event.EventType, &event.IdempotencyKey, &event.Status, &event.RetryCount); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (r *OutboxEventsRepository) GetOutboxPublishedWithoutPayment(ctx context.Context, tx *sql.Tx) ([]domain.OutboxEventReconciliation, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT o.id::text, o.aggregate_id, o.event_type, o.idempotency_key, o.status
		FROM outbox_events o
		WHERE o.status = 'PUBLISHED'
		AND NOT EXISTS (
			SELECT 1
			FROM payments p
			WHERE p.id::text = o.aggregate_id
		);
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []domain.OutboxEventReconciliation
	for rows.Next() {
		var event domain.OutboxEventReconciliation
		if err := rows.Scan(&event.ID, &event.AggregateID, &event.EventType, &event.IdempotencyKey, &event.Status); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}
