package repo

import (
	"context"
	"database/sql"
	"flowpay/transaction-processor/internal/domain"
	"fmt"
	"time"

	"github.com/lib/pq"
)

type OutboxEventRepository struct {
	db *sql.DB
}

func NewOutboxEventRepository(db *sql.DB) *OutboxEventRepository {
	return &OutboxEventRepository{db: db}
}

func (r *OutboxEventRepository) ClaimBatch(ctx context.Context, batchSize int, maxRetryCount int, leaseExpiryFromNow time.Time) ([]domain.OutboxEventType, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return []domain.OutboxEventType{}, err
	}

	committed := false

	defer func() {
		if !committed {
			tx.Rollback()
		}
	}()

	query := `
			SELECT id,
				aggregate_type,
				aggregate_id,
				event_type,
				event_version,
				payload,
				status,
				retry_count,
				trace_id,
				request_id,
				locked_until
			FROM outbox_events
			WHERE ((status = 'PENDING') OR (status = 'PROCESSING' AND locked_until < NOW())) AND retry_count < $2
			ORDER BY created_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED;
		`

	rows, err := tx.QueryContext(ctx, query, batchSize, maxRetryCount)

	switch {
	case err == nil:
	default:
		return []domain.OutboxEventType{}, err
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
			&e.RetryCount,
			&e.TraceID,
			&e.RequestID,
			&e.LockedUntil,
		)

		if err != nil {
			return nil, err
		}
		events = append(events, e)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(events) == 0 {
		committed = true
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return events, nil
	}

	updateQuery := `
			UPDATE outbox_events
			SET status = CASE
				WHEN status = 'PENDING' THEN 'PROCESSING'
				ELSE status
			END,
			locked_until = $2
			WHERE id = any($1)
			RETURNING
            id,
            aggregate_type,
            aggregate_id,
            event_type,
            event_version,
            payload,
            status,
            retry_count,
            trace_id,
            request_id,
            locked_until;
		`

	var ids []string
	for _, e := range events {
		ids = append(ids, e.ID)
	}

	updatedRows, err := tx.QueryContext(ctx, updateQuery, pq.Array(ids), leaseExpiryFromNow)

	if err != nil {
		return nil, err
	}

	defer updatedRows.Close()
	events = events[:0]
	for updatedRows.Next() {
		var e domain.OutboxEventType
		err := updatedRows.Scan(
			&e.ID,
			&e.AggregateType,
			&e.AggregateID,
			&e.EventType,
			&e.EventVersion,
			&e.Payload,
			&e.Status,
			&e.RetryCount,
			&e.TraceID,
			&e.RequestID,
			&e.LockedUntil,
		)
		if err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	if err := updatedRows.Err(); err != nil {
		return nil, err
	}

	committed = true
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return events, nil

}

func (r *OutboxEventRepository) MarkPublished(ctx context.Context, tx *sql.Tx, eventID string) error {
	query := `
		UPDATE outbox_events
		SET 
			status = 'PUBLISHED',
			published_at = NOW(),
			updated_at = NOW(),
			locked_until = NULL
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

func (r *OutboxEventRepository) MarkFailed(ctx context.Context, eventID string, errorCode string, errorText string) error {
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
		UPDATE outbox_events
		SET status = 'FAILED', 
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
		return fmt.Errorf("Transaction mark publish failure failed: Expected 1 row updated, got %d", rowsAffected)
	}

	committed = true
	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (r *OutboxEventRepository) MarkRetryableFailure(ctx context.Context, eventID string, errorCode string, errorText string) error {
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
		UPDATE outbox_events
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
