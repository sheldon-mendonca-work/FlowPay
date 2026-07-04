package repository

import (
	"context"
	"database/sql"
	"flowpay/notification-service/internal/domain"
)

type NotificationRepository struct {
	db *sql.DB
}

func NewNotificationRepository(db *sql.DB) *NotificationRepository {
	return &NotificationRepository{db: db}
}

func (r *NotificationRepository) InsertTimelineStep(ctx context.Context, step domain.PaymentTimelineStep) (bool, error) {
	query := `
		INSERT INTO payment_timeline_steps (
			id, trace_id, payment_id, step_name, status, request_id, completed_time
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (trace_id, step_name, status) DO NOTHING;
	`

	res, err := r.db.ExecContext(ctx, query,
		step.ID,
		step.TraceID,
		step.PaymentID,
		step.StepName,
		step.Status,
		step.RequestID,
		step.CompletedTime,
	)
	if err != nil {
		return false, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}

	return rowsAffected == 1, nil
}

func (r *NotificationRepository) GetTimelineStepsByTraceID(ctx context.Context, traceID string) ([]domain.PaymentTimelineStep, error) {
	query := `
		SELECT step_name, status, completed_time, payment_id
		FROM payment_timeline_steps
		WHERE trace_id = $1
		ORDER BY completed_time ASC, created_at ASC;
	`

	rows, err := r.db.QueryContext(ctx, query, traceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var steps []domain.PaymentTimelineStep
	for rows.Next() {
		step := domain.PaymentTimelineStep{TraceID: traceID}
		if err := rows.Scan(&step.StepName, &step.Status, &step.CompletedTime, &step.PaymentID); err != nil {
			return nil, err
		}
		steps = append(steps, step)
	}

	return steps, rows.Err()
}
