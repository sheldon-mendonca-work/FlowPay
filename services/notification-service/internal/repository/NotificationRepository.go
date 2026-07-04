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
			id, payment_id, step_name, status, trace_id, request_id, completed_time
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (payment_id, step_name, status) DO NOTHING;
	`

	res, err := r.db.ExecContext(ctx, query,
		step.ID,
		step.PaymentID,
		step.StepName,
		step.Status,
		step.TraceID,
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

func (r *NotificationRepository) GetTimelineStepsByPaymentID(ctx context.Context, paymentID string) ([]domain.PaymentTimelineStep, error) {
	query := `
		SELECT step_name, status, completed_time
		FROM payment_timeline_steps
		WHERE payment_id = $1
		ORDER BY completed_time ASC, created_at ASC;
	`

	rows, err := r.db.QueryContext(ctx, query, paymentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var steps []domain.PaymentTimelineStep
	for rows.Next() {
		step := domain.PaymentTimelineStep{PaymentID: paymentID}
		if err := rows.Scan(&step.StepName, &step.Status, &step.CompletedTime); err != nil {
			return nil, err
		}
		steps = append(steps, step)
	}

	return steps, rows.Err()
}
