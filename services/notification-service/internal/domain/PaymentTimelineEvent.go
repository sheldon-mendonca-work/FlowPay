package domain

import "time"

type PaymentTimelineEvent struct {
	PaymentID  string    `json:"payment_id"`
	StepName   string    `json:"step_name"`
	Status     string    `json:"status"`
	TraceID    string    `json:"trace_id"`
	RequestID  string    `json:"request_id"`
	OccurredAt time.Time `json:"occurred_at"`
}
