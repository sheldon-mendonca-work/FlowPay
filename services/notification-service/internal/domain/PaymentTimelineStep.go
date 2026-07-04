package domain

import "time"

type PaymentTimelineStep struct {
	ID            string
	PaymentID     string
	StepName      string
	Status        string
	TraceID       string
	RequestID     string
	CompletedTime time.Time
	CreatedAt     time.Time
}
