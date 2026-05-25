package domain

import "time"

type PaymentInitiatedEvent struct {
	ID             string
	SenderID       string
	ReceiverID     string
	IdempotencyKey string
	OwnerToken     string
	TraceID        string
	RequestID      string
	RetryCount     int8
	Amount         int64
	Currency       string
	CreatedAt      time.Time
}
