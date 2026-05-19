package domain

import "time"

type PaymentInitiatedEvent struct {
	ID         string
	SenderID   string
	ReceiverID string
	Amount     int64
	Currency   string
	CreatedAt  time.Time
}
