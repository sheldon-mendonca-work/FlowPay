package domain

import "time"

type Payment struct {
	ID         string
	SenderID   string
	ReceiverID string
	Amount     int64
	Currency   string
	Status     string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}
