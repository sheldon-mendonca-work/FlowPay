package domain

import "time"

type Payment struct {
	PaymentID      string
	UserID         string
	Amount         int64
	Currency       string
	Status         string
	IdempotencyKey string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}
