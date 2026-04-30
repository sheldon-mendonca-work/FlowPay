package domain

import "time"

type PaymentIdempotencyKey struct {
	IdempotencyKey string
	RequestHash    string
	ResponseBody   string
	Status         string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}
