package domain

import "time"

type PaymentIdempotencyKey struct {
	IdempotencyKey string
	RequestHash    string
	ResponseBody   string
	Status         string
	ErrorCode      string
	ErrorMessage   string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}
