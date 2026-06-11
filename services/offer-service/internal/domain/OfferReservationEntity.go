package domain

import "time"

type OfferReservationEntity struct {
	ID             string
	OfferID        string
	PaymentID      string
	AccountID      string
	IdempotencyKey string
	Status         string
	ExpiresAt      time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}
