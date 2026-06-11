package domain

import "time"

type OfferRedemptionEntity struct {
	ID             string
	OfferID        string
	AccountID      string
	ReservationID  string
	PaymentID      string
	IdempotencyKey string
	Status         string
	RefundReason   string
	RefundedByID   string
	RefundedByType string
	RefundSource   string
	RefundedAt     time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}
