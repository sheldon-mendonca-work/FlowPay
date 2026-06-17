package domain

import "time"

type PaymentSuccessEvent struct {
	PaymentID      string
	SenderID       string
	ReceiverID     string
	Amount         int64
	NetAmount      int64
	RetryCount     int8
	IdempotencyKey string
	ReservationID  string

	OfferID     *string
	OfferType   *string
	OfferAmount *int64

	Currency  string
	TraceID   string
	RequestID string

	CreatedAt time.Time
}
