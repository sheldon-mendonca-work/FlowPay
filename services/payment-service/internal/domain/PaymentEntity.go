package domain

import "time"

type Payment struct {
	ID                 string
	IdempotencyKey     string
	SenderID           string
	ReceiverID         string
	Amount             int64
	Currency           string
	OfferID            string
	OfferBenefitAmount int64
	OfferType          string
	OfferCode          string
	Status             string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}
