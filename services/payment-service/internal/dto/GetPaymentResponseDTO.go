package dto

import "time"

type GetPaymentResponseDTO struct {
	PaymentID          string    `json:"payment_id"`
	IdempotencyKey     string    `json:"idempotency_key"`
	SenderID           string    `json:"sender_id"`
	ReceiverID         string    `json:"receiver_id"`
	Amount             int64     `json:"amount"`
	Currency           string    `json:"currency"`
	OfferID            string    `json:"offer_id"`
	OfferBenefitAmount int64     `json:"offer_benefit_amount"`
	OfferType          string    `json:"offer_type"`
	OfferCode          string    `json:"offer_code"`
	Status             string    `json:"status"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}
