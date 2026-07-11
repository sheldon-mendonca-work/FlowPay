package dto

import "time"

type OfferSummaryDTO struct {
	OfferID        string `json:"offer_id"`
	OfferCode      string `json:"offer_code"`
	OfferType      string `json:"offer_type"`
	DiscountAmount int64  `json:"discount_amount"`
	CashbackAmount int64  `json:"cashback_amount"`
}

type PaymentPartyDTO struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	PaymentHandle string `json:"payment_handle"`
}

type GetPaymentResponseDTO struct {
	PaymentID      string           `json:"payment_id"`
	IdempotencyKey string           `json:"idempotency_key"`
	SenderID       string           `json:"sender_id"`
	ReceiverID     string           `json:"receiver_id"`
	Sender         *PaymentPartyDTO `json:"sender,omitempty"`
	Receiver       *PaymentPartyDTO `json:"receiver,omitempty"`
	Amount         int64            `json:"amount"`
	Currency       string           `json:"currency"`
	PaymentMethod  string           `json:"payment_method"`
	Status         string           `json:"status"`
	CreatedAt      time.Time        `json:"created_at"`
	UpdatedAt      time.Time        `json:"updated_at"`
	CompletedAt    *time.Time       `json:"completed_at,omitempty"`
	Offer          *OfferSummaryDTO `json:"offer,omitempty"`
}
