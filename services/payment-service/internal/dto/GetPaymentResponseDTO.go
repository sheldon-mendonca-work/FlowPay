package dto

import "time"

type GetPaymentResponseDTO struct {
	PaymentID      string    `json:"payment_id"`
	IdempotencyKey string    `json:"idempotency_key"`
	SenderID       string    `json:"sender_id"`
	ReceiverID     string    `json:"receiver_id"`
	Amount         int64     `json:"amount"`
	Currency       string    `json:"currency"`
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}
