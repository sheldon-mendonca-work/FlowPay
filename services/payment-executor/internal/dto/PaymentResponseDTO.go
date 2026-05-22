package dto

import "flowpay/payment-executor/internal/types"

// PaymentResponse defines the stubbed outgoing JSON structure
type PaymentResponseDTO struct {
	PaymentID string                  `json:"payment_id"`
	Status    types.PaymentStatusEnum `json:"status"`
}
