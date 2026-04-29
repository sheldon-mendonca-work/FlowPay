package dto

import "flowpay/payment-service/internal/types"

// PaymentResponse defines the stubbed outgoing JSON structure
type PaymentResponseDTO struct {
	PaymentID string                  `json:"payment_id"`
	Status    types.PaymentStatusEnum `json:"status"`
}
