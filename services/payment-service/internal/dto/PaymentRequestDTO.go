package dto

// PaymentRequest defines the incoming JSON structure
type PaymentRequestDTO struct {
	UserID         string  `json:"user_id"`
	Amount         float64 `json:"amount"`
	Currency       string  `json:"currency"`
	IdempotencyKey string  `json:"idempotency_key"`
}
