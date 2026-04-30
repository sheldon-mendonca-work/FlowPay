package dto

// PaymentRequest defines the incoming JSON structure
type PaymentRequestDTO struct {
	SenderID   string  `json:"sender_id"`
	ReceiverID string  `json:"receiver_id"`
	Amount     float64 `json:"amount"`
	Currency   string  `json:"currency"`
}
