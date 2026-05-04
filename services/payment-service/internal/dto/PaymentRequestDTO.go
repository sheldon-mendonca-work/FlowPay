package dto

// PaymentRequest defines the incoming JSON structure
type PaymentRequestDTO struct {
	SenderID   string `json:"sender_id"`
	ReceiverID string `json:"receiver_id"`
	Amount     int64  `json:"amount"` // minor units, e.g. paise
	Currency   string `json:"currency"`
}
