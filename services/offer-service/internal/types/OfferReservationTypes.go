package types

type ExistingReservation struct {
	ReservationID string
	PaymentID     string
	OfferID       string
}

type OfferReservedEvent struct {
	OfferID       string `json:"offer_id"`
	ReservationID string `json:"reservation_id"`
	PaymentID     string `json:"payment_id"`
	AccountID     string `json:"account_id"`

	SenderID               string `json:"sender_id"`
	ReceiverID             string `json:"receiver_id"`
	Amount                 int64  `json:"amount"`
	Currency               string `json:"currency"`
	PaymentIdempotencyKey  string `json:"payment_idempotency_key"`
	PromotionPoolAccountId string `json:"promotion_pool_account_id"`
	PaymentOwnerToken      string `json:"payment_owner_token"`

	OfferType            string `json:"offer_type"`
	OfferAmount          *int64 `json:"offer_amount"`
	OfferPercentage      *int16 `json:"offer_percentage"`
	MaxBenefitAmount     int64  `json:"max_benefit_amount"`
	MinimumPaymentAmount int32  `json:"minimum_payment_amount"`
	MaximumPaymentAmount *int32 `json:"maximum_payment_amount"`

	IdempotencyKey string `json:"idempotency_key"`
	TraceID        string `json:"trace_id"`
	RequestID      string `json:"request_id"`
}

type OfferRejectedEvent struct {
	OfferID   string `json:"offer_id"`
	PaymentID string `json:"payment_id"`
	AccountID string `json:"account_id"`

	SenderID   string `json:"sender_id"`
	ReceiverID string `json:"receiver_id"`
	Amount     int64  `json:"amount"`
	Currency   string `json:"currency"`

	PaymentIdempotencyKey  string `json:"payment_idempotency_key"`
	PromotionPoolAccountId string `json:"promotion_pool_account_id"`
	PaymentOwnerToken      string `json:"payment_owner_token"`
	TraceID                string `json:"trace_id"`
	RequestID              string `json:"request_id"`

	ErrorCode string `json:"error_code"`
	ErrorText string `json:"error_text"`
}
