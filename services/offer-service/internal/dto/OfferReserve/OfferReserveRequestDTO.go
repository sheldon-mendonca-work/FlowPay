package offerReserveDTO

type OfferReservationRequestDTO struct {
	AccountID              string `json:"account_id"`
	PaymentID              string `json:"payment_id"`
	SenderID               string `json:"sender_id"`
	ReceiverID             string `json:"receiver_id"`
	PaymentIdempotencyKey  string `json:"payment_idempotency_key"`
	PromotionPoolAccountId string `json:"promotion_pool_account_id"`
	PaymentOwnerToken      string `json:"payment_owner_token"`
	TraceID                string `json:"trace_id"`
	RequestID              string `json:"request_id"`
	Amount                 int64  `json:"amount"`
	Currency               string `json:"currency"`
}
