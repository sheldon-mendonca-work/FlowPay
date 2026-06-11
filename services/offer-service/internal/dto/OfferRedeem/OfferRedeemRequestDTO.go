package offerRedeemDTO

type OfferRedemptionRequestDTO struct {
	AccountID     string `json:"account_id"`
	PaymentID     string `json:"payment_id"`
	ReservationID string `json:"reservation_id"`
}
