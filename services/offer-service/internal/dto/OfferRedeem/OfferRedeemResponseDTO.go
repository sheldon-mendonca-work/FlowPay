package offerRedeemDTO

import (
	"time"
)

type OfferRedemptionResponseDTO struct {
	OfferID        string    `json:"offer_id"`
	Status         string    `json:"status"`
	RedemptionTime time.Time `json:"redemption_time"`
	RedemptionID   string    `json:"redemption_id"`
	PaymentID      string    `json:"payment_id"`
}
