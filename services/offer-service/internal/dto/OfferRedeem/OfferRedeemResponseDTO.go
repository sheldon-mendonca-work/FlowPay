package offerRedeemDTO

import (
	"flowpay/offer-service/internal/types"
	"time"
)

type OfferRedemptionResponseDTO struct {
	OfferID        string                `json:"offer_id"`
	Status         types.OfferStatusEnum `json:"status"`
	RedemptionTime time.Time             `json:"redemption_time"`
}
