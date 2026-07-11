package offerStatusUpdateDTO

import "time"

type OfferStatusUpdateResponseDTO struct {
	OfferID   string    `json:"offer_id"`
	Status    string    `json:"status"`
	UpdatedAt time.Time `json:"updated_at"`
}
