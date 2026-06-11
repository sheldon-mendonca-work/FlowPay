package offerReserveDTO

import (
	"time"
)

type OfferReservationResponseDTO struct {
	ReservationID string    `json:"reservation_id"`
	PaymentID     string    `json:"payment_id"`
	OfferID       string    `json:"offer_id"`
	AccountID     string    `json:"account_id"`
	Status        string    `json:"status"`
	ExpiresAt     time.Time `json:"expires_at"`
}
