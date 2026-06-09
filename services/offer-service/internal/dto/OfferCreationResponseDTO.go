package dto

import "flowpay/offer-service/internal/types"

type OfferCreationResponseDTO struct {
	OfferID string                `json:"offer_id"`
	Status  types.OfferStatusEnum `json:"status"`
}
