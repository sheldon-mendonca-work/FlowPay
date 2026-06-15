package api

import (
	"context"
	"flowpay/offer-service/internal/domain"
	offerReserveDTO "flowpay/offer-service/internal/dto/OfferReserve"
	"flowpay/offer-service/internal/service"
)

type KafkaOfferHandler struct {
	offerService *service.OfferService
}

func NewKafkaOfferHandler(offerService *service.OfferService) *KafkaOfferHandler {
	return &KafkaOfferHandler{offerService: offerService}
}

func (h *KafkaOfferHandler) HandlePaymentInitiated(
	ctx context.Context,
	event domain.PaymentInitiatedEvent,
) error {

	if event.OfferID == "" {
		return nil
	}

	_, err := h.offerService.ReserveOffer(
		ctx,
		offerReserveDTO.OfferReservationRequestDTO{
			AccountID: event.SenderID,
			PaymentID: event.ID,
		},
		event.IdempotencyKey,
		event.OfferID,
		event.RequestID,
		event.TraceID,
	)

	return err
}
