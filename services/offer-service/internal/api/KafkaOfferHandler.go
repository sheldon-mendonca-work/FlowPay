package api

import (
	"context"
	"encoding/json"
	"flowpay/offer-service/internal/constants"
	"flowpay/offer-service/internal/domain"
	offerRedeemDTO "flowpay/offer-service/internal/dto/OfferRedeem"
	offerReserveDTO "flowpay/offer-service/internal/dto/OfferReserve"
	flowpayOfferErrors "flowpay/offer-service/internal/errors"
	"flowpay/offer-service/internal/service"
	"flowpay/pkg/observability/logger"
	"flowpay/pkg/observability/tracing"

	"github.com/segmentio/kafka-go"
)

type KafkaOfferHandler struct {
	offerService *service.OfferService
}

func NewKafkaOfferHandler(offerService *service.OfferService) *KafkaOfferHandler {
	return &KafkaOfferHandler{offerService: offerService}
}

func (h *KafkaOfferHandler) HandlePaymentInitiated(
	ctx context.Context,
	payload kafka.Message,
) error {
	var event domain.PaymentInitiatedEvent
	if err := json.Unmarshal(payload.Value, &event); err != nil {
		logger.LogEvent(ctx, "ERROR", constants.ServiceName, "kafka_message_decode_failed", logger.Fields{
			"topic":      payload.Topic,
			"partition":  payload.Partition,
			"offset":     payload.Offset,
			"error_type": flowpayOfferErrors.ErrorTypeKafkaMessageDecoding,
			"error":      err.Error(),
		})
		return err
	}

	if event.OfferID == "" {
		return nil
	}

	ctx = tracing.WithTraceAndRequestIDs(ctx, event.TraceID, event.RequestID)

	_, err := h.offerService.ReserveOffer(
		ctx,
		offerReserveDTO.OfferReservationRequestDTO{
			AccountID:             event.SenderID,
			PaymentID:             event.ID,
			SenderID:              event.SenderID,
			ReceiverID:            event.ReceiverID,
			PaymentIdempotencyKey: event.IdempotencyKey,
			PaymentOwnerToken:     event.OwnerToken,
			TraceID:               event.TraceID,
			RequestID:             event.RequestID,
			Amount:                event.Amount,
			Currency:              event.Currency,
		},
		event.IdempotencyKey,
		event.OfferID,
		event.RequestID,
		event.TraceID,
	)

	if err != nil {
		logger.LogEvent(ctx, "ERROR", constants.ServiceName, "kafka_handle_payment_initiated_failed", logger.Fields{
			"error": err.Error(),
		})
	}

	return err
}

func (h *KafkaOfferHandler) HandlePaymentSuccess(
	ctx context.Context,
	payload kafka.Message,
) error {
	var event domain.PaymentSuccessEvent

	if err := json.Unmarshal(payload.Value, &event); err != nil {
		logger.LogEvent(ctx, "ERROR", constants.ServiceName, "kafka_message_decode_failed", logger.Fields{
			"topic":      payload.Topic,
			"partition":  payload.Partition,
			"offset":     payload.Offset,
			"error_type": flowpayOfferErrors.ErrorTypeKafkaMessageDecoding,
			"error":      err.Error(),
		})
		return err
	}

	if event.OfferID == nil || *event.OfferID == "" {
		return nil
	}

	ctx = tracing.WithTraceAndRequestIDs(ctx, event.TraceID, event.RequestID)

	_, err := h.offerService.RedeemOffer(
		ctx,
		offerRedeemDTO.OfferRedemptionRequestDTO{
			AccountID:     event.SenderID,
			PaymentID:     event.PaymentID,
			ReservationID: event.ReservationID,
		},
		event.IdempotencyKey,
		*event.OfferID,
		event.RequestID,
		event.TraceID,
	)

	if err != nil {
		logger.LogEvent(ctx, "ERROR", constants.ServiceName, "kafka_handle_payment_success_failed", logger.Fields{
			"error": err.Error(),
		})
	}

	return err
}
