package handler

import (
	"context"
	"encoding/json"
	"flowpay/payment-executor/internal/constants"
	"flowpay/payment-executor/internal/domain"
	flowpayPaymentErrors "flowpay/payment-executor/internal/errors"
	"flowpay/payment-executor/internal/service"
	"flowpay/pkg/observability/logger"
	"flowpay/pkg/observability/tracing"

	"github.com/segmentio/kafka-go"
)

type PaymentHandler struct {
	paymentExecutorService *service.PaymentExecutorService
}

func NewPaymentHandler(paymentExecutorService *service.PaymentExecutorService) *PaymentHandler {
	return &PaymentHandler{paymentExecutorService: paymentExecutorService}
}

func (h *PaymentHandler) HandlePaymentInitiated(ctx context.Context, payload kafka.Message) error {
	var event domain.PaymentInitiatedEvent

	if err := json.Unmarshal(payload.Value, &event); err != nil {
		logger.LogEvent(ctx, "ERROR", constants.PaymentExecutorServiceName, "payment_initiated_decoding_kafka_message_failed", logger.Fields{
			"topic":     payload.Topic,
			"partition": payload.Partition,
			"offset":    payload.Offset,
			"error":     err.Error(),
		})
		return err
	}

	ctx = tracing.WithTraceAndRequestIDs(
		ctx,
		event.TraceID,
		event.RequestID,
	)

	_, err := h.paymentExecutorService.HandlePaymentInitiated(
		ctx,
		event,
	)

	if err != nil {
		errorType := flowpayPaymentErrors.ToPaymentErrorType(err)

		logger.LogEvent(ctx, "ERROR", constants.PaymentExecutorServiceName, "message_processing_failed", logger.Fields{
			"topic":      payload.Topic,
			"partition":  payload.Partition,
			"offset":     payload.Offset,
			"error_type": errorType,
			"error":      err.Error(),
		})
	}

	return err
}

func (h *PaymentHandler) HandleOfferReserved(
	ctx context.Context,
	payload kafka.Message,
) error {

	var event domain.OfferOutboxEventType

	if err := json.Unmarshal(payload.Value, &event); err != nil {
		logger.LogEvent(ctx, "ERROR", constants.PaymentExecutorServiceName, "offer_reserved_decoding_kafka_message_failed", logger.Fields{
			"topic":     payload.Topic,
			"partition": payload.Partition,
			"offset":    payload.Offset,
			"error":     err.Error(),
		})
		return err
	}

	ctx = tracing.WithTraceAndRequestIDs(
		ctx,
		event.TraceID,
		event.RequestID,
	)

	_, err := h.paymentExecutorService.ExecuteOfferReservedPayment(
		ctx,
		event,
	)

	if err != nil {
		errorType := flowpayPaymentErrors.ToPaymentErrorType(err)

		logger.LogEvent(ctx, "ERROR", constants.PaymentExecutorServiceName, "message_processing_failed", logger.Fields{
			"topic":      payload.Topic,
			"partition":  payload.Partition,
			"offset":     payload.Offset,
			"error_type": errorType,
			"error":      err.Error(),
		})
	}
	return err
}
func (h *PaymentHandler) HandleOfferRejected(
	ctx context.Context,
	payload kafka.Message,
) error {

	var event domain.OfferOutboxEventType

	if err := json.Unmarshal(payload.Value, &event); err != nil {
		logger.LogEvent(ctx, "ERROR", constants.PaymentExecutorServiceName, "offer_rejected_decoding_kafka_message_failed", logger.Fields{
			"error": err.Error(),
		})
		return err
	}

	ctx = tracing.WithTraceAndRequestIDs(
		ctx,
		event.TraceID,
		event.RequestID,
	)

	_, err := h.paymentExecutorService.ExecuteOfferRejectedPayment(
		ctx,
		event,
	)

	if err != nil {
		errorType := flowpayPaymentErrors.ToPaymentErrorType(err)

		logger.LogEvent(ctx, "ERROR", constants.PaymentExecutorServiceName, "message_processing_failed", logger.Fields{
			"topic":      payload.Topic,
			"partition":  payload.Partition,
			"offset":     payload.Offset,
			"error_type": errorType,
			"error":      err.Error(),
		})
	}

	return err
}
