package handler

import (
	"context"
	"encoding/json"
	"time"

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
	start := time.Now()

	var event domain.PaymentInitiatedEvent
	if err := json.Unmarshal(payload.Value, &event); err != nil {
		logger.LogEvent(ctx, "ERROR", constants.PaymentExecutorServiceName, "payment_initiated_decoding_kafka_message_failed", logger.Fields{
			"topic":       payload.Topic,
			"partition":   payload.Partition,
			"offset":      payload.Offset,
			"error":       err.Error(),
			"outcome":     "decode_failed",
			"duration_ms": time.Since(start).Milliseconds(),
		})
		return err
	}

	ctx = tracing.WithTraceAndRequestIDs(ctx, event.TraceID, event.RequestID)

	_, err := h.paymentExecutorService.HandlePaymentInitiated(ctx, event)
	if err != nil {
		logger.LogEvent(ctx, "ERROR", constants.PaymentExecutorServiceName, "payment_initiated_processing_failed", logger.Fields{
			"topic":       payload.Topic,
			"partition":   payload.Partition,
			"offset":      payload.Offset,
			"payment_id":  event.ID,
			"error_type":  flowpayPaymentErrors.ToPaymentErrorType(err),
			"error":       err.Error(),
			"outcome":     "failed",
			"duration_ms": time.Since(start).Milliseconds(),
		})
		return err
	}

	logger.LogEvent(ctx, "INFO", constants.PaymentExecutorServiceName, "payment_initiated_processed", logger.Fields{
		"topic":       payload.Topic,
		"partition":   payload.Partition,
		"offset":      payload.Offset,
		"payment_id":  event.ID,
		"outcome":     "success",
		"duration_ms": time.Since(start).Milliseconds(),
	})
	return nil
}

func (h *PaymentHandler) HandleOfferReserved(ctx context.Context, payload kafka.Message) error {
	start := time.Now()

	var event domain.OfferOutboxEventType
	if err := json.Unmarshal(payload.Value, &event); err != nil {
		logger.LogEvent(ctx, "ERROR", constants.PaymentExecutorServiceName, "offer_reserved_decoding_kafka_message_failed", logger.Fields{
			"topic":       payload.Topic,
			"partition":   payload.Partition,
			"offset":      payload.Offset,
			"error":       err.Error(),
			"outcome":     "decode_failed",
			"duration_ms": time.Since(start).Milliseconds(),
		})
		return err
	}

	ctx = tracing.WithTraceAndRequestIDs(ctx, event.TraceID, event.RequestID)

	_, err := h.paymentExecutorService.ExecuteOfferReservedPayment(ctx, event)
	if err != nil {
		logger.LogEvent(ctx, "ERROR", constants.PaymentExecutorServiceName, "offer_reserved_processing_failed", logger.Fields{
			"topic":          payload.Topic,
			"partition":      payload.Partition,
			"offset":         payload.Offset,
			"idempotency_key": event.IdempotencyKey,
			"error_type":     flowpayPaymentErrors.ToPaymentErrorType(err),
			"error":          err.Error(),
			"outcome":        "failed",
			"duration_ms":    time.Since(start).Milliseconds(),
		})
		return err
	}

	logger.LogEvent(ctx, "INFO", constants.PaymentExecutorServiceName, "offer_reserved_processed", logger.Fields{
		"topic":           payload.Topic,
		"partition":       payload.Partition,
		"offset":          payload.Offset,
		"idempotency_key": event.IdempotencyKey,
		"outcome":         "success",
		"duration_ms":     time.Since(start).Milliseconds(),
	})
	return nil
}

func (h *PaymentHandler) HandleOfferRejected(ctx context.Context, payload kafka.Message) error {
	start := time.Now()

	var event domain.OfferOutboxEventType
	if err := json.Unmarshal(payload.Value, &event); err != nil {
		logger.LogEvent(ctx, "ERROR", constants.PaymentExecutorServiceName, "offer_rejected_decoding_kafka_message_failed", logger.Fields{
			"topic":       payload.Topic,
			"partition":   payload.Partition,
			"offset":      payload.Offset,
			"error":       err.Error(),
			"outcome":     "decode_failed",
			"duration_ms": time.Since(start).Milliseconds(),
		})
		return err
	}

	ctx = tracing.WithTraceAndRequestIDs(ctx, event.TraceID, event.RequestID)

	_, err := h.paymentExecutorService.ExecuteOfferRejectedPayment(ctx, event)
	if err != nil {
		logger.LogEvent(ctx, "ERROR", constants.PaymentExecutorServiceName, "offer_rejected_processing_failed", logger.Fields{
			"topic":           payload.Topic,
			"partition":       payload.Partition,
			"offset":          payload.Offset,
			"idempotency_key": event.IdempotencyKey,
			"error_type":      flowpayPaymentErrors.ToPaymentErrorType(err),
			"error":           err.Error(),
			"outcome":         "failed",
			"duration_ms":     time.Since(start).Milliseconds(),
		})
		return err
	}

	logger.LogEvent(ctx, "INFO", constants.PaymentExecutorServiceName, "offer_rejected_processed", logger.Fields{
		"topic":           payload.Topic,
		"partition":       payload.Partition,
		"offset":          payload.Offset,
		"idempotency_key": event.IdempotencyKey,
		"outcome":         "success",
		"duration_ms":     time.Since(start).Milliseconds(),
	})
	return nil
}
