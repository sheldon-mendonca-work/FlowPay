package api

import (
	"context"
	"encoding/json"
	"flowpay/notification-service/internal/constants"
	"flowpay/notification-service/internal/domain"
	flowpayNotificationErrors "flowpay/notification-service/internal/errors"
	"flowpay/notification-service/internal/service"
	"flowpay/pkg/observability/logger"
	"flowpay/pkg/observability/metrics"
	"flowpay/pkg/observability/tracing"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaNotificationHandler struct {
	notificationService *service.NotificationService
}

func NewKafkaNotificationHandler(notificationService *service.NotificationService) *KafkaNotificationHandler {
	return &KafkaNotificationHandler{notificationService: notificationService}
}

func (h *KafkaNotificationHandler) HandleNotificationTimeline(ctx context.Context, msg kafka.Message) error {
	start := time.Now()
	outcome := "success"

	defer func() {
		metrics.NotificationTimelineEventsTotal.WithLabelValues(constants.ServiceName, outcome).Inc()
		metrics.NotificationTimelineEventDuration.WithLabelValues(constants.ServiceName, outcome).Observe(time.Since(start).Seconds())
	}()

	var event domain.PaymentTimelineEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		outcome = "decode_failed"
		logger.LogEvent(ctx, "ERROR", constants.ServiceName, "handle_notification_kafka_event_failed", logger.Fields{
			"topic":       msg.Topic,
			"partition":   msg.Partition,
			"offset":      msg.Offset,
			"error":       err.Error(),
			"outcome":     outcome,
			"error_type":  flowpayNotificationErrors.ErrorTypeKafkaMessageDecoding,
			"duration_ms": time.Since(start).Milliseconds(),
		})
		return fmt.Errorf("%w: %v", flowpayNotificationErrors.ErrInvalidEventPayload, err)
	}

	eventCtx := tracing.WithTraceAndRequestIDs(ctx, event.TraceID, event.RequestID)

	logger.LogEvent(eventCtx, "INFO", constants.ServiceName, "notification_timeline_event_received", logger.Fields{
		"topic":      msg.Topic,
		"partition":  msg.Partition,
		"offset":     msg.Offset,
		"payment_id": event.PaymentID,
		"step_name":  event.StepName,
		"status":     event.Status,
		"error_type": flowpayNotificationErrors.ErrorTypeNone,
	})

	if err := h.notificationService.IngestTimelineEvent(eventCtx, event); err != nil {
		outcome = "ingest_failed"
		logger.LogEvent(eventCtx, "ERROR", constants.ServiceName, "notification_timeline_event_ingest_failed", logger.Fields{
			"payment_id":  event.PaymentID,
			"step_name":   event.StepName,
			"status":      event.Status,
			"error":       err.Error(),
			"error_type":  flowpayNotificationErrors.ToNotificationErrorType(err),
			"duration_ms": time.Since(start).Milliseconds(),
		})
		return err
	}

	logger.LogEvent(eventCtx, "INFO", constants.ServiceName, "notification_timeline_event_processed", logger.Fields{
		"payment_id":  event.PaymentID,
		"step_name":   event.StepName,
		"status":      event.Status,
		"error_type":  flowpayNotificationErrors.ErrorTypeNone,
		"duration_ms": time.Since(start).Milliseconds(),
	})

	return nil
}

func (h *KafkaNotificationHandler) HandleNotificationUser(ctx context.Context, msg kafka.Message) error {
	logger.LogEvent(ctx, "INFO", constants.ServiceName, "notification_user_event_ignored", logger.Fields{
		"topic":     msg.Topic,
		"partition": msg.Partition,
		"offset":    msg.Offset,
	})
	return nil
}

func (h *KafkaNotificationHandler) HandleReconciliationEvents(ctx context.Context, msg kafka.Message) error {
	logger.LogEvent(ctx, "INFO", constants.ServiceName, "reconciliation_event_ignored", logger.Fields{
		"topic":     msg.Topic,
		"partition": msg.Partition,
		"offset":    msg.Offset,
	})
	return nil
}
