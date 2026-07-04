package api

import (
	"context"
	"encoding/json"
	"flowpay/notification-service/internal/constants"
	"flowpay/notification-service/internal/domain"
	flowpayNotificationErrors "flowpay/notification-service/internal/errors"
	"flowpay/notification-service/internal/service"
	"flowpay/pkg/observability/logger"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type KafkaNotificationHandler struct {
	notificationService *service.NotificationService
}

func NewKafkaNotificationHandler(notificationService *service.NotificationService) *KafkaNotificationHandler {
	return &KafkaNotificationHandler{notificationService: notificationService}
}

func (h *KafkaNotificationHandler) HandleNotificationTimeline(ctx context.Context, msg kafka.Message) error {
	var event domain.PaymentTimelineEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("%w: %v", flowpayNotificationErrors.ErrInvalidEventPayload, err)
	}

	return h.notificationService.IngestTimelineEvent(ctx, event)
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
