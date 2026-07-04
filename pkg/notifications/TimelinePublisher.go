package notifications

import (
	"context"
	"encoding/json"
	"time"

	"flowpay/pkg/observability/logger"

	"github.com/segmentio/kafka-go"
)

// PaymentTimelineEvent is the wire format published to the notification.timeline
// Kafka topic. It must stay in sync with the decoding side in
// notification-service/internal/domain.PaymentTimelineEvent.
type PaymentTimelineEvent struct {
	PaymentID  string    `json:"payment_id"`
	StepName   string    `json:"step_name"`
	Status     string    `json:"status"`
	TraceID    string    `json:"trace_id"`
	RequestID  string    `json:"request_id"`
	OccurredAt time.Time `json:"occurred_at"`
}

// Canonical payment lifecycle step names, tracked end-to-end across
// payment-service, offer-service, payment-executor and the outbox publishers.
const (
	StepPaymentInitiated      = "PAYMENT_INITIATED"
	StepOfferEvaluated        = "OFFER_EVALUATED"
	StepOfferReserved         = "OFFER_RESERVED"
	StepPaymentValidated      = "PAYMENT_VALIDATED"
	StepAccountsUpdated       = "ACCOUNTS_UPDATED"
	StepPaymentPersisted      = "PAYMENT_PERSISTED"
	StepTransactionsCompleted = "TRANSACTIONS_COMPLETED"
	StepOutboxEventCreated    = "OUTBOX_EVENT_CREATED"
	StepKafkaPublished        = "KAFKA_PUBLISHED"
	StepOfferRedeemed         = "OFFER_REDEEMED"
	StepPromoPoolDebited      = "PROMO_POOL_DEBITED"
	StepCashbackCredited      = "CASHBACK_CREDITED"
	StepPaymentCompleted      = "PAYMENT_COMPLETED"

	StatusSuccess = "SUCCESS"
	StatusFailed  = "FAILED"
)

// TimelinePublisher is a best-effort, fire-and-forget notifier: a failure to publish
// is logged but never returned, so it can never fail the caller's business transaction.
type TimelinePublisher struct {
	writer *kafka.Writer
}

func NewTimelinePublisher(brokers []string, topic string) *TimelinePublisher {
	return &TimelinePublisher{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.Hash{},
		},
	}
}

func (p *TimelinePublisher) Publish(ctx context.Context, serviceName string, paymentID string, stepName string, status string, traceID string, requestID string) {
	if p == nil || p.writer == nil || traceID == "" {
		return
	}

	payload, err := json.Marshal(PaymentTimelineEvent{
		PaymentID:  paymentID,
		StepName:   stepName,
		Status:     status,
		TraceID:    traceID,
		RequestID:  requestID,
		OccurredAt: time.Now().UTC(),
	})
	if err != nil {
		logger.LogEvent(ctx, "ERROR", serviceName, "timeline_event_encode_failed", logger.Fields{
			"trace_id":  traceID,
			"step_name": stepName,
			"error":     err.Error(),
		})
		return
	}

	if err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(traceID),
		Value: payload,
	}); err != nil {
		logger.LogEvent(ctx, "WARN", serviceName, "timeline_event_publish_failed", logger.Fields{
			"trace_id":  traceID,
			"step_name": stepName,
			"error":     err.Error(),
		})
	}
}

func (p *TimelinePublisher) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}
