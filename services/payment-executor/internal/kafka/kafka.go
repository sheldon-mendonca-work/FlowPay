package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"flowpay/payment-executor/internal/constants"
	"flowpay/payment-executor/internal/domain"
	flowpayPaymentErrors "flowpay/payment-executor/internal/errors"
	"flowpay/pkg/observability/logger"
	"flowpay/pkg/observability/tracing"

	"github.com/segmentio/kafka-go"
)

type Handler func(ctx context.Context, event domain.PaymentInitiatedEvent) error

type KafkaConsumer struct {
	reader  *kafka.Reader
	handler Handler
}

func NewKafkaConsumer(brokers []string, topic string, groupID string, handler Handler) *KafkaConsumer {
	return &KafkaConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokers,
			Topic:          topic,
			GroupID:        groupID,
			MinBytes:       10e3,
			MaxBytes:       10e6,
			CommitInterval: 0,
		}),
		handler: handler,
	}
}

func (c *KafkaConsumer) Start(ctx context.Context) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			logger.LogEvent(ctx, "ERROR", constants.PaymentExecutorServiceName, "fetching_kafka_message_failed", logger.Fields{
				"topic":     msg.Topic,
				"partition": msg.Partition,
				"offset":    msg.Offset,
				"error":     err.Error(),
			})
			return err
		}

		logger.LogEvent(ctx, "INFO", constants.PaymentExecutorServiceName, "kafka_message_received", logger.Fields{
			"topic":      msg.Topic,
			"partition":  msg.Partition,
			"offset":     msg.Offset,
			"error_type": flowpayPaymentErrors.ErrorTypeNone,
		})

		event, err := c.decodeMessage(msg.Value)
		if err != nil {
			logger.LogEvent(ctx, "ERROR", constants.PaymentExecutorServiceName, "kafka_message_decode_failed", logger.Fields{
				"topic":      msg.Topic,
				"partition":  msg.Partition,
				"offset":     msg.Offset,
				"error_type": flowpayPaymentErrors.ErrorTypeKafkaMessageDecoding,
				"error":      err.Error(),
			})
			if err := c.commitMessage(ctx, msg); err != nil {
				return err
			}
			continue
		}

		ctx = tracing.WithTraceAndRequestIDs(ctx, event.TraceID, event.RequestID)

		if c.handler != nil {
			err = c.handler(ctx, event)
		}

		if err != nil {
			errorType := flowpayPaymentErrors.ToPaymentErrorType(err)
			logger.LogEvent(ctx, "ERROR", constants.PaymentExecutorServiceName, "payment_execution_failed", logger.Fields{
				"payment_id":      event.ID,
				"idempotency_key": event.IdempotencyKey,
				"topic":           msg.Topic,
				"partition":       msg.Partition,
				"offset":          msg.Offset,
				"error_type":      errorType,
				"error":           err.Error(),
			})
			if shouldCommitOnHandlerError(err) {
				if err := c.commitMessage(ctx, msg); err != nil {
					return err
				}
				logger.LogEvent(ctx, "WARN", constants.PaymentExecutorServiceName, "payment_execution_failed_committed", logger.Fields{
					"payment_id":      event.ID,
					"idempotency_key": event.IdempotencyKey,
					"topic":           msg.Topic,
					"partition":       msg.Partition,
					"offset":          msg.Offset,
					"error_type":      errorType,
				})
			} else {
				logger.LogEvent(ctx, "WARN", constants.PaymentExecutorServiceName, "payment_execution_failed_retryable", logger.Fields{
					"payment_id":      event.ID,
					"idempotency_key": event.IdempotencyKey,
					"topic":           msg.Topic,
					"partition":       msg.Partition,
					"offset":          msg.Offset,
					"error_type":      errorType,
				})
			}
			continue
		}

		if err := c.commitMessage(ctx, msg); err != nil {
			return err
		}

		logger.LogEvent(ctx, "INFO", constants.PaymentExecutorServiceName, "payment_execution_succeeded", logger.Fields{
			"payment_id":      event.ID,
			"idempotency_key": event.IdempotencyKey,
			"topic":           msg.Topic,
			"partition":       msg.Partition,
			"offset":          msg.Offset,
			"trace_id":        event.TraceID,
			"request_id":      event.RequestID,
			"error_type":      flowpayPaymentErrors.ErrorTypeNone,
		})
	}
}

func (c *KafkaConsumer) Close() error {
	if c.reader == nil {
		return nil
	}
	return c.reader.Close()
}

func (c *KafkaConsumer) decodeMessage(payload []byte) (domain.PaymentInitiatedEvent, error) {
	var event domain.PaymentInitiatedEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return domain.PaymentInitiatedEvent{}, err
	}
	return event, nil
}

func (c *KafkaConsumer) commitMessage(ctx context.Context, msg kafka.Message) error {
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	return nil
}

func shouldCommitOnHandlerError(err error) bool {
	switch flowpayPaymentErrors.ToPaymentErrorType(err) {
	case flowpayPaymentErrors.ErrorTypeValidationError,
		flowpayPaymentErrors.ErrorTypeInsufficientBalance,
		flowpayPaymentErrors.ErrorTypeIdempotencyMismatch,
		flowpayPaymentErrors.ErrorTypeIdempotencyInProgress:
		return true
	default:
		return false
	}
}
