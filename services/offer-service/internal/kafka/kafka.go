package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"flowpay/offer-service/internal/constants"
	"flowpay/offer-service/internal/domain"
	flowpayOfferErrors "flowpay/offer-service/internal/errors"
	"flowpay/pkg/observability/logger"
	"time"

	"github.com/segmentio/kafka-go"
)

type Handler func(ctx context.Context, event kafka.Message) error

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
			MaxWait:        250 * time.Millisecond,
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
			logger.LogEvent(ctx, "ERROR", constants.ServiceName, "fetching_kafka_message_failed", logger.Fields{
				"topic":     msg.Topic,
				"partition": msg.Partition,
				"offset":    msg.Offset,
				"error":     err.Error(),
			})
			return err
		}

		logger.LogEvent(ctx, "INFO", constants.ServiceName, "kafka_message_received", logger.Fields{
			"topic":      msg.Topic,
			"partition":  msg.Partition,
			"offset":     msg.Offset,
			"error_type": flowpayOfferErrors.ErrorTypeNone,
		})

		if c.handler != nil {
			err = c.handler(ctx, msg)
		}

		if err != nil {
			errorType := flowpayOfferErrors.ToOfferErrorType(err)
			logger.LogEvent(ctx, "ERROR", constants.ServiceName, "offer_execution_failed", logger.Fields{
				"topic":      msg.Topic,
				"partition":  msg.Partition,
				"offset":     msg.Offset,
				"error_type": errorType,
				"error":      err.Error(),
			})
			if shouldCommitOnHandlerError(err) {
				if err := c.commitMessage(ctx, msg); err != nil {
					return err
				}
				logger.LogEvent(ctx, "WARN", constants.ServiceName, "offer_execution_failed_committed", logger.Fields{
					"topic":      msg.Topic,
					"partition":  msg.Partition,
					"offset":     msg.Offset,
					"error_type": errorType,
				})
			} else {
				logger.LogEvent(ctx, "WARN", constants.ServiceName, "offer_execution_failed_retryable", logger.Fields{
					"topic":      msg.Topic,
					"partition":  msg.Partition,
					"offset":     msg.Offset,
					"error_type": errorType,
				})
			}
			continue
		}

		if err := c.commitMessage(ctx, msg); err != nil {
			return err
		}

		logger.LogEvent(ctx, "INFO", constants.ServiceName, "offer_execution_succeeded", logger.Fields{
			"topic":      msg.Topic,
			"partition":  msg.Partition,
			"offset":     msg.Offset,
			"error_type": flowpayOfferErrors.ErrorTypeNone,
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
	switch flowpayOfferErrors.ToOfferErrorType(err) {
	case flowpayOfferErrors.ErrorTypeValidationError,
		flowpayOfferErrors.ErrorTypeInsufficientBalance,
		flowpayOfferErrors.ErrorTypeIdempotencyMismatch,
		flowpayOfferErrors.ErrorTypeIdempotencyInProgress:
		return true
	default:
		return false
	}
}
