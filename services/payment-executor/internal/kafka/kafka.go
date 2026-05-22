package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"flowpay/payment-executor/internal/domain"
	flowpayPaymentErrors "flowpay/payment-executor/internal/errors"
	"log"

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
			return err
		}

		event, err := c.decodeMessage(msg.Value)
		if err != nil {
			log.Printf("failed to decode kafka message topic=%s partition=%d offset=%d: %v", msg.Topic, msg.Partition, msg.Offset, err)
			if err := c.commitMessage(ctx, msg); err != nil {
				return err
			}
			continue
		}

		if c.handler != nil {
			err = c.handler(ctx, event)
		}

		if err != nil {
			log.Printf("Failed to execute payment topic=%s partition=%d offset=%d: %v", msg.Topic, msg.Partition, msg.Offset, err)
			if shouldCommitOnHandlerError(err) {
				if err := c.commitMessage(ctx, msg); err != nil {
					return err
				}
			}
			continue
		}

		if err := c.commitMessage(ctx, msg); err != nil {
			return err
		}

		log.Printf("processed kafka message payment_id=%s topic=%s partition=%d offset=%d", event.ID, msg.Topic, msg.Partition, msg.Offset)
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
