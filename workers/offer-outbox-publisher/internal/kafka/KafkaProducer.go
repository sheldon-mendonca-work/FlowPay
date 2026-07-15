package kafka

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaProducer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string) *KafkaProducer {
	return &KafkaProducer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{}, // ensures same key → same partition
			BatchTimeout: 10 * time.Millisecond,
		},
	}
}

func (p *KafkaProducer) Publish(
	ctx context.Context,
	key string,
	eventID string,
	value []byte,
) error {
	return p.writer.WriteMessages(
		ctx,
		kafka.Message{
			Key:   []byte(key),
			Value: value,
			Headers: []kafka.Header{
				{
					Key:   "event_id",
					Value: []byte(eventID),
				},
			},
		},
	)
}
