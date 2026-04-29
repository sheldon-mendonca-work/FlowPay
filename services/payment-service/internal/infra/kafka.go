package infra

import (
	"flowpay/pkg/utils"
	"log"

	"github.com/segmentio/kafka-go"
)

func InitKafka() *kafka.Writer {
	broker := utils.GetEnv("KAFKA_BROKER", "192.168.0.161:9094")
	topic := utils.GetEnv("KAFKA_TOPIC", "payment.events")

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   topic,
		Async:   false,
	})

	log.Printf("Kafka writer configured for broker=%s topic=%s", broker, topic)

	return writer
}
