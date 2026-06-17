package main

import (
	"context"
	"flowpay/outbox-publisher/internal/db"
	"flowpay/outbox-publisher/internal/kafka"
	"flowpay/outbox-publisher/internal/repo"
	"flowpay/outbox-publisher/internal/worker"
	"flowpay/pkg/utils"
	"log"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	db := db.InitDB()
	defer db.Close()

	outboxRepository := repo.NewOutboxEventRepository(db)
	idempotencyRepository := repo.NewPaymentIdempotencyRepository(db)

	kafkaBroker := utils.GetEnv("KAFKA_BROKER", "localhost:9094")
	paymentInitiatedKafkaTopic := utils.GetEnv("KAFKA_TOPIC", "payment.initiated")
	offerInititatedKafkaTopic := utils.GetEnv("OFFER_INITIATED_KAFKA_TOPIC", "offer.initiated")
	paymentSuccessfulKafkaTopic := utils.GetEnv("OFFER_INITIATED_KAFKA_TOPIC", "payment.succeeded")

	kafkaProducer := kafka.NewProducer([]string{kafkaBroker}, paymentInitiatedKafkaTopic)
	offerInititatedKafkaProducer := kafka.NewProducer([]string{kafkaBroker}, offerInititatedKafkaTopic)
	paymentSuccessKafkaProducer := kafka.NewProducer([]string{kafkaBroker}, paymentSuccessfulKafkaTopic)

	outboxWorker := worker.NewOutboxWorker(db, *outboxRepository, *idempotencyRepository, *kafkaProducer, *offerInititatedKafkaProducer, *paymentSuccessKafkaProducer)

	log.Printf("transaction processor worker starting broker=%s topic=[%s, %s, %s]", kafkaBroker, paymentInitiatedKafkaTopic, offerInititatedKafkaTopic, paymentSuccessfulKafkaTopic)
	outboxWorker.Start(ctx)
}
