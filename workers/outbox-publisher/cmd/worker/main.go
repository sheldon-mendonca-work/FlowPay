package main

import (
	"context"
	"flowpay/outbox-publisher/internal/db"
	"flowpay/outbox-publisher/internal/kafka"
	"flowpay/outbox-publisher/internal/repo"
	"flowpay/outbox-publisher/internal/worker"
	"flowpay/pkg/notifications"
	"flowpay/pkg/utils"
	"log"
	"os/signal"
	"strings"
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
	paymentSuccessfulKafkaTopic := utils.GetEnv("PAYMENT_SUCCESS_KAFKA_TOPIC", "payment.succeeded")
	notificationTimelineKafkaTopic := utils.GetEnv("NOTIFICATION_TIMELINE_KAFKA_TOPIC", "notification.timeline")

	kafkaProducer := kafka.NewProducer([]string{kafkaBroker}, paymentInitiatedKafkaTopic)
	offerInititatedKafkaProducer := kafka.NewProducer([]string{kafkaBroker}, offerInititatedKafkaTopic)
	paymentSuccessKafkaProducer := kafka.NewProducer([]string{kafkaBroker}, paymentSuccessfulKafkaTopic)
	timelinePublisher := notifications.NewTimelinePublisher(strings.Split(kafkaBroker, ","), notificationTimelineKafkaTopic)
	defer timelinePublisher.Close()

	outboxWorker := worker.NewOutboxWorker(db, *outboxRepository, *idempotencyRepository, *kafkaProducer, *offerInititatedKafkaProducer, *paymentSuccessKafkaProducer, timelinePublisher)

	log.Printf("transaction processor worker starting broker=%s topic=[%s, %s, %s]", kafkaBroker, paymentInitiatedKafkaTopic, offerInititatedKafkaTopic, paymentSuccessfulKafkaTopic)
	outboxWorker.Start(ctx)
}
