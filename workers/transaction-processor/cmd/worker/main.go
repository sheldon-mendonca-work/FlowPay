package main

import (
	"context"
	"flowpay/pkg/utils"
	"flowpay/transaction-processor/internal/db"
	"flowpay/transaction-processor/internal/kafka"
	"flowpay/transaction-processor/internal/repo"
	"flowpay/transaction-processor/internal/worker"
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
	kafkaTopic := utils.GetEnv("KAFKA_TOPIC", "payment.initiated")

	kafkaProducer := kafka.NewProducer([]string{kafkaBroker}, kafkaTopic)

	outboxWorker := worker.NewOutboxWorker(db, *outboxRepository, *idempotencyRepository, *kafkaProducer)

	log.Printf("transaction processor worker starting broker=%s topic=%s", kafkaBroker, kafkaTopic)
	outboxWorker.Start(ctx)
}
