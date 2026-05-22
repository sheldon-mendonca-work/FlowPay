package main

import (
	"context"
	"flowpay/payment-executor/internal/handler"
	"flowpay/payment-executor/internal/infra"
	"flowpay/payment-executor/internal/kafka"
	"flowpay/payment-executor/internal/repository"
	"flowpay/payment-executor/internal/service"
	"flowpay/pkg/utils"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	db := infra.InitDB()

	paymentRepository := repository.NewPaymentRepository(db)
	accountRepository := repository.NewAccountRepository(db)
	transactionRepository := repository.NewTransactionRepository(db)
	idempotencyRepository := repository.NewPaymentIdempotencyRepository(db)
	outboxEventsRepository := repository.NewOutboxEventsRepository(db)
	paymentExecutorService := service.NewPaymentExecutorService(db, accountRepository, paymentRepository, transactionRepository, idempotencyRepository, outboxEventsRepository)

	paymentHandler := handler.NewPaymentHandler(paymentExecutorService)

	kafkaBroker := utils.GetEnv("KAFKA_BROKER", "localhost:9094")
	kafkaTopic := utils.GetEnv("KAFKA_TOPIC", "payment.initiated")
	kafkaGroupID := utils.GetEnv("KAFKA_GROUP_ID", "payment-executor-group")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	consumer := kafka.NewKafkaConsumer(strings.Split(kafkaBroker, ","), kafkaTopic, kafkaGroupID, paymentHandler.ExecutePayment)
	defer consumer.Close()

	if err := consumer.Start(ctx); err != nil {
		log.Fatalf("payment executor consumer stopped: %v", err)
	}
}
