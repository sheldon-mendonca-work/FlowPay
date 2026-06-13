package main

import (
	"context"
	"flowpay/offer-expiry-worker/internal/db"
	"flowpay/offer-expiry-worker/internal/repository"
	"flowpay/offer-expiry-worker/internal/worker"
	"flowpay/pkg/observability/metrics"
	"log"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	db := db.InitDB()
	defer db.Close()
	metrics.InitOfferExpiryWorkerMetrics()

	offerRepository := repository.NewOfferRepository(db)
	offerReservationRepository := repository.NewOfferReservationsRepository(db)

	outboxWorker := worker.NewOfferExpiryWorker(db, *offerRepository, *offerReservationRepository)

	log.Printf("offer expiry worker")
	outboxWorker.Start(ctx)
}
