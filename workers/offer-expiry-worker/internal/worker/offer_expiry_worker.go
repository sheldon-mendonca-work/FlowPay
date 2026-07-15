package worker

import (
	"context"
	"database/sql"
	offerExpiryWorkerConstants "flowpay/offer-expiry-worker/internal/constants"
	"flowpay/offer-expiry-worker/internal/repository"
	"flowpay/pkg/observability/logger"
	"flowpay/pkg/observability/metrics"
	"time"
)

type OfferExpiryWorker struct {
	db                   *sql.DB
	offerRepo            repository.OfferRepository
	offerReservationRepo repository.OfferReservationsRepository
	batchSize            int
}

func NewOfferExpiryWorker(db *sql.DB, offerRepo repository.OfferRepository, offerReservationRepo repository.OfferReservationsRepository) *OfferExpiryWorker {
	return &OfferExpiryWorker{
		db:                   db,
		offerRepo:            offerRepo,
		offerReservationRepo: offerReservationRepo,
		batchSize:            100,
	}
}

func (w *OfferExpiryWorker) processBatch(ctx context.Context) error {
	start := time.Now()

	defer func() {
		metrics.OfferExpiryWorkerDurationMs.Observe(
			float64(time.Since(start).Milliseconds()),
		)
	}()

	reservations, err := w.offerReservationRepo.ClaimBatch(
		ctx,
		w.batchSize,
	)
	if err != nil {
		metrics.OfferExpiryWorkerFailuresTotal.Inc()

		logger.LogEvent(
			ctx,
			"ERROR",
			offerExpiryWorkerConstants.ServiceName,
			"offer_expiry_claim_batch_failed",
			logger.Fields{
				"error": err.Error(),
			},
		)

		return err
	}

	batchSize := len(reservations)

	metrics.OfferExpiryWorkerBatchSize.Observe(
		float64(batchSize),
	)

	metrics.OfferExpiryWorkerExpiredTotal.Add(
		float64(batchSize),
	)

	if batchSize == 0 {
		return nil
	}

	for _, reservation := range reservations {
		logger.LogEvent(
			ctx,
			"INFO",
			offerExpiryWorkerConstants.ServiceName,
			"offer_reservation_expired",
			logger.Fields{
				"reservation_id":  reservation.ID,
				"offer_id":        reservation.OfferID,
				"payment_id":      reservation.PaymentID,
				"account_id":      reservation.AccountID,
				"idempotency_key": reservation.IdempotencyKey,
				"status":          reservation.Status,
			},
		)
	}

	logger.LogEvent(
		ctx,
		"INFO",
		offerExpiryWorkerConstants.ServiceName,
		"offer_expiry_batch_processed",
		logger.Fields{
			"expired_count": batchSize,
			"duration_ms":   time.Since(start).Milliseconds(),
		},
	)

	return nil
}

func (w *OfferExpiryWorker) Start(ctx context.Context) {
	// ticker := time.NewTicker(2 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	logger.LogEvent(
		ctx,
		"INFO",
		offerExpiryWorkerConstants.ServiceName,
		"offer_expiry_worker_started",
		logger.Fields{
			"batch_size": w.batchSize,
		},
	)

	for {
		select {

		case <-ctx.Done():
			logger.LogEvent(
				ctx,
				"INFO",
				offerExpiryWorkerConstants.ServiceName,
				"offer_expiry_worker_shutdown",
				logger.Fields{},
			)
			return

		case <-ticker.C:
			if err := w.processBatch(ctx); err != nil {

				logger.LogEvent(
					ctx,
					"ERROR",
					offerExpiryWorkerConstants.ServiceName,
					"offer_expiry_worker_batch_failed",
					logger.Fields{
						"error": err.Error(),
					},
				)
			}
		}
	}
}
