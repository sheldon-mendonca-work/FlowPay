package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	outboxPublisherConstants "flowpay/offer-outbox-publisher/internal/constants"
	"flowpay/offer-outbox-publisher/internal/domain"
	flowpayOutboxErrors "flowpay/offer-outbox-publisher/internal/errors"
	"flowpay/offer-outbox-publisher/internal/kafka"
	"flowpay/offer-outbox-publisher/internal/repo"
	"flowpay/pkg/notifications"
	"flowpay/pkg/observability/logger"
	"flowpay/pkg/observability/tracing"
	"fmt"
	"time"
)

type OutboxWorker struct {
	db                                    *sql.DB
	outboxRepo                            repo.OfferOutboxEventRepository
	offerReservationIdempotencyRepository repo.OfferReservationIdempotencyRepository
	offerRedemptionIdempotencyRepository  repo.OfferRedemptionIdempotencyRepository
	offerReservedKafkaProducer            kafka.KafkaProducer
	offerRejectedKafkaProducer            kafka.KafkaProducer
	timelinePublisher                     *notifications.TimelinePublisher
	batchSize                             int
}

func NewOutboxWorker(db *sql.DB, outboxRepo repo.OfferOutboxEventRepository, offerReservationIdempotencyRepository repo.OfferReservationIdempotencyRepository, offerRedemptionIdempotencyRepository repo.OfferRedemptionIdempotencyRepository, offerReservedKafkaProducer kafka.KafkaProducer, offerRejectedKafkaProducer kafka.KafkaProducer, timelinePublisher *notifications.TimelinePublisher) *OutboxWorker {
	return &OutboxWorker{
		db:                                    db,
		outboxRepo:                            outboxRepo,
		offerReservationIdempotencyRepository: offerReservationIdempotencyRepository,
		offerRedemptionIdempotencyRepository:  offerRedemptionIdempotencyRepository,
		offerReservedKafkaProducer:            offerReservedKafkaProducer,
		offerRejectedKafkaProducer:            offerRejectedKafkaProducer,
		timelinePublisher:                     timelinePublisher,
		batchSize:                             100,
	}
}

type offerOutboxPayload struct {
	PaymentID string `json:"payment_id"`
}

func leaseExpiryFromNow() time.Time {
	return time.Now().UTC().Add(5 * time.Minute)
}

func (w *OutboxWorker) processReservingBatch(ctx context.Context) error {

	// claim batches with lease
	events, err := w.outboxRepo.ClaimBatch(ctx, w.batchSize, outboxPublisherConstants.MaxKafkaRetryCount, leaseExpiryFromNow())
	if err != nil {
		logger.LogEvent(ctx, "ERROR", outboxPublisherConstants.ServiceName, "outbox_claim_batch_failed", logger.Fields{
			"error": err.Error(),
		})
		return err
	}

	for _, event := range events {
		eventCtx := tracing.WithTraceAndRequestIDs(ctx, event.TraceID, event.RequestID)
		err := w.processReservingEvent(eventCtx, event)
		if err != nil {
			logger.LogEvent(eventCtx, "ERROR", outboxPublisherConstants.ServiceName, "outbox_process_event_failed", logger.Fields{
				"event_id":   event.ID,
				"error":      err.Error(),
				"error_type": flowpayOutboxErrors.ToOutboxErrorType(err),
			})
			continue
		}
	}

	return nil
}

func (w *OutboxWorker) processReservingEvent(ctx context.Context, event domain.OfferOutboxEventType) error {
	logger.LogEvent(ctx, "INFO", outboxPublisherConstants.ServiceName, "process_received", logger.Fields{
		"id":             event.ID,
		"aggregate_type": event.AggregateType,
		"aggregate_id":   event.AggregateID,
		"event_type":     event.EventType,
		"event_version":  event.EventVersion,
		"status":         event.Status,
		"retry_count":    event.RetryCount,
		"error_type":     "NONE",
	})

	txCommitted := false
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		logger.LogEvent(ctx, "ERROR", outboxPublisherConstants.ServiceName, "failed_to_start_tx", logger.Fields{
			"error": err.Error(),
		})
		return err
	}

	defer func() {
		if !txCommitted {
			tx.Rollback()
			errorType := flowpayOutboxErrors.ToOutboxErrorType(err)
			errorText := ""
			if err != nil {
				errorText = err.Error()
			}
			var markError error
			var idempotencyError error
			if event.RetryCount+1 >= int8(outboxPublisherConstants.MaxKafkaRetryCount) {
				markError = w.outboxRepo.MarkFailed(ctx, event.ID, errorType, errorText)
				idempotencyError = w.offerReservationIdempotencyRepository.MarkFailed(ctx, event.IdempotencyKey, errorType, errorText)
			} else {
				markError = w.outboxRepo.MarkRetryableFailure(ctx, event.ID, errorType, errorText)
			}

			logger.LogEvent(ctx, "ERROR", outboxPublisherConstants.ServiceName, "outbox_worker_batch_failed", logger.Fields{
				"error": errorText,
			})
			if markError != nil {
				logger.LogEvent(ctx, "ERROR", outboxPublisherConstants.ServiceName, "outbox_event_mark_failure_failed", logger.Fields{
					"error": markError.Error(),
				})
			}
			if idempotencyError != nil {
				logger.LogEvent(ctx, "ERROR", outboxPublisherConstants.ServiceName, "idempotency_mark_failure_failed", logger.Fields{
					"error": idempotencyError.Error(),
				})
			}
		}
	}()

	switch event.EventType {

	case "OFFER_RESERVED":
		err = w.offerReservedKafkaProducer.Publish(
			ctx,
			event.AggregateID,
			event.ID,
			[]byte(event.Payload),
		)

	case "OFFER_REJECTED":
		err = w.offerRejectedKafkaProducer.Publish(
			ctx,
			event.AggregateID,
			event.ID,
			[]byte(event.Payload),
		)

	default:
		return fmt.Errorf("unknown event type")
	}
	if err != nil {
		err = flowpayOutboxErrors.ErrKafkaPublishFailed
		return err
	}

	var payload offerOutboxPayload
	if decodeErr := json.Unmarshal([]byte(event.Payload), &payload); decodeErr == nil {
		w.timelinePublisher.Publish(ctx, outboxPublisherConstants.ServiceName, payload.PaymentID, notifications.StepKafkaPublished, notifications.StatusSuccess, event.TraceID, event.RequestID)
	}

	err = w.outboxRepo.MarkPublished(ctx, tx, event.ID)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	txCommitted = true

	return nil
}

func (w *OutboxWorker) Start(ctx context.Context) {
	// ticker := time.NewTicker(2 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.LogEvent(ctx, "INFO", outboxPublisherConstants.ServiceName, "outbox_worker_shutdown", logger.Fields{})
			return
		case <-ticker.C:
			err := w.processReservingBatch(ctx)
			if err != nil {
				logger.LogEvent(ctx, "ERROR", outboxPublisherConstants.ServiceName, "outbox_worker_batch_failed", logger.Fields{
					"error": err.Error(),
				})
			}
		}
	}
}
