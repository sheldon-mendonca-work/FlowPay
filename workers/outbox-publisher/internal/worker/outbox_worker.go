package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	outboxPublisherConstants "flowpay/outbox-publisher/internal/constants"
	"flowpay/outbox-publisher/internal/domain"
	flowpayOutboxErrors "flowpay/outbox-publisher/internal/errors"
	"flowpay/outbox-publisher/internal/kafka"
	"flowpay/outbox-publisher/internal/repo"
	"flowpay/pkg/observability/logger"
	"flowpay/pkg/observability/tracing"
	"time"
)

type OutboxWorker struct {
	db                           *sql.DB
	outboxRepo                   repo.OutboxEventRepository
	idempotencyRepo              repo.PaymentIdempotencyRepository
	paymentInitatedKafkaProducer kafka.KafkaProducer
	offerInititatedKafkaProducer kafka.KafkaProducer
	paymentSuccessKafkaProducer  kafka.KafkaProducer
	batchSize                    int
}

func NewOutboxWorker(db *sql.DB, outboxRepo repo.OutboxEventRepository, idempotencyRepo repo.PaymentIdempotencyRepository, paymentInitatedKafkaProducer kafka.KafkaProducer, offerInititatedKafkaProducer kafka.KafkaProducer, paymentSuccessKafkaProducer kafka.KafkaProducer) *OutboxWorker {
	return &OutboxWorker{
		db:                           db,
		outboxRepo:                   outboxRepo,
		idempotencyRepo:              idempotencyRepo,
		paymentInitatedKafkaProducer: paymentInitatedKafkaProducer,
		offerInititatedKafkaProducer: offerInititatedKafkaProducer,
		paymentSuccessKafkaProducer:  paymentSuccessKafkaProducer,
		batchSize:                    10,
	}
}

func leaseExpiryFromNow() time.Time {
	return time.Now().UTC().Add(5 * time.Minute)
}

func (w *OutboxWorker) processBatch(ctx context.Context) error {

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
		err := w.processEvent(eventCtx, event)
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

func (w *OutboxWorker) processEvent(ctx context.Context, event domain.OutboxEventType) error {
	logger.LogEvent(ctx, "INFO", outboxPublisherConstants.ServiceName, "process_received", logger.Fields{
		"ID":            event.ID,
		"AggregateType": event.AggregateType,
		"AggregateID":   event.AggregateID,
		"EventType":     event.EventType,
		"EventVersion":  event.EventVersion,
		"Status":        event.Status,
		"retry_count":   event.RetryCount,
		"error_type":    "NONE",
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
				idempotencyError = w.idempotencyRepo.MarkFailed(ctx, event.IdempotencyKey, errorType, errorText)
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

	var eventPayload domain.OutboxEventType

	if err := json.Unmarshal([]byte(event.Payload), &eventPayload); err != nil {
		return err
	}

	switch eventPayload.EventType {
	case "offer_initiated":
		err = w.offerInititatedKafkaProducer.Publish(ctx, event.ID, event.AggregateID, []byte(event.Payload))
	case "payment_initiated":
		err = w.paymentInitatedKafkaProducer.Publish(ctx, event.ID, event.AggregateID, []byte(event.Payload))
	case "offer_applicable_payment_success", "payment_success":
		err = w.paymentSuccessKafkaProducer.Publish(ctx, event.ID, event.AggregateID, []byte(event.Payload))

	}

	if err != nil {
		err = flowpayOutboxErrors.ErrKafkaPublishFailed
		return err
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
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.LogEvent(ctx, "INFO", outboxPublisherConstants.ServiceName, "outbox_worker_shutdown", logger.Fields{})
			return
		case <-ticker.C:
			err := w.processBatch(ctx)
			if err != nil {
				logger.LogEvent(ctx, "ERROR", outboxPublisherConstants.ServiceName, "outbox_worker_batch_failed", logger.Fields{
					"error": err.Error(),
				})
			}
		}
	}
}
