package worker

import (
	"context"
	"database/sql"
	"flowpay/pkg/observability/logger"
	"flowpay/pkg/observability/tracing"
	transactionProcessorConstants "flowpay/transaction-processor/internal/constants"
	"flowpay/transaction-processor/internal/domain"
	flowpayOutboxErrors "flowpay/transaction-processor/internal/errors"
	"flowpay/transaction-processor/internal/kafka"
	"flowpay/transaction-processor/internal/repo"
	"time"
)

type OutboxWorker struct {
	db        *sql.DB
	repo      repo.OutboxEventRepository
	producer  kafka.KafkaProducer
	batchSize int
}

func NewOutboxWorker(db *sql.DB, repo repo.OutboxEventRepository, producer kafka.KafkaProducer) *OutboxWorker {
	return &OutboxWorker{
		db:        db,
		repo:      repo,
		producer:  producer,
		batchSize: 10,
	}
}

func leaseExpiryFromNow() time.Time {
	return time.Now().UTC().Add(5 * time.Minute)
}

func (w *OutboxWorker) processBatch(ctx context.Context) error {

	// claim batches with lease
	events, err := w.repo.ClaimBatch(ctx, w.batchSize, transactionProcessorConstants.MaxKafkaRetryCount, leaseExpiryFromNow())
	if err != nil {
		logger.LogEvent(ctx, "ERROR", transactionProcessorConstants.ServiceName, "outbox_claim_batch_failed", logger.Fields{
			"error": err.Error(),
		})
		return err
	}

	for _, event := range events {
		eventCtx := tracing.WithTraceAndRequestIDs(ctx, event.TraceID, event.RequestID)
		err := w.processEvent(eventCtx, event)
		if err != nil {
			logger.LogEvent(eventCtx, "ERROR", transactionProcessorConstants.ServiceName, "outbox_process_event_failed", logger.Fields{
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
	logger.LogEvent(ctx, "INFO", transactionProcessorConstants.ServiceName, "process_received", logger.Fields{
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
			if event.RetryCount+1 >= int8(transactionProcessorConstants.MaxKafkaRetryCount) {
				markError = w.repo.MarkFailed(ctx, event.ID, errorType, errorText)
			} else {
				markError = w.repo.MarkRetryableFailure(ctx, event.ID, errorType, errorText)
			}

			logger.LogEvent(ctx, "ERROR", transactionProcessorConstants.ServiceName, "outbox_worker_batch_failed", logger.Fields{
				"error": markError.Error(),
			})
		}
	}()

	err = w.producer.Publish(ctx, "payment.initiated", event.AggregateID, []byte(event.Payload))
	if err != nil {
		err = flowpayOutboxErrors.ErrKafkaPublishFailed
		return err
	}

	err = w.repo.MarkPublished(ctx, tx, event.ID)
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
			logger.LogEvent(ctx, "INFO", transactionProcessorConstants.ServiceName, "outbox_worker_shutdown", logger.Fields{})
			return
		case <-ticker.C:
			err := w.processBatch(ctx)
			if err != nil {
				logger.LogEvent(ctx, "ERROR", transactionProcessorConstants.ServiceName, "outbox_worker_batch_failed", logger.Fields{
					"error": err.Error(),
				})
			}
		}
	}
}
