package worker

import (
	"context"
	"database/sql"
	"flowpay/pkg/observability/logger"
	transactionProcessorConstants "flowpay/transaction-processor/internal/constants"
	"flowpay/transaction-processor/internal/domain"
	"flowpay/transaction-processor/internal/kafka"
	"flowpay/transaction-processor/internal/repo"
	"log"
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

func (w *OutboxWorker) processBatch(ctx context.Context) error {
	txCommitted := false
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer func() {
		if !txCommitted {
			tx.Rollback()
		}
	}()
	events, err := w.repo.FetchUnpublished(tx, ctx, w.batchSize)
	if err != nil {
		log.Println("Failed to fetch outbox events:", err)
		return err
	}

	for _, event := range events {
		err := w.processEvent(tx, ctx, event)
		if err != nil {
			log.Println("Failed to process event: ", event.ID, err)
			continue
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	txCommitted = true

	return nil
}

func (w *OutboxWorker) processEvent(tx *sql.Tx, ctx context.Context, event domain.OutboxEventType) error {
	logger.LogEvent(ctx, "INFO", transactionProcessorConstants.ServiceName, "process_received", logger.Fields{
		"ID":            event.ID,
		"AggregateType": event.AggregateType,
		"AggregateID":   event.AggregateID,
		"EventType":     event.EventType,
		"EventVersion":  event.EventVersion,
		"Status":        event.Status,
		"error_type":    "NONE",
	})

	err := w.producer.Publish(ctx, "payment.initiated", event.AggregateID, []byte(event.Payload))
	if err != nil {
		return err
	}

	err = w.repo.MarkPublished(ctx, tx, event.ID)
	if err != nil {
		return err
	}

	return nil
}

func (w *OutboxWorker) Start(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Outbox worker shutting down")
			return
		case <-ticker.C:
			err := w.processBatch(ctx)
			if err != nil {
				log.Println("Outbox worker batch failed: ", err)
			}
		}
	}
}
