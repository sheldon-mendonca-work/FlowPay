package domain

import (
	"time"
)

type OutboxEventType struct {
	ID            string
	AggregateType string // payment/order/wallet
	AggregateID   string // for now idempotency then later paymentid
	Payload       string
	EventType     string
	EventVersion  int8
	Status        string
	CreatedAt     time.Time
	PublishedAt   time.Time
}

const (
	OutboxEventPending    = "PENDING"
	OutboxEventProcessing = "PROCESSING"
	OutboxEventPublished  = "PUBLISHED"
)
