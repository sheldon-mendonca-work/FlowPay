package domain

import (
	"database/sql"
	"time"
)

type OfferOutboxEventType struct {
	ID             string
	AggregateType  string // payment/order/wallet
	AggregateID    string // for now idempotency then later paymentid
	IdempotencyKey string
	Payload        string
	EventType      string
	EventVersion   int8
	Status         string
	TraceID        string
	RequestID      string
	RetryCount     int8
	LockedUntil    sql.NullTime
	CreatedAt      time.Time
	PublishedAt    time.Time
}

const (
	OfferOutboxEventPending    = "PENDING"
	OfferOutboxEventProcessing = "PROCESSING"
	OfferOutboxEventPublished  = "PUBLISHED"
)
