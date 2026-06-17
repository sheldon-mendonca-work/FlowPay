package domain

import "time"

type OfferOutboxEvent struct {
	ID             string
	AggregateType  string
	AggregateID    string
	EventType      string
	EventVersion   int8
	IdempotencyKey string
	Payload        string
	Status         string
	LockedUntil    time.Time
	RetryCount     int8
	TraceID        string
	RequestID      string
	ErrorCode      string
	ErrorMessage   string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	PublishedAt    time.Time
}
