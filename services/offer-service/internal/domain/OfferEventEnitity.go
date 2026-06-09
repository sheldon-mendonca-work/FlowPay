package domain

import "time"

type OfferEventEntity struct {
	ID        string
	OfferID   string
	EventType string
	ActorID   string
	ActorType string
	Metadata  []byte
	CreatedAt time.Time
}
