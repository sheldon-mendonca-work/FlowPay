package domain

import "time"

type OfferIdempotencyKey struct {
	IdempotencyKey string

	RequestHash string

	Status string // IN_PROGRESS | COMPLETED | FAILED

	ResponseBody string

	ErrorCode    string
	ErrorMessage string

	OfferID string

	OwnerToken  string    // who owns processing
	LockedUntil time.Time // lease expiry

	CreatedAt time.Time
	UpdatedAt time.Time
}
