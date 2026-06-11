package domain

import "time"

type OfferRedemptionIdempotencyKeyEntity struct {
	IdempotencyKey string

	RequestHash string

	Status string // IN_PROGRESS | COMPLETED | FAILED

	ResponseBody string

	ErrorCode    string
	ErrorMessage string

	OfferID      string
	RedemptionID string

	OwnerToken  string    // who owns processing
	LockedUntil time.Time // lease expiry

	CreatedAt time.Time
	UpdatedAt time.Time
}
