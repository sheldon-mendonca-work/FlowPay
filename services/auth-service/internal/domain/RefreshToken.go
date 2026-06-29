package domain

import "time"

type RefreshToken struct {
	ID        string
	AccountID string
	TokenHash string
	ExpiresAt time.Time
	CreatedAt time.Time
}
