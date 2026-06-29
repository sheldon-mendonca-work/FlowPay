package domain

import "time"

type DefaultCredential struct {
	AccountID   string
	DisplayName string
	Description string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}
