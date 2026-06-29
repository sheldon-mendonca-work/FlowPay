package domain

import "time"

type Credentials struct {
	AccountID         string
	Email             string
	PasswordHash      string
	PasswordUpdatedAt time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
}
