package domain

import "time"

type User struct {
	ID        string
	AccountID string
	CompanyID string
	Role      string
	CreatedAt time.Time
	UpdatedAt time.Time
}
