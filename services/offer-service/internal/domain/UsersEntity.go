package domain

import "time"

type UsersEntity struct {
	ID        string
	AccountID string
	CompanyID string
	Role      string
	CreatedAt time.Time
	UpdatedAt time.Time
}
