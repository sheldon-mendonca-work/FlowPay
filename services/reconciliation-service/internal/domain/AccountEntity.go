package domain

import "time"

type Account struct {
	ID        string
	UserID    string
	Balance   int64
	Currency  string
	CreatedAt time.Time
	UpdatedAt time.Time
}
