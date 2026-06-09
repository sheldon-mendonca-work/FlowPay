package domain

import "time"

type Account struct {
	ID          string
	AccountName string
	Balance     int64
	Currency    string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}
