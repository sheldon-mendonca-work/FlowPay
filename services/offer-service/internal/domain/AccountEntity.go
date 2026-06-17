package domain

import "time"

type Account struct {
	ID                   string
	AccountName          string
	AccountType          string
	Balance              int32
	AllowNegativeBalance bool
	Currency             string
	CreatedAt            time.Time
	UpdatedAt            time.Time
}
