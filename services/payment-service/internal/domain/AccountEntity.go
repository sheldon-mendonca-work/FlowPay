package domain

import "time"

type Account struct {
	ID                   string
	AccountName          string
	PaymentHandle        string
	AccountType          string
	Balance              int64
	AllowNegativeBalance bool
	Currency             string
	CreatedAt            time.Time
	UpdatedAt            time.Time
}
