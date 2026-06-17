package domain

import "time"

type Transaction struct {
	ID                  string
	PaymentID           string
	AccountID           string
	Type                string
	Amount              int64
	Currency            string
	TransactionCategory string
	Status              string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}
