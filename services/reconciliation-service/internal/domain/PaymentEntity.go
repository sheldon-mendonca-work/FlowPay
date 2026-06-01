package domain

import "time"

type Payment struct {
	ID             string
	IdempotencyKey string
	SenderID       string
	ReceiverID     string
	Amount         int64
	Currency       string
	Status         string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type PaymentWithTransactionCounts struct {
	ID               string
	IdempotencyKey   string
	SenderID         string
	ReceiverID       string
	Amount           int64
	Currency         string
	Status           string
	TransactionCount int8
	DebitCount       int8
	CreditCount      int8
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type PaymentWithPaymentIdempotency struct {
	ID                    string
	IdempotencyKey        string
	PaymentIdempotencyKey string
	SenderID              string
	ReceiverID            string
	Amount                int64
	Currency              string
	Status                string
	IdempotencyStatus     string
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

type PaymentWithIdempotencyPayment struct {
	ID                    string
	IdempotencyKey        string
	IdempotencyPaymentKey string
	SenderID              string
	ReceiverID            string
	Amount                int64
	Currency              string
	Status                string
	IdempotencyStatus     string
	CreatedAt             time.Time
	UpdatedAt             time.Time
}
