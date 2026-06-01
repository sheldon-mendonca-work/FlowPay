package domain

type IdempotencyDuplicatePaymentID struct {
	PaymentID string
	RowCount  int
}

type IdempotencyExpiredInProgress struct {
	IdempotencyKey string
	PaymentID      string
	OwnerToken     string
	ExpiredFor     string
}

type IdempotencyOutboxPaymentIDMismatch struct {
	IdempotencyKey       string
	IdempotencyPaymentID string
	OutboxPaymentID      string
	OutboxEventID        string
}

type IdempotencyMultipleOutboxPaymentIDs struct {
	IdempotencyKey string
	PaymentIDCount int
}
