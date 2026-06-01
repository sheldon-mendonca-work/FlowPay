package domain

type TransactionLedgerCounts struct {
	PaymentID        string
	TransactionCount int
	DebitCount       int
	CreditCount      int
}

type TransactionAmountCurrencyImbalance struct {
	PaymentID           string
	DebitTransactionID  string
	CreditTransactionID string
	DebitAmount         int64
	CreditAmount        int64
	DebitCurrency       string
	CreditCurrency      string
}

type TransactionWithoutPayment struct {
	ID        string
	PaymentID string
	Type      string
	Amount    int64
	Currency  string
	Status    string
}

type TransactionStatusMismatch struct {
	ID                string
	PaymentID         string
	Type              string
	TransactionStatus string
	PaymentStatus     string
}
