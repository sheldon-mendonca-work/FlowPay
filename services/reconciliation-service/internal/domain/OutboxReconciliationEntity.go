package domain

type OutboxEventReconciliation struct {
	ID             string
	AggregateID    string
	EventType      string
	IdempotencyKey string
	Status         string
	RetryCount     int
	ErrorCode      string
	ErrorMessage   string
	Age            string
	StuckFor       string
}

type OutboxBacklogSummary struct {
	Status     string
	RetryCount int
	EventCount int
}
