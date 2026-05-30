package models

type Anomaly struct {
	CheckName   string
	EntityType  string // payment, outbox, ledger
	EntityID    string
	Description string
	Severity    Severity
}
