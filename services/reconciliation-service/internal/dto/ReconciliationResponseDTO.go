package dto

type ReconciliationStatusSummaryDTO struct {
	Type string
}

type ReconciliationResponseDTO struct {
	CheckName       string               `json:"check_name"`
	Status          string               `json:"status"`
	AnomalyCount    int                  `json:"anomaly_count"`
	ExecutionTimeMs int64                `json:"execution_time_ms"`
	Anomalies       []AnomalyResponseDTO `json:"anomalies"`
	SeveritySummary map[string]int8      `json:"severity_summary"`
	ChecksSummary   map[string]int8      `json:"check_summary"`
}

type AnomalyResponseDTO struct {
	CheckName   string `json:"check_name"`
	EntityType  string `json:"entity_type"`
	EntityID    string `json:"entity_id"`
	Description string `json:"description"`
	Severity    string `json:"severity"`
}
