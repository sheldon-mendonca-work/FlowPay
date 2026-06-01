package outbox

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func BuildBacklogSummaryAnomalies(summaries []domain.OutboxBacklogSummary) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(summaries))

	for _, summary := range summaries {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.OutboxBacklogSummaryCheckName,
			EntityType:  "outbox_status",
			EntityID:    fmt.Sprintf("%s:%d", summary.Status, summary.RetryCount),
			Description: fmt.Sprintf("outbox backlog summary status=%s retry_count=%d event_count=%d", summary.Status, summary.RetryCount, summary.EventCount),
			Severity:    models.INFO,
		})
	}

	return anomalies
}
