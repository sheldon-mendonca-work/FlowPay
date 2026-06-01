package outbox

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func BuildEligibleBacklogAnomalies(events []domain.OutboxEventReconciliation) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(events))

	for _, event := range events {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.OutboxEligibleBacklogCheckName,
			EntityType:  "outbox_event",
			EntityID:    event.ID,
			Description: fmt.Sprintf("outbox event is eligible backlog aggregate_id=%s event_type=%s idempotency_key=%s status=%s retry_count=%d age=%s", event.AggregateID, event.EventType, event.IdempotencyKey, event.Status, event.RetryCount, event.Age),
			Severity:    models.WARN,
		})
	}

	return anomalies
}
