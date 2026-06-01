package outbox

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func BuildStuckProcessingAnomalies(events []domain.OutboxEventReconciliation) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(events))

	for _, event := range events {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.OutboxStuckProcessingCheckName,
			EntityType:  "outbox_event",
			EntityID:    event.ID,
			Description: fmt.Sprintf("PROCESSING event is stuck aggregate_id=%s event_type=%s idempotency_key=%s retry_count=%d stuck_for=%s", event.AggregateID, event.EventType, event.IdempotencyKey, event.RetryCount, event.StuckFor),
			Severity:    models.WARN,
		})
	}

	return anomalies
}
