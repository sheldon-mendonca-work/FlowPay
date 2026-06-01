package outbox

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func BuildWithoutIdempotencyRowsAnomalies(events []domain.OutboxEventReconciliation) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(events))

	for _, event := range events {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.OutboxWithoutIdempotencyRowsCheckName,
			EntityType:  "outbox_event",
			EntityID:    event.ID,
			Description: fmt.Sprintf("outbox event has no idempotency row aggregate_id=%s event_type=%s idempotency_key=%s status=%s retry_count=%d", event.AggregateID, event.EventType, event.IdempotencyKey, event.Status, event.RetryCount),
			Severity:    models.CRITICAL,
		})
	}

	return anomalies
}
