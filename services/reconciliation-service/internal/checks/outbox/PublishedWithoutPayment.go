package outbox

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func BuildPublishedWithoutPaymentAnomalies(events []domain.OutboxEventReconciliation) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(events))

	for _, event := range events {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.OutboxPublishedWithoutPaymentCheckName,
			EntityType:  "outbox_event",
			EntityID:    event.ID,
			Description: fmt.Sprintf("published outbox event points to missing payment aggregate_id=%s event_type=%s idempotency_key=%s status=%s", event.AggregateID, event.EventType, event.IdempotencyKey, event.Status),
			Severity:    models.CRITICAL,
		})
	}

	return anomalies
}
