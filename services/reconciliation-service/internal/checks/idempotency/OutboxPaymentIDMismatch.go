package idempotency

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func BuildOutboxPaymentIDMismatchAnomalies(mismatches []domain.IdempotencyOutboxPaymentIDMismatch) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(mismatches))

	for _, mismatch := range mismatches {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.IdempotencyOutboxPaymentIDMismatchCheckName,
			EntityType:  "idempotency_key",
			EntityID:    mismatch.IdempotencyKey,
			Description: fmt.Sprintf("idempotency payment_id=%s differs from outbox aggregate_id=%s outbox_event_id=%s", mismatch.IdempotencyPaymentID, mismatch.OutboxPaymentID, mismatch.OutboxEventID),
			Severity:    models.CRITICAL,
		})
	}

	return anomalies
}
