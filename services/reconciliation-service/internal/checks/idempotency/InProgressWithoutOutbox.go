package idempotency

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func BuildInProgressWithoutOutboxAnomalies(idempotencies []domain.PaymentIdempotencyKey) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(idempotencies))

	for _, idempotency := range idempotencies {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.IdempotencyInProgressWithoutOutboxCheckName,
			EntityType:  "idempotency_key",
			EntityID:    idempotency.IdempotencyKey,
			Description: fmt.Sprintf("IN_PROGRESS idempotency row has no outbox event payment_id=%s owner_token=%s", idempotency.PaymentID, idempotency.OwnerToken),
			Severity:    models.CRITICAL,
		})
	}

	return anomalies
}
