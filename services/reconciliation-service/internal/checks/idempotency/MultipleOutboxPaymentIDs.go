package idempotency

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func BuildMultipleOutboxPaymentIDsAnomalies(rows []domain.IdempotencyMultipleOutboxPaymentIDs) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(rows))

	for _, row := range rows {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.IdempotencyMultipleOutboxPaymentIDsCheckName,
			EntityType:  "idempotency_key",
			EntityID:    row.IdempotencyKey,
			Description: fmt.Sprintf("idempotency key has %d distinct outbox aggregate payment IDs", row.PaymentIDCount),
			Severity:    models.CRITICAL,
		})
	}

	return anomalies
}
