package payment

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func BuildIdempotencyRowsWithoutPaymentsAnomalies(idemptencies []domain.PaymentIdempotencyKey) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(idemptencies))

	for _, idempotency := range idemptencies {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.PaymentsWithoutTransactionsCheckName,
			EntityType:  "idempotency_key",
			EntityID:    idempotency.IdempotencyKey,
			Description: fmt.Sprintf("No matching transactions found"),
			Severity:    models.CRITICAL,
		})
	}

	return anomalies
}
