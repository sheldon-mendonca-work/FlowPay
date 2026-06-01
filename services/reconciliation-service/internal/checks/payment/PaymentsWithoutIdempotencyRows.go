package payment

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func BuildPaymentsWithoutIdempotencyRowsAnomalies(payments []domain.Payment) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(payments))

	for _, payment := range payments {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.PaymentsWithoutIdempotencyRowsCheckName,
			EntityType:  "payment",
			EntityID:    payment.ID,
			Description: fmt.Sprintf("Idempotency row not found"),
			Severity:    models.CRITICAL,
		})
	}

	return anomalies
}
