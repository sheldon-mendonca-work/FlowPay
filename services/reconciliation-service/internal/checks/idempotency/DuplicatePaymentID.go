package idempotency

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func BuildDuplicatePaymentIDAnomalies(duplicates []domain.IdempotencyDuplicatePaymentID) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(duplicates))

	for _, duplicate := range duplicates {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.IdempotencyDuplicatePaymentIDCheckName,
			EntityType:  "payment",
			EntityID:    duplicate.PaymentID,
			Description: fmt.Sprintf("payment_id is referenced by %d idempotency rows", duplicate.RowCount),
			Severity:    models.CRITICAL,
		})
	}

	return anomalies
}
