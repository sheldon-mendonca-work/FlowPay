package idempotency

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func BuildCompletedWithoutPaymentAnomalies(idempotencies []domain.PaymentIdempotencyKey) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(idempotencies))

	for _, idempotency := range idempotencies {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.IdempotencyCompletedWithoutPaymentCheckName,
			EntityType:  "idempotency_key",
			EntityID:    idempotency.IdempotencyKey,
			Description: fmt.Sprintf("completed idempotency row points to missing payment_id=%s status=%s", idempotency.PaymentID, idempotency.Status),
			Severity:    models.CRITICAL,
		})
	}

	return anomalies
}
