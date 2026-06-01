package idempotency

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func BuildMissingPaymentIDAnomalies(idempotencies []domain.PaymentIdempotencyKey) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(idempotencies))

	for _, idempotency := range idempotencies {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.IdempotencyMissingPaymentIDCheckName,
			EntityType:  "idempotency_key",
			EntityID:    idempotency.IdempotencyKey,
			Description: fmt.Sprintf("idempotency row is missing canonical payment_id status=%s owner_token=%s", idempotency.Status, idempotency.OwnerToken),
			Severity:    models.CRITICAL,
		})
	}

	return anomalies
}
