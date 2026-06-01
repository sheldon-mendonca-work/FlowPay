package idempotency

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func BuildExpiredInProgressAnomalies(expiredRows []domain.IdempotencyExpiredInProgress) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(expiredRows))

	for _, expired := range expiredRows {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.IdempotencyExpiredInProgressCheckName,
			EntityType:  "idempotency_key",
			EntityID:    expired.IdempotencyKey,
			Description: fmt.Sprintf("expired IN_PROGRESS row payment_id=%s owner_token=%s expired_for=%s", expired.PaymentID, expired.OwnerToken, expired.ExpiredFor),
			Severity:    models.WARN,
		})
	}

	return anomalies
}
