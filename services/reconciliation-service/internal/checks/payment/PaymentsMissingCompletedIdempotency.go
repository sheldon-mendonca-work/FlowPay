package payment

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func PaymentsMissingCompletedIdempotencyAnamolies(payments []domain.PaymentWithIdempotencyPayment) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(payments))

	for _, payment := range payments {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.PaymentsMissingCompletedIdempotencyCheckName,
			EntityType:  "payment",
			EntityID:    payment.ID,
			Description: fmt.Sprintf("Payment missing for completed idempotency key: %s", payment.IdempotencyPaymentKey),
			Severity:    models.CRITICAL,
		})
	}

	return anomalies
}
