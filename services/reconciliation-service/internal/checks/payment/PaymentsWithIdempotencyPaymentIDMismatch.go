package payment

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func PaymentsWithIdempotencyPaymentIDMismatchAnamolies(payments []domain.PaymentWithPaymentIdempotency) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(payments))

	for _, payment := range payments {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.PaymentsWithIdempotencyPaymentIDMismatchCheckName,
			EntityType:  "payment",
			EntityID:    payment.ID,
			Description: fmt.Sprintf("Payment id mismatch for idempotency key: %s. PaymentID: %s, Idemptotency PaymentID: %s", payment.ID, payment.IdempotencyKey, payment.PaymentIdempotencyKey),
			Severity:    models.CRITICAL,
		})
	}

	return anomalies
}
