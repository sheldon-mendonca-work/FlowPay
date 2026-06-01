package payment

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func BuildPaymentsWithInvalidTransactionPairAnomalies(payments []domain.PaymentWithTransactionCounts) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(payments))

	for _, payment := range payments {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.PaymentsWithInvalidTransactionPairCheckName,
			EntityType:  "payment",
			EntityID:    payment.ID,
			Description: fmt.Sprintf("Expected exactly 1 DEBIT and 1 CREDIT transaction; found DEBIT=%d CREDIT=%d", payment.DebitCount, payment.CreditCount),
			Severity:    models.CRITICAL,
		})
	}

	return anomalies
}
