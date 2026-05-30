package payment

import (
	"fmt"

	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

const PaymentsWithoutTransactionsCheckName = "payments_without_transactions"

func BuildPaymentsWithoutTransactionsAnomalies(payments []domain.Payment) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(payments))

	for _, payment := range payments {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   PaymentsWithoutTransactionsCheckName,
			EntityType:  "payment",
			EntityID:    payment.ID,
			Description: fmt.Sprintf("payment %s has no matching transaction", payment.ID),
			Severity:    models.CRITICAL,
		})
	}

	return anomalies
}
