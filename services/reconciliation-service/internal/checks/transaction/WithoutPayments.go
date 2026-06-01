package transaction

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func BuildWithoutPaymentsAnomalies(transactions []domain.TransactionWithoutPayment) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(transactions))

	for _, transaction := range transactions {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.TransactionsWithoutPaymentsCheckName,
			EntityType:  "transaction",
			EntityID:    transaction.ID,
			Description: fmt.Sprintf("transaction points to missing payment_id=%s type=%s amount=%d currency=%s status=%s", transaction.PaymentID, transaction.Type, transaction.Amount, transaction.Currency, transaction.Status),
			Severity:    models.CRITICAL,
		})
	}

	return anomalies
}
