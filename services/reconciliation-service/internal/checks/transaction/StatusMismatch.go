package transaction

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func BuildStatusMismatchAnomalies(mismatches []domain.TransactionStatusMismatch) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(mismatches))

	for _, mismatch := range mismatches {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.TransactionsStatusMismatchCheckName,
			EntityType:  "transaction",
			EntityID:    mismatch.ID,
			Description: fmt.Sprintf("transaction status differs from payment payment_id=%s type=%s transaction_status=%s payment_status=%s", mismatch.PaymentID, mismatch.Type, mismatch.TransactionStatus, mismatch.PaymentStatus),
			Severity:    models.CRITICAL,
		})
	}

	return anomalies
}
