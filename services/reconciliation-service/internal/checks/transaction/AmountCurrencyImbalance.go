package transaction

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func BuildAmountCurrencyImbalanceAnomalies(imbalances []domain.TransactionAmountCurrencyImbalance) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(imbalances))

	for _, imbalance := range imbalances {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.TransactionsAmountCurrencyImbalanceCheckName,
			EntityType:  "payment",
			EntityID:    imbalance.PaymentID,
			Description: fmt.Sprintf("debit/credit mismatch debit_transaction_id=%s credit_transaction_id=%s debit=%d %s credit=%d %s", imbalance.DebitTransactionID, imbalance.CreditTransactionID, imbalance.DebitAmount, imbalance.DebitCurrency, imbalance.CreditAmount, imbalance.CreditCurrency),
			Severity:    models.CRITICAL,
		})
	}

	return anomalies
}
