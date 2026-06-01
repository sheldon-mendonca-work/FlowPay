package transaction

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func BuildMissingLedgerSideAnomalies(ledgers []domain.TransactionLedgerCounts) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(ledgers))

	for _, ledger := range ledgers {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.TransactionsMissingLedgerSideCheckName,
			EntityType:  "payment",
			EntityID:    ledger.PaymentID,
			Description: fmt.Sprintf("payment ledger has transaction_count=%d debit_count=%d credit_count=%d", ledger.TransactionCount, ledger.DebitCount, ledger.CreditCount),
			Severity:    models.CRITICAL,
		})
	}

	return anomalies
}
