package offer

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func BuildRedeemedCountDriftAnomalies(drifts []domain.OfferRedeemedCountDrift) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(drifts))

	for _, drift := range drifts {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.OfferRedeemedCountDriftCheckName,
			EntityType:  "offer",
			EntityID:    drift.OfferID,
			Description: fmt.Sprintf("offer_code=%s redeemed_count=%d does not match successful redemption count=%d", drift.OfferCode, drift.RedeemedCount, drift.ActualRedeemedCount),
			Severity:    models.CRITICAL,
		})
	}

	return anomalies
}
