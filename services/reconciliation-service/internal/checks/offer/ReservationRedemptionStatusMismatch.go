package offer

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func BuildReservationRedemptionStatusMismatchAnomalies(mismatches []domain.OfferReservationRedemptionMismatch) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(mismatches))

	for _, mismatch := range mismatches {
		redemptionStatus := "NONE"
		if mismatch.RedemptionStatus.Valid {
			redemptionStatus = mismatch.RedemptionStatus.String
		}

		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.OfferReservationRedemptionStatusMismatchCheckName,
			EntityType:  "offer_reservation",
			EntityID:    mismatch.ReservationID,
			Description: fmt.Sprintf("reservation status=%s does not agree with redemption status=%s offer_id=%s", mismatch.ReservationStatus, redemptionStatus, mismatch.OfferID),
			Severity:    models.CRITICAL,
		})
	}

	return anomalies
}
