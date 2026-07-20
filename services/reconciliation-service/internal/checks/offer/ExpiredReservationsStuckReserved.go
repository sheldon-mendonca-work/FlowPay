package offer

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func BuildExpiredReservationsStuckReservedAnomalies(reservations []domain.ExpiredOfferReservation) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(reservations))

	for _, reservation := range reservations {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.OfferExpiredReservationsStuckReservedCheckName,
			EntityType:  "offer_reservation",
			EntityID:    reservation.ID,
			Description: fmt.Sprintf("reservation still RESERVED past expires_at=%s offer_id=%s, likely missed by offer-expiry-worker", reservation.ExpiresAt.Format("2006-01-02T15:04:05Z"), reservation.OfferID),
			Severity:    models.WARN,
		})
	}

	return anomalies
}
