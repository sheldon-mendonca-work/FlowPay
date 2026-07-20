package offer

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func BuildReservationsWithoutPaymentAnomalies(reservations []domain.OfferReservationWithoutPayment) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(reservations))

	for _, reservation := range reservations {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.OfferReservationsWithoutPaymentCheckName,
			EntityType:  "offer_reservation",
			EntityID:    reservation.ID,
			Description: fmt.Sprintf("reservation references payment_id=%s that does not exist offer_id=%s status=%s", reservation.PaymentID, reservation.OfferID, reservation.Status),
			Severity:    models.CRITICAL,
		})
	}

	return anomalies
}
