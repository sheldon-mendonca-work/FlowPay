package offer

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func BuildRedeemedReservationsWithoutSuccessfulPaymentAnomalies(reservations []domain.RedeemedReservationWithoutSuccessfulPayment) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(reservations))

	for _, reservation := range reservations {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.OfferRedeemedReservationsWithoutSuccessfulPaymentCheckName,
			EntityType:  "offer_reservation",
			EntityID:    reservation.ReservationID,
			Description: fmt.Sprintf("reservation is REDEEMED offer_id=%s but linked payment_id=%s has status=%s (expected SUCCESS)", reservation.OfferID, reservation.PaymentID, reservation.PaymentStatus),
			Severity:    models.CRITICAL,
		})
	}

	return anomalies
}
