package offer

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func BuildSuccessfulPaymentsWithUnredeemedReservationAnomalies(payments []domain.SuccessfulPaymentWithUnredeemedReservation) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(payments))

	for _, payment := range payments {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.OfferSuccessfulPaymentsWithUnredeemedReservationCheckName,
			EntityType:  "payment",
			EntityID:    payment.PaymentID,
			Description: fmt.Sprintf("payment is SUCCESS but linked reservation_id=%s offer_id=%s has status=%s (expected REDEEMED)", payment.ReservationID, payment.OfferID, payment.ReservationStatus),
			Severity:    models.CRITICAL,
		})
	}

	return anomalies
}
