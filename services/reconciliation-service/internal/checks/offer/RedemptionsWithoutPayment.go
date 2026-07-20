package offer

import (
	"fmt"

	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/domain"
	"flowpay/reconciliation-service/internal/models"
)

func BuildRedemptionsWithoutPaymentAnomalies(redemptions []domain.OfferRedemptionWithoutPayment) []models.Anomaly {
	anomalies := make([]models.Anomaly, 0, len(redemptions))

	for _, redemption := range redemptions {
		anomalies = append(anomalies, models.Anomaly{
			CheckName:   constants.OfferRedemptionsWithoutPaymentCheckName,
			EntityType:  "offer_redemption",
			EntityID:    redemption.ID,
			Description: fmt.Sprintf("redemption references payment_id=%s that does not exist offer_id=%s reservation_id=%s status=%s", redemption.PaymentID, redemption.OfferID, redemption.ReservationID, redemption.Status),
			Severity:    models.CRITICAL,
		})
	}

	return anomalies
}
