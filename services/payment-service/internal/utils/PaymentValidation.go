package paymentServiceUtils

import "flowpay/payment-service/internal/domain"

func CheckPaymentPayloadIsMatching(payment1 domain.Payment, payment2 domain.Payment) bool {
	return payment1.UserID == payment2.UserID && payment1.Amount == payment2.Amount &&
		payment1.Currency == payment2.Currency
}
