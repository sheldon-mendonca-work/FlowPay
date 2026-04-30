package paymentServiceUtils

import "flowpay/payment-service/internal/domain"

func CheckPaymentPayloadIsMatching(payment1 domain.Payment, payment2 domain.Payment) bool {
	return payment1.SenderID == payment2.SenderID &&
		payment1.ReceiverID == payment2.ReceiverID &&
		payment1.Amount == payment2.Amount &&
		payment1.Currency == payment2.Currency
}
