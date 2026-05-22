package handler

import (
	"context"
	"flowpay/payment-executor/internal/domain"
	"flowpay/payment-executor/internal/service"
)

type PaymentHandler struct {
	paymentExecutorService *service.PaymentExecutorService
}

func NewPaymentHandler(paymentExecutorService *service.PaymentExecutorService) *PaymentHandler {
	return &PaymentHandler{paymentExecutorService: paymentExecutorService}
}

func (p *PaymentHandler) ExecutePayment(ctx context.Context, event domain.PaymentInitiatedEvent) error {
	_, paymentError := p.paymentExecutorService.ExecutePayment(ctx, event)
	return paymentError
}
