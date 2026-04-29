package service

import (
	"context"
	"crypto/rand"
	"fmt"
	"math"

	"flowpay/payment-service/internal/domain"
	"flowpay/payment-service/internal/dto"
	"flowpay/payment-service/internal/types"
	"flowpay/pkg/utils"
)

type PaymentRepository interface {
	CreatePayment(ctx context.Context, payment domain.Payment) error
	GetPaymentByIdempotencyKey(ctx context.Context, key string) (domain.Payment, error)
}

type PaymentService struct {
	repository PaymentRepository
}

func NewPaymentService(repository PaymentRepository) *PaymentService {
	return &PaymentService{
		repository: repository,
	}
}

func (s *PaymentService) CreatePayment(ctx context.Context, req dto.PaymentRequestDTO) (dto.PaymentResponseDTO, error) {
	paymentID, err := newPaymentID()
	if err != nil {
		return dto.PaymentResponseDTO{}, err
	}

	payment := domain.Payment{
		PaymentID:      paymentID,
		UserID:         req.UserID,
		Amount:         int64(math.Round(req.Amount * 100)),
		Currency:       req.Currency,
		Status:         string(types.CREATED),
		IdempotencyKey: req.IdempotencyKey,
	}

	err = s.repository.CreatePayment(ctx, payment)
	if err == nil {
		return toPaymentResponse(payment), nil
	}

	if utils.IsUniqueViolation(err) {
		existingPayment, getErr := s.repository.GetPaymentByIdempotencyKey(ctx, payment.IdempotencyKey)
		if getErr != nil {
			return dto.PaymentResponseDTO{}, getErr
		}
		return toPaymentResponse(existingPayment), nil
	}

	return dto.PaymentResponseDTO{}, err
}

func toPaymentResponse(payment domain.Payment) dto.PaymentResponseDTO {
	return dto.PaymentResponseDTO{
		PaymentID: payment.PaymentID,
		Status:    types.PaymentStatusEnum(payment.Status),
	}
}

func newPaymentID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate payment id: %w", err)
	}

	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}
