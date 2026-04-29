package service

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"flowpay/payment-service/internal/domain"
	"flowpay/payment-service/internal/dto"
	flowpayPaymentErrors "flowpay/payment-service/internal/errors"
)

func TestCreatePaymentReturnsTypedErrorForIdempotencyMismatch(t *testing.T) {
	repo := &stubPaymentRepository{
		existingPayment: domain.Payment{
			PaymentID:      "pay_existing",
			UserID:         "user-001",
			Amount:         1000,
			Currency:       "USD",
			Status:         "CREATED",
			IdempotencyKey: "idem-001",
		},
		createErr: fmt.Errorf("pq: duplicate key value violates unique constraint payments_idempotency_key_key"),
	}
	svc := NewPaymentService(repo)

	_, err := svc.CreatePayment(context.Background(), dto.PaymentRequestDTO{
		UserID:         "user-001",
		Amount:         20.00,
		Currency:       "USD",
		IdempotencyKey: "idem-001",
	})

	if !errors.Is(err, flowpayPaymentErrors.ErrIdempotencyMismatch) {
		t.Fatalf("expected ErrIdempotencyMismatch, got %v", err)
	}
	if repo.createCalls != 1 {
		t.Fatalf("expected one create call, got %d", repo.createCalls)
	}
	if repo.getByKeyCalls != 1 {
		t.Fatalf("expected one get-by-key call, got %d", repo.getByKeyCalls)
	}
}

func TestCreatePaymentReturnsExistingPaymentForMatchingDuplicate(t *testing.T) {
	repo := &stubPaymentRepository{
		existingPayment: domain.Payment{
			PaymentID:      "pay_existing",
			UserID:         "user-001",
			Amount:         1000,
			Currency:       "USD",
			Status:         "CREATED",
			IdempotencyKey: "idem-001",
		},
		createErr: fmt.Errorf("pq: duplicate key value violates unique constraint payments_idempotency_key_key"),
	}
	svc := NewPaymentService(repo)

	resp, err := svc.CreatePayment(context.Background(), dto.PaymentRequestDTO{
		UserID:         "user-001",
		Amount:         10.00,
		Currency:       "USD",
		IdempotencyKey: "idem-001",
	})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp.PaymentID != "pay_existing" {
		t.Fatalf("expected existing payment id, got %q", resp.PaymentID)
	}
	if repo.createCalls != 1 {
		t.Fatalf("expected one create call, got %d", repo.createCalls)
	}
	if repo.getByKeyCalls != 1 {
		t.Fatalf("expected one get-by-key call, got %d", repo.getByKeyCalls)
	}
}

type stubPaymentRepository struct {
	existingPayment domain.Payment
	createErr       error
	getByKeyErr     error
	createCalls     int
	getByKeyCalls   int
}

func (r *stubPaymentRepository) CreatePayment(_ context.Context, _ domain.Payment) error {
	r.createCalls++
	return r.createErr
}

func (r *stubPaymentRepository) GetPaymentByIdempotencyKey(_ context.Context, _ string) (domain.Payment, error) {
	r.getByKeyCalls++
	return r.existingPayment, r.getByKeyErr
}
