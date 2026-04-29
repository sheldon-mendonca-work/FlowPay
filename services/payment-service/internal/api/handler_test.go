package api

import (
	"context"
	"errors"
	"net/http"
	"testing"

	flowpayPaymentErrors "flowpay/payment-service/internal/errors"
)

func TestPaymentErrorResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		err        error
		wantBody   string
		wantStatus int
	}{
		{
			name:       "idempotency mismatch returns conflict",
			err:        flowpayPaymentErrors.ErrIdempotencyMismatch,
			wantBody:   "idempotency key reused with different payload",
			wantStatus: http.StatusConflict,
		},
		{
			name:       "wrapped idempotency mismatch returns conflict",
			err:        errors.Join(errors.New("service error"), flowpayPaymentErrors.ErrIdempotencyMismatch),
			wantBody:   "idempotency key reused with different payload",
			wantStatus: http.StatusConflict,
		},
		{
			name:       "deadline exceeded returns gateway timeout",
			err:        context.DeadlineExceeded,
			wantBody:   "payment request timed out",
			wantStatus: http.StatusGatewayTimeout,
		},
		{
			name:       "unknown error returns internal server error",
			err:        errors.New("postgres unavailable"),
			wantBody:   "failed to create payment",
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotBody, gotStatus := paymentErrorResponse(tt.err)

			if gotBody != tt.wantBody {
				t.Fatalf("expected body %q, got %q", tt.wantBody, gotBody)
			}
			if gotStatus != tt.wantStatus {
				t.Fatalf("expected status %d, got %d", tt.wantStatus, gotStatus)
			}
		})
	}
}
