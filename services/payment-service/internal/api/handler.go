package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"flowpay/payment-service/internal/constants"
	"flowpay/payment-service/internal/dto"
	"flowpay/payment-service/internal/service"
	"flowpay/pkg/observability/logger"
)

type Handler struct {
	paymentService *service.PaymentService
}

func NewHandler(paymentService *service.PaymentService) *Handler {
	return &Handler{paymentService: paymentService}
}

func WriteJSONError(w http.ResponseWriter, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": message,
	})
}

func validatePaymentRequest(req dto.PaymentRequestDTO) string {
	if strings.TrimSpace(req.UserID) == "" {
		return "user_id is required"
	}
	if req.Amount <= 0 {
		return "amount must be greater than 0"
	}
	if strings.TrimSpace(req.Currency) == "" {
		return "currency is required"
	}
	if strings.TrimSpace(req.IdempotencyKey) == "" {
		return "idempotency_key is required"
	}

	return ""
}

func (h *Handler) HandlePayment(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		WriteJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req dto.PaymentRequestDTO
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteJSONError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if validationError := validatePaymentRequest(req); validationError != "" {
		WriteJSONError(w, validationError, http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp, err := h.paymentService.CreatePayment(ctx, req)
	if err != nil {
		logger.LogWithRequest(r.Context(), constants.ServiceName, "create payment failed: %v", err)
		WriteJSONError(w, "failed to create payment", http.StatusInternalServerError)
		return
	}

	logger.LogWithRequest(r.Context(), constants.ServiceName, "payment accepted for user_id=%s idempotency_key=%s", req.UserID, req.IdempotencyKey)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(resp)
}
