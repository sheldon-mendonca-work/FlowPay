package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"flowpay/payment-service/internal/constants"
	"flowpay/payment-service/internal/dto"
	flowpayPaymentErrors "flowpay/payment-service/internal/errors"
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

func paymentErrorResponse(err error) (string, int) {
	switch {
	case errors.Is(err, flowpayPaymentErrors.ErrIdempotencyMismatch):
		return flowpayPaymentErrors.ErrIdempotencyMismatch.Error(), http.StatusConflict
	case errors.Is(err, context.DeadlineExceeded):
		return flowpayPaymentErrors.ErrPaymentRequestTimedOut.Error(), http.StatusGatewayTimeout
	case errors.Is(err, context.Canceled):
		return flowpayPaymentErrors.ErrPaymentRequestCanceled.Error(), http.StatusRequestTimeout
	default:
		return flowpayPaymentErrors.ErrCreatePaymentFailed.Error(), http.StatusInternalServerError
	}
}

func validatePaymentRequest(req dto.PaymentRequestDTO, reqIdempotencyKey string) error {
	if strings.TrimSpace(req.SenderID) == "" {
		return flowpayPaymentErrors.ErrSenderIDRequired
	}
	if strings.TrimSpace(req.ReceiverID) == "" {
		return flowpayPaymentErrors.ErrSenderIDRequired
	}
	if strings.TrimSpace(req.ReceiverID) == strings.TrimSpace(req.SenderID) {
		return flowpayPaymentErrors.ErrSenderIDRequired
	}
	if req.Amount <= 0 {
		return flowpayPaymentErrors.ErrAmountMustBeGreaterThanZero
	}
	if strings.TrimSpace(req.Currency) == "" {
		return flowpayPaymentErrors.ErrCurrencyRequired
	}
	if strings.TrimSpace(reqIdempotencyKey) == "" {
		return flowpayPaymentErrors.ErrIdempotencyKeyRequired
	}

	return nil
}

func (h *Handler) HandlePayment(w http.ResponseWriter, r *http.Request) {
	//  Check Method is Correct
	if r.Method != http.MethodPost {
		WriteJSONError(w, flowpayPaymentErrors.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
		return
	}

	// Decode the Request
	var req dto.PaymentRequestDTO
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteJSONError(w, flowpayPaymentErrors.ErrInvalidRequestBody.Error(), http.StatusBadRequest)
		return
	}
	// Read the idempotency key
	reqIdempotencyKey := r.Header.Get("Idempotency-Key")

	// Validate the request content
	if validationError := validatePaymentRequest(req, reqIdempotencyKey); validationError != nil {
		WriteJSONError(w, validationError.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Call the service method for handling this
	resp, err := h.paymentService.CreatePayment(ctx, req, reqIdempotencyKey)
	if err != nil {
		message, status := paymentErrorResponse(err)
		logger.LogWithRequest(
			r.Context(),
			constants.ServiceName,
			"create payment failed status=%d sender_id=%s receiver_id=%s idempotency_key=%s error=%v",
			status,
			req.SenderID,
			req.ReceiverID,
			reqIdempotencyKey,
			err,
		)
		WriteJSONError(w, message, status)
		return
	}

	logger.LogWithRequest(r.Context(), constants.ServiceName, "payment accepted for id=%s sender_id=%s receiver_id=%s idempotency_key=%s", resp.PaymentID, req.SenderID, req.ReceiverID, reqIdempotencyKey)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(resp)
}
