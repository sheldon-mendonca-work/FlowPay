package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	paymentServiceConstants "flowpay/payment-service/internal/constants"
	"flowpay/payment-service/internal/dto"
	flowpayPaymentErrors "flowpay/payment-service/internal/errors"
	"flowpay/payment-service/internal/service"
	"flowpay/pkg/observability/logger"
	"flowpay/pkg/observability/metrics"
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
	case errors.Is(err, flowpayPaymentErrors.ErrInsufficientBalance):
		return flowpayPaymentErrors.ErrInsufficientBalance.Error(), http.StatusBadRequest
	case errors.Is(err, flowpayPaymentErrors.ErrPaymentNotFound):
		return flowpayPaymentErrors.ErrPaymentNotFound.Error(), http.StatusNotFound
	case errors.Is(err, flowpayPaymentErrors.ErrIdempotencyMismatch):
		return flowpayPaymentErrors.ErrIdempotencyMismatch.Error(), http.StatusConflict
	case errors.Is(err, flowpayPaymentErrors.ErrIdempotencyInProgress):
		return flowpayPaymentErrors.ErrIdempotencyInProgress.Error(), http.StatusConflict
	case errors.Is(err, context.DeadlineExceeded):
		return flowpayPaymentErrors.ErrPaymentRequestTimedOut.Error(), http.StatusGatewayTimeout
	case errors.Is(err, context.Canceled):
		return flowpayPaymentErrors.ErrPaymentRequestCanceled.Error(), http.StatusRequestTimeout
	default:
		return flowpayPaymentErrors.ErrCreatePaymentFailed.Error(), http.StatusInternalServerError
	}
}

func paymentOutcome(status int, err error) string {
	switch {
	case err == nil && status == http.StatusAccepted:
		return "success"
	case err == nil && status == http.StatusOK:
		return "success"
	case errors.Is(err, flowpayPaymentErrors.ErrPaymentNotFound):
		return "payment_not_found"
	case errors.Is(err, flowpayPaymentErrors.ErrIdempotencyMismatch):
		return "idempotency_mismatch"
	case errors.Is(err, flowpayPaymentErrors.ErrIdempotencyInProgress):
		return "idempotency_in_progress"
	case errors.Is(err, flowpayPaymentErrors.ErrInsufficientBalance):
		return "insufficient_balance"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case status == http.StatusBadRequest:
		return "validation_error"
	case status == http.StatusMethodNotAllowed:
		return "method_not_allowed"
	default:
		return "internal_error"
	}
}

func paymentErrorType(err error) string {
	return flowpayPaymentErrors.ToPaymentErrorType(err)
}

func validatePaymentRequest(req dto.PaymentRequestDTO, reqIdempotencyKey string) error {
	if strings.TrimSpace(req.SenderID) == "" {
		return flowpayPaymentErrors.ErrSenderIDRequired
	}
	if strings.TrimSpace(req.ReceiverID) == "" {
		return flowpayPaymentErrors.ErrReceiverIDRequired
	}
	if strings.TrimSpace(req.ReceiverID) == strings.TrimSpace(req.SenderID) {
		return flowpayPaymentErrors.ErrSenderReceiverIDMatching
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

func (h *Handler) HandlePaymentPostMethod(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	reqIdempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	statusCode := http.StatusAccepted
	traceID := strings.TrimSpace(r.Header.Get("Trace-Id"))
	requestID := strings.TrimSpace(r.Header.Get("Request-Id"))

	var req dto.PaymentRequestDTO
	var serviceErr error

	defer func() {
		outcome := paymentOutcome(statusCode, serviceErr)
		metrics.PaymentRequestsTotal.WithLabelValues(paymentServiceConstants.ServiceName, outcome).Inc()
		metrics.PaymentRequestDuration.WithLabelValues(paymentServiceConstants.ServiceName, outcome).Observe(time.Since(start).Seconds())
	}()

	logger.LogEvent(r.Context(), "INFO", paymentServiceConstants.ServiceName, "payment_request_started", logger.Fields{
		"http_method":     r.Method,
		"http_path":       r.URL.Path,
		"idempotency_key": reqIdempotencyKey,
		"error_type":      flowpayPaymentErrors.ErrorTypeNone,
	})

	// Decode the Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		statusCode = http.StatusBadRequest
		logger.LogEvent(r.Context(), "WARN", paymentServiceConstants.ServiceName, "payment_request_rejected", logger.Fields{
			"http_method":     r.Method,
			"http_path":       r.URL.Path,
			"http_status":     statusCode,
			"outcome":         "invalid_json",
			"idempotency_key": reqIdempotencyKey,
			"error_type":      paymentErrorType(flowpayPaymentErrors.ErrInvalidRequestBody),
			"error":           err.Error(),
		})
		WriteJSONError(w, flowpayPaymentErrors.ErrInvalidRequestBody.Error(), http.StatusBadRequest)
		return
	}

	logger.LogEvent(r.Context(), "INFO", paymentServiceConstants.ServiceName, "payment_request_received", logger.Fields{
		"http_method":     r.Method,
		"http_path":       r.URL.Path,
		"idempotency_key": reqIdempotencyKey,
		"error_type":      flowpayPaymentErrors.ErrorTypeNone,
		"sender_id":       req.SenderID,
		"receiver_id":     req.ReceiverID,
		"amount":          req.Amount,
		"currency":        req.Currency,
	})

	// Validate the request content
	if validationError := validatePaymentRequest(req, reqIdempotencyKey); validationError != nil {
		statusCode = http.StatusBadRequest
		logger.LogEvent(r.Context(), "WARN", paymentServiceConstants.ServiceName, "payment_request_rejected", logger.Fields{
			"http_method":     r.Method,
			"http_path":       r.URL.Path,
			"http_status":     statusCode,
			"outcome":         "validation_error",
			"idempotency_key": reqIdempotencyKey,
			"error_type":      paymentErrorType(validationError),
			"sender_id":       req.SenderID,
			"receiver_id":     req.ReceiverID,
			"amount":          req.Amount,
			"currency":        req.Currency,
			"error":           validationError.Error(),
		})
		WriteJSONError(w, validationError.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Call the service method for handling this
	resp, err := h.paymentService.CreatePayment(ctx, req, reqIdempotencyKey, traceID, requestID)
	if err != nil {
		message, status := paymentErrorResponse(err)
		statusCode = status
		serviceErr = err
		logger.LogEvent(r.Context(), "ERROR", paymentServiceConstants.ServiceName, "payment_request_failed", logger.Fields{
			"http_method":      r.Method,
			"http_path":        r.URL.Path,
			"http_status":      status,
			"http_status_text": http.StatusText(status),
			"outcome":          paymentOutcome(status, err),
			"error_type":       paymentErrorType(err),
			"idempotency_key":  reqIdempotencyKey,
			"sender_id":        req.SenderID,
			"receiver_id":      req.ReceiverID,
			"amount":           req.Amount,
			"currency":         req.Currency,
			"error":            err.Error(),
			"duration_ms":      time.Since(start).Milliseconds(),
		})
		WriteJSONError(w, message, status)
		return
	}

	metrics.SuccessCount.WithLabelValues(paymentServiceConstants.ServiceName, r.URL.Path, r.Method, strconv.Itoa(http.StatusAccepted)).Inc()
	logger.LogEvent(r.Context(), "INFO", paymentServiceConstants.ServiceName, "payment_request_completed", logger.Fields{
		"http_method":      r.Method,
		"http_path":        r.URL.Path,
		"http_status":      http.StatusAccepted,
		"http_status_text": http.StatusText(http.StatusAccepted),
		"outcome":          "success",
		"error_type":       flowpayPaymentErrors.ErrorTypeNone,
		"payment_id":       resp.PaymentID,
		"idempotency_key":  reqIdempotencyKey,
		"sender_id":        req.SenderID,
		"receiver_id":      req.ReceiverID,
		"amount":           req.Amount,
		"currency":         req.Currency,
		"duration_ms":      time.Since(start).Milliseconds(),
	})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *Handler) HandlePayment(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		h.HandlePaymentPostMethod(w, r)
		return
	}

	statusCode := http.StatusAccepted
	reqIdempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))

	statusCode = http.StatusMethodNotAllowed
	logger.LogEvent(r.Context(), "WARN", paymentServiceConstants.ServiceName, "payment_request_rejected", logger.Fields{
		"http_method":     r.Method,
		"http_path":       r.URL.Path,
		"http_status":     statusCode,
		"outcome":         "method_not_allowed",
		"idempotency_key": reqIdempotencyKey,
		"error_type":      paymentErrorType(flowpayPaymentErrors.ErrMethodNotAllowed),
	})
	WriteJSONError(w, flowpayPaymentErrors.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
}

func (h *Handler) HandlePaymentByID(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		h.HandlePaymentGetByIDMethod(w, r)
		return
	}

	statusCode := http.StatusAccepted
	paymentID := r.PathValue("paymentID")

	statusCode = http.StatusMethodNotAllowed
	logger.LogEvent(r.Context(), "WARN", paymentServiceConstants.ServiceName, "payment_request_rejected", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"http_status": statusCode,
		"outcome":     "method_not_allowed",
		"payment_id":  paymentID,
		"error_type":  paymentErrorType(flowpayPaymentErrors.ErrMethodNotAllowed),
	})
	WriteJSONError(w, flowpayPaymentErrors.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
}

func (h *Handler) HandlePaymentGetByIDMethod(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	statusCode := http.StatusOK
	paymentID := strings.TrimSpace(r.PathValue("paymentID"))
	var serviceErr error

	defer func() {
		outcome := paymentOutcome(statusCode, serviceErr)
		metrics.PaymentRequestsTotal.WithLabelValues(paymentServiceConstants.ServiceName, outcome).Inc()
		metrics.PaymentRequestDuration.WithLabelValues(paymentServiceConstants.ServiceName, outcome).Observe(time.Since(start).Seconds())
	}()

	logger.LogEvent(r.Context(), "INFO", paymentServiceConstants.ServiceName, "payment_lookup_started", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"payment_id":  paymentID,
		"error_type":  flowpayPaymentErrors.ErrorTypeNone,
	})

	if paymentID == "" {
		statusCode = http.StatusBadRequest
		serviceErr = flowpayPaymentErrors.ErrInvalidRequestBody
		logger.LogEvent(r.Context(), "WARN", paymentServiceConstants.ServiceName, "payment_lookup_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": statusCode,
			"outcome":     "validation_error",
			"payment_id":  paymentID,
			"error_type":  paymentErrorType(serviceErr),
			"error":       "payment_id path parameter is required",
		})
		WriteJSONError(w, "payment_id is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp, err := h.paymentService.GetPaymentByID(ctx, paymentID)
	if err != nil {
		message, status := paymentErrorResponse(err)
		statusCode = status
		serviceErr = err
		logger.LogEvent(r.Context(), "ERROR", paymentServiceConstants.ServiceName, "payment_lookup_failed", logger.Fields{
			"http_method":      r.Method,
			"http_path":        r.URL.Path,
			"http_status":      status,
			"http_status_text": http.StatusText(status),
			"outcome":          paymentOutcome(status, err),
			"error_type":       paymentErrorType(err),
			"payment_id":       paymentID,
			"error":            err.Error(),
			"duration_ms":      time.Since(start).Milliseconds(),
		})
		WriteJSONError(w, message, status)
		return
	}

	metrics.SuccessCount.WithLabelValues(paymentServiceConstants.ServiceName, r.URL.Path, r.Method, strconv.Itoa(http.StatusOK)).Inc()
	logger.LogEvent(r.Context(), "INFO", paymentServiceConstants.ServiceName, "payment_lookup_completed", logger.Fields{
		"http_method":      r.Method,
		"http_path":        r.URL.Path,
		"http_status":      http.StatusOK,
		"http_status_text": http.StatusText(http.StatusOK),
		"outcome":          "success",
		"error_type":       flowpayPaymentErrors.ErrorTypeNone,
		"payment_id":       resp.PaymentID,
		"idempotency_key":  resp.IdempotencyKey,
		"sender_id":        resp.SenderID,
		"receiver_id":      resp.ReceiverID,
		"amount":           resp.Amount,
		"currency":         resp.Currency,
		"duration_ms":      time.Since(start).Milliseconds(),
	})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}
