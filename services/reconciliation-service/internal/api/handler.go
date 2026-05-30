package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"flowpay/pkg/observability/logger"
	"flowpay/reconciliation-service/internal/constants"
	"flowpay/reconciliation-service/internal/dto"
	flowpayReconciliationErrors "flowpay/reconciliation-service/internal/errors"
	"flowpay/reconciliation-service/internal/service"
)

type Handler struct {
	reconciliationService *service.ReconciliationService
}

func NewHandler(reconciliationService *service.ReconciliationService) *Handler {
	return &Handler{reconciliationService: reconciliationService}
}

func WriteJSONError(w http.ResponseWriter, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": message,
	})
}

func reconciliationErrorResponse(err error) (string, int) {
	switch {
	case errors.Is(err, flowpayReconciliationErrors.ErrMethodNotAllowed):
		return flowpayReconciliationErrors.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed
	case errors.Is(err, context.DeadlineExceeded):
		return flowpayReconciliationErrors.ErrReconciliationTimedOut.Error(), http.StatusGatewayTimeout
	case errors.Is(err, context.Canceled):
		return flowpayReconciliationErrors.ErrReconciliationCanceled.Error(), http.StatusRequestTimeout
	default:
		return flowpayReconciliationErrors.ErrReconciliationCheckFailed.Error(), http.StatusInternalServerError
	}
}

func reconciliationOutcome(status int, err error) string {
	switch {
	case err == nil && status == http.StatusOK:
		return "success"
	case errors.Is(err, flowpayReconciliationErrors.ErrMethodNotAllowed):
		return "method_not_allowed"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "internal_error"
	}
}

func reconciliationErrorType(err error) string {
	return flowpayReconciliationErrors.ToReconciliationErrorType(err)
}

func buildPaymentReconciliationResponse(anomalies []dto.AnomalyResponseDTO, executionTimeMs int64) dto.ReconciliationResponseDTO {
	status := "PASS"
	if len(anomalies) > 0 {
		status = "FAIL"
	}

	return dto.ReconciliationResponseDTO{
		CheckName:       service.PaymentChecksName,
		Status:          status,
		AnomalyCount:    len(anomalies),
		ExecutionTimeMs: executionTimeMs,
		Anomalies:       anomalies,
	}
}

func (h *Handler) HandlePaymentChecks(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if r.Method != http.MethodGet {
		err := flowpayReconciliationErrors.ErrMethodNotAllowed
		message, status := reconciliationErrorResponse(err)
		logger.LogEvent(r.Context(), "WARN", constants.ServiceName, "reconciliation_payment_request_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": status,
			"outcome":     reconciliationOutcome(status, err),
			"error_type":  reconciliationErrorType(err),
		})
		WriteJSONError(w, message, status)

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	anomalies, err := h.reconciliationService.RunPaymentChecks(ctx)
	if err != nil {
		message, status := reconciliationErrorResponse(err)
		logger.LogEvent(r.Context(), "ERROR", constants.ServiceName, "payment_reconciliation_failed", logger.Fields{
			"http_method":      r.Method,
			"http_path":        r.URL.Path,
			"http_status":      status,
			"http_status_text": http.StatusText(status),
			"outcome":          reconciliationOutcome(status, err),
			"error_type":       reconciliationErrorType(err),
			"error":            err.Error(),
			"duration_ms":      time.Since(start).Milliseconds(),
		})
		WriteJSONError(w, message, status)
		return
	}

	response := buildPaymentReconciliationResponse(anomalies, time.Since(start).Milliseconds())

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "payment_reconciliation_completed", logger.Fields{
		"http_method":      r.Method,
		"http_path":        r.URL.Path,
		"http_status":      http.StatusOK,
		"http_status_text": http.StatusText(http.StatusOK),
		"outcome":          reconciliationOutcome(http.StatusOK, nil),
		"error_type":       flowpayReconciliationErrors.ErrorTypeNone,
		"check_name":       response.CheckName,
		"check_status":     response.Status,
		"anomaly_count":    response.AnomalyCount,
		"duration_ms":      response.ExecutionTimeMs,
	})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		WriteJSONError(w, flowpayReconciliationErrors.ErrReconciliationCheckFailed.Error(), http.StatusInternalServerError)
	}
}
