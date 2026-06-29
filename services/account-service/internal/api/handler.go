package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"flowpay/account-service/internal/constants"
	"flowpay/account-service/internal/dto"
	accountErrors "flowpay/account-service/internal/errors"
	"flowpay/account-service/internal/service"
	"flowpay/pkg/observability/logger"
	"flowpay/pkg/observability/metrics"
)

type Handler struct {
	accountService *service.AccountService
}

func NewHandler(accountService *service.AccountService) *Handler {
	return &Handler{accountService: accountService}
}

func writeJSONError(w http.ResponseWriter, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func accountErrorResponse(err error) (string, int) {
	switch {
	case errors.Is(err, accountErrors.ErrAccountNotFound),
		errors.Is(err, accountErrors.ErrCompanyNotFound):
		return err.Error(), http.StatusNotFound
	case errors.Is(err, accountErrors.ErrAccountNameRequired),
		errors.Is(err, accountErrors.ErrCurrencyRequired),
		errors.Is(err, accountErrors.ErrInvalidAccountType),
		errors.Is(err, accountErrors.ErrCompanyNameRequired),
		errors.Is(err, accountErrors.ErrBusinessNameRequired),
		errors.Is(err, accountErrors.ErrRoleRequired),
		errors.Is(err, accountErrors.ErrInvalidRole):
		return err.Error(), http.StatusBadRequest
	case errors.Is(err, context.DeadlineExceeded):
		return "request timed out", http.StatusGatewayTimeout
	case errors.Is(err, context.Canceled):
		return "request canceled", http.StatusRequestTimeout
	default:
		return "internal server error", http.StatusInternalServerError
	}
}

func accountOutcome(status int, err error) string {
	switch {
	case err == nil:
		return "success"
	case errors.Is(err, accountErrors.ErrAccountNotFound),
		errors.Is(err, accountErrors.ErrCompanyNotFound):
		return "not_found"
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

func (h *Handler) HandleCreateAccount(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		logger.LogEvent(r.Context(), "WARN", constants.ServiceName, "account_create_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": http.StatusMethodNotAllowed,
			"outcome":     "method_not_allowed",
			"error_type":  accountErrors.ToAccountErrorType(accountErrors.ErrMethodNotAllowed),
		})
		writeJSONError(w, accountErrors.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
		return
	}

	start := time.Now()
	statusCode := http.StatusCreated
	var serviceErr error

	defer func() {
		outcome := accountOutcome(statusCode, serviceErr)
		metrics.AccountCreateRequestsTotal.WithLabelValues(constants.ServiceName, outcome).Inc()
		metrics.AccountCreateRequestDuration.WithLabelValues(constants.ServiceName, outcome).Observe(time.Since(start).Seconds())
	}()

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "account_create_started", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"error_type":  accountErrors.ErrorTypeNone,
	})

	var req dto.CreateAccountRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		statusCode = http.StatusBadRequest
		serviceErr = accountErrors.ErrInvalidRequestBody
		logger.LogEvent(r.Context(), "WARN", constants.ServiceName, "account_create_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": statusCode,
			"outcome":     "invalid_json",
			"error_type":  accountErrors.ToAccountErrorType(serviceErr),
			"error":       err.Error(),
		})
		writeJSONError(w, accountErrors.ErrInvalidRequestBody.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp, err := h.accountService.CreateAccount(ctx, req)
	if err != nil {
		message, status := accountErrorResponse(err)
		statusCode = status
		serviceErr = err
		logger.LogEvent(r.Context(), "ERROR", constants.ServiceName, "account_create_failed", logger.Fields{
			"http_method":      r.Method,
			"http_path":        r.URL.Path,
			"http_status":      status,
			"http_status_text": http.StatusText(status),
			"outcome":          accountOutcome(status, err),
			"error_type":       accountErrors.ToAccountErrorType(err),
			"error":            err.Error(),
			"duration_ms":      time.Since(start).Milliseconds(),
		})
		writeJSONError(w, message, status)
		return
	}

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "account_create_completed", logger.Fields{
		"http_method":      r.Method,
		"http_path":        r.URL.Path,
		"http_status":      http.StatusCreated,
		"http_status_text": http.StatusText(http.StatusCreated),
		"outcome":          "success",
		"error_type":       accountErrors.ErrorTypeNone,
		"account_id":       resp.AccountID,
		"duration_ms":      time.Since(start).Milliseconds(),
	})
	writeJSON(w, http.StatusCreated, resp)
}

func (h *Handler) HandleCreateCompany(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		logger.LogEvent(r.Context(), "WARN", constants.ServiceName, "company_create_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": http.StatusMethodNotAllowed,
			"outcome":     "method_not_allowed",
			"error_type":  accountErrors.ToAccountErrorType(accountErrors.ErrMethodNotAllowed),
		})
		writeJSONError(w, accountErrors.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
		return
	}

	start := time.Now()
	statusCode := http.StatusCreated
	var serviceErr error

	defer func() {
		outcome := accountOutcome(statusCode, serviceErr)
		metrics.CompanyCreateRequestsTotal.WithLabelValues(constants.ServiceName, outcome).Inc()
		metrics.CompanyCreateRequestDuration.WithLabelValues(constants.ServiceName, outcome).Observe(time.Since(start).Seconds())
	}()

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "company_create_started", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"error_type":  accountErrors.ErrorTypeNone,
	})

	var req dto.CreateCompanyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		statusCode = http.StatusBadRequest
		serviceErr = accountErrors.ErrInvalidRequestBody
		logger.LogEvent(r.Context(), "WARN", constants.ServiceName, "company_create_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": statusCode,
			"outcome":     "invalid_json",
			"error_type":  accountErrors.ToAccountErrorType(serviceErr),
			"error":       err.Error(),
		})
		writeJSONError(w, accountErrors.ErrInvalidRequestBody.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp, err := h.accountService.CreateCompany(ctx, req)
	if err != nil {
		message, status := accountErrorResponse(err)
		statusCode = status
		serviceErr = err
		logger.LogEvent(r.Context(), "ERROR", constants.ServiceName, "company_create_failed", logger.Fields{
			"http_method":      r.Method,
			"http_path":        r.URL.Path,
			"http_status":      status,
			"http_status_text": http.StatusText(status),
			"outcome":          accountOutcome(status, err),
			"error_type":       accountErrors.ToAccountErrorType(err),
			"error":            err.Error(),
			"duration_ms":      time.Since(start).Milliseconds(),
		})
		writeJSONError(w, message, status)
		return
	}

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "company_create_completed", logger.Fields{
		"http_method":      r.Method,
		"http_path":        r.URL.Path,
		"http_status":      http.StatusCreated,
		"http_status_text": http.StatusText(http.StatusCreated),
		"outcome":          "success",
		"error_type":       accountErrors.ErrorTypeNone,
		"company_id":       resp.CompanyID,
		"duration_ms":      time.Since(start).Milliseconds(),
	})
	writeJSON(w, http.StatusCreated, resp)
}

func (h *Handler) HandleCreateUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		logger.LogEvent(r.Context(), "WARN", constants.ServiceName, "user_create_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": http.StatusMethodNotAllowed,
			"outcome":     "method_not_allowed",
			"error_type":  accountErrors.ToAccountErrorType(accountErrors.ErrMethodNotAllowed),
		})
		writeJSONError(w, accountErrors.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
		return
	}

	start := time.Now()
	statusCode := http.StatusCreated
	var serviceErr error

	defer func() {
		outcome := accountOutcome(statusCode, serviceErr)
		metrics.UserCreateRequestsTotal.WithLabelValues(constants.ServiceName, outcome).Inc()
		metrics.UserCreateRequestDuration.WithLabelValues(constants.ServiceName, outcome).Observe(time.Since(start).Seconds())
	}()

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "user_create_started", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"error_type":  accountErrors.ErrorTypeNone,
	})

	var req dto.CreateUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		statusCode = http.StatusBadRequest
		serviceErr = accountErrors.ErrInvalidRequestBody
		logger.LogEvent(r.Context(), "WARN", constants.ServiceName, "user_create_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": statusCode,
			"outcome":     "invalid_json",
			"error_type":  accountErrors.ToAccountErrorType(serviceErr),
			"error":       err.Error(),
		})
		writeJSONError(w, accountErrors.ErrInvalidRequestBody.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	resp, err := h.accountService.CreateUser(ctx, req)
	if err != nil {
		message, status := accountErrorResponse(err)
		statusCode = status
		serviceErr = err
		logger.LogEvent(r.Context(), "ERROR", constants.ServiceName, "user_create_failed", logger.Fields{
			"http_method":      r.Method,
			"http_path":        r.URL.Path,
			"http_status":      status,
			"http_status_text": http.StatusText(status),
			"outcome":          accountOutcome(status, err),
			"error_type":       accountErrors.ToAccountErrorType(err),
			"error":            err.Error(),
			"duration_ms":      time.Since(start).Milliseconds(),
		})
		writeJSONError(w, message, status)
		return
	}

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "user_create_completed", logger.Fields{
		"http_method":      r.Method,
		"http_path":        r.URL.Path,
		"http_status":      http.StatusCreated,
		"http_status_text": http.StatusText(http.StatusCreated),
		"outcome":          "success",
		"error_type":       accountErrors.ErrorTypeNone,
		"user_id":          resp.UserID,
		"account_id":       resp.AccountID,
		"duration_ms":      time.Since(start).Milliseconds(),
	})
	writeJSON(w, http.StatusCreated, resp)
}
