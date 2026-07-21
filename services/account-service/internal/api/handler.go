package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"flowpay/account-service/internal/constants"
	"flowpay/account-service/internal/dto"
	accountErrors "flowpay/account-service/internal/errors"
	"flowpay/account-service/internal/service"
	"flowpay/pkg/observability/logger"
	"flowpay/pkg/observability/metrics"
	responseTypes "flowpay/pkg/types"
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
	_ = json.NewEncoder(w).Encode(responseTypes.APIResponse{
		Success: false,
		Code:    status,
		Message: message,
		Data:    nil,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(responseTypes.APIResponse{
		Success: true,
		Code:    status,
		Message: "success",
		Data:    v,
	})
}

func accountErrorResponse(err error) (string, int) {
	switch {
	case errors.Is(err, accountErrors.ErrAccountNotFound),
		errors.Is(err, accountErrors.ErrCompanyNotFound),
		errors.Is(err, accountErrors.ErrUserNotFound):
		return err.Error(), http.StatusNotFound
	case errors.Is(err, accountErrors.ErrAccountNameRequired),
		errors.Is(err, accountErrors.ErrPaymentHandleRequired),
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
		errors.Is(err, accountErrors.ErrCompanyNotFound),
		errors.Is(err, accountErrors.ErrUserNotFound):
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

	ctx, cancel := context.WithTimeout(r.Context(), 16500*time.Millisecond)
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

	ctx, cancel := context.WithTimeout(r.Context(), 16500*time.Millisecond)
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

func (h *Handler) HandleGetDefaultAccounts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		logger.LogEvent(r.Context(), "WARN", constants.ServiceName, "default_accounts_get_rejected", logger.Fields{
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
	statusCode := http.StatusOK
	var serviceErr error

	defer func() {
		outcome := accountOutcome(statusCode, serviceErr)
		metrics.DefaultAccountsGetRequestsTotal.WithLabelValues(constants.ServiceName, outcome).Inc()
		metrics.DefaultAccountsGetRequestDuration.WithLabelValues(constants.ServiceName, outcome).Observe(time.Since(start).Seconds())
	}()

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "default_accounts_get_started", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"error_type":  accountErrors.ErrorTypeNone,
	})

	ctx, cancel := context.WithTimeout(r.Context(), 16500*time.Millisecond)
	defer cancel()

	resp, err := h.accountService.GetDefaultAccounts(ctx)
	if err != nil {
		statusCode = http.StatusInternalServerError
		serviceErr = err
		logger.LogEvent(r.Context(), "ERROR", constants.ServiceName, "default_accounts_get_failed", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": statusCode,
			"outcome":     "internal_error",
			"error":       err.Error(),
			"duration_ms": time.Since(start).Milliseconds(),
		})
		writeJSONError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "default_accounts_get_completed", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"http_status": http.StatusOK,
		"outcome":     "success",
		"count":       len(resp.Accounts),
		"duration_ms": time.Since(start).Milliseconds(),
	})
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) HandleGetDefaultUsers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		logger.LogEvent(r.Context(), "WARN", constants.ServiceName, "default_users_get_rejected", logger.Fields{
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
	statusCode := http.StatusOK
	var serviceErr error

	defer func() {
		outcome := accountOutcome(statusCode, serviceErr)
		metrics.DefaultUsersGetRequestsTotal.WithLabelValues(constants.ServiceName, outcome).Inc()
		metrics.DefaultUsersGetRequestDuration.WithLabelValues(constants.ServiceName, outcome).Observe(time.Since(start).Seconds())
	}()

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "default_users_get_started", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"error_type":  accountErrors.ErrorTypeNone,
	})

	ctx, cancel := context.WithTimeout(r.Context(), 16500*time.Millisecond)
	defer cancel()

	resp, err := h.accountService.GetDefaultUsers(ctx)
	if err != nil {
		statusCode = http.StatusInternalServerError
		serviceErr = err
		logger.LogEvent(r.Context(), "ERROR", constants.ServiceName, "default_users_get_failed", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": statusCode,
			"outcome":     "internal_error",
			"error":       err.Error(),
			"duration_ms": time.Since(start).Milliseconds(),
		})
		writeJSONError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "default_users_get_completed", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"http_status": http.StatusOK,
		"outcome":     "success",
		"count":       len(resp.Users),
		"duration_ms": time.Since(start).Milliseconds(),
	})
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) HandleGetDefaultCompanies(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		logger.LogEvent(r.Context(), "WARN", constants.ServiceName, "default_companies_get_rejected", logger.Fields{
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
	statusCode := http.StatusOK
	var serviceErr error

	defer func() {
		outcome := accountOutcome(statusCode, serviceErr)
		metrics.DefaultCompaniesGetRequestsTotal.WithLabelValues(constants.ServiceName, outcome).Inc()
		metrics.DefaultCompaniesGetRequestDuration.WithLabelValues(constants.ServiceName, outcome).Observe(time.Since(start).Seconds())
	}()

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "default_companies_get_started", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"error_type":  accountErrors.ErrorTypeNone,
	})

	ctx, cancel := context.WithTimeout(r.Context(), 16500*time.Millisecond)
	defer cancel()

	resp, err := h.accountService.GetDefaultCompanies(ctx)
	if err != nil {
		statusCode = http.StatusInternalServerError
		serviceErr = err
		logger.LogEvent(r.Context(), "ERROR", constants.ServiceName, "default_companies_get_failed", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": statusCode,
			"outcome":     "internal_error",
			"error":       err.Error(),
			"duration_ms": time.Since(start).Milliseconds(),
		})
		writeJSONError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "default_companies_get_completed", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"http_status": http.StatusOK,
		"outcome":     "success",
		"count":       len(resp.Companies),
		"duration_ms": time.Since(start).Milliseconds(),
	})
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) HandleGetUserInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		logger.LogEvent(r.Context(), "WARN", constants.ServiceName, "user_info_get_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": http.StatusMethodNotAllowed,
			"outcome":     "method_not_allowed",
			"error_type":  accountErrors.ToAccountErrorType(accountErrors.ErrMethodNotAllowed),
		})
		writeJSONError(w, accountErrors.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
		return
	}

	callerAccountID := r.Header.Get("X-User-Id")
	if callerAccountID == "" {
		writeJSONError(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	start := time.Now()
	statusCode := http.StatusOK
	var serviceErr error

	defer func() {
		outcome := accountOutcome(statusCode, serviceErr)
		metrics.UserInfoGetRequestDuration.WithLabelValues(constants.ServiceName, outcome).Observe(time.Since(start).Seconds())
	}()

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "user_info_get_started", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"error_type":  accountErrors.ErrorTypeNone,
	})

	ctx, cancel := context.WithTimeout(r.Context(), 16500*time.Millisecond)
	defer cancel()

	resp, err := h.accountService.GetUserInfo(ctx, callerAccountID)
	if err != nil {
		message, status := accountErrorResponse(err)
		statusCode = status
		serviceErr = err
		logger.LogEvent(r.Context(), "ERROR", constants.ServiceName, "user_info_get_failed", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": statusCode,
			"outcome":     accountOutcome(statusCode, err),
			"error_type":  accountErrors.ToAccountErrorType(err),
			"error":       err.Error(),
			"duration_ms": time.Since(start).Milliseconds(),
		})
		writeJSONError(w, message, status)
		return
	}

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "user_info_get_completed", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"http_status": http.StatusOK,
		"outcome":     "success",
		"account_id":  callerAccountID,
		"duration_ms": time.Since(start).Milliseconds(),
	})
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) HandleGetDefaultList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		logger.LogEvent(r.Context(), "WARN", constants.ServiceName, "default_list_get_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": http.StatusMethodNotAllowed,
			"outcome":     "method_not_allowed",
			"error_type":  accountErrors.ToAccountErrorType(accountErrors.ErrMethodNotAllowed),
		})
		writeJSONError(w, accountErrors.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
		return
	}

	callerAccountID := r.Header.Get("X-User-Id")
	if callerAccountID == "" {
		writeJSONError(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	start := time.Now()
	statusCode := http.StatusOK
	var serviceErr error

	defer func() {
		outcome := accountOutcome(statusCode, serviceErr)
		metrics.DefaultListGetRequestDuration.WithLabelValues(constants.ServiceName, outcome).Observe(time.Since(start).Seconds())
	}()

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "default_list_get_started", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"error_type":  accountErrors.ErrorTypeNone,
	})

	var req dto.DefaultListRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		statusCode = http.StatusBadRequest
		serviceErr = accountErrors.ErrInvalidRequestBody
		writeJSONError(w, accountErrors.ErrInvalidRequestBody.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 16500*time.Millisecond)
	defer cancel()

	resp, err := h.accountService.GetDefaultList(ctx, callerAccountID, req.Type)
	if err != nil {
		message, status := accountErrorResponse(err)
		statusCode = status
		serviceErr = err
		logger.LogEvent(r.Context(), "ERROR", constants.ServiceName, "default_list_get_failed", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": statusCode,
			"outcome":     accountOutcome(statusCode, err),
			"error_type":  accountErrors.ToAccountErrorType(err),
			"error":       err.Error(),
			"duration_ms": time.Since(start).Milliseconds(),
		})
		writeJSONError(w, message, status)
		return
	}

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "default_list_get_completed", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"http_status": http.StatusOK,
		"outcome":     "success",
		"list_type":   req.Type,
		"duration_ms": time.Since(start).Milliseconds(),
	})
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) HandleListUserAccounts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		logger.LogEvent(r.Context(), "WARN", constants.ServiceName, "accounts_list_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": http.StatusMethodNotAllowed,
			"outcome":     "method_not_allowed",
			"error_type":  accountErrors.ToAccountErrorType(accountErrors.ErrMethodNotAllowed),
		})
		writeJSONError(w, accountErrors.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
		return
	}

	callerAccountID := r.Header.Get("X-User-Id")
	if callerAccountID == "" {
		writeJSONError(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	start := time.Now()
	statusCode := http.StatusOK
	var serviceErr error

	defer func() {
		outcome := accountOutcome(statusCode, serviceErr)
		metrics.AccountsListRequestDuration.WithLabelValues(constants.ServiceName, outcome).Observe(time.Since(start).Seconds())
	}()

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "accounts_list_started", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"error_type":  accountErrors.ErrorTypeNone,
	})

	var req dto.ListAccountsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		statusCode = http.StatusBadRequest
		serviceErr = accountErrors.ErrInvalidRequestBody
		writeJSONError(w, accountErrors.ErrInvalidRequestBody.Error(), http.StatusBadRequest)
		return
	}

	if req.PageSize == 0 {
		req.PageSize = 50
	}

	ctx, cancel := context.WithTimeout(r.Context(), 16500*time.Millisecond)
	defer cancel()

	resp, err := h.accountService.ListUserAccounts(ctx, callerAccountID, req.Search, req.Page, req.PageSize)
	if err != nil {
		message, status := accountErrorResponse(err)
		statusCode = status
		serviceErr = err
		logger.LogEvent(r.Context(), "ERROR", constants.ServiceName, "accounts_list_failed", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": statusCode,
			"outcome":     accountOutcome(statusCode, err),
			"error":       err.Error(),
			"duration_ms": time.Since(start).Milliseconds(),
		})
		writeJSONError(w, message, status)
		return
	}

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "accounts_list_completed", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"http_status": http.StatusOK,
		"outcome":     "success",
		"count":       len(resp.Accounts),
		"total":       resp.Total,
		"duration_ms": time.Since(start).Milliseconds(),
	})
	writeJSON(w, http.StatusOK, resp)
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

func (h *Handler) HandleGetBalance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, accountErrors.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
		return
	}

	accountID := strings.TrimSpace(r.PathValue("id"))
	if accountID == "" {
		writeJSONError(w, accountErrors.ErrAccountNotFound.Error(), http.StatusNotFound)
		return
	}

	start := time.Now()
	statusCode := http.StatusOK
	var serviceErr error

	defer func() {
		outcome := accountOutcome(statusCode, serviceErr)
		metrics.BalanceGetRequestDuration.WithLabelValues(constants.ServiceName, outcome).Observe(time.Since(start).Seconds())
	}()

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "balance_get_started", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"account_id":  accountID,
		"error_type":  accountErrors.ErrorTypeNone,
	})

	ctx, cancel := context.WithTimeout(r.Context(), 16500*time.Millisecond)
	defer cancel()

	resp, err := h.accountService.GetBalance(ctx, accountID)
	if err != nil {
		message, status := accountErrorResponse(err)
		statusCode = status
		serviceErr = err
		logger.LogEvent(r.Context(), "ERROR", constants.ServiceName, "balance_get_failed", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": status,
			"outcome":     accountOutcome(status, err),
			"error_type":  accountErrors.ToAccountErrorType(err),
			"error":       err.Error(),
			"account_id":  accountID,
			"duration_ms": time.Since(start).Milliseconds(),
		})
		writeJSONError(w, message, status)
		return
	}

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "balance_get_completed", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"http_status": http.StatusOK,
		"outcome":     "success",
		"account_id":  accountID,
		"duration_ms": time.Since(start).Milliseconds(),
	})
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) HandleGetUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, accountErrors.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
		return
	}

	userID := strings.TrimSpace(r.PathValue("id"))
	if userID == "" {
		writeJSONError(w, accountErrors.ErrUserNotFound.Error(), http.StatusNotFound)
		return
	}

	start := time.Now()
	statusCode := http.StatusOK
	var serviceErr error

	defer func() {
		outcome := accountOutcome(statusCode, serviceErr)
		metrics.UserGetRequestDuration.WithLabelValues(constants.ServiceName, outcome).Observe(time.Since(start).Seconds())
	}()

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "user_get_started", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"user_id":     userID,
		"error_type":  accountErrors.ErrorTypeNone,
	})

	ctx, cancel := context.WithTimeout(r.Context(), 16500*time.Millisecond)
	defer cancel()

	resp, err := h.accountService.GetUserByID(ctx, userID)
	if err != nil {
		message, status := accountErrorResponse(err)
		statusCode = status
		serviceErr = err
		logger.LogEvent(r.Context(), "ERROR", constants.ServiceName, "user_get_failed", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": status,
			"outcome":     accountOutcome(status, err),
			"error_type":  accountErrors.ToAccountErrorType(err),
			"error":       err.Error(),
			"user_id":     userID,
			"duration_ms": time.Since(start).Milliseconds(),
		})
		writeJSONError(w, message, status)
		return
	}

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "user_get_completed", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"http_status": http.StatusOK,
		"outcome":     "success",
		"user_id":     userID,
		"duration_ms": time.Since(start).Milliseconds(),
	})
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) HandleGetTransactions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, accountErrors.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
		return
	}

	accountID := strings.TrimSpace(r.PathValue("accountID"))
	if accountID == "" {
		writeJSONError(w, accountErrors.ErrAccountNotFound.Error(), http.StatusNotFound)
		return
	}

	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	size, _ := strconv.Atoi(r.URL.Query().Get("size"))
	if size <= 0 {
		size = 50
	}

	start := time.Now()
	statusCode := http.StatusOK
	var serviceErr error

	defer func() {
		outcome := accountOutcome(statusCode, serviceErr)
		metrics.TransactionsListRequestDuration.WithLabelValues(constants.ServiceName, outcome).Observe(time.Since(start).Seconds())
	}()

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "transactions_list_started", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"account_id":  accountID,
		"error_type":  accountErrors.ErrorTypeNone,
	})

	ctx, cancel := context.WithTimeout(r.Context(), 16500*time.Millisecond)
	defer cancel()

	resp, err := h.accountService.GetTransactions(ctx, accountID, page, size)
	if err != nil {
		message, status := accountErrorResponse(err)
		statusCode = status
		serviceErr = err
		logger.LogEvent(r.Context(), "ERROR", constants.ServiceName, "transactions_list_failed", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": status,
			"outcome":     accountOutcome(status, err),
			"error_type":  accountErrors.ToAccountErrorType(err),
			"error":       err.Error(),
			"account_id":  accountID,
			"duration_ms": time.Since(start).Milliseconds(),
		})
		writeJSONError(w, message, status)
		return
	}

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "transactions_list_completed", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"http_status": http.StatusOK,
		"outcome":     "success",
		"account_id":  accountID,
		"count":       len(resp.Transactions),
		"total":       resp.Total,
		"duration_ms": time.Since(start).Milliseconds(),
	})
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) HandleGetTransactionsByPaymentID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, accountErrors.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
		return
	}

	paymentID := strings.TrimSpace(r.PathValue("paymentID"))
	if paymentID == "" {
		writeJSONError(w, "payment_id is required", http.StatusBadRequest)
		return
	}

	start := time.Now()
	statusCode := http.StatusOK
	var serviceErr error

	defer func() {
		outcome := accountOutcome(statusCode, serviceErr)
		metrics.TransactionsListRequestDuration.WithLabelValues(constants.ServiceName, outcome).Observe(time.Since(start).Seconds())
	}()

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "payment_transactions_list_started", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"payment_id":  paymentID,
		"error_type":  accountErrors.ErrorTypeNone,
	})

	ctx, cancel := context.WithTimeout(r.Context(), 16500*time.Millisecond)
	defer cancel()

	resp, err := h.accountService.GetTransactionsByPaymentID(ctx, paymentID)
	if err != nil {
		message, status := accountErrorResponse(err)
		statusCode = status
		serviceErr = err
		logger.LogEvent(r.Context(), "ERROR", constants.ServiceName, "payment_transactions_list_failed", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": status,
			"outcome":     accountOutcome(status, err),
			"error_type":  accountErrors.ToAccountErrorType(err),
			"error":       err.Error(),
			"payment_id":  paymentID,
			"duration_ms": time.Since(start).Milliseconds(),
		})
		writeJSONError(w, message, status)
		return
	}

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "payment_transactions_list_completed", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"http_status": http.StatusOK,
		"outcome":     "success",
		"payment_id":  paymentID,
		"count":       len(resp.Transactions),
		"duration_ms": time.Since(start).Milliseconds(),
	})
	writeJSON(w, http.StatusOK, resp)
}
