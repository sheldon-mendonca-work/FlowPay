package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	authConstants "flowpay/auth-service/internal/constants"
	"flowpay/auth-service/internal/dto"
	authErrors "flowpay/auth-service/internal/errors"
	"flowpay/auth-service/internal/service"
	"flowpay/pkg/observability/logger"
	"flowpay/pkg/observability/metrics"
	responseTypes "flowpay/pkg/types"
)

type Handler struct {
	authService *service.AuthService
}

func NewHandler(authService *service.AuthService) *Handler {
	return &Handler{authService: authService}
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

func authErrorResponse(err error) (string, int) {
	switch {
	case errors.Is(err, authErrors.ErrAuthenticationRequired):
		return authErrors.ErrAuthenticationRequired.Error(), http.StatusUnauthorized
	case errors.Is(err, authErrors.ErrInvalidCredentials):
		return authErrors.ErrInvalidCredentials.Error(), http.StatusUnauthorized
	case errors.Is(err, authErrors.ErrEmailAlreadyExists):
		return authErrors.ErrEmailAlreadyExists.Error(), http.StatusConflict
	case errors.Is(err, authErrors.ErrInvalidRefreshToken):
		return authErrors.ErrInvalidRefreshToken.Error(), http.StatusUnauthorized
	case errors.Is(err, authErrors.ErrExpiredRefreshToken):
		return authErrors.ErrExpiredRefreshToken.Error(), http.StatusUnauthorized
	case errors.Is(err, authErrors.ErrAccountCreationFailed):
		return authErrors.ErrAccountCreationFailed.Error(), http.StatusBadGateway
	case errors.Is(err, context.DeadlineExceeded):
		return "request timed out", http.StatusGatewayTimeout
	default:
		return "internal server error", http.StatusInternalServerError
	}
}

func authOutcome(status int, err error) string {
	switch {
	case err == nil:
		return "success"
	case errors.Is(err, authErrors.ErrAuthenticationRequired):
		return "authentication_required"
	case errors.Is(err, authErrors.ErrInvalidCredentials):
		return "invalid_credentials"
	case errors.Is(err, authErrors.ErrEmailAlreadyExists):
		return "email_conflict"
	case errors.Is(err, authErrors.ErrInvalidRefreshToken), errors.Is(err, authErrors.ErrExpiredRefreshToken):
		return "invalid_token"
	case errors.Is(err, authErrors.ErrAccountCreationFailed):
		return "account_creation_failed"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case status == http.StatusBadRequest:
		return "validation_error"
	case status == http.StatusMethodNotAllowed:
		return "method_not_allowed"
	default:
		return "internal_error"
	}
}

func validateRegisterRequest(req dto.RegisterRequest) error {
	if strings.TrimSpace(req.Email) == "" {
		return authErrors.ErrEmailRequired
	}
	if strings.TrimSpace(req.Password) == "" {
		return authErrors.ErrPasswordRequired
	}
	if len(req.Password) < 8 {
		return authErrors.ErrPasswordTooShort
	}
	if strings.TrimSpace(req.AccountName) == "" {
		return authErrors.ErrNameRequired
	}
	if strings.TrimSpace(req.PaymentHandle) == "" {
		return authErrors.ErrPaymentHandleRequired
	}
	if strings.TrimSpace(req.AccountType) == "" {
		return authErrors.ErrTypeRequired
	}
	if strings.TrimSpace(req.Currency) == "" {
		return authErrors.ErrCurrencyRequired
	}
	return nil
}

func validateLoginRequest(req dto.LoginRequest) error {
	if strings.TrimSpace(req.Email) == "" {
		return authErrors.ErrEmailRequired
	}
	if strings.TrimSpace(req.Password) == "" {
		return authErrors.ErrPasswordRequired
	}
	return nil
}

func (h *Handler) HandleAuthRegisterRoute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		logger.LogEvent(r.Context(), "WARN", authConstants.ServiceName, "auth_register_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": http.StatusMethodNotAllowed,
			"outcome":     "method_not_allowed",
			"error_type":  authErrors.ToAuthErrorType(authErrors.ErrMethodNotAllowed),
		})
		writeJSONError(w, authErrors.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
		return
	}

	start := time.Now()
	statusCode := http.StatusCreated
	var serviceErr error

	defer func() {
		outcome := authOutcome(statusCode, serviceErr)
		metrics.UserCreationRequestsTotal.WithLabelValues(authConstants.ServiceName, outcome).Inc()
		metrics.UserCreationRequestDuration.WithLabelValues(authConstants.ServiceName, outcome).Observe(time.Since(start).Seconds())
	}()

	logger.LogEvent(r.Context(), "INFO", authConstants.ServiceName, "auth_register_started", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"error_type":  authErrors.ErrorTypeNone,
	})

	var req dto.RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		statusCode = http.StatusBadRequest
		logger.LogEvent(r.Context(), "WARN", authConstants.ServiceName, "auth_register_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": statusCode,
			"outcome":     "invalid_json",
			"error_type":  authErrors.ToAuthErrorType(authErrors.ErrInvalidRequestBody),
			"error":       err.Error(),
		})
		writeJSONError(w, authErrors.ErrInvalidRequestBody.Error(), http.StatusBadRequest)
		return
	}

	if err := validateRegisterRequest(req); err != nil {
		statusCode = http.StatusBadRequest
		serviceErr = err
		logger.LogEvent(r.Context(), "WARN", authConstants.ServiceName, "auth_register_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": statusCode,
			"outcome":     "validation_error",
			"error_type":  authErrors.ToAuthErrorType(err),
			"error":       err.Error(),
		})
		writeJSONError(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	resp, err := h.authService.Register(ctx, req)
	if err != nil {
		message, status := authErrorResponse(err)
		statusCode = status
		serviceErr = err
		logger.LogEvent(r.Context(), "ERROR", authConstants.ServiceName, "auth_register_failed", logger.Fields{
			"http_method":      r.Method,
			"http_path":        r.URL.Path,
			"http_status":      status,
			"http_status_text": http.StatusText(status),
			"outcome":          authOutcome(status, err),
			"error_type":       authErrors.ToAuthErrorType(err),
			"error":            err.Error(),
			"duration_ms":      time.Since(start).Milliseconds(),
		})
		writeJSONError(w, message, status)
		return
	}

	logger.LogEvent(r.Context(), "INFO", authConstants.ServiceName, "auth_register_completed", logger.Fields{
		"http_method":      r.Method,
		"http_path":        r.URL.Path,
		"http_status":      http.StatusCreated,
		"http_status_text": http.StatusText(http.StatusCreated),
		"outcome":          "success",
		"error_type":       authErrors.ErrorTypeNone,
		"duration_ms":      time.Since(start).Milliseconds(),
	})

	writeJSON(w, http.StatusCreated, resp)
}

func (h *Handler) HandleAuthLoginRoute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		logger.LogEvent(r.Context(), "WARN", authConstants.ServiceName, "auth_login_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": http.StatusMethodNotAllowed,
			"outcome":     "method_not_allowed",
			"error_type":  authErrors.ToAuthErrorType(authErrors.ErrMethodNotAllowed),
		})
		writeJSONError(w, authErrors.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
		return
	}

	start := time.Now()
	statusCode := http.StatusOK
	var serviceErr error

	defer func() {
		outcome := authOutcome(statusCode, serviceErr)
		metrics.AuthLoginRequestsTotal.WithLabelValues(authConstants.ServiceName, outcome).Inc()
		metrics.AuthLoginRequestDuration.WithLabelValues(authConstants.ServiceName, outcome).Observe(time.Since(start).Seconds())
	}()

	logger.LogEvent(r.Context(), "INFO", authConstants.ServiceName, "auth_login_started", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"error_type":  authErrors.ErrorTypeNone,
	})

	var req dto.LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		statusCode = http.StatusBadRequest
		logger.LogEvent(r.Context(), "WARN", authConstants.ServiceName, "auth_login_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": statusCode,
			"outcome":     "invalid_json",
			"error_type":  authErrors.ToAuthErrorType(authErrors.ErrInvalidRequestBody),
			"error":       err.Error(),
		})
		writeJSONError(w, authErrors.ErrInvalidRequestBody.Error(), http.StatusBadRequest)
		return
	}

	if err := validateLoginRequest(req); err != nil {
		statusCode = http.StatusBadRequest
		serviceErr = err
		logger.LogEvent(r.Context(), "WARN", authConstants.ServiceName, "auth_login_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": statusCode,
			"outcome":     "validation_error",
			"error_type":  authErrors.ToAuthErrorType(err),
			"error":       err.Error(),
		})
		writeJSONError(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 16500*time.Millisecond)
	defer cancel()

	resp, err := h.authService.Login(ctx, req)
	if err != nil {
		message, status := authErrorResponse(err)
		statusCode = status
		serviceErr = err
		logger.LogEvent(r.Context(), "ERROR", authConstants.ServiceName, "auth_login_failed", logger.Fields{
			"http_method":      r.Method,
			"http_path":        r.URL.Path,
			"http_status":      status,
			"http_status_text": http.StatusText(status),
			"outcome":          authOutcome(status, err),
			"error_type":       authErrors.ToAuthErrorType(err),
			"error":            err.Error(),
			"duration_ms":      time.Since(start).Milliseconds(),
		})
		writeJSONError(w, message, status)
		return
	}

	logger.LogEvent(r.Context(), "INFO", authConstants.ServiceName, "auth_login_completed", logger.Fields{
		"http_method":      r.Method,
		"http_path":        r.URL.Path,
		"http_status":      http.StatusOK,
		"http_status_text": http.StatusText(http.StatusOK),
		"outcome":          "success",
		"error_type":       authErrors.ErrorTypeNone,
		"duration_ms":      time.Since(start).Milliseconds(),
	})

	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) HandleAuthRefreshRoute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		logger.LogEvent(r.Context(), "WARN", authConstants.ServiceName, "auth_refresh_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": http.StatusMethodNotAllowed,
			"outcome":     "method_not_allowed",
			"error_type":  authErrors.ToAuthErrorType(authErrors.ErrMethodNotAllowed),
		})
		writeJSONError(w, authErrors.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
		return
	}

	start := time.Now()
	statusCode := http.StatusOK
	var serviceErr error

	defer func() {
		outcome := authOutcome(statusCode, serviceErr)
		metrics.AuthRefreshRequestsTotal.WithLabelValues(authConstants.ServiceName, outcome).Inc()
		metrics.AuthRefreshRequestDuration.WithLabelValues(authConstants.ServiceName, outcome).Observe(time.Since(start).Seconds())
	}()

	logger.LogEvent(r.Context(), "INFO", authConstants.ServiceName, "auth_refresh_started", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"error_type":  authErrors.ErrorTypeNone,
	})

	var req dto.RefreshRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		statusCode = http.StatusBadRequest
		logger.LogEvent(r.Context(), "WARN", authConstants.ServiceName, "auth_refresh_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": statusCode,
			"outcome":     "invalid_json",
			"error_type":  authErrors.ToAuthErrorType(authErrors.ErrInvalidRequestBody),
			"error":       err.Error(),
		})
		writeJSONError(w, authErrors.ErrInvalidRequestBody.Error(), http.StatusBadRequest)
		return
	}

	if strings.TrimSpace(req.RefreshToken) == "" {
		statusCode = http.StatusBadRequest
		serviceErr = authErrors.ErrRefreshTokenRequired
		logger.LogEvent(r.Context(), "WARN", authConstants.ServiceName, "auth_refresh_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": statusCode,
			"outcome":     "validation_error",
			"error_type":  authErrors.ToAuthErrorType(authErrors.ErrRefreshTokenRequired),
			"error":       authErrors.ErrRefreshTokenRequired.Error(),
		})
		writeJSONError(w, authErrors.ErrRefreshTokenRequired.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 16500*time.Millisecond)
	defer cancel()

	resp, err := h.authService.Refresh(ctx, req.RefreshToken)
	if err != nil {
		message, status := authErrorResponse(err)
		statusCode = status
		serviceErr = err
		logger.LogEvent(r.Context(), "ERROR", authConstants.ServiceName, "auth_refresh_failed", logger.Fields{
			"http_method":      r.Method,
			"http_path":        r.URL.Path,
			"http_status":      status,
			"http_status_text": http.StatusText(status),
			"outcome":          authOutcome(status, err),
			"error_type":       authErrors.ToAuthErrorType(err),
			"error":            err.Error(),
			"duration_ms":      time.Since(start).Milliseconds(),
		})
		writeJSONError(w, message, status)
		return
	}

	logger.LogEvent(r.Context(), "INFO", authConstants.ServiceName, "auth_refresh_completed", logger.Fields{
		"http_method":      r.Method,
		"http_path":        r.URL.Path,
		"http_status":      http.StatusOK,
		"http_status_text": http.StatusText(http.StatusOK),
		"outcome":          "success",
		"error_type":       authErrors.ErrorTypeNone,
		"duration_ms":      time.Since(start).Milliseconds(),
	})

	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) HandleAuthDefaultLoginRoute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		logger.LogEvent(r.Context(), "WARN", authConstants.ServiceName, "auth_default_login_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": http.StatusMethodNotAllowed,
			"outcome":     "method_not_allowed",
			"error_type":  authErrors.ToAuthErrorType(authErrors.ErrMethodNotAllowed),
		})
		writeJSONError(w, authErrors.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
		return
	}

	start := time.Now()
	statusCode := http.StatusOK
	var serviceErr error

	defer func() {
		outcome := authOutcome(statusCode, serviceErr)
		metrics.AuthDefaultLoginRequestsTotal.WithLabelValues(authConstants.ServiceName, outcome).Inc()
		metrics.AuthDefaultLoginRequestDuration.WithLabelValues(authConstants.ServiceName, outcome).Observe(time.Since(start).Seconds())
	}()

	accountID := strings.TrimSpace(r.URL.Query().Get("account_id"))
	if accountID == "" {
		statusCode = http.StatusBadRequest
		logger.LogEvent(r.Context(), "WARN", authConstants.ServiceName, "auth_default_login_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": statusCode,
			"outcome":     "validation_error",
			"error_type":  authErrors.ErrorTypeValidationError,
			"error":       "account_id is required",
		})
		writeJSONError(w, "account_id is required", http.StatusBadRequest)
		return
	}

	logger.LogEvent(r.Context(), "INFO", authConstants.ServiceName, "auth_default_login_started", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"account_id":  accountID,
		"error_type":  authErrors.ErrorTypeNone,
	})

	ctx, cancel := context.WithTimeout(r.Context(), 16500*time.Millisecond)
	defer cancel()

	resp, err := h.authService.DefaultLogin(ctx, accountID)
	if err != nil {
		message, status := authErrorResponse(err)
		statusCode = status
		serviceErr = err
		logger.LogEvent(r.Context(), "ERROR", authConstants.ServiceName, "auth_default_login_failed", logger.Fields{
			"http_method":      r.Method,
			"http_path":        r.URL.Path,
			"http_status":      status,
			"http_status_text": http.StatusText(status),
			"account_id":       accountID,
			"outcome":          authOutcome(status, err),
			"error_type":       authErrors.ToAuthErrorType(err),
			"error":            err.Error(),
			"duration_ms":      time.Since(start).Milliseconds(),
		})
		writeJSONError(w, message, status)
		return
	}

	logger.LogEvent(r.Context(), "INFO", authConstants.ServiceName, "auth_default_login_completed", logger.Fields{
		"http_method":      r.Method,
		"http_path":        r.URL.Path,
		"http_status":      http.StatusOK,
		"http_status_text": http.StatusText(http.StatusOK),
		"account_id":       accountID,
		"outcome":          "success",
		"error_type":       authErrors.ErrorTypeNone,
		"duration_ms":      time.Since(start).Milliseconds(),
	})

	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) HandleAuthDefaultLoginAccountRoute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		logger.LogEvent(r.Context(), "WARN", authConstants.ServiceName, "auth_default_login_account_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": http.StatusMethodNotAllowed,
			"outcome":     "method_not_allowed",
			"error_type":  authErrors.ToAuthErrorType(authErrors.ErrMethodNotAllowed),
		})
		writeJSONError(w, authErrors.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
		return
	}

	var req dto.DefaultLoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.LogEvent(r.Context(), "WARN", authConstants.ServiceName, "auth_default_login_account_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": http.StatusBadRequest,
			"outcome":     "invalid_json",
			"error_type":  authErrors.ToAuthErrorType(authErrors.ErrInvalidRequestBody),
			"error":       err.Error(),
		})
		writeJSONError(w, authErrors.ErrInvalidRequestBody.Error(), http.StatusBadRequest)
		return
	}

	accountID := strings.TrimSpace(req.AccountID)
	accountType := strings.TrimSpace(req.AccountType)

	if accountID == "" {
		logger.LogEvent(r.Context(), "WARN", authConstants.ServiceName, "auth_default_login_account_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": http.StatusBadRequest,
			"outcome":     "validation_error",
			"error_type":  authErrors.ErrorTypeValidationError,
			"error":       authErrors.ErrAccountIDRequired.Error(),
		})
		writeJSONError(w, authErrors.ErrAccountIDRequired.Error(), http.StatusBadRequest)
		return
	}
	if accountType == "" {
		logger.LogEvent(r.Context(), "WARN", authConstants.ServiceName, "auth_default_login_account_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": http.StatusBadRequest,
			"outcome":     "validation_error",
			"error_type":  authErrors.ErrorTypeValidationError,
			"error":       authErrors.ErrAccountTypeRequired.Error(),
		})
		writeJSONError(w, authErrors.ErrAccountTypeRequired.Error(), http.StatusBadRequest)
		return
	}

	start := time.Now()
	statusCode := http.StatusOK
	var serviceErr error

	defer func() {
		outcome := authOutcome(statusCode, serviceErr)
		metrics.AuthDefaultLoginAccountRequestsTotal.WithLabelValues(authConstants.ServiceName, outcome, accountType).Inc()
		metrics.AuthDefaultLoginAccountRequestDuration.WithLabelValues(authConstants.ServiceName, outcome, accountType).Observe(time.Since(start).Seconds())
	}()

	logger.LogEvent(r.Context(), "INFO", authConstants.ServiceName, "auth_default_login_account_started", logger.Fields{
		"http_method":  r.Method,
		"http_path":    r.URL.Path,
		"account_id":   accountID,
		"account_type": accountType,
		"error_type":   authErrors.ErrorTypeNone,
	})

	ctx, cancel := context.WithTimeout(r.Context(), 16500*time.Millisecond)
	defer cancel()

	resp, err := h.authService.DefaultLoginAccount(ctx, accountID, accountType)
	if err != nil {
		message, status := authErrorResponse(err)
		statusCode = status
		serviceErr = err
		logger.LogEvent(r.Context(), "ERROR", authConstants.ServiceName, "auth_default_login_account_failed", logger.Fields{
			"http_method":      r.Method,
			"http_path":        r.URL.Path,
			"http_status":      status,
			"http_status_text": http.StatusText(status),
			"account_id":       accountID,
			"account_type":     accountType,
			"outcome":          authOutcome(status, err),
			"error_type":       authErrors.ToAuthErrorType(err),
			"error":            err.Error(),
			"duration_ms":      time.Since(start).Milliseconds(),
		})
		writeJSONError(w, message, status)
		return
	}

	logger.LogEvent(r.Context(), "INFO", authConstants.ServiceName, "auth_default_login_account_completed", logger.Fields{
		"http_method":      r.Method,
		"http_path":        r.URL.Path,
		"http_status":      http.StatusOK,
		"http_status_text": http.StatusText(http.StatusOK),
		"account_id":       accountID,
		"account_type":     accountType,
		"outcome":          "success",
		"error_type":       authErrors.ErrorTypeNone,
		"duration_ms":      time.Since(start).Milliseconds(),
	})

	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) HandleAuthDefaultLoginUserRoute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		logger.LogEvent(r.Context(), "WARN", authConstants.ServiceName, "auth_default_login_user_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": http.StatusMethodNotAllowed,
			"outcome":     "method_not_allowed",
			"error_type":  authErrors.ToAuthErrorType(authErrors.ErrMethodNotAllowed),
		})
		writeJSONError(w, authErrors.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
		return
	}

	var req dto.DefaultLoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.LogEvent(r.Context(), "WARN", authConstants.ServiceName, "auth_default_login_user_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": http.StatusBadRequest,
			"outcome":     "invalid_json",
			"error_type":  authErrors.ToAuthErrorType(authErrors.ErrInvalidRequestBody),
			"error":       err.Error(),
		})
		writeJSONError(w, authErrors.ErrInvalidRequestBody.Error(), http.StatusBadRequest)
		return
	}

	accountID := strings.TrimSpace(req.AccountID)
	accountType := strings.TrimSpace(req.AccountType)

	if accountID == "" {
		logger.LogEvent(r.Context(), "WARN", authConstants.ServiceName, "auth_default_login_user_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": http.StatusBadRequest,
			"outcome":     "validation_error",
			"error_type":  authErrors.ErrorTypeValidationError,
			"error":       authErrors.ErrAccountIDRequired.Error(),
		})
		writeJSONError(w, authErrors.ErrAccountIDRequired.Error(), http.StatusBadRequest)
		return
	}
	if accountType == "" {
		logger.LogEvent(r.Context(), "WARN", authConstants.ServiceName, "auth_default_login_user_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": http.StatusBadRequest,
			"outcome":     "validation_error",
			"error_type":  authErrors.ErrorTypeValidationError,
			"error":       authErrors.ErrAccountTypeRequired.Error(),
		})
		writeJSONError(w, authErrors.ErrAccountTypeRequired.Error(), http.StatusBadRequest)
		return
	}

	start := time.Now()
	statusCode := http.StatusOK
	var serviceErr error

	defer func() {
		outcome := authOutcome(statusCode, serviceErr)
		metrics.AuthDefaultLoginUserRequestsTotal.WithLabelValues(authConstants.ServiceName, outcome, accountType).Inc()
		metrics.AuthDefaultLoginUserRequestDuration.WithLabelValues(authConstants.ServiceName, outcome, accountType).Observe(time.Since(start).Seconds())
	}()

	logger.LogEvent(r.Context(), "INFO", authConstants.ServiceName, "auth_default_login_user_started", logger.Fields{
		"http_method":  r.Method,
		"http_path":    r.URL.Path,
		"account_id":   accountID,
		"account_type": accountType,
		"error_type":   authErrors.ErrorTypeNone,
	})

	ctx, cancel := context.WithTimeout(r.Context(), 16500*time.Millisecond)
	defer cancel()

	resp, err := h.authService.DefaultLoginUser(ctx, accountID, accountType, start)
	if err != nil {
		message, status := authErrorResponse(err)
		statusCode = status
		serviceErr = err
		logger.LogEvent(r.Context(), "ERROR", authConstants.ServiceName, "auth_default_login_user_failed", logger.Fields{
			"http_method":      r.Method,
			"http_path":        r.URL.Path,
			"http_status":      status,
			"http_status_text": http.StatusText(status),
			"account_id":       accountID,
			"account_type":     accountType,
			"outcome":          authOutcome(status, err),
			"error_type":       authErrors.ToAuthErrorType(err),
			"error":            err.Error(),
			"duration_ms":      time.Since(start).Milliseconds(),
		})
		writeJSONError(w, message, status)
		return
	}

	logger.LogEvent(r.Context(), "INFO", authConstants.ServiceName, "auth_default_login_user_completed", logger.Fields{
		"http_method":      r.Method,
		"http_path":        r.URL.Path,
		"http_status":      http.StatusOK,
		"http_status_text": http.StatusText(http.StatusOK),
		"account_id":       accountID,
		"account_type":     accountType,
		"outcome":          "success",
		"error_type":       authErrors.ErrorTypeNone,
		"duration_ms":      time.Since(start).Milliseconds(),
	})

	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) HandleAuthLogoutRoute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		logger.LogEvent(r.Context(), "WARN", authConstants.ServiceName, "auth_logout_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": http.StatusMethodNotAllowed,
			"outcome":     "method_not_allowed",
			"error_type":  authErrors.ToAuthErrorType(authErrors.ErrMethodNotAllowed),
		})
		writeJSONError(w, authErrors.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
		return
	}

	start := time.Now()
	statusCode := http.StatusNoContent
	var serviceErr error

	defer func() {
		outcome := authOutcome(statusCode, serviceErr)
		metrics.AuthLogoutRequestsTotal.WithLabelValues(authConstants.ServiceName, outcome).Inc()
		metrics.AuthLogoutRequestDuration.WithLabelValues(authConstants.ServiceName, outcome).Observe(time.Since(start).Seconds())
	}()

	logger.LogEvent(r.Context(), "INFO", authConstants.ServiceName, "auth_logout_started", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"error_type":  authErrors.ErrorTypeNone,
	})

	var req dto.LogoutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		statusCode = http.StatusBadRequest
		logger.LogEvent(r.Context(), "WARN", authConstants.ServiceName, "auth_logout_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": statusCode,
			"outcome":     "invalid_json",
			"error_type":  authErrors.ToAuthErrorType(authErrors.ErrInvalidRequestBody),
			"error":       err.Error(),
		})
		writeJSONError(w, authErrors.ErrInvalidRequestBody.Error(), http.StatusBadRequest)
		return
	}

	if strings.TrimSpace(req.RefreshToken) == "" {
		statusCode = http.StatusBadRequest
		serviceErr = authErrors.ErrRefreshTokenRequired
		logger.LogEvent(r.Context(), "WARN", authConstants.ServiceName, "auth_logout_rejected", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": statusCode,
			"outcome":     "validation_error",
			"error_type":  authErrors.ToAuthErrorType(authErrors.ErrRefreshTokenRequired),
			"error":       authErrors.ErrRefreshTokenRequired.Error(),
		})
		writeJSONError(w, authErrors.ErrRefreshTokenRequired.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 16500*time.Millisecond)
	defer cancel()

	if err := h.authService.Logout(ctx, req.RefreshToken); err != nil {
		statusCode = http.StatusInternalServerError
		serviceErr = err
		logger.LogEvent(r.Context(), "ERROR", authConstants.ServiceName, "auth_logout_failed", logger.Fields{
			"http_method":      r.Method,
			"http_path":        r.URL.Path,
			"http_status":      statusCode,
			"http_status_text": http.StatusText(statusCode),
			"outcome":          "internal_error",
			"error_type":       authErrors.ToAuthErrorType(err),
			"error":            err.Error(),
			"duration_ms":      time.Since(start).Milliseconds(),
		})
		writeJSONError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	logger.LogEvent(r.Context(), "INFO", authConstants.ServiceName, "auth_logout_completed", logger.Fields{
		"http_method":      r.Method,
		"http_path":        r.URL.Path,
		"http_status":      http.StatusNoContent,
		"http_status_text": http.StatusText(http.StatusNoContent),
		"outcome":          "success",
		"error_type":       authErrors.ErrorTypeNone,
		"duration_ms":      time.Since(start).Milliseconds(),
	})

	w.WriteHeader(http.StatusNoContent)
}
