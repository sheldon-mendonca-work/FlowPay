package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"flowpay/api-gateway/internal/auth"
	"flowpay/pkg/observability/logger"
	"flowpay/pkg/observability/metrics"
	"flowpay/pkg/observability/tracing"
	responseTypes "flowpay/pkg/types"
)

type contextKey string

const accountIDKey contextKey = "account_id"
const userIDKey contextKey = "user_id"
const companyIDKey contextKey = "company_id"

func WriteError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(responseTypes.APIResponse{
		Success: false,
		Code:    status,
		Message: message,
		Data:    nil,
	})
}

func JWTAuth(secret, serviceName string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if !strings.HasPrefix(authHeader, "Bearer ") {
			metrics.GatewayUnauthorizedTotal.WithLabelValues(serviceName, "missing_token").Inc()
			logger.LogEvent(r.Context(), "WARN", serviceName, "gateway_request_unauthorized", logger.Fields{
				"http_method": r.Method,
				"http_path":   r.URL.Path,
				"reason":      "missing_token",
			})
			WriteError(w, http.StatusUnauthorized, "Authentication Token Not Found")
			return
		}

		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
		claims, err := auth.ParseJWT(tokenStr, secret)
		if err != nil {
			metrics.GatewayUnauthorizedTotal.WithLabelValues(serviceName, "invalid_token").Inc()
			logger.LogEvent(r.Context(), "WARN", serviceName, "gateway_request_unauthorized", logger.Fields{
				"http_method": r.Method,
				"http_path":   r.URL.Path,
				"reason":      "invalid_token",
			})
			WriteError(w, http.StatusUnauthorized, "Invalid Authentication Token")
			return
		}

		ctx := context.WithValue(r.Context(), userIDKey, claims.UserID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func RequestAndTrace(serviceName string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := strings.TrimSpace(r.Header.Get("X-Request-Id"))
		traceID := strings.TrimSpace(r.Header.Get("X-Trace-Id"))

		if len(requestID) == 0 {
			metrics.GatewayUnauthorizedTotal.WithLabelValues(serviceName, "missing_request_id").Inc()
			logger.LogEvent(r.Context(), "WARN", serviceName, "gateway_request_missing_request_id", logger.Fields{
				"http_method": r.Method,
				"http_path":   r.URL.Path,
				"reason":      "Request ID missing",
			})
			WriteError(w, http.StatusBadRequest, "Request ID missing")
			return
		}

		if len(traceID) == 0 {
			metrics.GatewayUnauthorizedTotal.WithLabelValues(serviceName, "missing_trace_id").Inc()
			logger.LogEvent(r.Context(), "WARN", serviceName, "gateway_request_missing_trace_id", logger.Fields{
				"http_method": r.Method,
				"http_path":   r.URL.Path,
				"reason":      "Trace ID missing",
			})
			WriteError(w, http.StatusBadRequest, "Trace ID missing")
			return
		}

		ctx := tracing.WithTraceAndRequestIDs(r.Context(), traceID, requestID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func GetAccountID(ctx context.Context) string {
	if v, ok := ctx.Value(accountIDKey).(string); ok {
		return v
	}
	return ""
}

func GetUserID(ctx context.Context) string {
	if v, ok := ctx.Value(userIDKey).(string); ok {
		return v
	}
	return ""
}

func GetCompanyID(ctx context.Context) string {
	if v, ok := ctx.Value(companyIDKey).(string); ok {
		return v
	}
	return ""
}
