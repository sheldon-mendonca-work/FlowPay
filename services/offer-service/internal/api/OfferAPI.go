package api

import (
	"context"
	"encoding/json"
	"errors"
	"flowpay/offer-service/internal/constants"
	offerCreateDTO "flowpay/offer-service/internal/dto/OfferCreate"
	offerRedeemDTO "flowpay/offer-service/internal/dto/OfferRedeem"
	offerReserveDTO "flowpay/offer-service/internal/dto/OfferReserve"
	flowpayOfferErrors "flowpay/offer-service/internal/errors"
	"flowpay/offer-service/internal/service"
	"flowpay/offer-service/internal/types"
	"flowpay/pkg/observability/logger"
	"flowpay/pkg/observability/metrics"
	responseTypes "flowpay/pkg/types"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Handler struct {
	offerService *service.OfferService
}

func NewHandler(offerService *service.OfferService) *Handler {
	return &Handler{offerService: offerService}
}

func offerOutcome(status int, err error) string {
	switch {
	case err == nil && status == http.StatusAccepted:
		return "success"
	case err == nil && status == http.StatusOK:
		return "success"
	case errors.Is(err, flowpayOfferErrors.ErrOfferNotFound):
		return "offer_not_found"
	case errors.Is(err, flowpayOfferErrors.ErrIdempotencyMismatch):
		return "idempotency_mismatch"
	case errors.Is(err, flowpayOfferErrors.ErrIdempotencyInProgress):
		return "idempotency_in_progress"
	case errors.Is(err, flowpayOfferErrors.ErrInsufficientBalance):
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

func offerErrorResponse(err error) (string, int) {
	switch {
	case errors.Is(err, flowpayOfferErrors.ErrInsufficientBalance):
		return flowpayOfferErrors.ErrInsufficientBalance.Error(), http.StatusBadRequest
	case errors.Is(err, flowpayOfferErrors.ErrOfferNotFound):
		return flowpayOfferErrors.ErrOfferNotFound.Error(), http.StatusNotFound
	case errors.Is(err, flowpayOfferErrors.ErrIdempotencyMismatch):
		return flowpayOfferErrors.ErrIdempotencyMismatch.Error(), http.StatusConflict
	case errors.Is(err, flowpayOfferErrors.ErrIdempotencyInProgress):
		return flowpayOfferErrors.ErrIdempotencyInProgress.Error(), http.StatusConflict
	case errors.Is(err, context.DeadlineExceeded):
		return flowpayOfferErrors.ErrOfferRequestTimedOut.Error(), http.StatusGatewayTimeout
	case errors.Is(err, context.Canceled):
		return flowpayOfferErrors.ErrOfferRequestCanceled.Error(), http.StatusRequestTimeout
	default:
		return flowpayOfferErrors.ErrCreateOfferFailed.Error(), http.StatusInternalServerError
	}
}

func WriteJSONError(w http.ResponseWriter, message string, status int) {
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

func (h *Handler) HandleOfferTransactions(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		h.handleOfferPostMethod(w, r)
		return
	}
	statusCode := http.StatusMethodNotAllowed
	logger.LogEvent(r.Context(), "WARN", constants.ServiceName, "offer_request_rejected", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"http_status": statusCode,
		"outcome":     "method_not_allowed",
		"error":       flowpayOfferErrors.ErrMethodNotAllowed,
		"error_type":  flowpayOfferErrors.ToOfferErrorType(flowpayOfferErrors.ErrMethodNotAllowed),
	})
	WriteJSONError(w, flowpayOfferErrors.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
}

func validateCreateOfferRequest(req offerCreateDTO.OfferCreationRequestDTO, idempotencyKey string) error {
	if strings.TrimSpace(idempotencyKey) == "" {
		return flowpayOfferErrors.ErrIdempotencyRequired
	}

	if strings.TrimSpace(req.CreatedBy) == "" {
		return flowpayOfferErrors.ErrCreatedByRequired
	}

	if strings.TrimSpace(req.CompanyId) == "" {
		return flowpayOfferErrors.ErrCompanyIdRequired
	}

	if strings.TrimSpace(req.OfferCode) == "" {
		return flowpayOfferErrors.ErrOfferCodeRequired
	}

	if strings.TrimSpace(req.OfferType) == "" {
		return flowpayOfferErrors.ErrOfferTypeRequired
	}

	if !types.IsValidOfferType(strings.TrimSpace(req.OfferType)) {
		return flowpayOfferErrors.ErrOfferTypeInvalid
	}

	// Exactly one of offer_amount or offer_percentage must be supplied.
	if req.OfferAmount == nil && req.OfferPercentage == nil {
		return flowpayOfferErrors.ErrOfferAmountOrPercentageRequired
	}

	if req.OfferAmount != nil && req.OfferPercentage != nil {
		return flowpayOfferErrors.ErrOfferAmountOrPercentageRequired
	}

	if req.OfferAmount != nil {
		if *req.OfferAmount <= 0 {
			return flowpayOfferErrors.ErrOfferAmountInvalid
		}
	}

	if req.OfferPercentage != nil {
		if *req.OfferPercentage <= 0 || *req.OfferPercentage > 100 {
			return flowpayOfferErrors.ErrOfferPercentageInvalid
		}
	}

	if req.MaxBenefitAmount < 0 {
		return flowpayOfferErrors.ErrMaxBenefitAmountInvalid
	}

	if req.MaxRedemptions <= 0 {
		return flowpayOfferErrors.ErrMaxRedemptionsInvalid
	}

	if req.MaxRedemptionsPerUser <= 0 {
		return flowpayOfferErrors.ErrMaxRedemptionsPerUserInvalid
	}

	if req.MaxRedemptionsPerUser > req.MaxRedemptions {
		return flowpayOfferErrors.ErrMaxRedemptionsPerUserInvalid
	}

	if req.MinimumPaymentAmount < 0 {
		return flowpayOfferErrors.ErrMinimumPaymentAmountNegative
	}

	if req.MaximumPaymentAmount < 0 {
		return flowpayOfferErrors.ErrMaximumPaymentAmountNegative
	}

	if req.MaximumPaymentAmount < req.MinimumPaymentAmount {
		return flowpayOfferErrors.ErrMinimumPaymentAmountInvalid
	}

	if req.StartTime.IsZero() {
		return flowpayOfferErrors.ErrStartTimeRequired
	}

	if req.EndTime.IsZero() {
		return flowpayOfferErrors.ErrEndTimeRequired
	}

	if !req.StartTime.Before(req.EndTime) {
		return flowpayOfferErrors.ErrStartTimeAfterEndTime
	}

	return nil
}

func validateOfferRedeemRequest(req offerRedeemDTO.OfferRedemptionRequestDTO, idempotencyKey string, offerID string) error {
	if strings.TrimSpace(idempotencyKey) == "" {
		return flowpayOfferErrors.ErrIdempotencyRequired
	}

	if strings.TrimSpace(req.AccountID) == "" {
		return flowpayOfferErrors.ErrAccountIdRequired
	}

	if strings.TrimSpace(req.PaymentID) == "" {
		return flowpayOfferErrors.ErrIdempotencyRequired
	}

	if strings.TrimSpace(req.ReservationID) == "" {
		return flowpayOfferErrors.ErrAccountIdRequired
	}

	return nil
}

func (h *Handler) HandleOfferIdReserveTransactions(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		h.handleOfferReservePost(w, r)
		return
	}
	statusCode := http.StatusMethodNotAllowed
	logger.LogEvent(r.Context(), "WARN", constants.ServiceName, "offer_reserve_request_rejected", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"http_status": statusCode,
		"outcome":     "method_not_allowed",
		"error":       flowpayOfferErrors.ErrMethodNotAllowed,
		"error_type":  flowpayOfferErrors.ToOfferErrorType(flowpayOfferErrors.ErrMethodNotAllowed),
	})
	WriteJSONError(w, flowpayOfferErrors.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
}

func (h *Handler) handleOfferRedeemPost(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	offerId := strings.TrimSpace(r.PathValue("offer_id"))
	reqIdempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	statusCode := http.StatusAccepted
	traceID := strings.TrimSpace(r.Header.Get("X-Trace-Id"))
	requestID := strings.TrimSpace(r.Header.Get("X-Request-Id"))

	var req offerRedeemDTO.OfferRedemptionRequestDTO
	var serviceErr error

	defer func() {
		outcome := offerOutcome(statusCode, serviceErr)
		metrics.OfferRedemptionRequestsTotal.WithLabelValues(constants.ServiceName, outcome).Inc()
		metrics.OfferRedemptionRequestDuration.WithLabelValues(constants.ServiceName, outcome).Observe(time.Since(start).Seconds())
	}()

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "offer_redeem_request_started", logger.Fields{
		"http_method":     r.Method,
		"http_path":       r.URL.Path,
		"idempotency_key": reqIdempotencyKey,
		"error_type":      flowpayOfferErrors.ErrorTypeNone,
	})

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		statusCode = http.StatusBadRequest
		logger.LogEvent(r.Context(), "WARN", constants.ServiceName, "offer_redeem_request_rejected", logger.Fields{
			"http_method":     r.Method,
			"http_path":       r.URL.Path,
			"http_status":     statusCode,
			"outcome":         "invalid_json",
			"idempotency_key": reqIdempotencyKey,
			"error_type":      flowpayOfferErrors.ToOfferErrorType(flowpayOfferErrors.ErrInvalidRequestBody),
			"error":           err.Error(),
		})
		WriteJSONError(w, flowpayOfferErrors.ErrInvalidRequestBody.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if validationError := validateOfferRedeemRequest(req, reqIdempotencyKey, offerId); validationError != nil {
		statusCode = http.StatusBadRequest

		logger.LogEvent(r.Context(), "WARN", constants.ServiceName, "offer_redeem_request_rejected", logger.Fields{
			"http_method":     r.Method,
			"http_path":       r.URL.Path,
			"http_status":     statusCode,
			"outcome":         "validation_error",
			"idempotency_key": reqIdempotencyKey,
			"error_type":      flowpayOfferErrors.ToOfferErrorType(validationError),
			"offer_id":        offerId,
			"account_id":      req.AccountID,
			"reservation_id":  req.ReservationID,
			"payment_id":      req.PaymentID,
			"error":           validationError.Error(),
		})
		WriteJSONError(w, validationError.Error(), http.StatusBadRequest)
		return
	}

	resp, err := h.offerService.RedeemOffer(ctx, req, reqIdempotencyKey, offerId, requestID, traceID)

	if err != nil {
		message, status := offerErrorResponse(err)
		serviceErr = err
		statusCode = status
		logger.LogEvent(r.Context(), "ERROR", constants.ServiceName, "offer_redeem_failed", logger.Fields{
			"http_method":      r.Method,
			"http_path":        r.URL.Path,
			"http_status":      status,
			"http_status_text": http.StatusText(status),
			"outcome":          offerOutcome(status, err),
			"error_type":       flowpayOfferErrors.ToOfferErrorType(err),
			"idempotency_key":  reqIdempotencyKey,
			"offer_id":         offerId,
			"account_id":       req.AccountID,
			"reservation_id":   req.ReservationID,
			"payment_id":       req.PaymentID,
			"error":            err.Error(),
			"duration_ms":      time.Since(start).Milliseconds(),
		})
		WriteJSONError(w, message, status)
		return
	}

	metrics.SuccessCount.WithLabelValues(constants.ServiceName, r.URL.Path, r.Method, strconv.Itoa(http.StatusAccepted)).Inc()
	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "offer_redeem_request_completed", logger.Fields{
		"http_method":      r.Method,
		"http_path":        r.URL.Path,
		"http_status":      http.StatusAccepted,
		"http_status_text": http.StatusText(http.StatusAccepted),
		"outcome":          "success",
		"error_type":       flowpayOfferErrors.ErrorTypeNone,
		"offer_id":         offerId,
		"account_id":       req.AccountID,
		"reservation_id":   req.ReservationID,
		"payment_id":       req.PaymentID,
		"duration_ms":      time.Since(start).Milliseconds(),
	})

	writeJSON(w, http.StatusAccepted, resp)
}

func validateOfferReserveRequest(req offerReserveDTO.OfferReservationRequestDTO, idempotencyKey string, offerId string) error {
	if strings.TrimSpace(idempotencyKey) == "" {
		return flowpayOfferErrors.ErrIdempotencyRequired
	}

	if strings.TrimSpace(req.AccountID) == "" {
		return flowpayOfferErrors.ErrAccountIdRequired
	}

	if strings.TrimSpace(req.PaymentID) == "" {
		return flowpayOfferErrors.ErrAccountIdRequired
	}

	if strings.TrimSpace(offerId) == "" {
		return flowpayOfferErrors.ErrOfferIdRequired
	}
	return nil
}

func (h *Handler) handleOfferReservePost(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	reqIdempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	offerId := strings.TrimSpace(r.PathValue("offer_id"))
	statusCode := http.StatusAccepted
	traceID := strings.TrimSpace(r.Header.Get("X-Trace-Id"))
	requestID := strings.TrimSpace(r.Header.Get("X-Request-Id"))

	var req offerReserveDTO.OfferReservationRequestDTO
	var serviceErr error

	defer func() {
		outcome := offerOutcome(statusCode, serviceErr)
		metrics.OfferReserveRequestsTotal.WithLabelValues(constants.ServiceName, outcome).Inc()
		metrics.OfferReserveRequestDuration.WithLabelValues(constants.ServiceName, outcome).Observe(time.Since(start).Seconds())
	}()

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "offer_reserve_request_started", logger.Fields{
		"http_method":     r.Method,
		"http_path":       r.URL.Path,
		"idempotency_key": reqIdempotencyKey,
		"error_type":      flowpayOfferErrors.ErrorTypeNone,
	})

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		statusCode = http.StatusBadRequest
		logger.LogEvent(r.Context(), "WARN", constants.ServiceName, "offer_reserve_request_rejected", logger.Fields{
			"http_method":     r.Method,
			"http_path":       r.URL.Path,
			"http_status":     statusCode,
			"outcome":         "invalid_json",
			"idempotency_key": reqIdempotencyKey,
			"error_type":      flowpayOfferErrors.ToOfferErrorType(flowpayOfferErrors.ErrInvalidRequestBody),
			"error":           err.Error(),
		})
		WriteJSONError(w, flowpayOfferErrors.ErrInvalidRequestBody.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if validationError := validateOfferReserveRequest(req, reqIdempotencyKey, offerId); validationError != nil {
		statusCode = http.StatusBadRequest

		logger.LogEvent(r.Context(), "WARN", constants.ServiceName, "offer_reserve_request_rejected", logger.Fields{
			"http_method":     r.Method,
			"http_path":       r.URL.Path,
			"http_status":     statusCode,
			"outcome":         "validation_error",
			"idempotency_key": reqIdempotencyKey,
			"error_type":      flowpayOfferErrors.ToOfferErrorType(validationError),
			"offer_id":        offerId,
			"account_id":      req.AccountID,
			"payment_id":      req.PaymentID,
			"error":           validationError.Error(),
		})
		WriteJSONError(w, validationError.Error(), http.StatusBadRequest)
		return
	}

	resp, err := h.offerService.ReserveOffer(ctx, req, reqIdempotencyKey, offerId, requestID, traceID)

	if err != nil {
		message, status := offerErrorResponse(err)
		serviceErr = err
		statusCode = status
		logger.LogEvent(r.Context(), "ERROR", constants.ServiceName, "offer_reserve_failed", logger.Fields{
			"http_method":      r.Method,
			"http_path":        r.URL.Path,
			"http_status":      status,
			"http_status_text": http.StatusText(status),
			"outcome":          offerOutcome(status, err),
			"error_type":       flowpayOfferErrors.ToOfferErrorType(err),
			"idempotency_key":  reqIdempotencyKey,
			"account_id":       req.AccountID,
			"offer_id":         offerId,
			"payment_id":       req.PaymentID,
			"error":            err.Error(),
			"duration_ms":      time.Since(start).Milliseconds(),
		})
		WriteJSONError(w, message, status)
		return
	}

	metrics.SuccessCount.WithLabelValues(constants.ServiceName, r.URL.Path, r.Method, strconv.Itoa(http.StatusAccepted)).Inc()
	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "offer_reserve_request_completed", logger.Fields{
		"http_method":      r.Method,
		"http_path":        r.URL.Path,
		"http_status":      http.StatusAccepted,
		"http_status_text": http.StatusText(http.StatusAccepted),
		"outcome":          "success",
		"error_type":       flowpayOfferErrors.ErrorTypeNone,
		"account_id":       req.AccountID,
		"offer_id":         offerId,
		"payment_id":       req.PaymentID,
		"duration_ms":      time.Since(start).Milliseconds(),
	})

	writeJSON(w, http.StatusAccepted, resp)
}

func (h *Handler) HandleOfferIdRedeemTransactions(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		h.handleOfferRedeemPost(w, r)
		return
	}
	statusCode := http.StatusMethodNotAllowed
	logger.LogEvent(r.Context(), "WARN", constants.ServiceName, "offer_redeem_request_rejected", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"http_status": statusCode,
		"outcome":     "method_not_allowed",
		"error":       flowpayOfferErrors.ErrMethodNotAllowed,
		"error_type":  flowpayOfferErrors.ToOfferErrorType(flowpayOfferErrors.ErrMethodNotAllowed),
	})
	WriteJSONError(w, flowpayOfferErrors.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
}

func (h *Handler) handleOfferPostMethod(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	reqIdempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	statusCode := http.StatusAccepted
	traceID := strings.TrimSpace(r.Header.Get("X-Trace-Id"))
	requestID := strings.TrimSpace(r.Header.Get("X-Request-Id"))

	var req offerCreateDTO.OfferCreationRequestDTO
	var serviceErr error

	defer func() {
		outcome := offerOutcome(statusCode, serviceErr)
		metrics.OfferCreationRequestsTotal.WithLabelValues(constants.ServiceName, outcome).Inc()
		metrics.OfferCreationRequestDuration.WithLabelValues(constants.ServiceName, outcome).Observe(time.Since(start).Seconds())
	}()

	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "offer_creation_request_started", logger.Fields{
		"http_method":     r.Method,
		"http_path":       r.URL.Path,
		"idempotency_key": reqIdempotencyKey,
		"error_type":      flowpayOfferErrors.ErrorTypeNone,
	})

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		statusCode = http.StatusBadRequest
		logger.LogEvent(r.Context(), "WARN", constants.ServiceName, "offer_creation_request_rejected", logger.Fields{
			"http_method":     r.Method,
			"http_path":       r.URL.Path,
			"http_status":     statusCode,
			"outcome":         "invalid_json",
			"idempotency_key": reqIdempotencyKey,
			"error_type":      flowpayOfferErrors.ToOfferErrorType(flowpayOfferErrors.ErrInvalidRequestBody),
			"error":           err.Error(),
		})
		WriteJSONError(w, flowpayOfferErrors.ErrInvalidRequestBody.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if validationError := validateCreateOfferRequest(req, reqIdempotencyKey); validationError != nil {
		statusCode = http.StatusBadRequest

		logger.LogEvent(r.Context(), "WARN", constants.ServiceName, "offer_creation_request_rejected", logger.Fields{
			"http_method":     r.Method,
			"http_path":       r.URL.Path,
			"http_status":     statusCode,
			"outcome":         "validation_error",
			"idempotency_key": reqIdempotencyKey,
			"error_type":      flowpayOfferErrors.ToOfferErrorType(validationError),
			"created_by":      req.CreatedBy,
			"offer_code":      req.OfferCode,
			"offer_type":      req.OfferType,
			"start_time":      req.StartTime,
			"end_time":        req.EndTime,
			"error":           validationError.Error(),
		})
		WriteJSONError(w, validationError.Error(), http.StatusBadRequest)
		return
	}

	resp, err := h.offerService.CreateNewOffer(ctx, req, reqIdempotencyKey, requestID, traceID)

	if err != nil {
		message, status := offerErrorResponse(err)
		serviceErr = err
		statusCode = status
		logger.LogEvent(r.Context(), "ERROR", constants.ServiceName, "offer_creation_failed", logger.Fields{
			"http_method":      r.Method,
			"http_path":        r.URL.Path,
			"http_status":      status,
			"http_status_text": http.StatusText(status),
			"outcome":          offerOutcome(status, err),
			"error_type":       flowpayOfferErrors.ToOfferErrorType(err),
			"idempotency_key":  reqIdempotencyKey,
			"created_by":       req.CreatedBy,
			"offer_code":       req.OfferCode,
			"offer_type":       req.OfferType,
			"start_time":       req.StartTime,
			"end_time":         req.EndTime,
			"error":            err.Error(),
			"duration_ms":      time.Since(start).Milliseconds(),
		})
		WriteJSONError(w, message, status)
		return
	}

	metrics.SuccessCount.WithLabelValues(constants.ServiceName, r.URL.Path, r.Method, strconv.Itoa(http.StatusAccepted)).Inc()
	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "offer_creation_request_completed", logger.Fields{
		"http_method":      r.Method,
		"http_path":        r.URL.Path,
		"http_status":      http.StatusAccepted,
		"http_status_text": http.StatusText(http.StatusAccepted),
		"outcome":          "success",
		"error_type":       flowpayOfferErrors.ErrorTypeNone,
		"offer_code":       req.OfferCode,
		"offer_type":       req.OfferType,
		"start_time":       req.StartTime,
		"end_time":         req.EndTime,
		"duration_ms":      time.Since(start).Milliseconds(),
	})

	writeJSON(w, http.StatusAccepted, resp)
}
