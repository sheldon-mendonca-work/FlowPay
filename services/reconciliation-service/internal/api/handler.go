package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"flowpay/pkg/observability/logger"
	"flowpay/pkg/observability/metrics"
	responseTypes "flowpay/pkg/types"
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

func reconciliationErrorResponse(err error) (string, int) {
	switch {
	case errors.Is(err, flowpayReconciliationErrors.ErrMethodDoesntExist):
		return flowpayReconciliationErrors.ErrMethodDoesntExist.Error(), http.StatusNotFound
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
	case errors.Is(err, flowpayReconciliationErrors.ErrMethodDoesntExist):
		return "method_does_not_exist"
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

func buildPaymentReconciliationResponse(anomalies []dto.AnomalyResponseDTO, checkName string, executionTimeMs int64) dto.ReconciliationResponseDTO {
	status := "PASS"
	if len(anomalies) > 0 {
		status = "FAIL"
	}

	checksSummary := make(map[string]int8)
	severitySummary := make(map[string]int8)

	for _, anamoly := range anomalies {
		severitySummary[anamoly.Severity]++
		checksSummary[anamoly.CheckName]++
	}

	return dto.ReconciliationResponseDTO{
		CheckName:       checkName,
		Status:          status,
		AnomalyCount:    len(anomalies),
		ExecutionTimeMs: executionTimeMs,
		SeveritySummary: severitySummary,
		ChecksSummary:   checksSummary,
		Anomalies:       anomalies,
	}
}

func (h *Handler) HandleReconciliationChecks(w http.ResponseWriter,
	r *http.Request,
	checkName string,
	run func(context.Context, string) ([]dto.AnomalyResponseDTO, error),
) {
	metrics.ReconciliationRunsTotal.Inc()
	start := time.Now()
	if r.Method != http.MethodGet {
		err := flowpayReconciliationErrors.ErrMethodNotAllowed
		message, status := reconciliationErrorResponse(err)
		logger.LogEvent(r.Context(), "WARN", constants.ServiceName, "reconciliation_"+checkName+"_request_rejected", logger.Fields{
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

	anomalies, err := run(ctx, checkName)
	if err != nil {
		duration := time.Since(start).Milliseconds()
		message, status := reconciliationErrorResponse(err)
		logger.LogEvent(r.Context(), "ERROR", constants.ServiceName, ""+checkName+"_reconciliation_failed", logger.Fields{
			"http_method":      r.Method,
			"http_path":        r.URL.Path,
			"http_status":      status,
			"http_status_text": http.StatusText(status),
			"outcome":          reconciliationOutcome(status, err),
			"error_type":       reconciliationErrorType(err),
			"error":            err.Error(),
			"duration_ms":      duration,
		})
		WriteJSONError(w, message, status)
		return
	}

	response := buildPaymentReconciliationResponse(anomalies, checkName, time.Since(start).Milliseconds())

	duration := time.Since(start).Milliseconds()

	metrics.ReconciliationDurationMs.Observe(float64(duration))
	metrics.ReconciliationAnomaliesTotal.Add(float64(len(anomalies)))

	if len(anomalies) > 0 {
		metrics.ReconciliationFailuresTotal.Inc()
	}
	logger.LogEvent(r.Context(), "INFO", constants.ServiceName, ""+checkName+"_reconciliation_completed", logger.Fields{
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

	writeJSON(w, http.StatusOK, response)

}

func (h *Handler) HandlePaymentChecks(w http.ResponseWriter, r *http.Request) {
	h.HandleReconciliationChecks(w, r, "all_payments", h.reconciliationService.RunPaymentChecks)

}

func (h *Handler) writeUnknownCheck(w http.ResponseWriter, r *http.Request) {
	err := flowpayReconciliationErrors.ErrMethodDoesntExist
	message, status := reconciliationErrorResponse(err)
	logger.LogEvent(r.Context(), "WARN", constants.ServiceName, "reconciliation_method_doesnt_exist", logger.Fields{
		"http_method": r.Method,
		"http_path":   r.URL.Path,
		"http_status": status,
		"outcome":     reconciliationOutcome(status, err),
		"error_type":  reconciliationErrorType(err),
	})
	WriteJSONError(w, message, status)
}

func (h *Handler) HandleIndividualPaymentCheck(w http.ResponseWriter, r *http.Request) {
	paymentCheckType := r.PathValue("payment_check_type")
	switch {
	case paymentCheckType == constants.PaymentsWithoutTransactionsCheckName:
		{
			h.HandleReconciliationChecks(w, r, constants.PaymentsWithoutTransactionsCheckName, h.reconciliationService.GetPaymentsWithoutTransactions)
			return
		}
	case paymentCheckType == constants.PaymentsWithInvalidTransactionPairCheckName:
		{
			h.HandleReconciliationChecks(w, r, constants.PaymentsWithInvalidTransactionPairCheckName, h.reconciliationService.GetPaymentsWithoutDebitOrCredit)
			return
		}
	case paymentCheckType == constants.PaymentsWithoutIdempotencyRowsCheckName:
		{
			h.HandleReconciliationChecks(w, r, constants.PaymentsWithoutIdempotencyRowsCheckName, h.reconciliationService.GetPaymentsWithoutIdempotencyRows)
			return
		}

	case paymentCheckType == constants.PaymentsWithIdempotencyPaymentIDMismatchCheckName:
		{
			h.HandleReconciliationChecks(w, r, constants.PaymentsWithIdempotencyPaymentIDMismatchCheckName, h.reconciliationService.GetPaymentsWithIdempotencyPaymentIDMismatch)
			return
		}

	case paymentCheckType == constants.PaymentsMissingCompletedIdempotencyCheckName:
		{
			h.HandleReconciliationChecks(w, r, constants.PaymentsMissingCompletedIdempotencyCheckName, h.reconciliationService.GetPaymentsMissingCompletedIdempotency)
			return
		}
	default:
		{
			h.writeUnknownCheck(w, r)
		}
	}
}

func (h *Handler) HandleIdempotencyChecks(w http.ResponseWriter, r *http.Request) {
	h.HandleReconciliationChecks(w, r, "all_idempotency", h.reconciliationService.RunIdempotencyChecks)
}

func (h *Handler) HandleIndividualIdempotencyCheck(w http.ResponseWriter, r *http.Request) {
	idempotencyCheckType := r.PathValue("idempotency_check_type")

	switch idempotencyCheckType {
	case constants.IdempotencyCompletedWithoutPaymentCheckName:
		h.HandleReconciliationChecks(w, r, idempotencyCheckType, h.reconciliationService.GetIdempotencyCompletedWithoutPayment)
	case constants.IdempotencyMissingPaymentIDCheckName:
		h.HandleReconciliationChecks(w, r, idempotencyCheckType, h.reconciliationService.GetIdempotencyMissingPaymentID)
	case constants.IdempotencyDuplicatePaymentIDCheckName:
		h.HandleReconciliationChecks(w, r, idempotencyCheckType, h.reconciliationService.GetIdempotencyDuplicatePaymentID)
	case constants.IdempotencyExpiredInProgressCheckName:
		h.HandleReconciliationChecks(w, r, idempotencyCheckType, h.reconciliationService.GetIdempotencyExpiredInProgress)
	case constants.IdempotencyInProgressWithoutOutboxCheckName:
		h.HandleReconciliationChecks(w, r, idempotencyCheckType, h.reconciliationService.GetIdempotencyInProgressWithoutOutbox)
	case constants.IdempotencyOutboxPaymentIDMismatchCheckName:
		h.HandleReconciliationChecks(w, r, idempotencyCheckType, h.reconciliationService.GetIdempotencyOutboxPaymentIDMismatch)
	case constants.IdempotencyMultipleOutboxPaymentIDsCheckName:
		h.HandleReconciliationChecks(w, r, idempotencyCheckType, h.reconciliationService.GetIdempotencyMultipleOutboxPaymentIDs)
	default:
		h.writeUnknownCheck(w, r)
	}
}

func (h *Handler) HandleOutboxChecks(w http.ResponseWriter, r *http.Request) {
	h.HandleReconciliationChecks(w, r, "all_outbox", h.reconciliationService.RunOutboxChecks)
}

func (h *Handler) HandleIndividualOutboxCheck(w http.ResponseWriter, r *http.Request) {
	outboxCheckType := r.PathValue("outbox_check_type")

	switch outboxCheckType {
	case constants.OutboxStuckProcessingCheckName:
		h.HandleReconciliationChecks(w, r, outboxCheckType, h.reconciliationService.GetOutboxStuckProcessing)
	case constants.OutboxFailedRetryExhaustedCheckName:
		h.HandleReconciliationChecks(w, r, outboxCheckType, h.reconciliationService.GetOutboxFailedRetryExhausted)
	case constants.OutboxEligibleBacklogCheckName:
		h.HandleReconciliationChecks(w, r, outboxCheckType, h.reconciliationService.GetOutboxEligibleBacklog)
	case constants.OutboxBacklogSummaryCheckName:
		h.HandleReconciliationChecks(w, r, outboxCheckType, h.reconciliationService.GetOutboxBacklogSummary)
	case constants.OutboxWithoutIdempotencyRowsCheckName:
		h.HandleReconciliationChecks(w, r, outboxCheckType, h.reconciliationService.GetOutboxWithoutIdempotencyRows)
	case constants.OutboxPublishedWithoutPaymentCheckName:
		h.HandleReconciliationChecks(w, r, outboxCheckType, h.reconciliationService.GetOutboxPublishedWithoutPayment)
	default:
		h.writeUnknownCheck(w, r)
	}
}

func (h *Handler) HandleTransactionChecks(w http.ResponseWriter, r *http.Request) {
	h.HandleReconciliationChecks(w, r, "all_transactions", h.reconciliationService.RunTransactionChecks)
}

func (h *Handler) HandleIndividualTransactionCheck(w http.ResponseWriter, r *http.Request) {
	transactionCheckType := r.PathValue("transaction_check_type")

	switch transactionCheckType {
	case constants.TransactionsMissingLedgerSideCheckName:
		h.HandleReconciliationChecks(w, r, transactionCheckType, h.reconciliationService.GetTransactionsMissingLedgerSide)
	case constants.TransactionsAmountCurrencyImbalanceCheckName:
		h.HandleReconciliationChecks(w, r, transactionCheckType, h.reconciliationService.GetTransactionsAmountCurrencyImbalance)
	case constants.TransactionsWithoutPaymentsCheckName:
		h.HandleReconciliationChecks(w, r, transactionCheckType, h.reconciliationService.GetTransactionsWithoutPayments)
	case constants.TransactionsStatusMismatchCheckName:
		h.HandleReconciliationChecks(w, r, transactionCheckType, h.reconciliationService.GetTransactionsStatusMismatch)
	default:
		h.writeUnknownCheck(w, r)
	}
}

func (h *Handler) HandleOfferChecks(w http.ResponseWriter, r *http.Request) {
	h.HandleReconciliationChecks(w, r, "all_offers", h.reconciliationService.RunOfferChecks)
}

func (h *Handler) HandleIndividualOfferCheck(w http.ResponseWriter, r *http.Request) {
	offerCheckType := r.PathValue("offer_check_type")

	switch offerCheckType {
	case constants.OfferSuccessfulPaymentsWithUnredeemedReservationCheckName:
		h.HandleReconciliationChecks(w, r, offerCheckType, h.reconciliationService.GetOfferSuccessfulPaymentsWithUnredeemedReservation)
	case constants.OfferRedeemedReservationsWithoutSuccessfulPaymentCheckName:
		h.HandleReconciliationChecks(w, r, offerCheckType, h.reconciliationService.GetOfferRedeemedReservationsWithoutSuccessfulPayment)
	case constants.OfferReservationsWithoutPaymentCheckName:
		h.HandleReconciliationChecks(w, r, offerCheckType, h.reconciliationService.GetOfferReservationsWithoutPayment)
	case constants.OfferRedemptionsWithoutPaymentCheckName:
		h.HandleReconciliationChecks(w, r, offerCheckType, h.reconciliationService.GetOfferRedemptionsWithoutPayment)
	case constants.OfferExpiredReservationsStuckReservedCheckName:
		h.HandleReconciliationChecks(w, r, offerCheckType, h.reconciliationService.GetOfferExpiredReservationsStuckReserved)
	case constants.OfferReservationRedemptionStatusMismatchCheckName:
		h.HandleReconciliationChecks(w, r, offerCheckType, h.reconciliationService.GetOfferReservationRedemptionStatusMismatch)
	case constants.OfferRedeemedCountDriftCheckName:
		h.HandleReconciliationChecks(w, r, offerCheckType, h.reconciliationService.GetOfferRedeemedCountDrift)
	default:
		h.writeUnknownCheck(w, r)
	}
}

func (h *Handler) HandleAllReconciliationChecks(w http.ResponseWriter, r *http.Request) {
	h.HandleReconciliationChecks(w, r, "all_reconciliation", h.reconciliationService.RunAllChecks)
}
