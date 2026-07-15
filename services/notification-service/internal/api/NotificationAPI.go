package api

import (
	"context"
	"encoding/json"
	"errors"
	"flowpay/notification-service/internal/constants"
	flowpayNotificationErrors "flowpay/notification-service/internal/errors"
	"flowpay/notification-service/internal/service"
	"flowpay/pkg/observability/logger"
	responseTypes "flowpay/pkg/types"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type Handler struct {
	notificationService *service.NotificationService
}

func NewHandler(notificationService *service.NotificationService) *Handler {
	return &Handler{notificationService: notificationService}
}

func notificationOutcome(status int, err error) string {
	switch {
	case err == nil && status == http.StatusAccepted:
		return "success"
	case err == nil && status == http.StatusOK:
		return "success"
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

const timelineHeartbeatInterval = 15 * time.Second
const flowpayMetricsInterval = 10 * time.Second

// HandleNotificationTimelineStream opens a Server-Sent Events connection for a trace_id.
// It immediately sends the current timeline snapshot, then pushes a fresh snapshot every
// time a new step is ingested for that trace.
func (h *Handler) HandleNotificationTimelineStream(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if r.Method != http.MethodGet {
		WriteJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	traceID := strings.TrimSpace(r.PathValue("trace_id"))
	if traceID == "" {
		WriteJSONError(w, flowpayNotificationErrors.ErrTraceIDRequired.Error(), http.StatusBadRequest)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		WriteJSONError(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	updates, unsubscribe := h.notificationService.Subscribe(traceID)
	defer unsubscribe()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	if timeline, err := h.notificationService.GetTimeline(ctx, traceID); err != nil {
		logger.LogEvent(ctx, "ERROR", constants.ServiceName, "notification_timeline_snapshot_failed", logger.Fields{
			"trace_id": traceID,
			"error":    err.Error(),
		})
	} else if writeSSEEvent(w, timeline) == nil {
		flusher.Flush()
	}

	heartbeat := time.NewTicker(timelineHeartbeatInterval)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case timeline, open := <-updates:
			if !open {
				return
			}
			if writeSSEEvent(w, timeline) != nil {
				return
			}
			flusher.Flush()
		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func writeSSEEvent(w http.ResponseWriter, event any) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "data: %s\n\n", payload)
	return err
}

// HandleGetTimelineByPaymentID returns a single timeline snapshot for a payment_id.
// Unlike HandleNotificationTimelineStream this is a plain JSON GET, not SSE - used by
// the Payment Details page which only has the payment id, not the trace id.
func (h *Handler) HandleGetTimelineByPaymentID(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	statusCode := http.StatusOK
	var serviceErr error

	defer func() {
		outcome := notificationOutcome(statusCode, serviceErr)
		logger.LogEvent(r.Context(), "INFO", constants.ServiceName, "payment_timeline_lookup_completed", logger.Fields{
			"http_method": r.Method,
			"http_path":   r.URL.Path,
			"http_status": statusCode,
			"outcome":     outcome,
			"duration_ms": time.Since(start).Milliseconds(),
		})
	}()

	if r.Method != http.MethodGet {
		statusCode = http.StatusMethodNotAllowed
		WriteJSONError(w, "method not allowed", statusCode)
		return
	}

	paymentID := strings.TrimSpace(r.PathValue("paymentID"))
	if paymentID == "" {
		statusCode = http.StatusBadRequest
		serviceErr = flowpayNotificationErrors.ErrPaymentIDRequired
		WriteJSONError(w, flowpayNotificationErrors.ErrPaymentIDRequired.Error(), statusCode)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 16500*time.Millisecond)
	defer cancel()

	timeline, err := h.notificationService.GetTimelineByPaymentID(ctx, paymentID)
	if err != nil {
		statusCode = http.StatusInternalServerError
		serviceErr = err
		WriteJSONError(w, "failed to load payment timeline", statusCode)
		return
	}

	writeJSON(w, http.StatusOK, timeline)
}

// HandleFlowpayMetricsStream opens an SSE connection pushing a FlowpayMetricsDTO
// snapshot immediately, then again every flowpayMetricsInterval.
func (h *Handler) HandleFlowpayMetricsStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		WriteJSONError(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	ctx := r.Context()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	if writeSSEEvent(w, h.notificationService.GetFlowpayMetrics(ctx)) == nil {
		flusher.Flush()
	}

	ticker := time.NewTicker(flowpayMetricsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if writeSSEEvent(w, h.notificationService.GetFlowpayMetrics(ctx)) != nil {
				return
			}
			flusher.Flush()
		}
	}
}
