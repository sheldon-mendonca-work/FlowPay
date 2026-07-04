package api

import (
	"context"
	"encoding/json"
	"errors"
	"flowpay/notification-service/internal/constants"
	"flowpay/notification-service/internal/dto"
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

// HandleNotificationTimelineStream opens a Server-Sent Events connection for a payment_id.
// It immediately sends the current timeline snapshot, then pushes a fresh snapshot every
// time a new step is ingested for that payment.
func (h *Handler) HandleNotificationTimelineStream(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if r.Method != http.MethodGet {
		WriteJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	paymentID := strings.TrimSpace(r.PathValue("payment_id"))
	if paymentID == "" {
		WriteJSONError(w, flowpayNotificationErrors.ErrPaymentIDRequired.Error(), http.StatusBadRequest)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		WriteJSONError(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	updates, unsubscribe := h.notificationService.Subscribe(paymentID)
	defer unsubscribe()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	if timeline, err := h.notificationService.GetTimeline(ctx, paymentID); err != nil {
		logger.LogEvent(ctx, "ERROR", constants.ServiceName, "notification_timeline_snapshot_failed", logger.Fields{
			"payment_id": paymentID,
			"error":      err.Error(),
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

func writeSSEEvent(w http.ResponseWriter, timeline dto.PaymentTimelineDTO) error {
	payload, err := json.Marshal(timeline)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "data: %s\n\n", payload)
	return err
}
