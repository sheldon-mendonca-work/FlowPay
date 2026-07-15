package dto

import "flowpay/notification-service/internal/types"

type PaymentTimelineDTO struct {
	TraceID       string                       `json:"trace_id"`
	PaymentID     string                       `json:"payment_id"`
	Status        types.NotificationStatusEnum `json:"status"`
	TimelineSteps []types.PaymentTimelineType  `json:"timeline_steps"`
	// TotalTime is the elapsed time in ms from the first step to the last, in
	// milliseconds. Only populated once Status is SUCCESS or FAILED.
	TotalTime *int64 `json:"total_time"`
}
