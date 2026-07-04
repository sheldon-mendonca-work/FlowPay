package dto

import "flowpay/notification-service/internal/types"

type PaymentTimelineDTO struct {
	PaymentID     string                       `json:"payment_id"`
	Status        types.NotificationStatusEnum `json:"status"`
	TimelineSteps []types.PaymentTimelineType  `json:"timeline_steps"`
}
