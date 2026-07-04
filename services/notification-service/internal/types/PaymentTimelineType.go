package types

import (
	"time"
)

type PaymentTimelineType struct {
	StepName      string                 `json:"step_name"`
	Status        NotificationStatusEnum `json:"status"`
	CompletedTime time.Time              `json:"completed_time"`
}
