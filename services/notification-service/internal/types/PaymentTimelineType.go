package types

import (
	"time"
)

type PaymentTimelineType struct {
	StepName      string                 `json:"step_name"`
	Status        NotificationStatusEnum `json:"status"`
	CompletedTime time.Time              `json:"completed_time"`
	// CompletedStepTime is the elapsed time in ms since the previous step completed
	// (i.e. since this step's service started processing). Nil for the first step.
	CompletedStepTime *int64 `json:"completed_step_time"`
}
