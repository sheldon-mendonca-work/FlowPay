package flowpayOutboxErrors

import (
	"context"
	"errors"
)

const (
	ErrorTypeNone         = "NONE"
	ErrorTypeKafkaPublish = "KAFKA_PUBLISH_FAILURE"
	ErrorTypeTimeout      = "TIMEOUT"
	ErrorTypeCanceled     = "CANCELED"
	ErrorTypeDBFailure    = "DB_FAILURE"
)

func ToOutboxErrorType(err error) string {
	switch {
	case err == nil:
		return ErrorTypeNone
	case errors.Is(err, ErrKafkaPublishFailed):
		return ErrorTypeKafkaPublish
	case errors.Is(err, context.DeadlineExceeded):
		return ErrorTypeTimeout
	case errors.Is(err, context.Canceled):
		return ErrorTypeCanceled
	default:
		return ErrorTypeDBFailure
	}
}
