package flowpayOutboxErrors

import "errors"

var (
	ErrKafkaPublishFailed = errors.New("kafka publish failed")
)
