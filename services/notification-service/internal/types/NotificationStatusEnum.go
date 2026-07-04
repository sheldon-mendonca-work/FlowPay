package types

type NotificationStatusEnum string

const (
	CREATED    NotificationStatusEnum = "CREATED"
	PROCESSING NotificationStatusEnum = "PROCESSING"
	SUCCESS    NotificationStatusEnum = "SUCCESS"
	FAILED     NotificationStatusEnum = "FAILED"
)
