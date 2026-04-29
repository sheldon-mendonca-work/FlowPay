package logger

import (
	"context"
	"flowpay/pkg/observability/tracing"
	"fmt"
	"log"
)

func LogWithRequest(ctx context.Context, serviceName string, format string, args ...interface{}) {
	traceID := tracing.GetTraceID(ctx)
	requestID := tracing.GetRequestID(ctx)
	log.Printf(
		"service=%s trace_id=%s request_id=%s %s",
		serviceName,
		traceID,
		requestID,
		fmt.Sprintf(format, args...),
	)
}
