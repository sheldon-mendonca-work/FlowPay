package logger

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"flowpay/pkg/observability/tracing"
)

var (
	jsonLogger  = log.New(os.Stdout, "", 0)
	traceLogger = log.New(os.Stderr, "", log.LstdFlags)
)

func LogWithRequest(ctx context.Context, serviceName string, format string, args ...interface{}) {
	traceID := tracing.GetTraceID(ctx)
	requestID := tracing.GetRequestID(ctx)
	traceLogger.Printf(
		"service=%s trace_id=%s request_id=%s %s",
		serviceName,
		traceID,
		requestID,
		fmt.Sprintf(format, args...),
	)
}

type Fields map[string]any

func LogEvent(ctx context.Context, level string, serviceName string, event string, fields Fields) {
	entry := Fields{
		"level":      level,
		"service":    serviceName,
		"event":      event,
		"trace_id":   tracing.GetTraceID(ctx),
		"request_id": tracing.GetRequestID(ctx),
		"timestamp":  time.Now().UTC().Format(time.RFC3339Nano),
	}

	for key, value := range fields {
		entry[key] = value
	}

	payload, err := json.Marshal(entry)
	if err != nil {
		traceLogger.Printf(`service=%s event=log_encoding_failed error=%v`, serviceName, err)
		return
	}

	jsonLogger.Print(string(payload))
}

func LogPlain(ctx context.Context, serviceName string, format string, args ...interface{}) {
	traceID := tracing.GetTraceID(ctx)
	requestID := tracing.GetRequestID(ctx)
	traceLogger.Printf(
		"[plain] service=%s trace_id=%s request_id=%s %s",
		serviceName,
		traceID,
		requestID,
		fmt.Sprintf(format, args...),
	)
}
