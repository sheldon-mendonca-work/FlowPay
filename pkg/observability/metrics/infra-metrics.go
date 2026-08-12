package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var DBOperationDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "flowpay",
		Subsystem: "db",
		Name:      "operation_duration_seconds",
		Help:      "Duration of database operations.",
		Buckets:   prometheus.DefBuckets,
	},
	[]string{
		"service",
		"operation",
		"table",
		"status",
	},
)

func MeasureDB(
	service string,
	operation string,
	table string,
	fn func() error,
) error {

	start := time.Now()

	err := fn()

	status := "success"
	if err != nil {
		status = "failure"
	}

	DBOperationDuration.
		WithLabelValues(service, operation, table, status).
		Observe(time.Since(start).Seconds())

	return err
}

func MeasureDB2[T any](
	service string,
	operation string,
	table string,
	fn func() (T, error),
) (T, error) {

	start := time.Now()

	result, err := fn()

	status := "success"
	if err != nil {
		status = "failure"
	}

	DBOperationDuration.
		WithLabelValues(service, operation, table, status).
		Observe(time.Since(start).Seconds())

	return result, err
}

func MeasureDB3[T any, V any](
	service string,
	operation string,
	table string,
	fn func() (T, V, error),
) (T, V, error) {

	start := time.Now()

	res1, res2, err := fn()

	status := "success"
	if err != nil {
		status = "failure"
	}

	DBOperationDuration.
		WithLabelValues(service, operation, table, status).
		Observe(time.Since(start).Seconds())

	return res1, res2, err
}

var RedisOperationDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "flowpay",
		Subsystem: "redis",
		Name:      "operation_duration_seconds",
		Help:      "Duration of Redis operations.",
		Buckets:   prometheus.DefBuckets,
	},
	[]string{
		"service",
		"operation",
		"key",
		"status",
	},
)

func MeasureRedis[T any](
	service string,
	operation string,
	key string,
	fn func() (T, error),
) (T, error) {

	start := time.Now()

	result, err := fn()

	status := "success"
	if err != nil {
		status = "failure"
	}

	RedisOperationDuration.
		WithLabelValues(service, operation, key, status).
		Observe(time.Since(start).Seconds())

	return result, err
}

var KafkaPublishDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "flowpay",
		Subsystem: "kafka",
		Name:      "publish_duration_seconds",
		Help:      "Duration of Kafka publish operations.",
		Buckets:   prometheus.DefBuckets,
	},
	[]string{
		"service",
		"topic",
		"status",
	},
)

func MeasureKafkaPublish(
	service string,
	topic string,
	fn func() error,
) error {

	start := time.Now()

	err := fn()

	status := "success"
	if err != nil {
		status = "failure"
	}

	KafkaPublishDuration.
		WithLabelValues(service, topic, status).
		Observe(time.Since(start).Seconds())

	return err
}

// func MeasureKafkaPublish[T any](
// 	service string,
// 	topic string,
// 	fn func() (T, error),
// ) (T, error) {

// 	start := time.Now()

// 	result, err := fn()

// 	status := "success"
// 	if err != nil {
// 		status = "failure"
// 	}

// 	KafkaPublishDuration.
// 		WithLabelValues(service, topic, status).
// 		Observe(time.Since(start).Seconds())

// 	return result, err
// }

var HTTPClientDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "flowpay",
		Subsystem: "http_client",
		Name:      "request_duration_seconds",
		Help:      "Duration of outbound HTTP requests.",
		Buckets:   prometheus.DefBuckets,
	},
	[]string{
		"service",
		"target_service",
		"method",
		"status",
	},
)

func MeasureHTTPClient[T any](
	service string,
	targetService string,
	method string,
	fn func() (T, error),
) (T, error) {

	start := time.Now()

	result, err := fn()

	status := "success"
	if err != nil {
		status = "failure"
	}

	HTTPClientDuration.
		WithLabelValues(service, targetService, method, status).
		Observe(time.Since(start).Seconds())

	return result, err
}

func InitInfraMetrics() {
	prometheus.MustRegister(
		DBOperationDuration,
		RedisOperationDuration,
		KafkaPublishDuration,
		HTTPClientDuration,
	)
}
