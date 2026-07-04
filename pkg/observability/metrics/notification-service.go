package metrics

import "github.com/prometheus/client_golang/prometheus"

var NotificationTimelineEventsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "notification_timeline_events_total",
		Help: "Total number of notification timeline kafka events processed by outcome.",
	},
	[]string{"service", "outcome"},
)

var NotificationTimelineEventDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "notification_timeline_event_duration_seconds",
		Help:    "Notification timeline kafka event processing latency in seconds.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"service", "outcome"},
)

func InitNotificationMetrics() {
	prometheus.MustRegister(NotificationTimelineEventsTotal, NotificationTimelineEventDuration)
}
