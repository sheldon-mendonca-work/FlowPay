// Package heartbeat lets the API gateway tell the deployment controller that
// the application is still in use, without ever calling the AWS EC2 API
// itself — the deployment controller is the only component allowed to do
// that.
package heartbeat

import (
	"context"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

const defaultInterval = time.Minute

type Notifier struct {
	url      string
	client   *http.Client
	interval time.Duration
	lastSent atomic.Int64 // UnixNano of the last heartbeat actually sent
}

// New creates a Notifier that sends at most one heartbeat per minute to url,
// regardless of how often Notify is called.
func New(url string) *Notifier {
	return NewWithInterval(url, defaultInterval)
}

// NewWithInterval is like New but with a configurable throttle interval,
// primarily so tests don't have to wait a full minute.
func NewWithInterval(url string, interval time.Duration) *Notifier {
	return &Notifier{
		url:      url,
		client:   &http.Client{Timeout: 5 * time.Second},
		interval: interval,
	}
}

// Notify signals that the application is in use. It never blocks the
// caller: the throttle check is a lock-free CAS, and the outbound HTTP call
// (when not throttled) happens on a background goroutine.
func (n *Notifier) Notify() {
	now := time.Now().UnixNano()
	last := n.lastSent.Load()
	if time.Duration(now-last) < n.interval {
		return
	}
	if !n.lastSent.CompareAndSwap(last, now) {
		return // another goroutine just claimed this slot
	}

	go n.send()
}

// Middleware calls Notify for every request without affecting request
// handling.
func (n *Notifier) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Notify()
		next.ServeHTTP(w, r)
	})
}

func (n *Notifier) send() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.url, nil)
	if err != nil {
		log.Printf("heartbeat: build request: %v", err)
		return
	}

	resp, err := n.client.Do(req)
	if err != nil {
		log.Printf("heartbeat: send failed: %v", err)
		return
	}
	defer resp.Body.Close()
}
