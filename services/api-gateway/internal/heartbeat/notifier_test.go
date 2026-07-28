package heartbeat

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestNotifier_ThrottlesToAtMostOnceWithinInterval(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	n := NewWithInterval(srv.URL, time.Hour)

	for i := 0; i < 10; i++ {
		n.Notify(true)
	}

	waitForCalls(t, &calls, 1)

	time.Sleep(50 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected exactly 1 call after throttling, got %d", got)
	}
}

func TestNotifier_FiresAgainAfterIntervalElapses(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	n := NewWithInterval(srv.URL, 20*time.Millisecond)

	n.Notify(true)
	waitForCalls(t, &calls, 1)

	time.Sleep(30 * time.Millisecond)
	n.Notify(true)
	waitForCalls(t, &calls, 2)
}

func TestNotifier_DoesNotSendOutsideProduction(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	n := NewWithInterval(srv.URL, time.Millisecond)

	for i := 0; i < 10; i++ {
		n.Notify(false)
	}

	time.Sleep(50 * time.Millisecond)
	if got := calls.Load(); got != 0 {
		t.Fatalf("expected no calls outside production, got %d", got)
	}
}

func waitForCalls(t *testing.T, calls *atomic.Int32, want int32) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if calls.Load() >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d calls, got %d", want, calls.Load())
}
