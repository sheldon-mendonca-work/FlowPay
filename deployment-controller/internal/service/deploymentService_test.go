package service

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	awsec2 "flowpay/deployment-controller/internal/aws"
	"flowpay/deployment-controller/internal/state"
)

// fakeEC2Client returns each instance in the given sequence on successive
// Describe calls, then keeps returning the last one — so tests can model an
// instance transitioning through states while polling.
type fakeEC2Client struct {
	mu          sync.Mutex
	instances   []awsec2.Instance
	idx         int
	describeErr error
	startCalls  atomic.Int32
	stopCalls   atomic.Int32
}

func newFakeEC2Client(instances ...awsec2.Instance) *fakeEC2Client {
	return &fakeEC2Client{instances: instances}
}

func (f *fakeEC2Client) Describe(ctx context.Context) (awsec2.Instance, error) {
	if f.describeErr != nil {
		return awsec2.Instance{}, f.describeErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	inst := f.instances[f.idx]
	if f.idx < len(f.instances)-1 {
		f.idx++
	}
	return inst, nil
}

func (f *fakeEC2Client) Start(ctx context.Context) error {
	f.startCalls.Add(1)
	return nil
}

func (f *fakeEC2Client) Stop(ctx context.Context) error {
	f.stopCalls.Add(1)
	return nil
}

// newHealthServer starts an httptest server that answers unhealthy for the
// first failCount requests, then 200 OK. It returns "127.0.0.1" and the
// port as a string, ready to plug into a fake Instance's PublicIP / the
// service's healthPort.
func newHealthServer(t *testing.T, failCount int) (publicIP, healthPort string) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if int(hits.Add(1)) <= failCount {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	port := srv.Listener.Addr().(*net.TCPAddr).Port
	return "127.0.0.1", strconv.Itoa(port)
}

func TestStartInstance_AlreadyRunning(t *testing.T) {
	ec2 := newFakeEC2Client(awsec2.Instance{State: awsec2.StateRunning, PublicIP: "1.2.3.4"})
	svc := NewDeploymentService(ec2, state.New(), time.Hour, time.Millisecond, "8000")

	resp, err := svc.StartInstance(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != StateRunning || resp.PublicIP != "1.2.3.4" {
		t.Fatalf("expected {RUNNING 1.2.3.4}, got %+v", resp)
	}
	if ec2.startCalls.Load() != 0 {
		t.Fatalf("expected Start not to be called, got %d calls", ec2.startCalls.Load())
	}
}

func TestStartInstance_AlreadyPending(t *testing.T) {
	ec2 := newFakeEC2Client(awsec2.Instance{State: awsec2.StatePending})
	svc := NewDeploymentService(ec2, state.New(), time.Hour, time.Millisecond, "8000")

	resp, err := svc.StartInstance(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != StateStarting {
		t.Fatalf("expected STARTING, got %+v", resp)
	}
	if ec2.startCalls.Load() != 0 {
		t.Fatalf("expected Start not to be called, got %d calls", ec2.startCalls.Load())
	}
}

func TestStartInstance_StopsToRunningPollsAndWaitsForHealth(t *testing.T) {
	publicIP, healthPort := newHealthServer(t, 1) // unhealthy once, then healthy

	ec2 := newFakeEC2Client(
		awsec2.Instance{State: awsec2.StateStopped},
		awsec2.Instance{State: awsec2.StatePending},
		awsec2.Instance{State: awsec2.StateRunning, PublicIP: publicIP},
	)
	svc := NewDeploymentService(ec2, state.New(), time.Hour, 5*time.Millisecond, healthPort)

	resp, err := svc.StartInstance(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != StateRunning || resp.PublicIP != publicIP {
		t.Fatalf("expected {RUNNING %s}, got %+v", publicIP, resp)
	}
	if ec2.startCalls.Load() != 1 {
		t.Fatalf("expected Start to be called once, got %d calls", ec2.startCalls.Load())
	}
}

func TestStopInstance_AlreadyStopped(t *testing.T) {
	ec2 := newFakeEC2Client(awsec2.Instance{State: awsec2.StateStopped})
	svc := NewDeploymentService(ec2, state.New(), time.Hour, time.Millisecond, "8000")

	resp, err := svc.StopInstance(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != StateStopped {
		t.Fatalf("expected STOPPED, got %+v", resp)
	}
	if ec2.stopCalls.Load() != 0 {
		t.Fatalf("expected Stop not to be called, got %d calls", ec2.stopCalls.Load())
	}
}

func TestStopInstance_StopsWhenRunningPollsUntilStopped(t *testing.T) {
	ec2 := newFakeEC2Client(
		awsec2.Instance{State: awsec2.StateRunning},
		awsec2.Instance{State: awsec2.StateStopping},
		awsec2.Instance{State: awsec2.StateStopped},
	)
	svc := NewDeploymentService(ec2, state.New(), time.Hour, 5*time.Millisecond, "8000")

	resp, err := svc.StopInstance(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != StateStopped {
		t.Fatalf("expected STOPPED, got %+v", resp)
	}
	if ec2.stopCalls.Load() != 1 {
		t.Fatalf("expected Stop to be called once, got %d calls", ec2.stopCalls.Load())
	}
}

func TestStatus_RunningAndHealthy(t *testing.T) {
	publicIP, healthPort := newHealthServer(t, 0)
	ec2 := newFakeEC2Client(awsec2.Instance{State: awsec2.StateRunning, PublicIP: publicIP})
	svc := NewDeploymentService(ec2, state.New(), time.Hour, time.Millisecond, healthPort)

	resp, err := svc.Status(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != StateRunning || resp.PublicIP != publicIP {
		t.Fatalf("expected {RUNNING %s}, got %+v", publicIP, resp)
	}
}

func TestStatus_RunningButNotYetHealthy(t *testing.T) {
	// Server that never returns 200 within this test's single check.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	port := srv.Listener.Addr().(*net.TCPAddr).Port

	ec2 := newFakeEC2Client(awsec2.Instance{State: awsec2.StateRunning, PublicIP: "127.0.0.1"})
	svc := NewDeploymentService(ec2, state.New(), time.Hour, time.Millisecond, strconv.Itoa(port))

	resp, err := svc.Status(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != StateStarting || resp.PublicIP != "" {
		t.Fatalf("expected {STARTING \"\"}, got %+v", resp)
	}
}

func TestStatus_Stopped(t *testing.T) {
	ec2 := newFakeEC2Client(awsec2.Instance{State: awsec2.StateStopped})
	svc := NewDeploymentService(ec2, state.New(), time.Hour, time.Millisecond, "8000")

	resp, err := svc.Status(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != StateStopped {
		t.Fatalf("expected STOPPED, got %+v", resp)
	}
}

func TestMonitorIdle_StopsInstanceAfterTimeout(t *testing.T) {
	ec2 := newFakeEC2Client(
		awsec2.Instance{State: awsec2.StateRunning},
		awsec2.Instance{State: awsec2.StateStopping},
		awsec2.Instance{State: awsec2.StateStopped},
	)
	st := state.New()
	st.TouchAt(time.Now().Add(-2 * time.Hour))

	svc := NewDeploymentService(ec2, st, time.Hour, 5*time.Millisecond, "8000")

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	svc.MonitorIdle(ctx, 10*time.Millisecond)

	if ec2.stopCalls.Load() == 0 {
		t.Fatalf("expected Stop to be called at least once after idle timeout, got 0")
	}
}

func TestMonitorIdle_DoesNotStopWhileRecentHeartbeat(t *testing.T) {
	ec2 := newFakeEC2Client(awsec2.Instance{State: awsec2.StateRunning})
	st := state.New() // heartbeat is "now"

	svc := NewDeploymentService(ec2, st, time.Hour, 5*time.Millisecond, "8000")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	svc.MonitorIdle(ctx, 10*time.Millisecond)

	if ec2.stopCalls.Load() != 0 {
		t.Fatalf("expected Stop not to be called, got %d calls", ec2.stopCalls.Load())
	}
}

func TestHeartbeat_UpdatesLastHeartbeat(t *testing.T) {
	svc := NewDeploymentService(newFakeEC2Client(), state.New(), time.Hour, time.Millisecond, "8000")

	before := time.Now()
	svc.Heartbeat()

	if svc.LastHeartbeat().Before(before) {
		t.Fatalf("expected LastHeartbeat to be updated to at least %v, got %v", before, svc.LastHeartbeat())
	}
}
