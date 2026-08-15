package service

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	awsec2 "flowpay/deployment-controller/internal/aws"
	"flowpay/deployment-controller/internal/state"
	"flowpay/deployment-controller/internal/utils"
)

// maxWait bounds how long a single Start/Stop operation will poll AWS and
// the app's health endpoint before giving up, so a stuck instance can't
// hang a request (or the idle monitor) forever.
const maxWait = 5 * time.Minute

type DeploymentService struct {
	ec2          awsec2.EC2Client
	state        *state.DeploymentState
	idleTimeout  time.Duration
	pollInterval time.Duration
	healthPort   string
	healthClient *http.Client
}

func NewDeploymentService(ec2Client awsec2.EC2Client, deploymentState *state.DeploymentState, idleTimeout, pollInterval time.Duration, healthPort string) *DeploymentService {
	return &DeploymentService{
		ec2:          ec2Client,
		state:        deploymentState,
		idleTimeout:  idleTimeout,
		pollInterval: pollInterval,
		healthPort:   healthPort,
		healthClient: &http.Client{Timeout: 3 * time.Second},
	}
}

// StartInstance ensures the instance is running and the app on it is
// healthy, starting it if necessary.
//
//   - running: returns immediately with the current public IP.
//   - pending: returns immediately as STARTING (a start is already underway).
//   - stopping: returns immediately as STOPPING (can't start mid-stop; retry later).
//   - stopped: calls StartInstances, then blocks polling AWS until the
//     instance is running and its /health endpoint returns 200, then
//     returns RUNNING with the public IP.
func (s *DeploymentService) StartInstance(ctx context.Context) (StatusResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, maxWait)
	defer cancel()

	info, err := s.ec2.Describe(ctx)
	if err != nil {
		return StatusResponse{}, err
	}

	switch info.State {
	case awsec2.StateRunning:
		return StatusResponse{Status: StateRunning, PublicIP: info.PublicIP}, nil
	case awsec2.StatePending:
		return StatusResponse{Status: StateStarting}, nil
	case awsec2.StateStopping:
		return StatusResponse{Status: StateStopping}, nil
	}

	if err := s.ec2.Start(ctx); err != nil {
		return StatusResponse{}, err
	}

	publicIP, err := s.waitForState(ctx, awsec2.StateRunning)
	if err != nil {
		return StatusResponse{}, err
	}

	if err := s.waitForHealthy(ctx, publicIP); err != nil {
		return StatusResponse{}, err
	}

	s.Heartbeat()

	return StatusResponse{Status: StateRunning, PublicIP: publicIP}, nil
}

// StopInstance ensures the instance is stopped, stopping it if necessary.
//
//   - stopped: returns immediately.
//   - stopping: returns immediately as STOPPING (already underway).
//   - running/pending: calls StopInstances, then blocks polling AWS until
//     the instance is stopped.
func (s *DeploymentService) StopInstance(ctx context.Context) (StatusResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, maxWait)
	defer cancel()

	info, err := s.ec2.Describe(ctx)
	if err != nil {
		return StatusResponse{}, err
	}

	switch info.State {
	case awsec2.StateStopped:
		return StatusResponse{Status: StateStopped}, nil
	case awsec2.StateStopping:
		return StatusResponse{Status: StateStopping}, nil
	}

	if err := s.ec2.Stop(ctx); err != nil {
		return StatusResponse{}, err
	}

	if _, err := s.waitForState(ctx, awsec2.StateStopped); err != nil {
		return StatusResponse{}, err
	}

	return StatusResponse{Status: StateStopped}, nil
}

// Status is a read-only snapshot: it never starts or stops the instance. A
// running instance is only reported RUNNING once its /health endpoint
// answers 200; otherwise it's reported STARTING, since it isn't actually
// usable yet.
func (s *DeploymentService) Status(ctx context.Context) (StatusResponse, error) {
	info, err := s.ec2.Describe(ctx)
	if err != nil {
		return StatusResponse{}, err
	}

	log.Println(info)
	log.Println("FLOWPAY_API_BASE_URL" + utils.GetEnv("FLOWPAY_API_BASE_URL", ""))
	switch info.State {
	case awsec2.StateRunning:
		healthURL := fmt.Sprintf("http://%s/health", info.PublicIP)
		if apiURL := utils.GetEnv("FLOWPAY_API_BASE_URL", ""); apiURL != "" {
			healthURL = strings.TrimRight(apiURL, "/") + "/health"
		}

		if s.isHealthy(ctx, healthURL) {
			return StatusResponse{
				Status:   StateRunning,
				PublicIP: info.PublicIP,
			}, nil
		}

		return StatusResponse{Status: StateStarting}, nil
	case awsec2.StatePending:
		return StatusResponse{Status: StateStarting}, nil
	case awsec2.StateStopping:
		return StatusResponse{Status: StateStopping}, nil
	default:
		return StatusResponse{Status: StateStopped}, nil
	}
}

// ProxyTarget resolves the current application URL on the instance.
//
// It reuses the same running-instance URL that Status and StartInstance
// expose, so the proxy always points at the same destination callers see in
// deployment/status and deployment/start.
func (s *DeploymentService) ProxyTarget(ctx context.Context) (string, error) {
	resp, err := s.Status(ctx)
	if err != nil {
		return "", err
	}

	if resp.Status != StateRunning || resp.PublicIP == "" {
		return "", fmt.Errorf("deployment instance is not running")
	}

	return fmt.Sprintf("http://%s:%s", resp.PublicIP, s.healthPort), nil
}

// Heartbeat records that the application is still in use.
func (s *DeploymentService) Heartbeat() {
	s.state.Touch()
}

// LastHeartbeat returns the timestamp of the most recent heartbeat.
func (s *DeploymentService) LastHeartbeat() time.Time {
	return s.state.LastHeartbeat()
}

// MonitorIdle blocks, checking on each tick of interval whether the instance
// has gone longer than idleTimeout without a heartbeat, and stopping it if
// so. It returns when ctx is cancelled.
func (s *DeploymentService) MonitorIdle(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if time.Since(s.state.LastHeartbeat()) <= s.idleTimeout {
				continue
			}
			if _, err := s.StopInstance(ctx); err != nil {
				log.Printf("idle monitor: failed to stop instance: %v", err)
			}
		}
	}
}

// waitForState polls Describe every pollInterval until the instance reaches
// want, returning its public IP at that point.
func (s *DeploymentService) waitForState(ctx context.Context, want awsec2.InstanceState) (string, error) {
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		info, err := s.ec2.Describe(ctx)
		if err != nil {
			return "", err
		}
		if info.State == want {
			return info.PublicIP, nil
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
}

// waitForHealthy polls the app's /health endpoint every pollInterval until
// it returns 200.
func (s *DeploymentService) waitForHealthy(ctx context.Context, publicIP string) error {
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		if s.isHealthy(ctx, publicIP) {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *DeploymentService) isHealthy(ctx context.Context, healthURL string) bool {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		healthURL,
		nil,
	)
	if err != nil {
		return false
	}

	resp, err := s.healthClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}
