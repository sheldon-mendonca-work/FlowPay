// Package state holds the deployment controller's in-memory operational
// state. It is intentionally not persisted: it is transient (last-seen
// liveness), and resetting it on restart is acceptable.
package state

import (
	"sync"
	"time"
)

type LifecycleState string

const (
	Stopped  LifecycleState = "STOPPED"
	Starting LifecycleState = "STARTING"
	Running  LifecycleState = "RUNNING"
	Stopping LifecycleState = "STOPPING"
)

type DeploymentState struct {
	mu            sync.RWMutex
	lastHeartbeat time.Time
	lifecycle     LifecycleState
}

func New() *DeploymentState {
	return &DeploymentState{
		lastHeartbeat: time.Now(),
		lifecycle:     Stopped,
	}
}

func (s *DeploymentState) Touch() {
	s.TouchAt(time.Now())
}

func (s *DeploymentState) TouchAt(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lastHeartbeat = t
}

func (s *DeploymentState) LastHeartbeat() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.lastHeartbeat
}

func (s *DeploymentState) Lifecycle() LifecycleState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.lifecycle
}

func (s *DeploymentState) SetLifecycle(lifecycle LifecycleState) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lifecycle = lifecycle
}
