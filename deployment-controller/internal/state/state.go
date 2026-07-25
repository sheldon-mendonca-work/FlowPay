// Package state holds the deployment controller's in-memory operational
// state. It is intentionally not persisted: it is transient (last-seen
// liveness), and resetting it on restart is acceptable.
package state

import (
	"sync"
	"time"
)

type DeploymentState struct {
	mu            sync.RWMutex
	lastHeartbeat time.Time
}

// New creates a DeploymentState with the heartbeat clock started at the
// current time, so a fresh restart doesn't look instantly idle.
func New() *DeploymentState {
	return &DeploymentState{lastHeartbeat: time.Now()}
}

// Touch records a heartbeat at the current time.
func (s *DeploymentState) Touch() {
	s.TouchAt(time.Now())
}

// TouchAt records a heartbeat at the given time. Exposed separately from
// Touch so tests can simulate idle periods without waiting in real time.
func (s *DeploymentState) TouchAt(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastHeartbeat = t
}

// LastHeartbeat returns the timestamp of the most recent heartbeat.
func (s *DeploymentState) LastHeartbeat() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastHeartbeat
}
