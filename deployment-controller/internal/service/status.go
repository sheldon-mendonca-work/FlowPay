package service

// State is the deployment controller's own lifecycle status, as exposed to
// API callers — distinct from the raw AWS instance state.
type State string

const (
	StateStopped  State = "STOPPED"
	StateStarting State = "STARTING"
	StateRunning  State = "RUNNING"
	StateStopping State = "STOPPING"
)

type StatusResponse struct {
	Status   State  `json:"status"`
	PublicIP string `json:"publicIp"`
}
