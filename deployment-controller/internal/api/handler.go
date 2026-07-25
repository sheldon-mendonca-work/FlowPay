package handler

import (
	"encoding/json"
	"net/http"

	"flowpay/deployment-controller/internal/service"
)

type Handler struct {
	deploymentService *service.DeploymentService
}

func NewHandler(deploymentService *service.DeploymentService) *Handler {
	return &Handler{deploymentService: deploymentService}
}

func (h *Handler) GetStatus(w http.ResponseWriter, r *http.Request) {
	resp, err := h.deploymentService.Status(r.Context())
	writeStatus(w, resp, err)
}

func (h *Handler) StartInstance(w http.ResponseWriter, r *http.Request) {
	resp, err := h.deploymentService.StartInstance(r.Context())
	writeStatus(w, resp, err)
}

func (h *Handler) StopInstance(w http.ResponseWriter, r *http.Request) {
	resp, err := h.deploymentService.StopInstance(r.Context())
	writeStatus(w, resp, err)
}

func (h *Handler) Heartbeat(w http.ResponseWriter, r *http.Request) {
	h.deploymentService.Heartbeat()
	w.WriteHeader(http.StatusNoContent)
}

func writeStatus(w http.ResponseWriter, resp service.StatusResponse, err error) {
	w.Header().Set("Content-Type", "application/json")

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	json.NewEncoder(w).Encode(resp)
}
