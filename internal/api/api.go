package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/francisoffiong/cron-mesh/internal/jobs"
	"github.com/google/uuid"
)

type LeaderStatus interface {
	IsLeader() bool
}

type Server struct {
	store  *jobs.Store
	leader LeaderStatus
	mux    *http.ServeMux
}

func New(store *jobs.Store, leader LeaderStatus) *Server {
	s := &Server{store: store, leader: leader, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.healthz)
	s.mux.HandleFunc("GET /v1/status", s.status)
	s.mux.HandleFunc("POST /v1/jobs", s.createJob)
	s.mux.HandleFunc("GET /v1/jobs", s.listJobs)
	s.mux.HandleFunc("GET /v1/jobs/{id}", s.getJob)
	s.mux.HandleFunc("DELETE /v1/jobs/{id}", s.deleteJob)
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) status(w http.ResponseWriter, _ *http.Request) {
	role := "standby"
	if s.leader != nil && s.leader.IsLeader() {
		role = "leader"
	}
	writeJSON(w, http.StatusOK, map[string]string{"role": role})
}

type createJobRequest struct {
	Name                      string          `json:"name"`
	CronExpr                  string          `json:"cronExpr"`
	QueueName                 string          `json:"queueName"`
	Payload                   json.RawMessage `json:"payload"`
	MisfireGracePeriodSeconds int             `json:"misfireGracePeriodSeconds"`
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.Name == "" || req.CronExpr == "" || req.QueueName == "" {
		writeError(w, http.StatusBadRequest, "name, cronExpr, and queueName are required")
		return
	}

	job, err := s.store.Create(r.Context(), jobs.Job{
		Name:                      req.Name,
		CronExpr:                  req.CronExpr,
		QueueName:                 req.QueueName,
		Payload:                   req.Payload,
		MisfireGracePeriodSeconds: req.MisfireGracePeriodSeconds,
	})
	if errors.Is(err, jobs.ErrDuplicate) {
		writeError(w, http.StatusConflict, "job name already exists")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, job)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	list, err := s.store.List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if list == nil {
		list = []jobs.Job{}
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}
	job, err := s.store.Get(r.Context(), id)
	if errors.Is(err, jobs.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) deleteJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}
	if err := s.store.Deactivate(r.Context(), id); errors.Is(err, jobs.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	} else if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
