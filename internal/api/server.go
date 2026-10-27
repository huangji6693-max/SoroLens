// Package api serves SoroLens's read-only JSON API. Every endpoint mirrors
// something the web UI shows, so anything browsable is also machine-readable.
//
// The API depends only on source.EventSource, so it behaves identically in
// standalone and upstream mode.
package api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/sorotrail/sorolens/internal/source"
)

// DefaultHeartbeatInterval is how often GET /api/events/stream writes an SSE
// heartbeat comment so idle connections survive intermediate proxies.
const DefaultHeartbeatInterval = 15 * time.Second

// Server holds the API's dependencies.
type Server struct {
	src               source.EventSource
	log               *slog.Logger
	broker            *Broker
	heartbeatInterval time.Duration
}

// New builds an API server over the given event source.
func New(src source.EventSource, log *slog.Logger) *Server {
	return &Server{
		src:               src,
		log:               log,
		heartbeatInterval: DefaultHeartbeatInterval,
	}
}

// WithBroker attaches a live event Broker for GET /api/events/stream. When nil,
// or when running in upstream mode, the stream endpoint reports 501 Not
// Implemented.
func (s *Server) WithBroker(b *Broker) *Server {
	s.broker = b
	return s
}

// WithHeartbeatInterval overrides DefaultHeartbeatInterval for SSE keep-alives.
func (s *Server) WithHeartbeatInterval(d time.Duration) *Server {
	if d > 0 {
		s.heartbeatInterval = d
	}
	return s
}

// Routes returns the API router. cmd/sorolens mounts it at /api, except for
// /health which is mounted at the root.
//
// contributors: new read endpoints go here. Anything that mutates state needs
// authentication designed first — see CONTRIBUTING.md.
func (s *Server) Routes() chi.Router {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)

	// Long-lived SSE streams outlive the per-request timeout applied to
	// ordinary JSON endpoints.
	r.Get("/events/stream", s.handleEventStream)

	r.Group(func(r chi.Router) {
		r.Use(middleware.Timeout(60 * time.Second))

		r.Get("/contracts", s.handleListContracts)
		r.Get("/contracts/{id}", s.handleGetContract)
		r.Get("/contracts/{id}/events", s.handleContractEvents)
		r.Get("/contracts/{id}/stats", s.handleContractStats)
		r.Get("/events", s.handleListEvents)
		r.Get("/events/{id}", s.handleGetEvent)
		r.Get("/stats", s.handleStats)
		r.Get("/version", s.handleVersion)
	})

	return r
}

// HealthHandler serves GET /health at the root. It reports 200 when the
// configured source is reachable and 503 when it is not, so it works as a
// container or load-balancer probe.
func (s *Server) HealthHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		status := s.src.Status(r.Context())
		code := http.StatusOK
		if !status.Healthy {
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, status)
	}
}
