package api

import (
	"context"
	"net/http"
	"time"
)

// InternalAPIServer is the SoHoLINK orchestrator's internal HTTP server.
// Unlike APIServer, this listener is bound to a Docker-network-only address
// and serves plain HTTP. It is not exposed via the Cloudflare tunnel.
// The job submit and preflight endpoints are called by the portal
// process against the orchestrator's NodeRegistry —
// the one that actually receives agent heartbeats.
//
// Trust model: the listener address binding is the security boundary. The
// /internal/ path prefix is documentation, not a control.
type InternalAPIServer struct {
	srv *http.Server
}

// NewInternal constructs an InternalAPIServer that wraps orch.SubmitJob.
// addr is the internal-only listen address (e.g. ":8083"); network isolation
// is enforced by Docker — port 8083 is not published externally.
func NewInternal(orch jobSubmitter, addr string) *InternalAPIServer {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/jobs/submit", handleInternalSubmitJob(orch))
	mux.HandleFunc("POST /internal/jobs/preflight", handleInternalPreflightJob(orch))

	return &InternalAPIServer{
		srv: &http.Server{
			Addr:         addr,
			Handler:      mux,
			ReadTimeout:  15 * time.Second,
			WriteTimeout: 15 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
	}
}

// Start begins accepting connections on the internal address. Plain HTTP —
// Docker network isolation is the security boundary.
func (s *InternalAPIServer) Start(ctx context.Context) error {
	return s.srv.ListenAndServe()
}

// Shutdown gracefully drains active connections within the context deadline.
func (s *InternalAPIServer) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}
