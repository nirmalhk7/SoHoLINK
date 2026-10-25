package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/orchestrator"
)

type jobPreflighter interface {
	PreflightJob(orchestrator.SubmitJobRequest) (orchestrator.JobPreflight, error)
}

// Uses the same private listener and live registry as job submission.
func handleInternalPreflightJob(orch jobSubmitter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
		var req orchestrator.SubmitJobRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid preflight request")
			return
		}
		checker, ok := orch.(jobPreflighter)
		if !ok {
			writeError(w, http.StatusServiceUnavailable, "preflight unavailable")
			return
		}
		result, err := checker.PreflightJob(req)
		if err != nil {
			if errors.Is(err, orchestrator.ErrInvalidPreflight) {
				writeError(w, http.StatusBadRequest, "invalid preflight request")
			} else {
				slog.Warn("internal preflight unavailable", "error", err)
				writeError(w, http.StatusServiceUnavailable, "preflight unavailable")
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	}
}
