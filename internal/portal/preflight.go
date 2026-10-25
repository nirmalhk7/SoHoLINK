package portal

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/metrics"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/orchestrator"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/types"
)

type jobPreflighter interface {
	PreflightJob(orchestrator.SubmitJobRequest) (orchestrator.JobPreflight, error)
}

type jobCheckForm struct {
	ContainerImage, WorkloadType, CPUCores, RAMMB, StorageGB, Country string
	GPURequired                                                       bool
}

type jobCheckData struct {
	Email           string
	IsAuthenticated bool
	Form            jobCheckForm
	Result          *orchestrator.JobPreflight
	Blockers        []string
	Error           string
}

func (ps *PortalServer) handleJobCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	claims, _ := ClaimsFromContext(r.Context())
	data := jobCheckData{Email: claims.Email, IsAuthenticated: true, Form: jobCheckForm{WorkloadType: "batch_compute", CPUCores: "2", RAMMB: "4096", StorageGB: "0"}}
	if r.Method == http.MethodGet {
		ps.renderTemplate(w, "consumer_job_check.html", data)
		return
	}
	form, req, err := parseComputeJob(w, r, claims.UserID)
	data.Form = form
	if err != nil {
		data.Error = err.Error()
		w.WriteHeader(http.StatusBadRequest)
	} else {
		status := ps.checkJob(&data, req)
		if status != http.StatusOK {
			w.WriteHeader(status)
		}
	}
	ps.renderTemplate(w, "consumer_job_check.html", data)
}

// parseComputeJob is shared by preview and confirmation. Identity always comes
// from the session. Resource/country values are preserved, never inferred from
// locality or silently relaxed, and no member narrative or job payload is read.
func parseComputeJob(w http.ResponseWriter, r *http.Request, consumer string) (jobCheckForm, orchestrator.SubmitJobRequest, error) {
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	if err := r.ParseForm(); err != nil {
		return jobCheckForm{}, orchestrator.SubmitJobRequest{}, errors.New("Could not read job requirements.")
	}
	f := jobCheckForm{
		ContainerImage: strings.TrimSpace(r.PostForm.Get("container_image")), WorkloadType: r.PostForm.Get("workload_type"),
		CPUCores: r.PostForm.Get("cpu_cores"), RAMMB: r.PostForm.Get("ram_mb"), StorageGB: r.PostForm.Get("storage_gb"),
		Country: strings.ToUpper(strings.TrimSpace(r.PostForm.Get("country"))), GPURequired: r.PostForm.Get("gpu_required") == "true",
	}
	req := orchestrator.SubmitJobRequest{ConsumerID: consumer, ContainerImage: f.ContainerImage, WorkloadType: types.MarketplaceWorkloadType(f.WorkloadType), CountryConstraint: f.Country, GPURequired: f.GPURequired}
	if req.WorkloadType != types.MarketplaceAppHosting && req.WorkloadType != types.MarketplaceBatchCompute && req.WorkloadType != types.MarketplaceAIInference {
		return f, req, errors.New("Choose a compute workload: app hosting, batch compute, or AI inference.")
	}
	if f.ContainerImage == "" {
		return f, req, errors.New("Enter an approved image pinned to its SHA-256 digest.")
	}
	for _, field := range []struct {
		name, value string
		target      *int
		minimum     int
	}{
		{"CPU cores", f.CPUCores, &req.CPUCores, 1}, {"RAM (MB)", f.RAMMB, &req.RAMMB, 1}, {"Disk (GB)", f.StorageGB, &req.StorageGB, 0},
	} {
		n, err := strconv.Atoi(field.value)
		if err != nil || n < field.minimum {
			return f, req, fmt.Errorf("%s must be a whole number of at least %d.", field.name, field.minimum)
		}
		*field.target = n
	}
	if f.Country != "" && (len(f.Country) != 2 || f.Country[0] < 'A' || f.Country[0] > 'Z' || f.Country[1] < 'A' || f.Country[1] > 'Z') {
		return f, req, errors.New("Country must be a two-letter code, or empty for any country.")
	}
	if gpu := r.PostForm.Get("gpu_required"); gpu != "" && gpu != "true" {
		return f, req, errors.New("Invalid GPU requirement.")
	}
	return f, req, nil
}

func (ps *PortalServer) checkJob(data *jobCheckData, req orchestrator.SubmitJobRequest) int {
	checker, ok := ps.orch.(jobPreflighter)
	if !ok {
		data.Error = "Availability check is unavailable. Please try again later."
		return http.StatusServiceUnavailable
	}
	result, err := checker.PreflightJob(req)
	if err != nil {
		if errors.Is(err, orchestrator.ErrInvalidPreflight) {
			data.Error = "Check the compute job requirements."
			return http.StatusBadRequest
		}
		slog.Warn("job preflight unavailable", "error", err)
		data.Error = "Availability check is unavailable. Please try again later."
		return http.StatusServiceUnavailable
	}
	data.Result = &result
	for _, b := range result.Blockers {
		text := map[string]string{
			"offline": "Nodes are offline.", "country": "Nodes are outside your required country.",
			"gpu": "Nodes do not advertise a GPU.", "cpu": "Nodes have fewer CPU cores than requested.",
			"ram": "Nodes have less RAM than requested.", "storage": "Nodes have less disk than requested.",
			"compute_disabled":  "Nodes are not offering compute work.",
			"image_not_allowed": "Image is not approved. Use a digest-pinned image from the coordinator's allowlist.",
			"workload_mismatch": "This approved image does not support the selected workload type.",
		}[b.Code]
		if text != "" {
			if b.Code != "image_not_allowed" && b.Code != "workload_mismatch" {
				text = fmt.Sprintf("%d: %s", b.Count, text)
			}
			data.Blockers = append(data.Blockers, text)
		}
	}
	return http.StatusOK
}

// handleCheckedJobSubmit does not trust an earlier preview. SubmitJob reloads
// the allowlist and matches current registry state before dispatch. The existing
// node-list submission route stays unchanged for transitional portal clients.
func (ps *PortalServer) handleCheckedJobSubmit(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	claims, _ := ClaimsFromContext(r.Context())
	form, req, err := parseComputeJob(w, r, claims.UserID)
	data := jobCheckData{Email: claims.Email, IsAuthenticated: true, Form: form}
	w.Header().Set("Cache-Control", "no-store")
	if err != nil {
		data.Error = err.Error()
		w.WriteHeader(http.StatusBadRequest)
		ps.renderTemplate(w, "consumer_job_check.html", data)
		return
	}
	if ps.orch == nil {
		http.Error(w, "Job submission is unavailable.", http.StatusServiceUnavailable)
		return
	}
	resp, err := ps.orch.SubmitJob(r.Context(), req)
	if err != nil {
		slog.Warn("checked job submission failed", "error", err)
		status := ps.checkJob(&data, req)
		if status == http.StatusOK {
			status = http.StatusConflict
			data.Error = "Job submission could not be confirmed. Check your dashboard before trying again."
		}
		w.WriteHeader(status)
		ps.renderTemplate(w, "consumer_job_check.html", data)
		return
	}
	metrics.JobsSubmittedTotal.WithLabelValues(string(req.WorkloadType)).Inc()
	http.Redirect(w, r, "/consumer/job/"+resp.JobID, http.StatusSeeOther)
}
