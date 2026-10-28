package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/orchclient"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/orchestrator"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/types"
)

// Exercise the actual portal client against the internal API and registry.
// Direct orchestrator tests cannot catch a missing process-to-process route.
func TestPreflightThroughInternalClient(t *testing.T) {
	path := filepath.Join(t.TempDir(), "allowlist.json")
	if err := os.WriteFile(path, []byte(`{"version":1,"entries":[{"digest":"sha256:`+strings.Repeat("0", 64)+`","type":"compute"}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	registry := orchestrator.NewNodeRegistry()
	registry.Register(orchestrator.NodeEntry{NodeID: "private-node", ParticipantID: "owner", Status: "online", CountryCode: "US", HardwareProfile: orchestrator.HardwareProfile{CPUCores: 4, RAMMB: 8192}})
	orch := orchestrator.New(nil, registry, nil, nil, path, false, 0)
	server := httptest.NewServer(NewInternal(orch, "").srv.Handler)
	defer server.Close()
	checker, ok := any(orchclient.New(server.URL)).(interface {
		PreflightJob(context.Context, orchestrator.SubmitJobRequest) (orchestrator.JobPreflight, error)
	})
	if !ok {
		t.Fatal("portal's HTTP client cannot check availability")
	}
	req := orchestrator.SubmitJobRequest{ConsumerID: "buyer", WorkloadType: types.MarketplaceBatchCompute, ContainerImage: "example/worker@sha256:" + strings.Repeat("0", 64), CPUCores: 2, RAMMB: 4096, CountryConstraint: "US"}
	got, err := checker.PreflightJob(context.Background(), req)
	if err != nil || !got.Ready || got.EligibleNodes != 1 || !got.HardwareChecked || got.CheckedAt.IsZero() {
		t.Fatalf("check through HTTP: %+v, %v", got, err)
	}
	req.CountryConstraint = "CA"
	got, err = checker.PreflightJob(context.Background(), req)
	if err != nil || got.Ready || len(got.Blockers) != 1 || got.Blockers[0].Code != "country" {
		t.Fatalf("country lost in transport: %+v, %v", got, err)
	}
	req.CPUCores = 0
	if _, err := checker.PreflightJob(context.Background(), req); !errors.Is(err, orchestrator.ErrInvalidPreflight) {
		t.Fatalf("validation error lost in transport: %v", err)
	}
	req.CPUCores = 2
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := checker.PreflightJob(context.Background(), req); err == nil || errors.Is(err, orchestrator.ErrInvalidPreflight) || strings.Contains(err.Error(), path) {
		t.Fatalf("configuration error must stay unavailable and sanitized: %v", err)
	}

}

func TestPreflightInternalBodyLimitsAndErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing-allowlist.json")
	orch := orchestrator.New(nil, orchestrator.NewNodeRegistry(), nil, nil, path, false, 0)
	server := httptest.NewServer(NewInternal(orch, "").srv.Handler)
	defer server.Close()
	req := orchestrator.SubmitJobRequest{ConsumerID: "buyer", WorkloadType: types.MarketplaceBatchCompute, ContainerImage: "example/worker@sha256:" + strings.Repeat("0", 64), CPUCores: 2, RAMMB: 4096}
	encoded, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(server.URL+"/internal/jobs/preflight", "application/json", strings.NewReader(string(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	responseBody, err := io.ReadAll(resp.Body)
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable || strings.Contains(string(responseBody), path) || !strings.Contains(string(responseBody), "preflight unavailable") {
		t.Fatalf("configuration details exposed: status %d body %s", resp.StatusCode, responseBody)
	}
	// Valid JSON with an ignored field distinguishes a body cap from decoder EOF.
	oversized := strings.TrimSuffix(string(encoded), "}") + `,"Pad":"` + strings.Repeat("x", 17000) + `"}`
	for _, body := range []string{"not json", oversized} {
		resp, err := http.Post(server.URL+"/internal/jobs/preflight", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		if err := resp.Body.Close(); err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("invalid body: status %d", resp.StatusCode)
		}
	}
}
