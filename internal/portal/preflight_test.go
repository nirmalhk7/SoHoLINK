package portal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/agent"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/orchestrator"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/scheduler"
)

const checkImage = "example/worker@sha256:0000000000000000000000000000000000000000000000000000000000000000"

func checkPortal(t *testing.T) (*PortalServer, *orchestrator.NodeRegistry) {
	t.Helper()
	al, err := json.Marshal(agent.Allowlist{Version: 1, Entries: []agent.AllowlistEntry{{Digest: strings.Split(checkImage, "@")[1], Type: agent.WorkloadCompute}}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "allowlist.json")
	if err := os.WriteFile(path, al, 0600); err != nil {
		t.Fatal(err)
	}
	r := orchestrator.NewNodeRegistry()
	r.Register(orchestrator.NodeEntry{NodeID: "secret-node", ParticipantID: "secret-owner", CountryCode: "US", Status: "online", HardwareProfile: orchestrator.HardwareProfile{CPUCores: 4, RAMMB: 8192, StorageGB: 40, GPUPresent: true}})
	return newTestPortalServerWithOrch(t, nil, orchestrator.New(nil, r, nil, nil, path, false, 0)), r
}

func jobForm() url.Values {
	return url.Values{"container_image": {checkImage}, "workload_type": {"batch_compute"}, "cpu_cores": {"2"}, "ram_mb": {"4096"}, "storage_gb": {"20"}, "country": {"US"}, "gpu_required": {"true"}}
}

func checkRequest(t *testing.T, ps *PortalServer, method, path string, form url.Values, authenticated bool) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if authenticated {
		r.AddCookie(&http.Cookie{Name: "session_token", Value: mustCreateToken(t, ps.sm, "buyer", "buyer@example.org")})
	}
	w := httptest.NewRecorder()
	ps.srv.Handler.ServeHTTP(w, r)
	return w
}

func TestJobCheckRequiresSessionAndRendersForm(t *testing.T) {
	ps, _ := checkPortal(t)
	for _, tc := range []struct{ method, path string }{{"GET", "/consumer/job/check"}, {"POST", "/consumer/job/check"}, {"POST", "/consumer/job/check/submit"}} {
		w := checkRequest(t, ps, tc.method, tc.path, jobForm(), false)
		if w.Code != http.StatusFound || w.Header().Get("Location") != "/login" {
			t.Fatalf("unprotected %s %s: %d", tc.method, tc.path, w.Code)
		}
	}
	w := checkRequest(t, ps, "GET", "/consumer/job/check", nil, true)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "Check availability") {
		t.Fatalf("form: %d %s", w.Code, w.Body.String())
	}
}

func TestJobCheckRendersRealEligibilityWithoutIdentities(t *testing.T) {
	ps, registry := checkPortal(t)
	w := checkRequest(t, ps, "POST", "/consumer/job/check", jobForm(), true)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "Matches available hardware") {
		t.Fatalf("preview: %d %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "Submit job") || !strings.Contains(w.Body.String(), "not a reservation") {
		t.Fatal("preview missing confirmation or availability caveat")
	}
	if strings.Contains(w.Body.String(), "secret-node") || strings.Contains(w.Body.String(), "secret-owner") {
		t.Fatal("preview disclosed node identity")
	}
	registry.UpdateOptOut("secret-node", orchestrator.NodeOptOutState{OptOutCompute: true})
	w = checkRequest(t, ps, "POST", "/consumer/job/check/submit", jobForm(), true)
	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "Cannot place this job now") {
		t.Fatalf("stale preview bypassed consent: %d %s", w.Code, w.Body.String())
	}
}

func TestJobCheckValidatesSameFieldsOnPreviewAndSubmit(t *testing.T) {
	ps, _ := checkPortal(t)
	for _, path := range []string{"/consumer/job/check", "/consumer/job/check/submit"} {
		for _, field := range []string{"cpu_cores", "ram_mb", "storage_gb"} {
			form := jobForm()
			form.Set(field, "-1")
			w := checkRequest(t, ps, "POST", path, form, true)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("%s %s: %d", path, field, w.Code)
			}
		}
		form := jobForm()
		form.Set("workload_type", "object_storage")
		if w := checkRequest(t, ps, "POST", path, form, true); w.Code != http.StatusBadRequest {
			t.Fatalf("storage accepted: %d", w.Code)
		}
	}
}

func TestJobCheckSubmissionPreservesRequestAndSessionIdentity(t *testing.T) {
	stub := &stubOrchestrator{}
	ps := newTestPortalServerWithOrch(t, nil, stub)
	form := jobForm()
	form.Set("consumer_id", "another-member")
	w := checkRequest(t, ps, "POST", "/consumer/job/check/submit", form, true)
	if w.Code != http.StatusSeeOther {
		t.Fatalf("submit: %d %s", w.Code, w.Body.String())
	}
	got := stub.lastReq
	if got.ConsumerID != "buyer" || got.CPUCores != 2 || got.RAMMB != 4096 || got.StorageGB != 20 || got.CountryConstraint != "US" || !got.GPURequired || got.ContainerImage != checkImage || got.WorkloadType != "batch_compute" {
		t.Fatalf("request changed: %+v", got)
	}
}

func TestJobCheckImageAndConfigurationErrors(t *testing.T) {
	ps, _ := checkPortal(t)
	form := jobForm()
	form.Set("container_image", "ubuntu:latest")
	w := checkRequest(t, ps, "POST", "/consumer/job/check", form, true)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "Image is not approved") || !strings.Contains(w.Body.String(), "Hardware availability was not checked") || strings.Contains(w.Body.String(), ">Submit job</button>") {
		t.Fatalf("image rejection: %d %s", w.Code, w.Body.String())
	}
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("availability response can be cached")
	}
	ps.orch = orchestrator.New(nil, orchestrator.NewNodeRegistry(), nil, nil, "/missing-private-allowlist", false, 0)
	w = checkRequest(t, ps, "POST", "/consumer/job/check", jobForm(), true)
	if w.Code != http.StatusServiceUnavailable || strings.Contains(w.Body.String(), "/missing-private-allowlist") {
		t.Fatalf("configuration failure: %d %s", w.Code, w.Body.String())
	}
}

// This uses the same disposable database fixture as existing portal tests.
// A successful preview writes nothing; confirmation must create exactly one
// scheduled job with the same constraints and the authenticated participant.
func TestJobCheckCreatesJobOnlyOnConfirmation(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	buyer := seedParticipant(t, db, "preflight-buyer@test.com", "pass1234")
	owner := seedParticipant(t, db, "preflight-owner@test.com", "pass1234")
	node := seedNode(t, db, owner, "online", "A", "US")
	path := filepath.Join(t.TempDir(), "allowlist.json")
	al, _ := json.Marshal(agent.Allowlist{Version: 1, Entries: []agent.AllowlistEntry{{Digest: strings.Split(checkImage, "@")[1], Type: agent.WorkloadCompute}}})
	if err := os.WriteFile(path, al, 0600); err != nil {
		t.Fatal(err)
	}
	registry := orchestrator.NewNodeRegistry()
	registry.Register(orchestrator.NodeEntry{NodeID: node, ParticipantID: owner, CountryCode: "US", Status: "online", HardwareProfile: orchestrator.HardwareProfile{CPUCores: 4, RAMMB: 8192, StorageGB: 40, GPUPresent: true}})
	orch := orchestrator.New(db, registry, []byte("test-only-job-token-secret"), scheduler.Schedule, path, false, 0)
	ps := newTestPortalServerWithOrch(t, db, orch)
	request := func(path string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", path, strings.NewReader(jobForm().Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.AddCookie(&http.Cookie{Name: "session_token", Value: mustCreateToken(t, ps.sm, buyer, "preflight-buyer@test.com")})
		w := httptest.NewRecorder()
		ps.srv.Handler.ServeHTTP(w, r)
		return w
	}
	w := request("/consumer/job/check")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "Matches available hardware") {
		t.Fatalf("preview: %d %s", w.Code, w.Body.String())
	}
	var count int
	if err := db.Pool.QueryRow(ctx, "SELECT count(*) FROM jobs").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("preview created %d jobs", count)
	}
	w = request("/consumer/job/check/submit")
	if w.Code != http.StatusSeeOther {
		t.Fatalf("submit: %d %s", w.Code, w.Body.String())
	}
	id := strings.TrimPrefix(w.Header().Get("Location"), "/consumer/job/")
	var participant, placedNode, status, country, image string
	var cpu, ram, disk int
	var gpu bool
	if err := db.Pool.QueryRow(ctx, "SELECT participant_id::text, node_id::text, status::text, country_constraint, container_image, cpu_cores, ram_mb, storage_gb, gpu_required FROM jobs WHERE id = $1", id).Scan(&participant, &placedNode, &status, &country, &image, &cpu, &ram, &disk, &gpu); err != nil {
		t.Fatal(err)
	}
	if participant != buyer || placedNode != node || status != "scheduled" || country != "US" || image != checkImage || cpu != 2 || ram != 4096 || disk != 20 || !gpu {
		t.Fatalf("persisted job differs: %s %s %s %s %s %d %d %d %v", participant, placedNode, status, country, image, cpu, ram, disk, gpu)
	}
	if err := db.Pool.QueryRow(ctx, "SELECT count(*) FROM jobs").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("confirmation created %d jobs, want 1", count)
	}
}
