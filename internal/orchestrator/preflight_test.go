package orchestrator

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/agent"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/types"
)

func TestMatchExplanationUsesPlacementFilters(t *testing.T) {
	req := MatchRequest{WorkloadType: types.MarketplaceBatchCompute, CountryConstraint: "US", CPUCores: 4, RAMMB: 8192, StorageGB: 20, GPURequired: true, ExcludeConsumerParticipantID: "buyer"}
	for _, tc := range []struct {
		name   string
		change func(*NodeEntry)
		code   string
	}{
		{"eligible", func(n *NodeEntry) {}, ""},
		{"offline", func(n *NodeEntry) { n.Status = "offline" }, "offline"},
		{"country", func(n *NodeEntry) { n.CountryCode = "GB" }, "country"},
		{"gpu", func(n *NodeEntry) { n.HardwareProfile.GPUPresent = false }, "gpu"},
		{"cpu", func(n *NodeEntry) { n.HardwareProfile.CPUCores = 2 }, "cpu"},
		{"ram", func(n *NodeEntry) { n.HardwareProfile.RAMMB = 4096 }, "ram"},
		{"storage", func(n *NodeEntry) { n.HardwareProfile.StorageGB = 10 }, "storage"},
		{"consent", func(n *NodeEntry) { n.OptOutCompute = true }, "compute_disabled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := NewNodeRegistry()
			n := newOnlineNode("private-node", "US", 8, 16384, 100, true)
			tc.change(&n)
			r.Register(n)
			report := r.ExplainMatch(req)
			matches, _ := r.FindMatch(req)
			if report.EligibleNodes != len(matches) {
				t.Fatalf("preview %d != placement %d", report.EligibleNodes, len(matches))
			}
			if tc.code == "" {
				if report.EligibleNodes != 1 || len(report.Blockers) != 0 {
					t.Fatalf("unexpected report: %+v", report)
				}
			} else if len(report.Blockers) != 1 || report.Blockers[0].Code != tc.code || report.Blockers[0].Count != 1 {
				t.Fatalf("blockers = %+v, want %s x1", report.Blockers, tc.code)
			}
			encoded, _ := json.Marshal(report)
			if strings.Contains(string(encoded), "private-node") || strings.Contains(string(encoded), "test-provider") {
				t.Fatal("explanation leaked identities")
			}
		})
	}
}

func TestMatchExplanationHidesOwnAndExcludedNodes(t *testing.T) {
	r := NewNodeRegistry()
	own := newOnlineNode("own", "US", 8, 16384, 100, true)
	own.ParticipantID = "buyer"
	r.Register(own)
	r.Register(newOnlineNode("excluded", "US", 8, 16384, 100, true))
	report := r.ExplainMatch(MatchRequest{ExcludeConsumerParticipantID: "buyer", ExcludedNodeIDs: []string{"excluded"}})
	if report.EligibleNodes != 0 || len(report.Blockers) != 0 {
		t.Fatalf("excluded nodes entered report: %+v", report)
	}
}

func TestJobPreflightIsReadOnlyAndRechecksAvailability(t *testing.T) {
	r := NewNodeRegistry()
	r.Register(newOnlineNode("one", "US", 8, 16384, 100, false))
	// A nil database, token secret, and scheduler prove preflight never enters
	// dispatch, DB writes, payment, or job-token generation.
	o := New(nil, r, nil, nil, writeInstrAllowlist(t), false, 0)
	sink := &captureRecorder{}
	o.AttachDemandSounding(sink, testLadder())
	req := SubmitJobRequest{ConsumerID: "buyer", WorkloadType: types.MarketplaceBatchCompute, ContainerImage: instrComputeImage, CPUCores: 2, RAMMB: 4096}
	before := time.Now()
	preview, err := o.PreflightJob(req)
	if err != nil {
		t.Fatal(err)
	}
	if !preview.Ready || preview.EligibleNodes != 1 || preview.RequiredNodes != 1 || preview.CheckedAt.Before(before) {
		t.Fatalf("unexpected preview: %+v", preview)
	}
	if len(sink.shapes) != 0 || len(sink.rejs) != 0 {
		t.Fatal("preview polluted submitted demand")
	}
	n, _ := r.Get("one")
	if n.InFlight != 0 {
		t.Fatal("preview reserved capacity")
	}
	r.UpdateOptOut("one", NodeOptOutState{OptOutCompute: true})
	preview, err = o.PreflightJob(req)
	if err != nil || preview.Ready {
		t.Fatalf("preview ignored new opt-out: %+v %v", preview, err)
	}
	if _, err := o.SubmitJob(context.Background(), req); err == nil || !strings.Contains(err.Error(), "find nodes") {
		t.Fatalf("submission did not recheck: %v", err)
	}
}

func TestJobPreflightChecksImageAndComputeScope(t *testing.T) {
	o := New(nil, NewNodeRegistry(), nil, nil, writeInstrAllowlist(t), false, 0)
	req := SubmitJobRequest{ConsumerID: "buyer", WorkloadType: types.MarketplaceBatchCompute, ContainerImage: "ubuntu:latest", CPUCores: 2, RAMMB: 4096}
	preview, err := o.PreflightJob(req)
	if err != nil || preview.Ready || len(preview.Blockers) != 1 || preview.Blockers[0].Code != "image_not_allowed" {
		t.Fatalf("unpinned image: %+v %v", preview, err)
	}
	req.ContainerImage = instrComputeImage
	req.WorkloadType = types.MarketplaceObjectStorage
	if _, err := o.PreflightJob(req); err == nil {
		t.Fatal("storage entered compute-only preflight")
	}
	req.WorkloadType = types.MarketplaceBatchCompute
	o.allowlistPath = "missing-allowlist"
	if _, err := o.PreflightJob(req); err == nil {
		t.Fatal("missing configuration reported as available")
	}
}

func TestJobPreflightRejectsImageWorkloadMismatch(t *testing.T) {
	path := writeInstrAllowlist(t)
	al, err := loadAllowlist(path)
	if err != nil {
		t.Fatal(err)
	}
	al.Entries[0].Type = agent.WorkloadStorage
	data, err := json.Marshal(al)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	o := New(nil, NewNodeRegistry(), nil, nil, path, false, 0)
	req := SubmitJobRequest{ConsumerID: "buyer", WorkloadType: types.MarketplaceBatchCompute, ContainerImage: instrComputeImage, CPUCores: 2, RAMMB: 4096}
	result, err := o.PreflightJob(req)
	if err != nil || result.Ready || result.HardwareChecked || len(result.Blockers) != 1 || result.Blockers[0].Code != "workload_mismatch" {
		t.Fatalf("mismatched image: %+v %v", result, err)
	}
	if _, err := o.SubmitJob(context.Background(), req); err == nil || !strings.Contains(err.Error(), "workload type mismatch") {
		t.Fatalf("submission accepted mismatched image: %v", err)
	}
}

func TestMatchExplanationCountsOnlyFirstFailure(t *testing.T) {
	r := NewNodeRegistry()
	n := newOnlineNode("one", "GB", 1, 1024, 1, false)
	n.OptOutCompute = true
	r.Register(n)
	report := r.ExplainMatch(MatchRequest{WorkloadType: types.MarketplaceBatchCompute, CountryConstraint: "US", CPUCores: 8, RAMMB: 16384, StorageGB: 100, GPURequired: true})
	if len(report.Blockers) != 1 || report.Blockers[0].Code != "country" || report.Blockers[0].Count != 1 {
		t.Fatalf("multiple failures miscounted: %+v", report)
	}
}
