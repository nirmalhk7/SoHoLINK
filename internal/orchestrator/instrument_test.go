package orchestrator

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/agent"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/sounding"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/types"
)

// captureRecorder is a synchronous, in-memory sounding.Recorder for asserting
// exactly what the instrumentation emits. Unlike the real async Sink it records
// on the calling goroutine, so tests need no synchronization with a drain loop.
type captureRecorder struct {
	mu     sync.Mutex
	shapes []sounding.JobShape
	rejs   []sounding.Rejection
	caps   []sounding.CapacitySnapshot
}

func (r *captureRecorder) RecordJobShape(js sounding.JobShape) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.shapes = append(r.shapes, js)
}
func (r *captureRecorder) RecordRejection(rj sounding.Rejection) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rejs = append(r.rejs, rj)
}
func (r *captureRecorder) RecordCapacity(c sounding.CapacitySnapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.caps = append(r.caps, c)
}

// downSink models a telemetry sink whose backend is unavailable: its methods
// accept the record and drop it (returning nothing), exactly like the real
// async *Sink, which enqueues without ever surfacing a DB error to the caller.
// It is the realistic "broken sink" for a placement-level fail-open assertion.
// (A recorder that PANICS is a contract violation the real Sink never commits —
// Record* is non-blocking and panic-free — so it is deliberately not modeled;
// SubmitJob intentionally has no recover() that would mask genuine bugs.)
type downSink struct{ calls int }

func (d *downSink) RecordJobShape(sounding.JobShape)         { d.calls++ }
func (d *downSink) RecordRejection(sounding.Rejection)       { d.calls++ }
func (d *downSink) RecordCapacity(sounding.CapacitySnapshot) { d.calls++ }

func testLadder() sounding.Ladder {
	return sounding.NewLadder([]sounding.Tier{
		{Name: "cumulus", Order: 1, CPUCeiling: 2, MemCeiling: 4096, DiskCeiling: 20480, State: "available"},
		{Name: "congestus", Order: 2, CPUCeiling: 8, MemCeiling: 16384, DiskCeiling: 102400, State: "available"},
		{Name: "cumulonimbus", Order: 3, CPUCeiling: 32, MemCeiling: 65536, DiskCeiling: 512000, State: "available"},
		{Name: "storm", Order: 4, CPUCeiling: 128, MemCeiling: 262144, DiskCeiling: 2097152, State: "coming_soon"},
	})
}

func writeInstrAllowlist(t *testing.T) string {
	t.Helper()
	al := agent.Allowlist{
		Version:  1,
		IssuedAt: time.Now(),
		Entries: []agent.AllowlistEntry{{
			Name:   "soholink/compute-worker",
			Digest: "sha256:0000000000000000000000000000000000000000000000000000000000000000",
			Type:   agent.WorkloadCompute,
			Egress: agent.EgressOutbound,
		}},
	}
	data, err := json.Marshal(al)
	if err != nil {
		t.Fatalf("marshal allowlist: %v", err)
	}
	path := filepath.Join(t.TempDir(), "allowlist.json")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write allowlist: %v", err)
	}
	return path
}

const findNodesError = "find nodes"

const instrComputeImage = "soholink/compute-worker@sha256:0000000000000000000000000000000000000000000000000000000000000000"

// TestSubmitJob_Rejection_RecordsReasonAndOperatorID: on the no-nodes path
// (empty registry, which fails inside FindMatch BEFORE any DB access), the
// instrumentation must emit a rejection carrying the authenticated operator_id
// and a valid reason, plus an unplaced job-shape — while SubmitJob returns its
// usual findNodesError error unchanged.
func TestSubmitJob_Rejection_RecordsReasonAndOperatorID(t *testing.T) {
	rec := &captureRecorder{}
	orch := New(nil, NewNodeRegistry(), nil, nil, writeInstrAllowlist(t), false, 0)
	orch.AttachDemandSounding(rec, testLadder())

	ctx := sounding.ContextWithOperatorID(context.Background(), "cloudy")
	_, err := orch.SubmitJob(ctx, SubmitJobRequest{
		ConsumerID:     "c1",
		WorkloadType:   types.MarketplaceBatchCompute,
		ContainerImage: instrComputeImage,
		CPUCores:       4,
		RAMMB:          8192,
		StorageGB:      50,
	})
	if err == nil || !strings.Contains(err.Error(), findNodesError) {
		t.Fatalf("expected unchanged 'find nodes' error, got: %v", err)
	}

	if len(rec.rejs) != 1 {
		t.Fatalf("expected 1 rejection, got %d", len(rec.rejs))
	}
	rj := rec.rejs[0]
	if rj.OperatorID != "cloudy" {
		t.Errorf("rejection operator_id: got %q, want %q", rj.OperatorID, "cloudy")
	}
	// 4 vCPU / 8192 MB / 50GB fits congestus (available) → no_capacity, wanted=congestus.
	if rj.Reason != sounding.ReasonNoCapacity {
		t.Errorf("rejection reason: got %q, want %q", rj.Reason, sounding.ReasonNoCapacity)
	}
	if rj.WantedRung != "congestus" {
		t.Errorf("rejection wanted_rung: got %q, want %q", rj.WantedRung, "congestus")
	}
	if rj.WorkloadType != string(types.MarketplaceBatchCompute) {
		t.Errorf("rejection workload_type: got %q", rj.WorkloadType)
	}

	if len(rec.shapes) != 1 {
		t.Fatalf("expected 1 unplaced job-shape, got %d", len(rec.shapes))
	}
	if sh := rec.shapes[0]; sh.Placed || sh.Rung != "" || sh.OperatorID != "cloudy" {
		t.Errorf("unplaced shape wrong: %+v", sh)
	}
	// Rejection job-shape and rejection share the minted correlation job_id.
	if rec.shapes[0].JobID != rj.JobID || rj.JobID == "" {
		t.Errorf("shape/rejection job_id mismatch: shape=%q rej=%q", rec.shapes[0].JobID, rj.JobID)
	}
}

// TestSubmitJob_Rejection_TooBig: a job larger than every available tier is
// classified too_big with the coming_soon storm tier as the wanted rung.
func TestSubmitJob_Rejection_TooBig(t *testing.T) {
	rec := &captureRecorder{}
	orch := New(nil, NewNodeRegistry(), nil, nil, writeInstrAllowlist(t), false, 0)
	orch.AttachDemandSounding(rec, testLadder())

	_, err := orch.SubmitJob(context.Background(), SubmitJobRequest{
		ConsumerID:     "c1",
		WorkloadType:   types.MarketplaceBatchCompute,
		ContainerImage: instrComputeImage,
		CPUCores:       64, // over cumulonimbus's 32 vCPU ceiling
		RAMMB:          8192,
	})
	if err == nil || !strings.Contains(err.Error(), findNodesError) {
		t.Fatalf("expected 'find nodes' error, got: %v", err)
	}
	if len(rec.rejs) != 1 {
		t.Fatalf("expected 1 rejection, got %d", len(rec.rejs))
	}
	if rj := rec.rejs[0]; rj.Reason != sounding.ReasonTooBig || rj.WantedRung != "storm" {
		t.Errorf("expected too_big/storm, got %q/%q", rj.Reason, rj.WantedRung)
	}
	if rec.shapes[0].Footprint <= 1.0 {
		t.Errorf("expected footprint > 1 for a towering job, got %v", rec.shapes[0].Footprint)
	}
}

// TestSubmitJob_FailOpen_BrokenSinkDoesNotAffectPlacement: the no-nodes path
// exercises the instrumentation (recordRejection is called), so this is the
// path where a broken sink could leak into placement. With a "down" sink the
// SubmitJob error must be byte-for-byte identical to the nil-sink case, the
// instrumentation must have been invoked, and nothing must panic or block.
func TestSubmitJob_FailOpen_BrokenSinkDoesNotAffectPlacement(t *testing.T) {
	req := SubmitJobRequest{
		ConsumerID:     "c1",
		WorkloadType:   types.MarketplaceBatchCompute,
		ContainerImage: instrComputeImage,
		CPUCores:       4,
		RAMMB:          8192,
	}
	path := writeInstrAllowlist(t)

	nilOrch := New(nil, NewNodeRegistry(), nil, nil, path, false, 0)
	_, nilErr := nilOrch.SubmitJob(context.Background(), req)

	down := &downSink{}
	brokenOrch := New(nil, NewNodeRegistry(), nil, nil, path, false, 0)
	brokenOrch.AttachDemandSounding(down, testLadder())
	_, brokenErr := brokenOrch.SubmitJob(context.Background(), req)

	if nilErr == nil || brokenErr == nil {
		t.Fatalf("expected errors, got nil=%v broken=%v", nilErr, brokenErr)
	}
	if nilErr.Error() != brokenErr.Error() {
		t.Fatalf("telemetry changed the placement error: nil=%q broken=%q", nilErr.Error(), brokenErr.Error())
	}
	// The instrumentation was actually reached on this path (job-shape + rejection).
	if down.calls == 0 {
		t.Fatal("expected the broken sink to be invoked on the rejection path")
	}
}

// TestSubmitJob_NoSink_NoPanic: with telemetry unattached (the default), the
// no-nodes path behaves exactly as before — proving placement is unchanged when
// the sink is absent.
func TestSubmitJob_NoSink_NoPanic(t *testing.T) {
	orch := New(nil, NewNodeRegistry(), nil, nil, writeInstrAllowlist(t), false, 0)
	_, err := orch.SubmitJob(context.Background(), SubmitJobRequest{
		ConsumerID:     "c1",
		WorkloadType:   types.MarketplaceBatchCompute,
		ContainerImage: instrComputeImage,
		CPUCores:       4,
		RAMMB:          8192,
	})
	if err == nil || !strings.Contains(err.Error(), findNodesError) {
		t.Fatalf("expected 'find nodes' error with no sink, got: %v", err)
	}
}

// An unchecked GB-to-MB multiplication wraps large accepted requests negative.
func TestRejectionTelemetrySaturatesDiskOverflow(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("overflow requires a 64-bit int")
	}
	for _, tc := range []struct {
		name string
		gb   int64
		want int64
	}{
		{"ordinary", 20, 20480},
		{"last exact", 9007199254740991, 9223372036854774784},
		{"first overflow", 9007199254740992, math.MaxInt64},
		{"largest input", math.MaxInt64, math.MaxInt64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink := &captureRecorder{}
			o := New(nil, NewNodeRegistry(), nil, nil, writeInstrAllowlist(t), false, 0)
			o.AttachDemandSounding(sink, testLadder())
			_, err := o.SubmitJob(context.Background(), SubmitJobRequest{ConsumerID: "buyer", WorkloadType: types.MarketplaceBatchCompute, ContainerImage: instrComputeImage, CPUCores: 2, RAMMB: 4096, StorageGB: int(tc.gb)})
			if err == nil || !strings.Contains(err.Error(), findNodesError) {
				t.Fatalf("expected placement rejection, got %v", err)
			}
			if len(sink.shapes) != 1 || len(sink.rejs) != 1 {
				t.Fatalf("missing demand telemetry: %+v %+v", sink.shapes, sink.rejs)
			}
			if got := sink.shapes[0].DiskMB; got != tc.want {
				t.Fatalf("disk MB = %d, want %d", got, tc.want)
			}
		})
	}
}
