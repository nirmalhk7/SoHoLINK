package orchestrator

import (
	"context"
	"math"

	"github.com/google/uuid"

	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/sounding"
)

// This file holds the demand-sounding instrumentation invoked from SubmitJob.
// Every function here is OBSERVATION ONLY and must be hot-path-safe:
//   - guarded by a nil-sink check (telemetry disabled => zero work),
//   - reads only already-computed request fields + the immutable ladder,
//   - hands off to the sink's non-blocking, fail-open enqueue,
//   - never returns an error and never alters placement control flow.

// shapeDims projects a SubmitJobRequest onto the demand-sounding units:
// CPU in vCPU, memory in MB, disk in MB (StorageGB is GB → ×1024).
func shapeDims(req SubmitJobRequest) (cpu float64, memMB, diskMB int64) {
	disk := int64(req.StorageGB)
	if disk > math.MaxInt64/1024 {
		diskMB = math.MaxInt64
	} else {
		diskMB = disk * 1024
	}
	return float64(req.CPUCores), int64(req.RAMMB), diskMB
}

// recordPlacement records the shape of a job that WAS placed (Placed=true),
// tagged with the rung it fit. jobID is the real, committed job UUID.
func (o *Orchestrator) recordPlacement(ctx context.Context, req SubmitJobRequest, jobID string) {
	if o.sink == nil {
		return
	}
	cpu, memMB, diskMB := shapeDims(req)
	rung, _ := o.ladder.FitRung(cpu, memMB, diskMB)
	o.sink.RecordJobShape(sounding.JobShape{
		OperatorID:   sounding.OperatorIDOrUnknown(ctx),
		JobID:        jobID,
		WorkloadType: string(req.WorkloadType),
		Intensity:    o.ladder.Intensity(cpu),
		DurationEst:  0, // SubmitJobRequest carries no duration estimate yet
		CPU:          cpu,
		MemMB:        memMB,
		DiskMB:       diskMB,
		Footprint:    o.ladder.Footprint(cpu, memMB, diskMB),
		Placed:       true,
		Rung:         rung,
	})
}

// recordRejection records a placement rejection (the unmet-demand signal) plus
// the shape of the unplaced job (Placed=false). The rejected job never gets a
// jobs-table row, so a telemetry-only UUID is minted for correlation — job_id
// has no FK in the demand-sounding tables (migration 025), so this is sound.
func (o *Orchestrator) recordRejection(ctx context.Context, req SubmitJobRequest) {
	if o.sink == nil {
		return
	}
	cpu, memMB, diskMB := shapeDims(req)
	footprint := o.ladder.Footprint(cpu, memMB, diskMB)
	reason, wantedRung := o.ladder.ClassifyRejection(cpu, memMB, diskMB)
	op := sounding.OperatorIDOrUnknown(ctx)
	jobID := uuid.New().String()
	wt := string(req.WorkloadType)

	o.sink.RecordJobShape(sounding.JobShape{
		OperatorID:   op,
		JobID:        jobID,
		WorkloadType: wt,
		Intensity:    o.ladder.Intensity(cpu),
		DurationEst:  0,
		CPU:          cpu,
		MemMB:        memMB,
		DiskMB:       diskMB,
		Footprint:    footprint,
		Placed:       false,
		Rung:         "",
	})
	o.sink.RecordRejection(sounding.Rejection{
		OperatorID:   op,
		JobID:        jobID,
		WorkloadType: wt,
		Reason:       reason,
		Footprint:    footprint,
		WantedRung:   wantedRung,
	})
}
