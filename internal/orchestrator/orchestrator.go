package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/agent"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/sounding"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/store"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/types"
)

const jobTokenTTL = 24 * time.Hour

// SLATier controls how many nodes are selected for placement.
type SLATier int

const (
	SLAStandard SLATier = 1 // single node
	SLAReliable SLATier = 2 // two nodes
	SLAPremium  SLATier = 3 // three nodes
)

// PlacementContext carries requester-side soft signals into the scheduler.
// Defined here (not in internal/scheduler) because the scheduler imports the
// orchestrator package; defining it scheduler-side would create an import
// cycle. All fields are soft: empty values simply contribute nothing to the
// locality score. RequesterCountry is NEVER copied into
// MatchRequest.CountryConstraint — the hard residency filter stays a
// consumer-stated constraint, not an inferred one.
type PlacementContext struct {
	RequesterParticipantID string
	RequesterRegion        string
	RequesterCountry       string
}

// ScheduleFunc scores and ranks a candidate list, returning the top N nodes
// for the given tier. Injected at construction to avoid a circular import
// between the orchestrator and scheduler packages. pctx supplies requester
// locality for soft scoring; pass PlacementContext{} when geo is irrelevant.
type ScheduleFunc func(candidates []NodeEntry, tier SLATier, pctx PlacementContext) ([]NodeEntry, error)

// SubmitJobRequest describes a consumer's workload placement request.
type SubmitJobRequest struct {
	ConsumerID        string
	WorkloadType      types.MarketplaceWorkloadType
	ContainerImage    string
	CountryConstraint string
	CPUCores          int
	RAMMB             int
	GPURequired       bool
	StorageGB         int
}

// Validate checks all required fields and returns the first error found.
func (r SubmitJobRequest) Validate() error {
	if r.ConsumerID == "" {
		return fmt.Errorf("ConsumerID is required")
	}
	if r.WorkloadType == "" {
		return fmt.Errorf("WorkloadType is required")
	}
	if !r.WorkloadType.IsValid() {
		return fmt.Errorf("unknown WorkloadType %q", r.WorkloadType)
	}
	return nil
}

// SubmitJobResponse carries the placement result returned to the consumer.
type SubmitJobResponse struct {
	JobID                   string
	NodeID                  string
	JobToken                string
	ProviderStripeAccountID string
}

// Orchestrator coordinates job placement across the node registry and database.
type Orchestrator struct {
	db                  *store.DB
	registry            *NodeRegistry
	tokenSecret         []byte
	schedule            ScheduleFunc
	allowlistPath       string
	printConfirmEnabled bool
	confirmationWindow  time.Duration

	// Demand-sounding telemetry (step 2). OBSERVATION ONLY: both are optional
	// and default to their zero values, so an Orchestrator built by New without
	// AttachDemandSounding records nothing and its placement behavior is
	// byte-for-byte unchanged. sink is nil-checked before every use; a nil sink
	// and a zero ladder are both safe. See instrument.go.
	sink   sounding.Recorder
	ladder sounding.Ladder
}

// AttachDemandSounding wires the demand-sounding telemetry sink and rung ladder
// onto the orchestrator. It is separate from New (rather than a constructor
// parameter) so that existing callers and tests keep the New signature and run
// with telemetry disabled. Call once at startup, before serving traffic; it is
// not safe to call concurrently with SubmitJob.
func (o *Orchestrator) AttachDemandSounding(sink sounding.Recorder, ladder sounding.Ladder) {
	o.sink = sink
	o.ladder = ladder
}

// New constructs an Orchestrator. schedule is called during SubmitJob to rank
// candidates; pass scheduler.Schedule from internal/scheduler.
// printConfirmEnabled gates the awaiting_confirmation path for print jobs.
// confirmationWindow sets the auto-decline deadline (e.g. 4*time.Hour).
func New(
	db *store.DB,
	registry *NodeRegistry,
	tokenSecret []byte,
	schedule ScheduleFunc,
	allowlistPath string,
	printConfirmEnabled bool,
	confirmationWindow time.Duration,
) *Orchestrator {
	return &Orchestrator{
		db:                  db,
		registry:            registry,
		tokenSecret:         tokenSecret,
		schedule:            schedule,
		allowlistPath:       allowlistPath,
		printConfirmEnabled: printConfirmEnabled,
		confirmationWindow:  confirmationWindow,
	}
}

// loadAllowlist reads and parses the signed allowlist from disk.
// Per Defense 3 design (B7 commit 5), the orchestrator does not verify
// the Ed25519 signature here — the operator-placed file is trusted; the
// agent's signature verification is the security boundary for workload
// identity. This loader is for consistency checking only.
//
// Fail-closed: any error here causes SubmitJob to reject. An empty or
// missing file means no submits are accepted, matching the agent's
// posture of "no allowlist = no work."
func loadAllowlist(path string) (*agent.Allowlist, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("load allowlist %s: %w", path, err)
	}
	al := &agent.Allowlist{}
	if err := json.Unmarshal(data, al); err != nil {
		return nil, fmt.Errorf("parse allowlist %s: %w", path, err)
	}
	return al, nil
}

// validateSubmission is shared by preflight and dispatch.
func (o *Orchestrator) validateSubmission(req SubmitJobRequest) error {
	if err := req.Validate(); err != nil {
		return fmt.Errorf("submit job: %w", err)
	}

	// Defense 3 (B7 commit 5): verify marketplace workload type, mapping,
	// and allowlist entry all agree on the workload's agent type. Fail-closed
	// on missing or unparseable allowlist (no allowlist = no submits).
	al, err := loadAllowlist(o.allowlistPath)
	if err != nil {
		return fmt.Errorf("submit job: %w", err)
	}
	entry, err := al.Lookup(req.ContainerImage)
	if err != nil {
		return fmt.Errorf("submit job: image not in allowlist: %w", err)
	}
	expectedAgentType, ok := marketplaceToAgent[req.WorkloadType]
	if !ok {
		// Should be unreachable: req.Validate() rejects unknown marketplace
		// types, and MustValidateWorkloadMapping at startup ensures every
		// known type has a mapping entry.
		return fmt.Errorf("submit job: no mapping for workload type %q", req.WorkloadType)
	}
	if entry.Type != expectedAgentType {
		return fmt.Errorf("%w: marketplace=%s maps to agent=%s, but allowlist entry for %s declares agent=%s",
			errWorkloadImageMismatch, req.WorkloadType, expectedAgentType, req.ContainerImage, entry.Type)
	}

	return nil
}

// SubmitJob validates the request, finds matching nodes, runs them through
// the scheduler, writes the job to PostgreSQL, generates a signed job token,
// and returns the placement result.
func (o *Orchestrator) SubmitJob(ctx context.Context, req SubmitJobRequest) (SubmitJobResponse, error) {
	if err := o.validateSubmission(req); err != nil {
		return SubmitJobResponse{}, err
	}

	candidates, err := o.registry.FindMatch(MatchRequest{
		WorkloadType:                 req.WorkloadType,
		CountryConstraint:            req.CountryConstraint,
		CPUCores:                     req.CPUCores,
		RAMMB:                        req.RAMMB,
		GPURequired:                  req.GPURequired,
		StorageGB:                    req.StorageGB,
		ExcludeConsumerParticipantID: req.ConsumerID,
	})
	if err != nil {
		// Placement rejection — the purest unmet-demand signal. Record it
		// fire-and-forget AFTER the decision, then return the placement error
		// unchanged. Telemetry never alters the error or blocks the return.
		o.recordRejection(ctx, req)
		return SubmitJobResponse{}, fmt.Errorf("find nodes: %w", err)
	}

	scheduled, err := o.schedule(candidates, SLAStandard, o.requesterPlacementContext(ctx, req.ConsumerID))
	if err != nil {
		return SubmitJobResponse{}, fmt.Errorf("schedule: %w", err)
	}
	node := scheduled[0]

	isPrintJob := req.WorkloadType == types.MarketplacePrintTraditional ||
		req.WorkloadType == types.MarketplacePrint3D

	jobID := uuid.New().String()

	tx, err := o.db.Pool.Begin(ctx)
	if err != nil {
		return SubmitJobResponse{}, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback on non-commit paths is intentional

	var countryConstraint *string
	if req.CountryConstraint != "" {
		countryConstraint = &req.CountryConstraint
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO jobs (
			id, participant_id, node_id, workload_type, status,
			country_constraint, cpu_cores, ram_mb, storage_gb, gpu_required,
			container_image
		) VALUES (
			$1, $2, $3, $4::workload_type, 'pending'::job_status,
			$5, $6, $7, $8, $9, $10
		)`,
		jobID, req.ConsumerID, node.NodeID, req.WorkloadType,
		countryConstraint, req.CPUCores, req.RAMMB, req.StorageGB, req.GPURequired,
		req.ContainerImage,
	)
	if err != nil {
		return SubmitJobResponse{}, fmt.Errorf("insert job: %w", err)
	}

	token, err := GenerateJobToken(jobID, node.NodeID, jobTokenTTL, o.tokenSecret)
	if err != nil {
		return SubmitJobResponse{}, fmt.Errorf("generate job token: %w", err)
	}

	if isPrintJob && o.printConfirmEnabled {
		// Resolve a specific enabled printer for this node. The HasEnabledPrinter
		// flag in FindMatch is set from heartbeat data; a brief registry/DB skew
		// is possible, so ErrNoRows here is a detectable synchronization gap.
		//
		// Picks the lowest printer_id lexicographically for determinism. Does not
		// yet discriminate by printer type — node_printers (migration 014) has no
		// type column, so a node enabled for any printing matches both
		// print_traditional and print_3d. Printer-type discrimination is a B8
		// concern; tracked in CLAUDE.md TODOs.
		var printerID string
		if err := tx.QueryRow(ctx,
			`SELECT printer_id FROM node_printers
			 WHERE node_id = $1 AND enabled = TRUE
			 ORDER BY printer_id LIMIT 1`,
			node.NodeID,
		).Scan(&printerID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return SubmitJobResponse{}, fmt.Errorf("resolve printer: node %s passed FindMatch but has no enabled printer (registry/DB drift)", node.NodeID)
			}
			return SubmitJobResponse{}, fmt.Errorf("resolve printer: %w", err)
		}

		specHash, err := canonicalJobSpecHash(req)
		if err != nil {
			return SubmitJobResponse{}, fmt.Errorf("spec hash: %w", err)
		}

		deadline := time.Now().Add(o.confirmationWindow)
		if _, err := tx.Exec(ctx, `
			UPDATE jobs
			SET job_token             = $1,
			    status                = 'awaiting_confirmation'::job_status,
			    printer_id            = $2,
			    spec_hash             = $3,
			    confirmation_deadline = $4,
			    updated_at            = NOW()
			WHERE id = $5`,
			token, printerID, specHash, deadline, jobID,
		); err != nil {
			return SubmitJobResponse{}, fmt.Errorf("update job to awaiting_confirmation: %w", err)
		}
	} else {
		if _, err := tx.Exec(ctx,
			`UPDATE jobs SET job_token = $1, status = 'scheduled'::job_status WHERE id = $2`,
			token, jobID,
		); err != nil {
			return SubmitJobResponse{}, fmt.Errorf("update job to scheduled: %w", err)
		}
	}

	var stripeAccountID string
	err = tx.QueryRow(ctx, `
		SELECT COALESCE(p.stripe_account_id, '')
		FROM participants p
		INNER JOIN nodes n ON n.participant_id = p.id
		WHERE n.id = $1`,
		node.NodeID,
	).Scan(&stripeAccountID)
	if err != nil {
		return SubmitJobResponse{}, fmt.Errorf("fetch provider stripe account: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return SubmitJobResponse{}, fmt.Errorf("commit transaction: %w", err)
	}

	// Advisory in-flight counter (B4): count the committed placement against
	// the chosen node so the scheduler's per-in-flight penalty spreads load.
	// Both scheduled and awaiting_confirmation placements count — the node is
	// spoken for until the job reaches a terminal-for-placement state.
	o.registry.AddInFlight(node.NodeID, +1)

	// Placement succeeded and is durably committed — record the placed job's
	// shape fire-and-forget. Runs only on the committed path so the record
	// reflects a real, persisted job_id.
	o.recordPlacement(ctx, req, jobID)

	return SubmitJobResponse{
		JobID:                   jobID,
		NodeID:                  node.NodeID,
		JobToken:                token,
		ProviderStripeAccountID: stripeAccountID,
	}, nil
}

// canonicalJobSpecHash returns a SHA-256 digest of the job spec fields that
// the contributor sees and acknowledges at confirmation time. Field declaration
// order is load-bearing — encoding/json marshals struct fields in declaration
// order, so reordering this struct silently changes all hashes.
func canonicalJobSpecHash(req SubmitJobRequest) ([]byte, error) {
	type spec struct {
		WorkloadType      string `json:"workload_type"`
		ContainerImage    string `json:"container_image"`
		CPUCores          int    `json:"cpu_cores"`
		RAMMB             int    `json:"ram_mb"`
		StorageGB         int    `json:"storage_gb"`
		GPURequired       bool   `json:"gpu_required"`
		CountryConstraint string `json:"country_constraint"`
	}
	b, err := json.Marshal(spec{
		WorkloadType:      string(req.WorkloadType),
		ContainerImage:    req.ContainerImage,
		CPUCores:          req.CPUCores,
		RAMMB:             req.RAMMB,
		StorageGB:         req.StorageGB,
		GPURequired:       req.GPURequired,
		CountryConstraint: req.CountryConstraint,
	})
	if err != nil {
		return nil, err
	}
	h := sha256.Sum256(b)
	return h[:], nil
}

// requesterPlacementContext builds the soft-locality context for the
// scheduler from the requester's participant row. The geo columns (migration
// 027) are nullable and unpopulated until the portal collects them; empty
// values mean the locality term contributes 0. A lookup failure degrades to
// an empty context with a warning — locality is a soft signal and must never
// fail placement.
func (o *Orchestrator) requesterPlacementContext(ctx context.Context, participantID string) PlacementContext {
	pctx := PlacementContext{RequesterParticipantID: participantID}
	if o.db == nil || participantID == "" {
		return pctx
	}
	if err := o.db.Pool.QueryRow(ctx,
		`SELECT COALESCE(country_code, ''), COALESCE(region, '') FROM participants WHERE id = $1`,
		participantID,
	).Scan(&pctx.RequesterCountry, &pctx.RequesterRegion); err != nil {
		slog.Warn("placement context: requester geo lookup failed; locality contributes 0",
			"participant_id", participantID, "error", err)
		pctx.RequesterCountry, pctx.RequesterRegion = "", ""
	}
	return pctx
}

// RerouteDeclinedJob attempts to find a new node for a job in 'declined' status.
// It reads the job's stored spec, queries job_node_declines for all nodes that
// have already declined, then calls FindMatch with those nodes excluded.
//
// No single transaction wraps the whole operation: the final UPDATE guards with
// AND status = 'declined'::job_status so concurrent reroute workers (or a future
// multi-instance deployment) see a no-op on a row another worker already moved
// forward. The two read queries (job row + decline records) running outside a
// transaction means we may read slightly stale data in a race, but the UPDATE
// guard makes the outcome safe regardless — at worst, we do redundant work that
// produces no row update.
func (o *Orchestrator) RerouteDeclinedJob(ctx context.Context, jobID string) error {
	var (
		workloadType          string
		cpuCores              int
		ramMB                 int
		storageGB             int
		gpuRequired           bool
		countryConstraint     string
		specHash              []byte
		consumerParticipantID string
		previousNodeID        string
	)
	err := o.db.Pool.QueryRow(ctx,
		`SELECT workload_type, COALESCE(cpu_cores, 0), COALESCE(ram_mb, 0),
		        COALESCE(storage_gb, 0), gpu_required, COALESCE(country_constraint, ''),
		        spec_hash, COALESCE(participant_id::text, ''), COALESCE(node_id::text, '')
		 FROM jobs WHERE id = $1`,
		jobID,
	).Scan(&workloadType, &cpuCores, &ramMB, &storageGB, &gpuRequired, &countryConstraint, &specHash, &consumerParticipantID, &previousNodeID)
	if err != nil {
		return fmt.Errorf("reroute: read job %s: %w", jobID, err)
	}

	rows, err := o.db.Pool.Query(ctx,
		`SELECT node_id FROM job_node_declines WHERE job_id = $1`, jobID)
	if err != nil {
		return fmt.Errorf("reroute: read declines for %s: %w", jobID, err)
	}
	var excludedIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return fmt.Errorf("reroute: scan decline: %w", err)
		}
		excludedIDs = append(excludedIDs, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reroute: declines rows: %w", err)
	}

	candidates, findErr := o.registry.FindMatch(MatchRequest{
		WorkloadType:                 types.MarketplaceWorkloadType(workloadType),
		CountryConstraint:            countryConstraint,
		CPUCores:                     cpuCores,
		RAMMB:                        ramMB,
		StorageGB:                    storageGB,
		GPURequired:                  gpuRequired,
		ExcludedNodeIDs:              excludedIDs,
		ExcludeConsumerParticipantID: consumerParticipantID,
	})
	if findErr != nil {
		// No eligible nodes remain — fail the job. The guarded UPDATE means a
		// concurrent worker that already moved the row forward makes this a
		// no-op; the in-flight decrement fires only when we actually flipped it.
		ct, err := o.db.Pool.Exec(ctx,
			`UPDATE jobs SET status = 'failed'::job_status, updated_at = NOW()
			 WHERE id = $1 AND status = 'declined'::job_status`,
			jobID,
		)
		if err != nil {
			return fmt.Errorf("reroute: fail job %s: %w", jobID, err)
		}
		if ct.RowsAffected() == 1 && previousNodeID != "" {
			o.registry.AddInFlight(previousNodeID, -1)
		}
		return nil
	}

	scheduled, err := o.schedule(candidates, SLAStandard, o.requesterPlacementContext(ctx, consumerParticipantID))
	if err != nil {
		return fmt.Errorf("reroute: schedule %s: %w", jobID, err)
	}
	node := scheduled[0]

	token, err := GenerateJobToken(jobID, node.NodeID, jobTokenTTL, o.tokenSecret)
	if err != nil {
		return fmt.Errorf("reroute: generate token: %w", err)
	}

	isPrint := workloadType == string(types.MarketplacePrintTraditional) ||
		workloadType == string(types.MarketplacePrint3D)

	var ct pgconn.CommandTag
	if isPrint {
		var printerID string
		if err := o.db.Pool.QueryRow(ctx,
			`SELECT printer_id FROM node_printers
			 WHERE node_id = $1 AND enabled = TRUE
			 ORDER BY printer_id LIMIT 1`,
			node.NodeID,
		).Scan(&printerID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("reroute: node %s passed FindMatch but has no enabled printer (registry/DB drift)", node.NodeID)
			}
			return fmt.Errorf("reroute: resolve printer: %w", err)
		}

		deadline := time.Now().Add(o.confirmationWindow)
		ct, err = o.db.Pool.Exec(ctx,
			`UPDATE jobs
			 SET node_id              = $1,
			     job_token            = $2,
			     printer_id           = $3,
			     confirmation_deadline = $4,
			     status               = 'awaiting_confirmation'::job_status,
			     updated_at           = NOW()
			 WHERE id = $5 AND status = 'declined'::job_status`,
			node.NodeID, token, printerID, deadline, jobID,
		)
		if err != nil {
			return fmt.Errorf("reroute: update job %s: %w", jobID, err)
		}
	} else {
		// Compute/storage declines (protocol Decline path) carry no printer
		// and no per-job confirmation: rebind straight back to scheduled so
		// the next node's poll picks the job up.
		ct, err = o.db.Pool.Exec(ctx,
			`UPDATE jobs
			 SET node_id    = $1,
			     job_token  = $2,
			     status     = 'scheduled'::job_status,
			     updated_at = NOW()
			 WHERE id = $3 AND status = 'declined'::job_status`,
			node.NodeID, token, jobID,
		)
		if err != nil {
			return fmt.Errorf("reroute: update job %s: %w", jobID, err)
		}
	}

	// Advisory in-flight rebind accounting (B4): only when the guarded UPDATE
	// actually moved the row. The previous node may already have been
	// decremented by a protocol Decline — AddInFlight clamps at zero, and the
	// counter is a soft scheduling signal, so the occasional double decrement
	// is accepted rather than tracked.
	if ct.RowsAffected() == 1 {
		o.registry.AddInFlight(node.NodeID, +1)
		if previousNodeID != "" {
			o.registry.AddInFlight(previousNodeID, -1)
		}
	}
	return nil
}

// RescheduleStaleJob rebinds a scheduled job away from a node that is no
// longer online (evicted from the registry or marked offline). Mirrors
// RerouteDeclinedJob's exclusion + guarded-UPDATE shape, with one deliberate
// divergence: when no candidates remain the job is LEFT in scheduled with a
// warning rather than failed — under idle-first placement a machine that went
// quiet may wake and reclaim its queue, and the reaper retries on every tick.
//
// Print workloads are skipped: a scheduled print job's node binding was
// produced by (or gated on) the contributor-confirmation flow, and silently
// rebinding it would bypass that consent path.
func (o *Orchestrator) RescheduleStaleJob(ctx context.Context, jobID, oldNodeID string) error {
	var (
		workloadType          string
		cpuCores              int
		ramMB                 int
		storageGB             int
		gpuRequired           bool
		countryConstraint     string
		consumerParticipantID string
	)
	err := o.db.Pool.QueryRow(ctx,
		`SELECT workload_type, COALESCE(cpu_cores, 0), COALESCE(ram_mb, 0),
		        COALESCE(storage_gb, 0), gpu_required, COALESCE(country_constraint, ''),
		        COALESCE(participant_id::text, '')
		 FROM jobs WHERE id = $1`,
		jobID,
	).Scan(&workloadType, &cpuCores, &ramMB, &storageGB, &gpuRequired, &countryConstraint, &consumerParticipantID)
	if err != nil {
		return fmt.Errorf("reschedule stale: read job %s: %w", jobID, err)
	}

	if workloadType == string(types.MarketplacePrintTraditional) ||
		workloadType == string(types.MarketplacePrint3D) {
		return nil
	}

	rows, err := o.db.Pool.Query(ctx,
		`SELECT node_id FROM job_node_declines WHERE job_id = $1`, jobID)
	if err != nil {
		return fmt.Errorf("reschedule stale: read declines for %s: %w", jobID, err)
	}
	excludedIDs := []string{oldNodeID}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return fmt.Errorf("reschedule stale: scan decline: %w", err)
		}
		excludedIDs = append(excludedIDs, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reschedule stale: declines rows: %w", err)
	}

	candidates, findErr := o.registry.FindMatch(MatchRequest{
		WorkloadType:                 types.MarketplaceWorkloadType(workloadType),
		CountryConstraint:            countryConstraint,
		CPUCores:                     cpuCores,
		RAMMB:                        ramMB,
		StorageGB:                    storageGB,
		GPURequired:                  gpuRequired,
		ExcludedNodeIDs:              excludedIDs,
		ExcludeConsumerParticipantID: consumerParticipantID,
	})
	if findErr != nil {
		// Deliberate divergence from RerouteDeclinedJob: leave the job
		// scheduled and retry next tick — the bound node may wake.
		slog.Warn("reschedule stale: no candidates; leaving job scheduled",
			"job_id", jobID, "node_id", oldNodeID, "error", findErr)
		return nil
	}

	scheduled, err := o.schedule(candidates, SLAStandard, o.requesterPlacementContext(ctx, consumerParticipantID))
	if err != nil {
		return fmt.Errorf("reschedule stale: schedule %s: %w", jobID, err)
	}
	node := scheduled[0]

	token, err := GenerateJobToken(jobID, node.NodeID, jobTokenTTL, o.tokenSecret)
	if err != nil {
		return fmt.Errorf("reschedule stale: generate token: %w", err)
	}

	ct, err := o.db.Pool.Exec(ctx,
		`UPDATE jobs
		 SET node_id    = $1,
		     job_token  = $2,
		     updated_at = NOW()
		 WHERE id = $3 AND status = 'scheduled'::job_status AND node_id = $4`,
		node.NodeID, token, jobID, oldNodeID,
	)
	if err != nil {
		return fmt.Errorf("reschedule stale: update job %s: %w", jobID, err)
	}
	if ct.RowsAffected() == 1 {
		o.registry.AddInFlight(node.NodeID, +1)
		o.registry.AddInFlight(oldNodeID, -1)
	}
	return nil
}

// rescheduleStale finds scheduled non-print jobs bound to nodes that are no
// longer online and rebinds each via RescheduleStaleJob. Called on every tick
// of StartDeclineRerouteLoop, after expireDispatched and before
// expirePickedUp/rerouteDeclined.
func (o *Orchestrator) rescheduleStale(ctx context.Context) {
	rows, err := o.db.Pool.Query(ctx,
		`SELECT id, COALESCE(node_id::text, '') FROM jobs
		 WHERE status = 'scheduled'::job_status
		   AND workload_type NOT IN ('print_traditional'::workload_type, 'print_3d'::workload_type)
		 LIMIT 100`)
	if err != nil {
		slog.Error("reschedule stale: query scheduled jobs", "error", err)
		return
	}
	type binding struct{ jobID, nodeID string }
	var bindings []binding
	for rows.Next() {
		var b binding
		if err := rows.Scan(&b.jobID, &b.nodeID); err != nil {
			slog.Error("reschedule stale: scan job", "error", err)
			rows.Close()
			return
		}
		bindings = append(bindings, b)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		slog.Error("reschedule stale: rows", "error", err)
		return
	}

	for _, b := range bindings {
		if b.nodeID == "" || o.registry.IsOnline(b.nodeID) {
			continue
		}
		if err := o.RescheduleStaleJob(ctx, b.jobID, b.nodeID); err != nil {
			slog.Error("reschedule stale job", "job_id", b.jobID, "node_id", b.nodeID, "error", err)
		}
	}
}

// rerouteDeclined finds all declined jobs and attempts to reroute each one.
// Called on every tick of StartDeclineRerouteLoop.
func (o *Orchestrator) rerouteDeclined(ctx context.Context) {
	rows, err := o.db.Pool.Query(ctx,
		`SELECT id FROM jobs WHERE status = 'declined'::job_status LIMIT 100`)
	if err != nil {
		slog.Error("reroute: query declined jobs", "error", err)
		return
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			slog.Error("reroute: scan job id", "error", err)
			rows.Close()
			return
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		slog.Error("reroute: declined rows", "error", err)
		return
	}

	for _, id := range ids {
		if err := o.RerouteDeclinedJob(ctx, id); err != nil {
			slog.Error("reroute declined job", "job_id", id, "error", err)
		}
	}
}

// ExpireConfirmation flips a single job from awaiting_confirmation to declined
// if its deadline has passed. Returns flipped=true when the row was actually
// changed; flipped=false with err=nil is the lost-race case (a portal action
// confirmed or extended the row between the caller's SELECT and this UPDATE).
// Race-safe: the UPDATE re-checks both status and confirmation_deadline.
func (o *Orchestrator) ExpireConfirmation(ctx context.Context, jobID string) (bool, error) {
	ct, err := o.db.Pool.Exec(ctx,
		`UPDATE jobs
		 SET status      = 'declined'::job_status,
		     declined_at = NOW(),
		     updated_at  = NOW()
		 WHERE id = $1
		   AND status = 'awaiting_confirmation'::job_status
		   AND confirmation_deadline < NOW()`,
		jobID,
	)
	if err != nil {
		return false, fmt.Errorf("expire: update job %s: %w", jobID, err)
	}
	return ct.RowsAffected() == 1, nil
}

// expireConfirmations finds awaiting_confirmation jobs whose deadline has
// passed and flips each to declined via ExpireConfirmation. Called on every
// tick of StartDeclineRerouteLoop, before rerouteDeclined so freshly-expired
// jobs are rerouted in the same tick.
func (o *Orchestrator) expireConfirmations(ctx context.Context) {
	rows, err := o.db.Pool.Query(ctx,
		`SELECT id FROM jobs
		 WHERE status = 'awaiting_confirmation'::job_status
		   AND confirmation_deadline < NOW()
		 LIMIT 100`)
	if err != nil {
		slog.Error("expire: query awaiting_confirmation jobs", "error", err)
		return
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			slog.Error("expire: scan job id", "error", err)
			rows.Close()
			return
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		slog.Error("expire: awaiting_confirmation rows", "error", err)
		return
	}

	for _, id := range ids {
		flipped, err := o.ExpireConfirmation(ctx, id)
		if err != nil {
			slog.Error("expire: update job to declined", "job_id", id, "error", err)
			continue
		}
		if flipped {
			slog.Info("auto-declined expired confirmation", "job_id", id)
		} else {
			slog.Debug("expire: lost race on auto-decline", "job_id", id)
		}
	}
}

// ExpireDispatched flips a single job from dispatched back to scheduled if it
// has been in dispatched state for more than 2 minutes without a /started
// confirmation. Returns flipped=true when the row was actually changed;
// flipped=false with err=nil is the lost-race case (the agent called /started
// between the caller's SELECT and this UPDATE).
// Race-safe: the UPDATE re-checks both status and updated_at.
func (o *Orchestrator) ExpireDispatched(ctx context.Context, jobID string) (bool, error) {
	ct, err := o.db.Pool.Exec(ctx,
		`UPDATE jobs
		 SET status     = 'scheduled'::job_status,
		     updated_at = NOW()
		 WHERE id = $1
		   AND status = 'dispatched'::job_status
		   AND updated_at < NOW() - INTERVAL '2 minutes'`,
		jobID,
	)
	if err != nil {
		return false, fmt.Errorf("expire dispatched: update job %s: %w", jobID, err)
	}
	return ct.RowsAffected() == 1, nil
}

// expireDispatched finds dispatched jobs that have not received a /started
// confirmation within 2 minutes and reverts each to scheduled. Called on
// every tick of StartDeclineRerouteLoop, after expireConfirmations and before
// rerouteDeclined.
func (o *Orchestrator) expireDispatched(ctx context.Context) {
	rows, err := o.db.Pool.Query(ctx,
		`SELECT id FROM jobs
		 WHERE status = 'dispatched'::job_status
		   AND updated_at < NOW() - INTERVAL '2 minutes'
		 LIMIT 100`)
	if err != nil {
		slog.Error("expire dispatched: query dispatched jobs", "error", err)
		return
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			slog.Error("expire dispatched: scan job id", "error", err)
			rows.Close()
			return
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		slog.Error("expire dispatched: rows", "error", err)
		return
	}

	for _, id := range ids {
		flipped, err := o.ExpireDispatched(ctx, id)
		if err != nil {
			slog.Error("expire dispatched: update job", "job_id", id, "error", err)
			continue
		}
		if flipped {
			slog.Info("dispatched expired — returned to scheduled", "job_id", id)
		} else {
			slog.Debug("expire dispatched lost race", "job_id", id)
		}
	}
}

// ExpirePickedUp advances a single picked_up print job to delivered when the
// 7-day dispute window has elapsed. Returns (true, nil) if the job was
// advanced, (false, nil) if the window has not elapsed or the job is no longer
// in picked_up status (lost-race). Callers treat flipped=false, err=nil as benign.
//
// C5: does NOT call ComputeMetering — print metering is deferred to C9.
func (o *Orchestrator) ExpirePickedUp(ctx context.Context, jobID string) (bool, error) {
	ct, err := o.db.Pool.Exec(ctx,
		`UPDATE jobs
		 SET status       = 'delivered'::job_status,
		     delivered_at = NOW(),
		     completed_at = NOW(),
		     updated_at   = NOW()
		 WHERE id = $1
		   AND status = 'picked_up'::job_status
		   AND picked_up_at < NOW() - INTERVAL '7 days'`,
		jobID,
	)
	if err != nil {
		return false, fmt.Errorf("expire picked_up: update job %s: %w", jobID, err)
	}
	return ct.RowsAffected() == 1, nil
}

// expirePickedUp finds picked_up print jobs whose 7-day dispute window has
// elapsed and auto-advances each to delivered. Called on every tick of
// StartDeclineRerouteLoop, after expireDispatched and before rerouteDeclined.
//
// C5: no metering — print payout eligibility is deferred to C9.
func (o *Orchestrator) expirePickedUp(ctx context.Context) {
	rows, err := o.db.Pool.Query(ctx,
		`SELECT id FROM jobs
		 WHERE status = 'picked_up'::job_status
		   AND picked_up_at < NOW() - INTERVAL '7 days'
		 LIMIT 100`)
	if err != nil {
		slog.Error("expire picked_up: query", "error", err)
		return
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			slog.Error("expire picked_up: scan", "error", err)
			rows.Close()
			return
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		slog.Error("expire picked_up: rows", "error", err)
		return
	}

	for _, id := range ids {
		flipped, err := o.ExpirePickedUp(ctx, id)
		if err != nil {
			slog.Error("expire picked_up: update job", "job_id", id, "error", err)
			continue
		}
		if flipped {
			slog.Info("picked_up expired — auto-advanced to delivered", "job_id", id)
		} else {
			slog.Debug("expire picked_up lost race", "job_id", id)
		}
	}
}

// StartDeclineRerouteLoop runs a background goroutine that periodically (a)
// auto-declines awaiting_confirmation jobs whose deadline has passed, (b)
// reverts stale dispatched jobs back to scheduled, (c) rebinds scheduled jobs
// whose node is no longer online, (d) auto-advances stale picked_up print
// jobs to delivered, and (e) re-dispatches declined jobs to a different node.
// All five passes run on the same 30s ticker in expire-then-reroute order.
// Stops when ctx is cancelled. Run only from cmd/orchestrator — not from
// cmd/portal, which has a separate registry instance that never receives
// agent heartbeats.
func (o *Orchestrator) StartDeclineRerouteLoop(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				o.expireConfirmations(ctx)
				o.expireDispatched(ctx)
				o.rescheduleStale(ctx)
				o.expirePickedUp(ctx)
				o.rerouteDeclined(ctx)
			}
		}
	}()
}
