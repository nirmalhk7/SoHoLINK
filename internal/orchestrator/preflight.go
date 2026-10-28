package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/agent"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/types"
)

// ErrInvalidPreflight distinguishes invalid input from unavailable configuration.
var ErrInvalidPreflight = errors.New("invalid compute job request")

var errWorkloadImageMismatch = errors.New("submit job: workload type mismatch")

// MatchBlocker counts nodes excluded by their FIRST failed placement check.
// Counts are not additive across independent filters: order matches FindMatch.
// Node IDs, owners, locations, and individual opt-out settings are never returned.
type MatchBlocker struct {
	Code  string
	Count int
}

// Ordered by the first failed placement check. The portal uses the same catalog
// to describe the aggregate reasons returned by the private API.
var blockerReasons = []struct{ code, description string }{
	{"offline", "Nodes are offline."},
	{"country", "Nodes are outside your required country."},
	{"gpu", "Nodes do not advertise a GPU."},
	{"cpu", "Nodes have fewer CPU cores than requested."},
	{"ram", "Nodes have less RAM than requested."},
	{"storage", "Nodes have less disk than requested."},
	{"compute_disabled", "Nodes are not offering compute work."},
	{"storage_disabled", "Nodes are not offering storage work."},
	{"printing_disabled", "Nodes are not offering printing work."},
	{"image_not_allowed", "Image is not approved. Use a digest-pinned image from the coordinator's allowlist."},
	{"workload_mismatch", "This approved image does not support the selected workload type."},
}

// Description explains a blocker without disclosing node identities. Unknown
// codes can arrive from a newer coordinator and must not imply an empty registry.
func (b MatchBlocker) Description() string {
	text := "Nodes cannot meet another placement requirement."
	for _, reason := range blockerReasons {
		if reason.code == b.Code {
			text = reason.description
			break
		}
	}
	if b.Code == "image_not_allowed" || b.Code == "workload_mismatch" {
		return text
	}
	return fmt.Sprintf("%d: %s", b.Count, text)
}

// MatchExplanation is a point-in-time aggregate, never a capacity reservation.
type MatchExplanation struct {
	EligibleNodes int
	Blockers      []MatchBlocker
}

// ExplainMatch evaluates exactly the same predicate as FindMatch under one
// registry read lock. Own and explicitly excluded nodes are invisible here.
func (r *NodeRegistry) ExplainMatch(req MatchRequest) MatchExplanation {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, explanation := r.matchLocked(req)
	return explanation
}

// matchLocked is the single placement-filter implementation. Callers hold r.mu.
// Keep check order stable so each excluded node contributes one reason only.
func (r *NodeRegistry) matchLocked(req MatchRequest) ([]NodeEntry, MatchExplanation) {
	excluded := make(map[string]bool, len(req.ExcludedNodeIDs))
	for _, id := range req.ExcludedNodeIDs {
		excluded[id] = true
	}
	var candidates []NodeEntry
	counts := make(map[string]int)
	for _, node := range r.nodes {
		if excluded[node.NodeID] || (req.ExcludeConsumerParticipantID != "" && node.ParticipantID == req.ExcludeConsumerParticipantID) {
			continue
		}
		if code := placementBlocker(node, req); code != "" {
			counts[code]++
			continue
		}
		candidates = append(candidates, node)
	}
	report := MatchExplanation{EligibleNodes: len(candidates)}
	for _, reason := range blockerReasons {
		if counts[reason.code] > 0 {
			report.Blockers = append(report.Blockers, MatchBlocker{Code: reason.code, Count: counts[reason.code]})
		}
	}
	return candidates, report
}

func placementBlocker(node NodeEntry, req MatchRequest) string {
	switch {
	case node.Status != "online":
		return "offline"
	case req.CountryConstraint != "" && node.CountryCode != req.CountryConstraint:
		return "country"
	case req.GPURequired && !node.HardwareProfile.GPUPresent:
		return "gpu"
	case node.HardwareProfile.CPUCores < req.CPUCores:
		return "cpu"
	case node.HardwareProfile.RAMMB < req.RAMMB:
		return "ram"
	case node.HardwareProfile.StorageGB < req.StorageGB:
		return "storage"
	}
	// Preserve legacy callers' empty/unknown workload behavior, as FindMatch did.
	if cat, err := MarketplaceToAgent(req.WorkloadType); err == nil {
		switch cat {
		case agent.WorkloadCompute:
			if node.OptOutCompute {
				return "compute_disabled"
			}
		case agent.WorkloadStorage:
			if node.OptOutStorage {
				return "storage_disabled"
			}
		case agent.WorkloadPrintTraditional, agent.WorkloadPrint3D:
			if node.OptOutPrinting || !node.HasEnabledPrinter {
				return "printing_disabled"
			}
		}
	}
	return ""
}

// JobPreflight exposes placement facts, never a selected node or job token.
type JobPreflight struct {
	MatchExplanation
	Ready           bool
	HardwareChecked bool
	RequiredNodes   int
	CheckedAt       time.Time
}

// PreflightJob checks a compute request without scheduling, DB access, demand
// telemetry, job creation, or payment. SubmitJob repeats validation and matching
// on confirmation. Readiness is registry eligibility, not a runtime guarantee:
// agents retain their local consent and execution gates.
func (o *Orchestrator) PreflightJob(_ context.Context, req SubmitJobRequest) (JobPreflight, error) {
	result := JobPreflight{RequiredNodes: int(SLAStandard), CheckedAt: time.Now().UTC()}
	if err := req.Validate(); err != nil {
		return result, fmt.Errorf("%w: %v", ErrInvalidPreflight, err)
	}
	if req.WorkloadType != types.MarketplaceAppHosting && req.WorkloadType != types.MarketplaceBatchCompute && req.WorkloadType != types.MarketplaceAIInference {
		return result, fmt.Errorf("%w: preflight supports compute workloads only", ErrInvalidPreflight)
	}
	if req.CPUCores <= 0 || req.RAMMB <= 0 || req.StorageGB < 0 || req.ContainerImage == "" {
		return result, fmt.Errorf("%w: image, positive CPU/RAM, and non-negative disk are required", ErrInvalidPreflight)
	}
	if err := o.validateSubmission(req); err != nil {
		switch {
		case errors.Is(err, agent.ErrImageNotAllowed):
			result.Blockers = []MatchBlocker{{Code: "image_not_allowed", Count: 1}}
		case errors.Is(err, errWorkloadImageMismatch):
			result.Blockers = []MatchBlocker{{Code: "workload_mismatch", Count: 1}}
		default:
			return result, err
		}
		return result, nil
	}
	result.MatchExplanation = o.registry.ExplainMatch(MatchRequest{
		WorkloadType: req.WorkloadType, CountryConstraint: req.CountryConstraint,
		CPUCores: req.CPUCores, RAMMB: req.RAMMB, StorageGB: req.StorageGB,
		GPURequired: req.GPURequired, ExcludeConsumerParticipantID: req.ConsumerID,
	})
	result.HardwareChecked = true
	result.Ready = result.EligibleNodes >= result.RequiredNodes
	return result, nil
}
