package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/sounding"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/types"
)

// HardwareProfile holds the agent-reported capabilities of a node.
type HardwareProfile struct {
	CPUCores      int
	RAMMB         int
	GPUPresent    bool
	StorageGB     int
	BandwidthMbps int
}

// NodeEntry is the in-memory representation of a registered node.
type NodeEntry struct {
	NodeID          string
	ParticipantID   string // owner of this node; matches participants.id in the DB. Legacy field name was ProviderID (pre-migration 011, before unified participants table).
	NodeClass       string
	CountryCode     string
	Region          string
	HardwareProfile HardwareProfile
	LastHeartbeat   time.Time
	Status          string

	// Opt-out fields, refreshed by handleHeartbeat after each heartbeat.
	// FindMatch uses these to skip nodes that have opted out of a workload
	// category. Agent-side enforcement remains the canonical gate; this is
	// defense-in-depth at dispatch time. Default zero values mean "not opted
	// out, no enabled printers" — safe for fresh nodes that haven't yet
	// reported via heartbeat.
	OptOutCompute     bool
	OptOutStorage     bool
	OptOutPrinting    bool
	HasEnabledPrinter bool

	// Advisory load state, refreshed by handleHeartbeat via UpdateLoad.
	// Self-reported and spoofable — consumed only by the scheduler's soft
	// idle-first scoring (never a hard filter). Zero values mean "never
	// sampled": LoadSampledAt.IsZero() marks the sample absent, and the
	// scheduler scores absent/stale samples 0.0 so silence never outranks
	// an honest busy reporter.
	OwnerActive   bool
	CPUUtilPct    float64
	LoadSampledAt time.Time

	// InFlight counts placements currently assigned to this node (advisory,
	// clamped >= 0 by AddInFlight). Incremented at placement/rebind time,
	// decremented on decline and on terminal-for-placement completion.
	InFlight int
}

// NodeLoadState carries the advisory load fields from a heartbeat into the
// in-memory registry via UpdateLoad.
type NodeLoadState struct {
	OwnerActive bool
	CPUUtilPct  float64 // 0-100 scale, matching the codebase CPU% convention
	SampledAt   time.Time
}

// NodeOptOutState carries opt-out flags from the DB into the in-memory
// registry via UpdateOptOut.
type NodeOptOutState struct {
	OptOutCompute     bool
	OptOutStorage     bool
	OptOutPrinting    bool
	HasEnabledPrinter bool
}

// MatchRequest describes the resource requirements for a workload placement.
type MatchRequest struct {
	// WorkloadType drives both dispatch and opt-out filtering: candidate nodes
	// that have opted out of the corresponding agent category (compute / storage
	// / printing) are excluded from the result. The agent-side opt-out store
	// remains the canonical enforcement gate; this filter is defense-in-depth.
	// WorkloadType="" disables opt-out filtering (legacy callers / tests).
	WorkloadType                 types.MarketplaceWorkloadType
	CountryConstraint            string // empty = any country
	CPUCores                     int
	RAMMB                        int
	GPURequired                  bool
	StorageGB                    int
	ExcludedNodeIDs              []string // nodes that have already declined this job
	ExcludeConsumerParticipantID string   // Exclude nodes owned by this participant for ALL workload types (approved operator decision, feat/protocol-integration): routing a job to hardware its own requester owns lets the platform take a share of a transaction the participant could perform unaided. Originally C5 print-only ("compute/storage self-use is legitimate"); that narrower rationale is superseded — the print history is preserved in the C5 commit trail.
}

// NodeRegistry is a concurrency-safe in-memory store of active nodes.
type NodeRegistry struct {
	mu    sync.RWMutex
	nodes map[string]NodeEntry
}

func NewNodeRegistry() *NodeRegistry {
	return &NodeRegistry{nodes: make(map[string]NodeEntry)}
}

// Register adds or replaces a node entry.
func (r *NodeRegistry) Register(entry NodeEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodes[entry.NodeID] = entry
}

// Heartbeat updates the lastHeartbeat timestamp for a node.
// Returns an error if the node is not registered.
func (r *NodeRegistry) Heartbeat(nodeID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.nodes[nodeID]
	if !ok {
		return fmt.Errorf("heartbeat: node %s not found", nodeID)
	}
	entry.LastHeartbeat = time.Now()
	r.nodes[nodeID] = entry
	return nil
}

// UpdateOptOut overwrites a node's opt-out fields. Returns an error if the
// node is not registered. Callers (typically handleHeartbeat) read current
// opt-out state from the DB and forward it here so FindMatch can filter
// without DB access.
func (r *NodeRegistry) UpdateOptOut(nodeID string, state NodeOptOutState) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.nodes[nodeID]
	if !ok {
		return fmt.Errorf("update opt-out: node %s not found", nodeID)
	}
	entry.OptOutCompute = state.OptOutCompute
	entry.OptOutStorage = state.OptOutStorage
	entry.OptOutPrinting = state.OptOutPrinting
	entry.HasEnabledPrinter = state.HasEnabledPrinter
	r.nodes[nodeID] = entry
	return nil
}

// UpdateLoad overwrites a node's advisory load fields. Returns an error if
// the node is not registered. Callers (typically handleHeartbeat) forward the
// heartbeat's self-reported load sample here so the scheduler's idle-first
// scoring can read it without DB access.
func (r *NodeRegistry) UpdateLoad(nodeID string, state NodeLoadState) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.nodes[nodeID]
	if !ok {
		return fmt.Errorf("update load: node %s not found", nodeID)
	}
	entry.OwnerActive = state.OwnerActive
	entry.CPUUtilPct = state.CPUUtilPct
	entry.LoadSampledAt = state.SampledAt
	r.nodes[nodeID] = entry
	return nil
}

// AddInFlight adjusts a node's advisory in-flight placement counter by delta,
// clamping at zero. Missing nodes are a silent no-op — the counter is a soft
// scheduling signal, never an invariant, so an eviction race must not fail
// the placement path that calls this.
func (r *NodeRegistry) AddInFlight(nodeID string, delta int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.nodes[nodeID]
	if !ok {
		return
	}
	entry.InFlight += delta
	if entry.InFlight < 0 {
		entry.InFlight = 0
	}
	r.nodes[nodeID] = entry
}

// IsOnline reports whether the node is present in the registry with
// Status "online". Used by the scheduled-staleness reaper to detect jobs
// bound to nodes that have been evicted or gone offline.
func (r *NodeRegistry) IsOnline(nodeID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.nodes[nodeID]
	return ok && entry.Status == "online"
}

// Get returns a copy of the node's registry entry, if present.
func (r *NodeRegistry) Get(nodeID string) (NodeEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.nodes[nodeID]
	return entry, ok
}

// Evict removes a node from the registry.
func (r *NodeRegistry) Evict(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.nodes, nodeID)
}

// FindMatch returns all online nodes that satisfy req.
// Go map iteration is intentionally random, so candidate order is
// non-deterministic. Phase 1 Step 4 (Scheduler) scores and ranks this list.
// CountryConstraint is a hard requirement when non-empty.
func (r *NodeRegistry) FindMatch(req MatchRequest) ([]NodeEntry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	candidates, _ := r.matchLocked(req)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no available nodes match request")
	}
	return candidates, nil
}

// CapacityInputs returns a point-in-time supply snapshot of every ONLINE node,
// shaped for demand-sounding aggregation (sounding.AggregateCapacity). It is a
// read-only helper for the capacity sampler; it does NOT touch matching or
// placement. Units mirror operator_capacity_snapshots: VCPUs from CPUCores,
// MemMB from RAMMB, DiskMB from StorageGB (×1024).
//
// OperatorID is set to sounding.OperatorUnknown: nodes carry a ParticipantID,
// not an operator_id, and the node→operator mapping (frontend-as-operator) is
// not yet wired. When it lands, populate OperatorID here.
func (r *NodeRegistry) CapacityInputs() []sounding.NodeCapacityInput {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]sounding.NodeCapacityInput, 0, len(r.nodes))
	for _, node := range r.nodes {
		if node.Status != "online" {
			continue
		}
		out = append(out, sounding.NodeCapacityInput{
			OperatorID:        sounding.OperatorUnknown,
			NodeClass:         node.NodeClass,
			VCPUs:             node.HardwareProfile.CPUCores,
			MemMB:             int64(node.HardwareProfile.RAMMB),
			DiskMB:            int64(node.HardwareProfile.StorageGB) * 1024,
			OptOutCompute:     node.OptOutCompute,
			OptOutStorage:     node.OptOutStorage,
			OptOutPrinting:    node.OptOutPrinting,
			HasEnabledPrinter: node.HasEnabledPrinter,
		})
	}
	return out
}

// StartEvictionLoop runs a background goroutine that evicts nodes whose last
// heartbeat is older than maxAge. It checks every 30 seconds and stops when
// ctx is cancelled.
func StartEvictionLoop(ctx context.Context, registry *NodeRegistry, maxAge time.Duration) {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				registry.evictStale(maxAge)
			}
		}
	}()
}

func (r *NodeRegistry) evictStale(maxAge time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cutoff := time.Now().Add(-maxAge)
	for id, node := range r.nodes {
		if node.LastHeartbeat.Before(cutoff) {
			delete(r.nodes, id)
		}
	}
}
