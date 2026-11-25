package api

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/NTARI-RAND/sohocloud-protocol/canon"
	"github.com/NTARI-RAND/sohocloud-protocol/fees"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/notify"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/operator"
)

// This file implements the LOCAL-ONLY :8090 GOVERNANCE backend (step 4). It is
// the admin surface reached over an SSH tunnel, NEVER exposed on public
// soholink.org (governance-separation invariant, CLAUDE.md). Three actions:
//
//   (a) FEES     — sign+publish a sohocloud-protocol fees.FeeDeclaration with the
//                  coordinator's key (loaded from env at construction, NEVER
//                  hardcoded, NEVER reachable from a public handler); monotonic
//                  Seq + non-retroactive EffectiveAt per SPEC §5.3. The signed
//                  current declaration is exposed for the PUBLIC /fees read via
//                  the repository, not via this port.
//   (b) DISCONNECT / REVOKE an operator — status='revoked' so GetActiveKeyMap
//                  immediately returns nil for it (kill switch, high blast radius).
//   (c) MESSAGING — compose a message and send to operators and/or members
//                  (selectable separately or together) via the net/smtp Notifier.
//                  Members are TRANSITIONAL (Cloudy-owned; see MemberEmails).
//
// GOVERNANCE SEPARATION / NO SSRF: the coordinator private key lives ONLY in this
// process, only in GovernanceServer.coordKey, and is referenced ONLY by the fees
// handler on this mux. No public handler imports or reaches this type. The server
// additionally binds loopback and enforces a loopback-source guard on every
// request as defense-in-depth, so even a misconfigured bind or an SSRF attempt
// from a public handler to 127.0.0.1:8090 is rejected at the handler.

// governanceRepo is the subset of *operator.Repository the :8090 handlers need.
// An interface so the handlers can be unit-tested with a fake (no DB).
type governanceRepo interface {
	PublishFeeDeclaration(ctx context.Context, decl fees.FeeDeclaration) error
	CurrentFeeDeclaration(ctx context.Context, coordinatorID string) (fees.FeeDeclaration, error)
	Revoke(ctx context.Context, operatorID string) error
	Disconnect(ctx context.Context, operatorID string) error
	OperatorEmails(ctx context.Context) ([]string, error)
	MemberEmails(ctx context.Context) ([]string, error)
}

// GovernanceServer is the :8090 admin backend. It holds the coordinator signing
// key (from env), the operator repository, and the Notifier for messaging. It is
// constructed only by the orchestrator/admin binary, which loads the key from a
// secret — never by any public-facing wiring.
type GovernanceServer struct {
	repo          governanceRepo
	notifier      notify.Notifier
	coordKey      ed25519.PrivateKey // coordinator signing key; NEVER logged, NEVER on a public handler
	coordinatorID string             // this coordinator's id, bound into every FeeDeclaration
	srv           *http.Server

	// Console (Stage-4 step 3) GET-page fields, populated by ConfigureConsole
	// (governance_console.go). Zero-valued on a POST-only server: the GET routes
	// then render a 500 "template not found", the same failure mode as the portal
	// with a missing template, and the static-asset route 404s. These stay on
	// the LOCAL-ONLY :8090 mux.
	consoleRepo   consoleGovRepo
	templatePaths []string
	staticDir     string

	// sounding is the demand-sounding read model behind GET /admin/sounding,
	// populated by ConfigureSounding (governance_sounding.go). Nil on a server
	// without that call: the route renders a 500, the same failure mode as an
	// unconfigured console. Stays on the LOCAL-ONLY :8090 mux.
	sounding soundingReadModel
}

// GovernanceConfig configures the :8090 server. CoordinatorKey and CoordinatorID
// come from env/secret at the call site (house rule: no secrets in source).
// Addr MUST be a loopback address (e.g. "127.0.0.1:8090"); NewGovernanceServer
// rejects a non-loopback bind so the surface cannot be published by
// misconfiguration.
type GovernanceConfig struct {
	Addr           string             // loopback only, e.g. "127.0.0.1:8090"
	CoordinatorID  string             // e.g. "soholink"
	CoordinatorKey ed25519.PrivateKey // coordinator signing key (64 bytes)
}

// Errors from GovernanceServer construction.
var (
	// ErrGovernanceNotLoopback is returned when the configured bind address is
	// not a loopback address. The governance surface MUST bind loopback only.
	ErrGovernanceNotLoopback = errors.New("api: governance address must be a loopback address (127.0.0.1 / ::1)")
	// ErrGovernanceBadKey is returned when the coordinator signing key is absent
	// or the wrong length for Ed25519.
	ErrGovernanceBadKey = errors.New("api: coordinator signing key is missing or not a valid ed25519 private key")
)

// NewGovernanceServer constructs the :8090 governance backend. It verifies the
// bind address is loopback and that the coordinator key is a well-formed Ed25519
// private key (with a sign-then-verify self-test, matching the house convention
// for asymmetric-key loaders). It does NOT start listening; call Start.
func NewGovernanceServer(repo governanceRepo, notifier notify.Notifier, cfg GovernanceConfig) (*GovernanceServer, error) {
	if !isLoopbackAddr(cfg.Addr) {
		return nil, ErrGovernanceNotLoopback
	}
	if len(cfg.CoordinatorKey) != ed25519.PrivateKeySize {
		return nil, ErrGovernanceBadKey
	}
	// Sign-then-verify self-test on a constant probe (house rule: any asymmetric
	// key loader proves the public half matches the seed before use). Catches a
	// 64-byte-but-mismatched key that would otherwise silently produce
	// unverifiable fee declarations.
	pub, ok := cfg.CoordinatorKey.Public().(ed25519.PublicKey)
	if !ok {
		return nil, ErrGovernanceBadKey
	}
	probe := []byte("soholink-coordinator-key-self-test-v1")
	if !ed25519.Verify(pub, probe, ed25519.Sign(cfg.CoordinatorKey, probe)) {
		return nil, ErrGovernanceBadKey
	}

	g := &GovernanceServer{
		repo:          repo,
		notifier:      notifier,
		coordKey:      cfg.CoordinatorKey,
		coordinatorID: cfg.CoordinatorID,
	}

	mux := http.NewServeMux()
	g.registerRoutes(mux)

	g.srv = &http.Server{
		Addr:         cfg.Addr,
		Handler:      g.loopbackOnly(mux), // defense-in-depth source guard
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	return g, nil
}

// registerRoutes wires the :8090 admin routes. These are LOCAL-ONLY. There is no
// overlap with the public onboarding routes and this mux is served on the
// loopback listener only.
func (g *GovernanceServer) registerRoutes(mux *http.ServeMux) {
	// Action routes (Stage-2 backend; POST/JSON, plus the JSON current-fees read).
	mux.HandleFunc("POST /admin/fees", g.handlePublishFees)
	mux.HandleFunc("GET /admin/fees/current", g.handleCurrentFees)
	mux.HandleFunc("POST /admin/operators/{id}/revoke", g.handleRevoke)
	mux.HandleFunc("POST /admin/operators/{id}/disconnect", g.handleDisconnect)
	mux.HandleFunc("POST /admin/messages", g.handleSendMessage)

	// Console GET pages (Stage-4 step 3; render the admin_*/gov_*.html templates
	// against the read models — governance_console.go). These are LOCAL-ONLY: the
	// same loopback-bound, loopback-source-guarded mux serves them. A POST-only
	// server (ConfigureConsole not called) still registers these — they render a
	// 500 "template not found" until templates are wired, which is harmless.
	mux.HandleFunc("GET /admin/operators", g.handleAdminOperatorsPage)
	mux.HandleFunc("GET /admin/operators/{id}", g.handleAdminOperatorDetailPage)
	mux.HandleFunc("GET /admin/fees", g.handleAdminFeesPage)
	mux.HandleFunc("GET /admin/messaging", g.handleAdminMessagingPage)

	// Console static assets (the portal design-system CSS and the brand mark the
	// admin templates reference), served from the web/static directory
	// ConfigureConsole derives beside the templates dir — mirroring the portal's
	// /static wiring (internal/portal/server.go). Zero-valued on a POST-only
	// server: the route then 404s — harmless, like the unwired console pages
	// above (which fail 500). LOCAL-ONLY like everything on this mux.
	mux.HandleFunc("GET /static/", g.handleStatic)

	// Demand-sounding dashboard (governance_sounding.go). Server-rendered SVG
	// charts over the migration-025 hypertables; LOCAL-ONLY like the rest.
	mux.HandleFunc("GET /admin/sounding", g.handleAdminSoundingPage)
}

// Start begins serving the :8090 governance surface on the (loopback) listener.
// It binds explicitly so a bind failure surfaces before serving, rather than
// relying on ListenAndServe's implicit bind.
func (g *GovernanceServer) Start() error {
	ln, err := net.Listen("tcp", g.srv.Addr)
	if err != nil {
		return fmt.Errorf("api: bind governance listener %q: %w", g.srv.Addr, err)
	}
	// Final belt-and-suspenders: refuse to serve if the actual bound address is
	// somehow not loopback (e.g. a hostname that resolved to a routable IP).
	if !isLoopbackListener(ln) {
		_ = ln.Close()
		return ErrGovernanceNotLoopback
	}
	return g.srv.Serve(ln)
}

// Shutdown gracefully drains the governance server.
func (g *GovernanceServer) Shutdown(ctx context.Context) error {
	return g.srv.Shutdown(ctx)
}

// -----------------------------------------------------------------------------
// (a) FEES — sign + publish; expose current.
// -----------------------------------------------------------------------------

type publishFeesRequest struct {
	ContributorShareBps int    `json:"contributor_share_bps"` // e.g. 6500
	PlatformFeeBps      int    `json:"platform_fee_bps"`      // e.g. 3500
	Seq                 uint64 `json:"seq"`                   // strictly monotonic per coordinator
	EffectiveAt         string `json:"effective_at"`          // RFC3339; strictly later than current
}

type feeDeclarationResponse struct {
	CoordinatorID       string `json:"coordinator_id"`
	ContributorShareBps int    `json:"contributor_share_bps"`
	PlatformFeeBps      int    `json:"platform_fee_bps"`
	Seq                 uint64 `json:"seq"`
	EffectiveAt         string `json:"effective_at"` // RFC3339
	Signature           string `json:"signature"`    // base64 std of the ed25519 signature
	Balanced            bool   `json:"balanced"`     // shares sum to 10000
}

// handlePublishFees builds a fees.FeeDeclaration from the admin's terms, SIGNS it
// with the coordinator key held only by this process, and publishes it via the
// repository (which enforces monotonic Seq + non-retroactive EffectiveAt). The
// signing happens HERE on the loopback surface — never on a public handler.
func (g *GovernanceServer) handlePublishFees(w http.ResponseWriter, r *http.Request) {
	var req publishFeesRequest
	if err := decodeJSON(w, r, &req); err != nil {
		return
	}
	if req.ContributorShareBps < 0 || req.PlatformFeeBps < 0 {
		writeError(w, http.StatusBadRequest, "basis points must be non-negative")
		return
	}
	effectiveAt, err := time.Parse(time.RFC3339, strings.TrimSpace(req.EffectiveAt))
	if err != nil {
		writeError(w, http.StatusBadRequest, "effective_at must be RFC3339")
		return
	}
	if !canon.ValidTime(effectiveAt) {
		writeError(w, http.StatusBadRequest, "effective_at must fit the protocol's signed 64-bit Unix nanosecond range")
		return
	}

	// PostgreSQL stores effective_at to microsecond precision. Sign only
	// timestamps that the repository can serve back exactly.
	if !effectiveAt.Equal(effectiveAt.Truncate(time.Microsecond)) {
		writeError(w, http.StatusBadRequest, "effective_at must be at microsecond precision or coarser")
		return
	}

	decl := fees.FeeDeclaration{
		CoordinatorID: g.coordinatorID,
		Terms: fees.Terms{
			ContributorShareBps: req.ContributorShareBps,
			PlatformFeeBps:      req.PlatformFeeBps,
		},
		EffectiveAt: effectiveAt,
		Seq:         req.Seq,
	}
	// Sign with the coordinator key (loaded from env at construction). This is the
	// ONLY place the key is used, and this handler is only reachable on loopback.
	decl.Sign(g.coordKey)

	err = g.repo.PublishFeeDeclaration(r.Context(), decl)
	switch {
	case err == nil:
		writeJSON(w, http.StatusCreated, feeToResponse(decl))
	case errors.Is(err, operator.ErrFeeSeqNotMonotonic):
		writeError(w, http.StatusConflict, "seq must strictly exceed the current declaration")
	case errors.Is(err, operator.ErrFeeEffectiveAtRetroactive):
		writeError(w, http.StatusConflict, "effective_at must be strictly later than the current declaration (non-retroactive)")
	case errors.Is(err, operator.ErrFeeUnsigned):
		// Should not happen: we always sign above. Surfaced defensively.
		writeError(w, http.StatusInternalServerError, "declaration was not signed")
	default:
		writeError(w, http.StatusInternalServerError, "could not publish fee declaration")
	}
}

// handleCurrentFees returns the current signed declaration. This is served on
// :8090 for admin inspection; the identical artifact is served on the PUBLIC
// /fees read by the portal via CurrentFeeDeclaration (that public read lives on
// the portal mux, not here).
func (g *GovernanceServer) handleCurrentFees(w http.ResponseWriter, r *http.Request) {
	decl, err := g.repo.CurrentFeeDeclaration(r.Context(), g.coordinatorID)
	if errors.Is(err, operator.ErrNoFeeDeclaration) {
		writeError(w, http.StatusNotFound, "no fee declaration published yet")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load fee declaration")
		return
	}
	writeJSON(w, http.StatusOK, feeToResponse(decl))
}

// feeToResponse renders a signed declaration for JSON output.
func feeToResponse(decl fees.FeeDeclaration) feeDeclarationResponse {
	return feeDeclarationResponse{
		CoordinatorID:       decl.CoordinatorID,
		ContributorShareBps: decl.Terms.ContributorShareBps,
		PlatformFeeBps:      decl.Terms.PlatformFeeBps,
		Seq:                 decl.Seq,
		EffectiveAt:         decl.EffectiveAt.UTC().Format(time.RFC3339Nano),
		Signature:           base64.StdEncoding.EncodeToString(decl.Signature),
		Balanced:            decl.Terms.Balanced(),
	}
}

// -----------------------------------------------------------------------------
// (b) DISCONNECT / REVOKE.
// -----------------------------------------------------------------------------

// handleRevoke flips an operator to status='revoked'. After this GetActiveKeyMap
// returns nil for the operator immediately — every fronted member is denied
// (high blast radius). Idempotent.
func (g *GovernanceServer) handleRevoke(w http.ResponseWriter, r *http.Request) {
	operatorID := r.PathValue("id")
	err := g.repo.Revoke(r.Context(), operatorID)
	g.writeLifecycleResult(w, operatorID, "revoked", err)
}

// handleDisconnect flips status='revoked' AND revokes the operator's active keys
// (a hard disconnect reflected in the registry). GetActiveKeyMap already returns
// nil once status != 'active'; the key-state change is belt-and-suspenders for
// the admin detail view. Idempotent.
func (g *GovernanceServer) handleDisconnect(w http.ResponseWriter, r *http.Request) {
	operatorID := r.PathValue("id")
	err := g.repo.Disconnect(r.Context(), operatorID)
	g.writeLifecycleResult(w, operatorID, "disconnected", err)
}

// writeLifecycleResult maps a revoke/disconnect result to a status.
func (g *GovernanceServer) writeLifecycleResult(w http.ResponseWriter, operatorID, action string, err error) {
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, map[string]string{"operator_id": operatorID, "status": action})
	case errors.Is(err, operator.ErrOperatorNotFound):
		writeError(w, http.StatusNotFound, "operator not found")
	default:
		writeError(w, http.StatusInternalServerError, "could not "+action+" operator")
	}
}

// -----------------------------------------------------------------------------
// (c) MESSAGING — operators and/or members via the net/smtp Notifier.
// -----------------------------------------------------------------------------

type sendMessageRequest struct {
	Subject   string `json:"subject"`
	Body      string `json:"body"`
	Operators bool   `json:"operators"` // include operator recipients
	Members   bool   `json:"members"`   // include member (participant) recipients — TRANSITIONAL
}

type sendMessageResponse struct {
	Recipients int      `json:"recipients"` // number of addresses attempted
	Sent       int      `json:"sent"`       // number that succeeded
	Failed     []string `json:"failed"`     // addresses that failed to send (if any)
}

// handleSendMessage composes one message and sends it to the selected audience.
// Operators and members are selectable separately or together. Members are
// TRANSITIONAL (Cloudy-owned; SoHoLINK still holds member records pending the
// Cloudy migration — see operator.MemberEmails). Delivery uses the same
// zero-cost net/smtp Notifier as the onboarding 2FA path.
func (g *GovernanceServer) handleSendMessage(w http.ResponseWriter, r *http.Request) {
	var req sendMessageRequest
	if err := decodeJSON(w, r, &req); err != nil {
		return
	}
	req.Subject = strings.TrimSpace(req.Subject)
	if req.Subject == "" || strings.TrimSpace(req.Body) == "" {
		writeError(w, http.StatusBadRequest, "subject and body are required")
		return
	}
	if !req.Operators && !req.Members {
		writeError(w, http.StatusBadRequest, "select at least one audience (operators and/or members)")
		return
	}

	// Collect recipients, deduplicated across audiences.
	seen := make(map[string]struct{})
	var recipients []string
	add := func(emails []string) {
		for _, e := range emails {
			if _, dup := seen[e]; dup {
				continue
			}
			seen[e] = struct{}{}
			recipients = append(recipients, e)
		}
	}
	if req.Operators {
		emails, err := g.repo.OperatorEmails(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, "could not load operator recipients")
			return
		}
		add(emails)
	}
	if req.Members {
		// TRANSITIONAL: member records are Cloudy-owned and hosted here pending
		// migration. This branch (and MemberEmails) moves to Cloudy with them.
		emails, err := g.repo.MemberEmails(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, "could not load member recipients")
			return
		}
		add(emails)
	}

	resp := sendMessageResponse{Recipients: len(recipients)}
	for _, to := range recipients {
		if err := g.notifier.Send(notify.Message{To: to, Subject: req.Subject, Body: req.Body}); err != nil {
			resp.Failed = append(resp.Failed, to)
			continue
		}
		resp.Sent++
	}
	writeJSON(w, http.StatusOK, resp)
}

// -----------------------------------------------------------------------------
// Loopback enforcement (governance separation / SSRF defense-in-depth).
// -----------------------------------------------------------------------------

// loopbackOnly wraps the governance mux so every request whose source is not a
// loopback address is rejected 403 BEFORE reaching any handler. Combined with the
// loopback bind this is defense-in-depth: even if the surface were somehow bound
// to a routable interface, or a public handler attempted an SSRF to
// 127.0.0.1:8090, a non-loopback source is refused. A loopback source is the
// admin over an SSH tunnel (the tunnel terminates on the host as 127.0.0.1).
func (g *GovernanceServer) loopbackOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			writeError(w, http.StatusForbidden, "governance surface is local-only")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// isLoopbackAddr reports whether a host:port bind address targets loopback. It
// accepts the literal loopback IPs and "localhost". An empty host (":8090") is
// REJECTED — that binds all interfaces, which would publish the surface.
func isLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "" {
		return false // ":8090" binds 0.0.0.0 — never allowed for governance
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// isLoopbackListener reports whether an already-bound listener is on a loopback
// address. Guards against a hostname that resolved to a routable IP.
func isLoopbackListener(ln net.Listener) bool {
	tcp, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		return false
	}
	return tcp.IP.IsLoopback()
}

// compile-time assertion that *operator.Repository satisfies governanceRepo.
var _ governanceRepo = (*operator.Repository)(nil)
