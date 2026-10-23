package portal

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/crypto/bcrypt"

	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/metrics"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/orchestrator"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/payment"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/store"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/types"
)

// jobSubmitter is the subset of orchestrator.Orchestrator used by the portal.
// Defined as an interface so tests can inject a stub without a real Orchestrator.
type jobSubmitter interface {
	SubmitJob(ctx context.Context, req orchestrator.SubmitJobRequest) (orchestrator.SubmitJobResponse, error)
}

// PortalServer is the SoHoLINK marketplace portal HTTP server. It sits behind
// NGINX, which handles TLS termination, so ListenAndServe (not TLS) is used.
type PortalServer struct {
	srv           *http.Server
	metricsSrv    *http.Server
	db            *store.DB
	sm            *SessionManager
	payment       *payment.Client
	orch          jobSubmitter
	baseURL       string
	templatePaths []string
	limiter       *LoginRateLimiter
	webhookSecret string
}

// onboardingData is the template data for provider_onboarding.html.
type onboardingData struct {
	Email              string
	ISPTier            string
	DisclosureAccepted bool
	StripeComplete     bool
	IsAuthenticated    bool
}

// ProfileRow is a single resource profile row for provider_provision.html.
type ProfileRow struct {
	ID            string
	Name          string
	IsDefault     bool
	CPUEnabled    bool
	GPUPct        int
	RAMPct        int
	StorageGB     int
	BandwidthMbps int
}

// provisionData is the template data for provider_provision.html.
type provisionData struct {
	Email           string
	NodeID          string
	Profiles        []ProfileRow
	UptimePct       float64
	EligibleClasses []string
	IsAuthenticated bool
}

// DisputeRow is a single row in the dispute queue table.
type DisputeRow struct {
	ID                string
	Status            string
	Reason            string
	ConsumerRefundPct int
	CreatedAt         time.Time
	PaymentIntentID   string
	JobID             string
	ConsumerEmail     string
	NodeClass         string
	CountryCode       string
}

// DisputeQueueData is the template data for dispute_queue.html.
type DisputeQueueData struct {
	Email           string
	Disputes        []DisputeRow
	IsAuthenticated bool
}

// NodeListing is a single marketplace node entry.
type NodeListing struct {
	ID            string
	CountryCode   string
	CPUCores      int
	RAMGB         int64
	StorageGB     int
	BandwidthMbps int
	EstHrRate     float64
}

// ClassGroup groups NodeListings by node class for the marketplace template.
type ClassGroup struct {
	Class     string
	ClassName string
	Nodes     []NodeListing
	MinRate   float64
	MaxRate   float64
}

// MarketplaceData is the template data for consumer_marketplace.html.
type MarketplaceData struct {
	Email           string
	Classes         []ClassGroup
	CPURateHr       float64
	RAMRateHr       float64
	IsAuthenticated bool
}

// ActiveJobRow is a job currently running on a participant's node.
type ActiveJobRow struct {
	JobID     string
	NodeID    string
	Workload  string
	CPUCores  int
	RAMMB     int
	StartedAt time.Time
}

// SubmittedJobRow is a job submitted by the participant as a buyer.
type SubmittedJobRow struct {
	JobID     string
	Status    string
	Workload  string
	CreatedAt time.Time
}

// NodeRow is a single node entry for the dashboard node list.
type NodeRow struct {
	ID            string
	Hostname      string
	Status        string
	NodeClass     string
	CountryCode   string
	UptimePct     float64
	LastHeartbeat time.Time
}

// DashboardData is the template data for provider_dashboard.html.
type DashboardData struct {
	Email                string
	IsAuthenticated      bool
	HasNodes             bool
	NodeCount            int64
	UptimePct            float64
	UptimeClass          string
	ThisMonthDollars     float64
	TotalDollars         float64
	PendingPayoutDollars float64
	TotalJobs            int64
	ActiveJobs           []ActiveJobRow
	SubmittedJobs        []SubmittedJobRow
	Nodes                []NodeRow
	// HasStripe is true when the participant has connected a Stripe account
	// for payouts. Drives the "Connect Stripe" CTA on the dashboard.
	HasStripe bool
	// RegToken is set after the participant clicks "Get Node Token".
	// Empty on normal dashboard loads.
	RegToken string
}

// JobStatusData is the template data for consumer_job_status.html.
type JobStatusData struct {
	JobID           string
	Status          string
	NodeID          string
	FailureCause    string
	CreatedAt       time.Time
	Email           string
	IsAuthenticated bool
}

// JobConfirmData is the template data for contributor_job_confirm.html.
type JobConfirmData struct {
	Email                string
	IsAuthenticated      bool
	JobID                string
	WorkloadType         string
	ContainerImage       string
	CPUCores             int
	RAMMB                int
	StorageGB            int
	GPURequired          bool
	CountryConstraint    string
	PrinterID            string
	SpecHashHex          string
	ConfirmationDeadline time.Time
	Status               string
}

// OptOutPageData is the template data for opt_out.html.
type OptOutPageData struct {
	IsAuthenticated bool
	Nodes           []OptOutNodeRow
}

// OptOutNodeRow is one node's opt-out state for the /opt-out page.
type OptOutNodeRow struct {
	ID             string
	Hostname       string
	OptOutCompute  bool
	OptOutStorage  bool
	OptOutPrinting bool
	Version        int
	SyncStatus     string
	Printers       []PrinterRow
}

// PrinterRow is one printer attached to a node, with its current enabled state.
type PrinterRow struct {
	PrinterID   string
	DisplayName string
	Enabled     bool
}

// optOutAPIResponse is the JSON shape returned from GET /api/opt-out.
type optOutAPIResponse struct {
	NodeID        string             `json:"node_id"`
	Compute       bool               `json:"compute"`
	Storage       bool               `json:"storage"`
	Printing      bool               `json:"printing"`
	Version       int                `json:"version"`
	UpdatedAt     time.Time          `json:"updated_at"`
	LastHeartbeat *time.Time         `json:"last_heartbeat,omitempty"`
	Printers      []optOutPrinterDTO `json:"printers"`
}

type optOutPrinterDTO struct {
	PrinterID string `json:"printer_id"`
	Name      string `json:"printer_name"`
	Enabled   bool   `json:"enabled"`
}

// optOutPostRequest is the JSON body for POST /api/opt-out.
type optOutPostRequest struct {
	NodeID   string             `json:"node_id"`
	Compute  bool               `json:"compute"`
	Storage  bool               `json:"storage"`
	Printing bool               `json:"printing"`
	Printers []optOutPrinterDTO `json:"printers"`
}

// Option customizes a PortalServer at construction. Options are applied before
// routes are registered so they can influence route wiring (e.g. mounting the
// public operator console, which re-parents GET / to the operator landing).
type Option func(*portalOptions)

// portalOptions accumulates the effect of the functional options.
type portalOptions struct {
	// operatorRoutes, when non-nil, registers the PUBLIC operator-console routes
	// (GET / operator landing, /operators/apply, /operators/{id}/*, /fees, and the
	// onboarding POST routes) onto the portal mux. When set, the portal does NOT
	// register its own GET / member index — the operator console's GET /{$} owns
	// the root per design §11 (member routes stay live under their own paths).
	operatorRoutes func(mux *http.ServeMux)
}

// WithOperatorConsole mounts the public operator console onto the portal mux.
// register is typically (*api.OnboardingServer).RegisterRoutes, which wires the
// reshaped landing (GET /{$}), /operators/apply, the /operators/{id}/* funnel,
// /fees, and the onboarding POST/JSON routes. Supplying this re-parents the root
// to the operator landing; every member route (/login, /dashboard, /marketplace,
// …) remains registered and live on the same mux (design §11, non-destructive).
func WithOperatorConsole(register func(mux *http.ServeMux)) Option {
	return func(o *portalOptions) { o.operatorRoutes = register }
}

// New constructs a PortalServer. It walks templatesDir recursively to collect
// all .html file paths (not parsed yet — see renderTemplate), registers routes,
// and builds the http.Server. metricsAddr is the address for the plain HTTP
// metrics server (e.g. ":9090") — not wrapped with session auth.
func New(db *store.DB, addr string, privateKey ed25519.PrivateKey, templatesDir string, paymentClient *payment.Client, baseURL string, orch jobSubmitter, metricsAddr string, webhookSecret string, opts ...Option) (*PortalServer, error) {
	var options portalOptions
	for _, opt := range opts {
		opt(&options)
	}
	var paths []string
	if err := filepath.WalkDir(templatesDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && filepath.Ext(path) == ".html" {
			paths = append(paths, path)
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("new portal: walk templates: %w", err)
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("new portal: no templates found in %s", templatesDir)
	}

	sm := NewSessionManager(privateKey)
	ps := &PortalServer{
		db:            db,
		sm:            sm,
		payment:       paymentClient,
		orch:          orch,
		baseURL:       baseURL,
		templatePaths: paths,
	}
	ps.limiter = NewLoginRateLimiter(5, 15*time.Minute)
	ps.webhookSecret = webhookSecret

	mux := http.NewServeMux()

	// Public routes.
	// When the operator console is mounted (design §11), it owns the root via its
	// own GET /{$} (operator landing) and also serves /fees + the /operators/*
	// funnel. In that case the portal does NOT register its own GET / member index
	// so the two do not collide on the root pattern. Every member route below stays
	// registered and live regardless. The member index remains reachable at /portal.
	if options.operatorRoutes != nil {
		options.operatorRoutes(mux)
		mux.HandleFunc("GET /portal", ps.handleIndex)
	} else {
		mux.HandleFunc("GET /", ps.handleIndex)
	}
	mux.HandleFunc("GET /join", ps.handleJoinPage)
	mux.HandleFunc("GET /download", ps.handleDownloadPage)
	mux.HandleFunc("GET /privacy", ps.handlePrivacyPage)
	mux.HandleFunc("GET /login", ps.handleLoginPage)
	mux.HandleFunc("POST /login", ps.handleLogin)
	mux.HandleFunc("POST /stripe/webhook", ps.handleStripeWebhook)
	mux.HandleFunc("GET /register", ps.handleRegisterPage)
	mux.HandleFunc("POST /register", ps.handleRegister)
	mux.HandleFunc("GET /logout", ps.handleLogout)
	mux.Handle("GET /static/",
		http.StripPrefix("/static/", http.FileServer(http.Dir(filepath.Join(templatesDir, "..", "static")))))

	// Token refresh — any authenticated user.
	mux.Handle("POST /auth/refresh",
		RequireAuth(sm, http.HandlerFunc(ps.handleRefresh)))

	// Protected routes.
	mux.Handle("GET /dashboard",
		RequireAuth(sm, http.HandlerFunc(ps.handleDashboard)))
	mux.Handle("GET /marketplace",
		RequireAuth(sm, http.HandlerFunc(ps.handleConsumerMarketplace)))
	mux.Handle("GET /consumer/jobs",
		RequireAuth(sm, http.HandlerFunc(ps.handleJobHistory)))
	mux.Handle("GET /consumer/marketplace",
		RequireAuth(sm, http.HandlerFunc(ps.handleConsumerMarketplace)))
	mux.Handle("POST /consumer/job",
		RequireAuth(sm, http.HandlerFunc(ps.handleSubmitJob)))
	mux.Handle("GET /consumer/job/{id}",
		RequireAuth(sm, http.HandlerFunc(ps.handleJobStatus)))
	mux.Handle("GET /consumer/job/{id}/status-stream",
		RequireAuth(sm, http.HandlerFunc(ps.handleJobStatusStream)))
	mux.Handle("POST /consumer/job/{id}/picked-up",
		RequireAuth(sm, http.HandlerFunc(ps.handleConsumerPickedUp)))
	mux.Handle("POST /consumer/job/{id}/delivered",
		RequireAuth(sm, http.HandlerFunc(ps.handleConsumerDelivered)))
	mux.Handle("POST /consumer/job/{id}/contest-no-show",
		RequireAuth(sm, http.HandlerFunc(ps.handleConsumerContestNoShow)))
	mux.Handle("GET /dispute/queue",
		RequireAuth(sm, http.HandlerFunc(ps.handleDisputeQueue)))
	mux.Handle("POST /dispute/{id}/resolve",
		RequireAuth(sm, http.HandlerFunc(ps.handleDisputeResolve)))
	mux.Handle("POST /dispute/{id}/review",
		RequireAuth(sm, http.HandlerFunc(ps.handleDisputeReview)))
	mux.Handle("GET /provider/onboarding",
		RequireAuth(sm, http.HandlerFunc(ps.handleProviderOnboardingPage)))
	mux.Handle("POST /provider/onboarding",
		RequireAuth(sm, http.HandlerFunc(ps.handleProviderOnboarding)))
	mux.Handle("GET /provider/onboarding/return",
		RequireAuth(sm, http.HandlerFunc(ps.handleProviderOnboardingReturn)))
	mux.Handle("GET /provider/provision",
		RequireAuth(sm, http.HandlerFunc(ps.handleProviderProvision)))
	mux.Handle("POST /provider/provision/profile",
		RequireAuth(sm, http.HandlerFunc(ps.handleAddProfile)))
	mux.Handle("POST /node/token",
		RequireAuth(sm, http.HandlerFunc(ps.handleGenerateNodeToken)))
	mux.Handle("GET /opt-out",
		RequireAuth(sm, http.HandlerFunc(ps.handleOptOutPage)))
	mux.Handle("GET /api/opt-out",
		RequireAuth(sm, http.HandlerFunc(ps.handleGetOptOut)))
	mux.Handle("POST /api/opt-out",
		RequireAuth(sm, http.HandlerFunc(ps.handlePostOptOut)))
	mux.Handle("GET /provider/job/{id}/confirm",
		RequireAuth(sm, http.HandlerFunc(ps.handleJobConfirm)))
	mux.Handle("POST /provider/job/{id}/confirm",
		RequireAuth(sm, http.HandlerFunc(ps.handleJobConfirmAction)))
	mux.Handle("POST /provider/job/{id}/no-show",
		RequireAuth(sm, http.HandlerFunc(ps.handleProviderNoShow)))

	ps.srv = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", metrics.Handler())
	ps.metricsSrv = &http.Server{
		Addr:         metricsAddr,
		Handler:      metricsMux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	return ps, nil
}

// renderTemplate builds a fresh template set from layout.html and the requested
// page file, then executes the named page template. Parsing per-request avoids
// the shared-set redefinition problem: every page defines {{define "content"}},
// which would collide if all files were parsed into one set at startup.
func (ps *PortalServer) renderTemplate(w http.ResponseWriter, page string, data any) {
	var layoutPath, pagePath, bannerPath string
	for _, p := range ps.templatePaths {
		switch filepath.Base(p) {
		case "layout.html":
			layoutPath = p
		case "transitional_banner.html":
			bannerPath = p
		case page:
			pagePath = p
		}
	}
	if layoutPath == "" || pagePath == "" {
		http.Error(w, "template not found", http.StatusInternalServerError)
		return
	}

	// The transitional banner partial is parsed into every set so member pages
	// can invoke {{template "transitional_banner" .}}; operator/console pages
	// simply do not call it. Base filenames stay unique (the ParseFiles
	// requirement), and an unused define in a set is harmless.
	files := []string{layoutPath, pagePath}
	if bannerPath != "" {
		files = append(files, bannerPath)
	}
	tmpl, err := template.ParseFiles(files...)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if err := tmpl.ExecuteTemplate(w, page, data); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}

// Start begins accepting HTTP connections. Blocks until the server is shut down.
func (ps *PortalServer) Start(_ context.Context) error {
	return ps.srv.ListenAndServe()
}

// StartMetrics starts a plain HTTP server on the metrics address serving only
// /metrics. It shuts down automatically when ctx is cancelled.
func (ps *PortalServer) StartMetrics(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		_ = ps.metricsSrv.Shutdown(context.Background())
	}()
	return ps.metricsSrv.ListenAndServe()
}

// Shutdown gracefully drains active connections within the context deadline.
func (ps *PortalServer) Shutdown(ctx context.Context) error {
	return ps.srv.Shutdown(ctx)
}

func (ps *PortalServer) handleRefresh(w http.ResponseWriter, r *http.Request) {
	claims, _ := ClaimsFromContext(r.Context())

	w.Header().Set("Content-Type", "application/json")

	if claims.ExpiresAt-time.Now().Unix() >= 300 {
		// More than 5 minutes remaining — no refresh needed.
		json.NewEncoder(w).Encode(map[string]bool{"refreshed": false}) //nolint:errcheck
		return
	}

	token, err := ps.sm.CreateToken(SessionClaims{
		UserID:    claims.UserID,
		Email:     claims.Email,
		ExpiresAt: time.Now().Add(15 * time.Minute).Unix(),
	})
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name:     "session_token",
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   900,
	})

	json.NewEncoder(w).Encode(map[string]bool{"refreshed": true}) //nolint:errcheck
}

func (ps *PortalServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	ps.renderTemplate(w, "index.html", nil)
}

func (ps *PortalServer) handleJoinPage(w http.ResponseWriter, r *http.Request) {
	ps.renderTemplate(w, "join.html", struct{ IsAuthenticated bool }{})
}

func (ps *PortalServer) handleDownloadPage(w http.ResponseWriter, r *http.Request) {
	ps.renderTemplate(w, "download.html", struct{ IsAuthenticated bool }{})
}

func (ps *PortalServer) handlePrivacyPage(w http.ResponseWriter, r *http.Request) {
	ps.renderTemplate(w, "privacy.html", struct{ IsAuthenticated bool }{})
}

func (ps *PortalServer) handleLoginPage(w http.ResponseWriter, r *http.Request) {
	ps.renderTemplate(w, "login.html", nil)
}

func (ps *PortalServer) handleLogin(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	// Rate-limit by client IP.
	ip, _, splitErr := net.SplitHostPort(r.RemoteAddr)
	if splitErr != nil {
		ip = r.RemoteAddr
	}
	if !ps.limiter.Allow(ip) {
		http.Error(w, "too many failed attempts, try again later", http.StatusTooManyRequests)
		return
	}

	email := strings.TrimSpace(r.FormValue("email"))
	password := r.FormValue("password")

	if email == "" || password == "" {
		http.Error(w, "email and password are required", http.StatusBadRequest)
		return
	}

	var userID, hash string
	err := ps.db.Pool.QueryRow(r.Context(),
		`SELECT id, COALESCE(password_hash, '') FROM participants WHERE email = $1`,
		email,
	).Scan(&userID, &hash)
	if err != nil {
		// Row not found or DB error — return 401 to avoid account enumeration.
		ps.limiter.RecordFailure(ip)
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)); err != nil {
		ps.limiter.RecordFailure(ip)
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
		return
	}

	ps.limiter.Reset(ip)

	token, err := ps.sm.CreateToken(SessionClaims{
		UserID:    userID,
		Email:     email,
		ExpiresAt: time.Now().Add(15 * time.Minute).Unix(),
	})
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name:     "session_token",
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   900,
	})

	http.Redirect(w, r, "/dashboard", http.StatusSeeOther)
}

// registerData is the template data for register.html.
type registerData struct {
	Error           string
	Email           string
	SOHOName        string
	IsAuthenticated bool
}

func (ps *PortalServer) handleRegisterPage(w http.ResponseWriter, r *http.Request) {
	ps.renderTemplate(w, "register.html", registerData{})
}

func (ps *PortalServer) handleLogout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     "session_token",
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	})
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (ps *PortalServer) handleRegister(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	email := strings.TrimSpace(r.FormValue("email"))
	password := r.FormValue("password")
	sohoName := strings.TrimSpace(r.FormValue("soho_name"))

	re := registerData{Email: email, SOHOName: sohoName}

	if email == "" || sohoName == "" {
		re.Error = "all fields are required"
		ps.renderTemplate(w, "register.html", re)
		return
	}
	if len(password) < 8 {
		re.Error = "password must be at least 8 characters"
		ps.renderTemplate(w, "register.html", re)
		return
	}

	// Check email not already taken.
	var existing string
	err := ps.db.Pool.QueryRow(r.Context(),
		`SELECT id FROM participants WHERE email = $1`, email,
	).Scan(&existing)
	if err == nil {
		re.Error = "email already registered"
		ps.renderTemplate(w, "register.html", re)
		return
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	var userID string
	if err := ps.db.Pool.QueryRow(r.Context(),
		`INSERT INTO participants (email, password_hash, display_name, soho_name)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		email, string(hash), sohoName, sohoName,
	).Scan(&userID); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	token, err := ps.sm.CreateToken(SessionClaims{
		UserID:    userID,
		Email:     email,
		ExpiresAt: time.Now().Add(15 * time.Minute).Unix(),
	})
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name:     "session_token",
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   900,
	})

	http.Redirect(w, r, "/dashboard", http.StatusSeeOther)
}

func (ps *PortalServer) handleDashboard(w http.ResponseWriter, r *http.Request) {
	claims, _ := ClaimsFromContext(r.Context())
	data, err := ps.buildDashboardData(r.Context(), claims)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	ps.renderTemplate(w, "dashboard.html", data)
}

// buildDashboardData runs the four dashboard queries and returns a populated
// DashboardData. RegToken is always empty — callers set it when needed.
func (ps *PortalServer) buildDashboardData(ctx context.Context, claims SessionClaims) (DashboardData, error) {
	// Participant payout state — drives the "Connect Stripe" CTA.
	var stripeAccountID string
	if err := ps.db.Pool.QueryRow(ctx,
		`SELECT COALESCE(stripe_account_id, '') FROM participants WHERE id = $1`,
		claims.UserID,
	).Scan(&stripeAccountID); err != nil {
		return DashboardData{}, err
	}
	hasStripe := stripeAccountID != ""

	// Look up the most recent unused, unexpired registration token. Surfaces
	// the token across page reloads so a participant can leave and come back
	// without losing it. If no token exists, existingToken stays "". DB errors
	// here are deliberately ignored — subsequent queries surface them loudly.
	var existingToken string
	_ = ps.db.Pool.QueryRow(ctx,
		`SELECT token FROM node_registration_tokens
		 WHERE participant_id = $1 AND used_at IS NULL AND expires_at > NOW()
		 ORDER BY created_at DESC LIMIT 1`,
		claims.UserID,
	).Scan(&existingToken)

	var nodeCount int64
	var uptimePct float64
	if err := ps.db.Pool.QueryRow(ctx,
		`SELECT COUNT(*), COALESCE(AVG(uptime_pct), 0) FROM nodes WHERE participant_id = $1`,
		claims.UserID,
	).Scan(&nodeCount, &uptimePct); err != nil {
		return DashboardData{}, err
	}

	// Per-node detail rows.
	var nodes []NodeRow
	nodeRows, err := ps.db.Pool.Query(ctx, `
		SELECT id, hostname, status::text, node_class::text, country_code,
		       uptime_pct, COALESCE(last_heartbeat_at, created_at)
		FROM nodes
		WHERE participant_id = $1
		ORDER BY created_at ASC`,
		claims.UserID,
	)
	if err != nil {
		return DashboardData{}, err
	}
	defer nodeRows.Close()
	for nodeRows.Next() {
		var n NodeRow
		if err := nodeRows.Scan(
			&n.ID, &n.Hostname, &n.Status, &n.NodeClass,
			&n.CountryCode, &n.UptimePct, &n.LastHeartbeat,
		); err != nil {
			return DashboardData{}, err
		}
		nodes = append(nodes, n)
	}
	if err := nodeRows.Err(); err != nil {
		return DashboardData{}, err
	}

	uptimeClass := ""
	switch {
	case uptimePct >= 95:
		uptimeClass = "A"
	case uptimePct >= 85:
		uptimeClass = "B"
	case uptimePct >= 70:
		uptimeClass = "C"
	}

	var totalCents, thisMonthCents, pendingCents, totalJobs int64
	if nodeCount > 0 {
		if err := ps.db.Pool.QueryRow(ctx, `
			SELECT
			    COALESCE(SUM(jm.contributor_earned_cents), 0),
			    COALESCE(SUM(CASE WHEN DATE_TRUNC('month', jm.computed_at) = DATE_TRUNC('month', NOW())
			                 THEN jm.contributor_earned_cents ELSE 0 END), 0),
			    COUNT(DISTINCT j.id)
			FROM nodes n
			JOIN jobs j ON j.node_id = n.id
			JOIN job_metering jm ON jm.job_id = j.id
			WHERE n.participant_id = $1`,
			claims.UserID,
		).Scan(&totalCents, &thisMonthCents, &totalJobs); err != nil {
			return DashboardData{}, err
		}

		if err := ps.db.Pool.QueryRow(ctx, `
			SELECT COALESCE(SUM(jm.contributor_earned_cents), 0)
			FROM nodes n
			JOIN jobs j ON j.node_id = n.id
			JOIN job_metering jm ON jm.job_id = j.id
			WHERE n.participant_id = $1
			  AND j.completed_at > NOW() - INTERVAL '24 hours'`,
			claims.UserID,
		).Scan(&pendingCents); err != nil {
			return DashboardData{}, err
		}
	}

	var activeJobs []ActiveJobRow
	if nodeCount > 0 {
		aRows, err := ps.db.Pool.Query(ctx, `
			SELECT j.id, j.node_id, j.workload_type, j.cpu_cores, j.ram_mb,
			       COALESCE(j.started_at, j.created_at)
			FROM jobs j
			JOIN nodes n ON n.id = j.node_id
			WHERE n.participant_id = $1 AND j.status = 'running'::job_status
			ORDER BY j.started_at DESC`,
			claims.UserID,
		)
		if err != nil {
			return DashboardData{}, err
		}
		defer aRows.Close()
		for aRows.Next() {
			var row ActiveJobRow
			if err := aRows.Scan(&row.JobID, &row.NodeID, &row.Workload,
				&row.CPUCores, &row.RAMMB, &row.StartedAt); err != nil {
				return DashboardData{}, err
			}
			activeJobs = append(activeJobs, row)
		}
		if err := aRows.Err(); err != nil {
			return DashboardData{}, err
		}
	}

	sRows, err := ps.db.Pool.Query(ctx, `
		SELECT id, status, workload_type, created_at
		FROM jobs
		WHERE participant_id = $1
		ORDER BY created_at DESC
		LIMIT 10`,
		claims.UserID,
	)
	if err != nil {
		return DashboardData{}, err
	}
	defer sRows.Close()
	var submittedJobs []SubmittedJobRow
	for sRows.Next() {
		var row SubmittedJobRow
		if err := sRows.Scan(&row.JobID, &row.Status, &row.Workload, &row.CreatedAt); err != nil {
			return DashboardData{}, err
		}
		submittedJobs = append(submittedJobs, row)
	}
	if err := sRows.Err(); err != nil {
		return DashboardData{}, err
	}

	return DashboardData{
		Email:                claims.Email,
		IsAuthenticated:      true,
		HasNodes:             nodeCount > 0,
		NodeCount:            nodeCount,
		UptimePct:            uptimePct,
		UptimeClass:          uptimeClass,
		ThisMonthDollars:     float64(thisMonthCents) / 100.0,
		TotalDollars:         float64(totalCents) / 100.0,
		PendingPayoutDollars: float64(pendingCents) / 100.0,
		TotalJobs:            totalJobs,
		ActiveJobs:           activeJobs,
		SubmittedJobs:        submittedJobs,
		Nodes:                nodes,
		HasStripe:            hasStripe,
		RegToken:             existingToken,
	}, nil
}

func (ps *PortalServer) handleConsumerMarketplace(w http.ResponseWriter, r *http.Request) {
	claims, _ := ClaimsFromContext(r.Context())

	// Step 1 — fetch current platform rates.
	rateRows, err := ps.db.Pool.Query(r.Context(),
		`SELECT resource_type, base_rate
		 FROM resource_pricing
		 WHERE (effective_until IS NULL OR effective_until > NOW())
		 ORDER BY resource_type, effective_from DESC`,
	)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	defer rateRows.Close()

	rates := make(map[string]float64)
	for rateRows.Next() {
		var rt string
		var rate float64
		if err := rateRows.Scan(&rt, &rate); err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		// Keep only the first (most recent) row per resource_type.
		if _, seen := rates[rt]; !seen {
			rates[rt] = rate
		}
	}
	if err := rateRows.Err(); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Step 2 — fetch online nodes with their default resource profile.
	nodeRows, err := ps.db.Pool.Query(r.Context(),
		`SELECT
		     n.id, n.node_class, n.country_code,
		     (n.hardware_profile->>'CPUCores')::int        AS cpu_cores,
		     (n.hardware_profile->>'RAMMB')::bigint / 1024 AS ram_gb,
		     COALESCE(rp.ram_pct, 100)                     AS ram_pct,
		     COALESCE(rp.cpu_enabled, true)                AS cpu_enabled,
		     COALESCE(rp.storage_gb, 0)                    AS storage_gb,
		     COALESCE(rp.bandwidth_mbps, 0)                AS bandwidth_mbps,
		     COALESCE(rp.price_multiplier, 1.0)            AS price_multiplier
		 FROM nodes n
		 LEFT JOIN resource_profiles rp
		     ON rp.node_id = n.id AND rp.is_default = TRUE
		 WHERE n.status = 'online'
		   AND (
		       (n.node_class = 'A' AND n.uptime_pct >= 95.0) OR
		       (n.node_class = 'B' AND n.uptime_pct >= 85.0) OR
		       (n.node_class = 'C' AND n.uptime_pct >= 70.0) OR
		       (n.node_class = 'D' AND n.uptime_pct >= 80.0)
		   )
		 ORDER BY n.node_class, n.country_code`,
	)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	defer nodeRows.Close()

	classMap := make(map[string][]NodeListing)
	classOrder := []string{}

	for nodeRows.Next() {
		var (
			id, nodeClass, country string
			cpuCores               int
			ramGB                  int64
			ramPct                 int
			cpuEnabled             bool
			storageGB, bwMbps      int
			priceMultiplier        float64
		)
		if err := nodeRows.Scan(&id, &nodeClass, &country, &cpuCores, &ramGB,
			&ramPct, &cpuEnabled, &storageGB, &bwMbps, &priceMultiplier); err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		// Step 3 — compute available resources and estimated hourly rate.
		availCPU := float64(cpuCores)
		if !cpuEnabled {
			availCPU = 0
		}
		availRAM := float64(ramGB) * float64(ramPct) / 100.0
		estHrRate := (availCPU*rates["cpu_core_hr"] + availRAM*rates["ram_gb_hr"]) * priceMultiplier

		listing := NodeListing{
			ID:            id,
			CountryCode:   country,
			CPUCores:      cpuCores,
			RAMGB:         ramGB,
			StorageGB:     storageGB,
			BandwidthMbps: bwMbps,
			EstHrRate:     estHrRate,
		}

		if _, exists := classMap[nodeClass]; !exists {
			classOrder = append(classOrder, nodeClass)
		}
		classMap[nodeClass] = append(classMap[nodeClass], listing)
	}
	if err := nodeRows.Err(); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Step 4 — build ClassGroup slice in stable order.
	classNames := map[string]string{
		"A": "Class A — Dedicated Server",
		"B": "Class B — Mobile GPU",
		"C": "Class C — Smart TV",
		"D": "Class D — NAS / Storage",
	}

	groups := make([]ClassGroup, 0, len(classOrder))
	for _, cls := range classOrder {
		nodes := classMap[cls]
		minRate, maxRate := nodes[0].EstHrRate, nodes[0].EstHrRate
		for _, n := range nodes[1:] {
			if n.EstHrRate < minRate {
				minRate = n.EstHrRate
			}
			if n.EstHrRate > maxRate {
				maxRate = n.EstHrRate
			}
		}
		name, ok := classNames[cls]
		if !ok {
			name = "Class " + cls
		}
		groups = append(groups, ClassGroup{
			Class:     cls,
			ClassName: name,
			Nodes:     nodes,
			MinRate:   minRate,
			MaxRate:   maxRate,
		})
	}

	ps.renderTemplate(w, "consumer_marketplace.html", MarketplaceData{
		Email:           claims.Email,
		Classes:         groups,
		CPURateHr:       rates["cpu_core_hr"],
		RAMRateHr:       rates["ram_gb_hr"],
		IsAuthenticated: true,
	})
}

func (ps *PortalServer) handleDisputeQueue(w http.ResponseWriter, r *http.Request) {
	claims, _ := ClaimsFromContext(r.Context())

	var isStaff bool
	_ = ps.db.Pool.QueryRow(r.Context(),
		`SELECT is_staff FROM participants WHERE id = $1`, claims.UserID,
	).Scan(&isStaff)
	if !isStaff {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	rows, err := ps.db.Pool.Query(r.Context(),
		`SELECT
		     d.id, d.status, d.reason, d.consumer_refund_pct,
		     d.created_at, d.payment_intent_id,
		     j.id AS job_id,
		     c.email AS consumer_email,
		     n.node_class, n.country_code
		 FROM disputes d
		 JOIN jobs j ON j.id = d.job_id
		 JOIN participants c ON c.id = d.participant_id
		 JOIN nodes n ON n.id = d.node_id
		 WHERE d.status IN ('open', 'under_review')
		 ORDER BY d.created_at ASC`,
	)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var disputes []DisputeRow
	for rows.Next() {
		var d DisputeRow
		if err := rows.Scan(
			&d.ID, &d.Status, &d.Reason, &d.ConsumerRefundPct,
			&d.CreatedAt, &d.PaymentIntentID,
			&d.JobID, &d.ConsumerEmail,
			&d.NodeClass, &d.CountryCode,
		); err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		disputes = append(disputes, d)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	ps.renderTemplate(w, "dispute_queue.html", DisputeQueueData{
		Email:           claims.Email,
		Disputes:        disputes,
		IsAuthenticated: true,
	})
}

func (ps *PortalServer) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	claims, _ := ClaimsFromContext(r.Context())

	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	nodeID := r.FormValue("node_id")
	if nodeID == "" {
		http.Error(w, "node_id is required", http.StatusBadRequest)
		return
	}

	var status string
	err := ps.db.Pool.QueryRow(r.Context(),
		`SELECT status FROM nodes WHERE id = $1`,
		nodeID,
	).Scan(&status)
	if err != nil {
		http.Error(w, "node not found", http.StatusNotFound)
		return
	}
	if status != "online" {
		http.Error(w, "node is not online", http.StatusConflict)
		return
	}

	workloadTypeStr := r.FormValue("workload_type")
	if workloadTypeStr == "" {
		workloadTypeStr = string(types.MarketplaceAppHosting)
	}
	wt, err := types.ParseMarketplaceWorkloadType(workloadTypeStr)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid workload_type: %s", err), http.StatusBadRequest)
		return
	}
	containerImage := r.FormValue("container_image")
	if containerImage == "" {
		http.Error(w, "container_image is required", http.StatusBadRequest)
		return
	}

	cpuCores := 2
	ramMB := 4096
	if v := r.FormValue("cpu_cores"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cpuCores = n
		}
	}
	if v := r.FormValue("ram_mb"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			ramMB = n
		}
	}

	resp, err := ps.orch.SubmitJob(r.Context(), orchestrator.SubmitJobRequest{
		ConsumerID:     claims.UserID,
		WorkloadType:   wt,
		ContainerImage: containerImage,
		CPUCores:       cpuCores,
		RAMMB:          ramMB,
	})
	if err != nil {
		http.Error(w, "failed to submit job", http.StatusInternalServerError)
		return
	}

	metrics.JobsSubmittedTotal.WithLabelValues(string(wt)).Inc()
	http.Redirect(w, r, "/consumer/job/"+resp.JobID, http.StatusSeeOther)
}

func (ps *PortalServer) handleJobStatus(w http.ResponseWriter, r *http.Request) {
	claims, _ := ClaimsFromContext(r.Context())
	jobID := r.PathValue("id")

	var data JobStatusData
	data.Email = claims.Email
	data.JobID = jobID
	data.IsAuthenticated = true

	err := ps.db.Pool.QueryRow(r.Context(),
		`SELECT status, COALESCE(node_id::text, ''), COALESCE(failure_cause, ''), created_at
		 FROM jobs WHERE id = $1 AND participant_id = $2`,
		jobID, claims.UserID,
	).Scan(&data.Status, &data.NodeID, &data.FailureCause, &data.CreatedAt)
	if err != nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	ps.renderTemplate(w, "consumer_job_status.html", data)
}

func (ps *PortalServer) handleJobConfirm(w http.ResponseWriter, r *http.Request) {
	claims, _ := ClaimsFromContext(r.Context())
	jobID := r.PathValue("id")

	var data JobConfirmData
	data.Email = claims.Email
	data.IsAuthenticated = true
	data.JobID = jobID

	var specHash []byte
	err := ps.db.Pool.QueryRow(r.Context(),
		`SELECT j.workload_type, COALESCE(j.container_image, ''),
		        COALESCE(j.cpu_cores, 0), COALESCE(j.ram_mb, 0), COALESCE(j.storage_gb, 0),
		        j.gpu_required, COALESCE(j.country_constraint, ''),
		        COALESCE(j.printer_id, ''), j.spec_hash,
		        j.confirmation_deadline, j.status
		 FROM jobs j
		 JOIN nodes n ON n.id = j.node_id
		 WHERE j.id = $1 AND n.participant_id = $2`,
		jobID, claims.UserID,
	).Scan(
		&data.WorkloadType, &data.ContainerImage,
		&data.CPUCores, &data.RAMMB, &data.StorageGB,
		&data.GPURequired, &data.CountryConstraint,
		&data.PrinterID, &specHash,
		&data.ConfirmationDeadline, &data.Status,
	)
	if err != nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	data.SpecHashHex = hex.EncodeToString(specHash)
	ps.renderTemplate(w, "contributor_job_confirm.html", data)
}

func (ps *PortalServer) handleJobConfirmAction(w http.ResponseWriter, r *http.Request) {
	claims, _ := ClaimsFromContext(r.Context())
	jobID := r.PathValue("id")

	// Re-verify ownership before any mutation.
	var status string
	err := ps.db.Pool.QueryRow(r.Context(),
		`SELECT j.status
		 FROM jobs j
		 JOIN nodes n ON n.id = j.node_id
		 WHERE j.id = $1 AND n.participant_id = $2`,
		jobID, claims.UserID,
	).Scan(&status)
	if err != nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	// Already processed — show the current state.
	if status != "awaiting_confirmation" {
		http.Redirect(w, r, "/provider/job/"+jobID+"/confirm", http.StatusSeeOther)
		return
	}

	action := r.FormValue("action")
	if action != "confirm" && action != "decline" {
		http.Error(w, "invalid action", http.StatusBadRequest)
		return
	}

	if action == "confirm" {
		tag, execErr := ps.db.Pool.Exec(r.Context(),
			`UPDATE jobs SET status = 'scheduled'::job_status, confirmed_at = NOW()
			 WHERE id = $1 AND status = 'awaiting_confirmation'::job_status`,
			jobID)
		if execErr != nil {
			slog.Error("job confirm failed", "job_id", jobID, "error", execErr)
			http.Error(w, "failed to update job", http.StatusInternalServerError)
			return
		}
		if tag.RowsAffected() == 0 {
			http.Redirect(w, r, "/provider/job/"+jobID+"/confirm", http.StatusSeeOther)
			return
		}
	} else {
		// Decline: wrap in a transaction so the decline record is written
		// atomically with the status change. If the INSERT fails after the UPDATE
		// succeeds, the reroute worker would re-dispatch to the same node.
		tx, txErr := ps.db.Pool.Begin(r.Context())
		if txErr != nil {
			slog.Error("job decline begin tx", "job_id", jobID, "error", txErr)
			http.Error(w, "failed to update job", http.StatusInternalServerError)
			return
		}
		defer tx.Rollback(r.Context()) //nolint:errcheck

		var nodeID string
		scanErr := tx.QueryRow(r.Context(),
			`UPDATE jobs SET status = 'declined'::job_status, declined_at = NOW()
			 WHERE id = $1 AND status = 'awaiting_confirmation'::job_status
			 RETURNING node_id`,
			jobID).Scan(&nodeID)
		if errors.Is(scanErr, pgx.ErrNoRows) {
			http.Redirect(w, r, "/provider/job/"+jobID+"/confirm", http.StatusSeeOther)
			return
		}
		if scanErr != nil {
			slog.Error("job decline update", "job_id", jobID, "error", scanErr)
			http.Error(w, "failed to update job", http.StatusInternalServerError)
			return
		}

		if _, execErr := tx.Exec(r.Context(),
			`INSERT INTO job_node_declines (job_id, node_id) VALUES ($1, $2)`,
			jobID, nodeID); execErr != nil {
			slog.Error("job decline insert record", "job_id", jobID, "error", execErr)
			http.Error(w, "failed to record decline", http.StatusInternalServerError)
			return
		}

		if commitErr := tx.Commit(r.Context()); commitErr != nil {
			slog.Error("job decline commit", "job_id", jobID, "error", commitErr)
			http.Error(w, "failed to update job", http.StatusInternalServerError)
			return
		}
	}

	http.Redirect(w, r, "/provider/job/"+jobID+"/confirm", http.StatusSeeOther)
}

func (ps *PortalServer) handleJobStatusStream(w http.ResponseWriter, r *http.Request) {
	claims, _ := ClaimsFromContext(r.Context())
	jobID := r.PathValue("id")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher.Flush()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			var status, nodeID string
			err := ps.db.Pool.QueryRow(r.Context(),
				`SELECT status, COALESCE(node_id::text, '') FROM jobs WHERE id = $1 AND participant_id = $2`,
				jobID, claims.UserID,
			).Scan(&status, &nodeID)
			if err != nil {
				fmt.Fprintf(w, "data: {\"error\":\"job not found\"}\n\n")
				flusher.Flush()
				return
			}

			fmt.Fprintf(w, "data: {\"status\":%q,\"node_id\":%q}\n\n", status, nodeID)
			flusher.Flush()

			if status == "completed" || status == "failed" {
				return
			}
		}
	}
}

func (ps *PortalServer) handleDisputeResolve(w http.ResponseWriter, r *http.Request) {
	claims, _ := ClaimsFromContext(r.Context())

	var isStaff bool
	_ = ps.db.Pool.QueryRow(r.Context(),
		`SELECT is_staff FROM participants WHERE id = $1`, claims.UserID,
	).Scan(&isStaff)
	if !isStaff {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	disputeID := r.PathValue("id")

	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	consumerRefundPct, err := strconv.Atoi(r.FormValue("consumer_refund_pct"))
	if err != nil || consumerRefundPct < 0 || consumerRefundPct > 100 {
		http.Error(w, "consumer_refund_pct must be between 0 and 100", http.StatusBadRequest)
		return
	}

	var (
		paymentIntentID string
		jobID           string
	)
	err = ps.db.Pool.QueryRow(r.Context(),
		`SELECT d.payment_intent_id, j.id
		 FROM disputes d
		 JOIN jobs j ON j.id = d.job_id
		 WHERE d.id = $1 AND d.status IN ('open', 'under_review')`,
		disputeID,
	).Scan(&paymentIntentID, &jobID)
	if err != nil {
		http.Error(w, "dispute not found or already resolved", http.StatusNotFound)
		return
	}

	if consumerRefundPct > 0 {
		var amountCents int64
		err = ps.db.Pool.QueryRow(r.Context(),
			`SELECT amount_cents FROM jobs WHERE id = $1`,
			jobID,
		).Scan(&amountCents)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		refundAmount := amountCents * int64(consumerRefundPct) / 100
		if refundAmount > 0 {
			if err := ps.payment.CreateRefund(r.Context(), paymentIntentID, refundAmount); err != nil {
				http.Error(w, "failed to issue refund", http.StatusInternalServerError)
				return
			}
		}
	}

	_, err = ps.db.Pool.Exec(r.Context(),
		`UPDATE disputes
		 SET status = 'resolved', consumer_refund_pct = $1, arbiter_participant_id = $2,
		     arbiter_notes = 'resolved via terminal', resolved_at = NOW(), updated_at = NOW()
		 WHERE id = $3`,
		consumerRefundPct, claims.UserID, disputeID,
	)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, "/dispute/queue", http.StatusSeeOther)
}

func (ps *PortalServer) handleDisputeReview(w http.ResponseWriter, r *http.Request) {
	claims, _ := ClaimsFromContext(r.Context())

	var isStaff bool
	_ = ps.db.Pool.QueryRow(r.Context(),
		`SELECT is_staff FROM participants WHERE id = $1`, claims.UserID,
	).Scan(&isStaff)
	if !isStaff {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	disputeID := r.PathValue("id")

	_, err := ps.db.Pool.Exec(r.Context(),
		`UPDATE disputes SET status = 'under_review', arbiter_participant_id = $1, updated_at = NOW()
		 WHERE id = $2 AND status = 'open'`,
		claims.UserID, disputeID,
	)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, "/dispute/queue", http.StatusSeeOther)
}

func (ps *PortalServer) handleProviderOnboardingPage(w http.ResponseWriter, r *http.Request) {
	claims, _ := ClaimsFromContext(r.Context())

	var ispTier string
	var disclosureAt *time.Time
	var stripeComplete bool
	err := ps.db.Pool.QueryRow(r.Context(),
		`SELECT COALESCE(isp_tier, ''), disclosure_accepted_at, stripe_onboarding_complete
		 FROM participants WHERE id = $1`,
		claims.UserID,
	).Scan(&ispTier, &disclosureAt, &stripeComplete)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	if disclosureAt != nil && stripeComplete {
		http.Redirect(w, r, "/provider/provision", http.StatusSeeOther)
		return
	}

	ps.renderTemplate(w, "provider_onboarding.html", onboardingData{
		Email:              claims.Email,
		ISPTier:            ispTier,
		DisclosureAccepted: disclosureAt != nil,
		StripeComplete:     stripeComplete,
		IsAuthenticated:    true,
	})
}

func (ps *PortalServer) handleProviderOnboarding(w http.ResponseWriter, r *http.Request) {
	claims, _ := ClaimsFromContext(r.Context())

	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	ispTier := r.FormValue("isp_tier")
	disclosure := r.FormValue("disclosure_accepted")

	validTiers := map[string]bool{"business": true, "residential": true, "cellular": true}
	if !validTiers[ispTier] {
		http.Error(w, "invalid ISP tier", http.StatusBadRequest)
		return
	}
	if ispTier == "cellular" {
		http.Error(w, "Cellular connections are not eligible for Class A node listings", http.StatusBadRequest)
		return
	}
	if disclosure != "true" {
		http.Error(w, "disclosure acceptance is required", http.StatusBadRequest)
		return
	}

	_, err := ps.db.Pool.Exec(r.Context(),
		`UPDATE participants SET isp_tier = $1, disclosure_accepted_at = NOW(), updated_at = NOW() WHERE id = $2`,
		ispTier, claims.UserID,
	)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	accountID, err := ps.payment.CreateConnectedAccount(r.Context(), claims.Email, claims.Email)
	if err != nil {
		http.Error(w, "failed to create Stripe account", http.StatusInternalServerError)
		return
	}

	_, err = ps.db.Pool.Exec(r.Context(),
		`UPDATE participants SET stripe_account_id = $1, updated_at = NOW() WHERE id = $2`,
		accountID, claims.UserID,
	)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	onboardingURL, err := ps.payment.CreateOnboardingLink(
		r.Context(), accountID,
		ps.baseURL+"/provider/onboarding",
		ps.baseURL+"/provider/onboarding/return",
	)
	if err != nil {
		http.Error(w, "failed to create onboarding link", http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, onboardingURL, http.StatusSeeOther)
}

func (ps *PortalServer) handleProviderOnboardingReturn(w http.ResponseWriter, r *http.Request) {
	claims, _ := ClaimsFromContext(r.Context())

	var stripeAccountID string
	err := ps.db.Pool.QueryRow(r.Context(),
		`SELECT COALESCE(stripe_account_id, '') FROM participants WHERE id = $1`,
		claims.UserID,
	).Scan(&stripeAccountID)
	if err != nil || stripeAccountID == "" {
		http.Redirect(w, r, "/provider/onboarding", http.StatusSeeOther)
		return
	}

	status, err := ps.payment.CheckOnboardingStatus(r.Context(), stripeAccountID)
	if err != nil {
		http.Redirect(w, r, "/provider/onboarding", http.StatusSeeOther)
		return
	}

	if status.TransfersActive && !status.RequirementsPending {
		_, err = ps.db.Pool.Exec(r.Context(),
			`UPDATE participants SET stripe_onboarding_complete = TRUE, onboarding_complete = TRUE, updated_at = NOW() WHERE id = $1`,
			claims.UserID,
		)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, "/provider/provision", http.StatusSeeOther)
		return
	}

	http.Redirect(w, r, "/provider/onboarding", http.StatusSeeOther)
}

func (ps *PortalServer) handleProviderProvision(w http.ResponseWriter, r *http.Request) {
	claims, _ := ClaimsFromContext(r.Context())

	var onboardingComplete bool
	err := ps.db.Pool.QueryRow(r.Context(),
		`SELECT onboarding_complete FROM participants WHERE id = $1`,
		claims.UserID,
	).Scan(&onboardingComplete)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if !onboardingComplete {
		http.Redirect(w, r, "/provider/onboarding", http.StatusSeeOther)
		return
	}

	// Get the provider's first node ID (used as target for new profiles).
	var nodeID string
	_ = ps.db.Pool.QueryRow(r.Context(),
		`SELECT id FROM nodes WHERE participant_id = $1 ORDER BY created_at ASC LIMIT 1`,
		claims.UserID,
	).Scan(&nodeID)

	rows, err := ps.db.Pool.Query(r.Context(),
		`SELECT id, name, is_default, cpu_enabled, gpu_pct, ram_pct, storage_gb, bandwidth_mbps
		 FROM resource_profiles
		 WHERE node_id IN (SELECT id FROM nodes WHERE participant_id = $1)
		 ORDER BY is_default DESC, created_at ASC`,
		claims.UserID,
	)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var profiles []ProfileRow
	for rows.Next() {
		var p ProfileRow
		if err := rows.Scan(&p.ID, &p.Name, &p.IsDefault, &p.CPUEnabled,
			&p.GPUPct, &p.RAMPct, &p.StorageGB, &p.BandwidthMbps); err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		profiles = append(profiles, p)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	var uptimePct float64
	_ = ps.db.Pool.QueryRow(r.Context(),
		`SELECT COALESCE(AVG(uptime_pct), 100.0) FROM nodes WHERE participant_id = $1`,
		claims.UserID,
	).Scan(&uptimePct)

	var eligibleClasses []string
	if uptimePct >= 95.0 {
		eligibleClasses = append(eligibleClasses, "A")
	}
	if uptimePct >= 85.0 {
		eligibleClasses = append(eligibleClasses, "B")
	}
	if uptimePct >= 80.0 {
		eligibleClasses = append(eligibleClasses, "D")
	}
	if uptimePct >= 70.0 {
		eligibleClasses = append(eligibleClasses, "C")
	}

	ps.renderTemplate(w, "provider_provision.html", provisionData{
		Email:           claims.Email,
		NodeID:          nodeID,
		Profiles:        profiles,
		UptimePct:       uptimePct,
		EligibleClasses: eligibleClasses,
		IsAuthenticated: true,
	})
}

// RunNodeGauge queries the online node count every 60 seconds and updates
// the NodesOnlineGauge metric. Intended to be run in a goroutine.
func RunNodeGauge(ctx context.Context, db *store.DB) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var count int64
			if err := db.Pool.QueryRow(ctx,
				`SELECT COUNT(*) FROM nodes WHERE status = 'online'`,
			).Scan(&count); err == nil {
				metrics.NodesOnlineGauge.Set(float64(count))
			}
		}
	}
}

func (ps *PortalServer) handleAddProfile(w http.ResponseWriter, r *http.Request) {
	claims, _ := ClaimsFromContext(r.Context())

	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	nodeID := r.FormValue("node_id")
	name := strings.TrimSpace(r.FormValue("name"))
	isDefault := r.FormValue("is_default") == "true"
	cpuEnabled := r.FormValue("cpu_enabled") == "true"

	if nodeID == "" {
		http.Error(w, "node_id is required", http.StatusBadRequest)
		return
	}
	if name == "" {
		http.Error(w, "profile name is required", http.StatusBadRequest)
		return
	}

	ramPct, err := strconv.Atoi(r.FormValue("ram_pct"))
	if err != nil || ramPct < 1 || ramPct > 100 {
		http.Error(w, "ram_pct must be between 1 and 100", http.StatusBadRequest)
		return
	}
	storageGB, err := strconv.Atoi(r.FormValue("storage_gb"))
	if err != nil || storageGB < 0 {
		http.Error(w, "storage_gb must be 0 or greater", http.StatusBadRequest)
		return
	}
	bandwidthMbps, err := strconv.Atoi(r.FormValue("bandwidth_mbps"))
	if err != nil || bandwidthMbps < 0 {
		http.Error(w, "bandwidth_mbps must be 0 or greater", http.StatusBadRequest)
		return
	}
	priceMultiplier, err := strconv.ParseFloat(r.FormValue("price_multiplier"), 64)
	if err != nil || priceMultiplier < 0.5 || priceMultiplier > 2.0 {
		http.Error(w, "price_multiplier must be between 0.50 and 2.00", http.StatusBadRequest)
		return
	}

	// Verify the node belongs to the authenticated provider.
	var participantID string
	err = ps.db.Pool.QueryRow(r.Context(),
		`SELECT participant_id FROM nodes WHERE id = $1`,
		nodeID,
	).Scan(&participantID)
	if err != nil {
		http.Error(w, "node not found", http.StatusNotFound)
		return
	}
	if participantID != claims.UserID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	// Clear any existing default for this node before inserting a new one.
	if isDefault {
		_, err = ps.db.Pool.Exec(r.Context(),
			`UPDATE resource_profiles SET is_default = FALSE WHERE node_id = $1`,
			nodeID,
		)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
	}

	_, err = ps.db.Pool.Exec(r.Context(),
		`INSERT INTO resource_profiles
		 (node_id, name, is_default, cpu_enabled, gpu_pct, ram_pct, storage_gb, bandwidth_mbps, price_multiplier)
		 VALUES ($1, $2, $3, $4, 0, $5, $6, $7, $8)`,
		nodeID, name, isDefault, cpuEnabled, ramPct, storageGB, bandwidthMbps, priceMultiplier,
	)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, "/provider/provision", http.StatusSeeOther)
}

// handleGenerateNodeToken issues a single-use registration token tied to the
// authenticated participant. The token is displayed once on the dashboard so
// the installer wizard can copy it during first-run node claim.
func (ps *PortalServer) handleGenerateNodeToken(w http.ResponseWriter, r *http.Request) {
	claims, _ := ClaimsFromContext(r.Context())

	// Cap unused tokens at 1 per participant. If they already have an unused
	// unexpired token, just re-render the dashboard — buildDashboardData will
	// surface the existing token. This stops both accidental double-clicks and
	// abusive direct API hits to /node/token.
	var unusedCount int
	if err := ps.db.Pool.QueryRow(r.Context(),
		`SELECT COUNT(*) FROM node_registration_tokens
		 WHERE participant_id = $1 AND used_at IS NULL AND expires_at > NOW()`,
		claims.UserID,
	).Scan(&unusedCount); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if unusedCount >= 1 {
		data, err := ps.buildDashboardData(r.Context(), claims)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		ps.renderTemplate(w, "dashboard.html", data)
		return
	}

	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	token := hex.EncodeToString(raw)

	_, err := ps.db.Pool.Exec(r.Context(),
		`INSERT INTO node_registration_tokens (token, participant_id) VALUES ($1, $2)`,
		token, claims.UserID,
	)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Generate a SPIRE join token tied to this registration token's TTL.
	// The spire-server binary is available via the shared socket volume.
	spireJoinToken := ""
	spireExpires := time.Time{}
	spireOut, spireErr := exec.CommandContext(r.Context(),
		"/opt/spire/bin/spire-server", "token", "generate",
		"-socketPath", "/run/spire-server/private/api.sock",
		"-ttl", "604800", // 7 days in seconds
	).Output()
	if spireErr == nil {
		// Output format: "Token: <token>\n"
		line := strings.TrimSpace(string(spireOut))
		if after, ok := strings.CutPrefix(line, "Token: "); ok {
			spireJoinToken = after
			spireExpires = time.Now().Add(7 * 24 * time.Hour)
			_, _ = ps.db.Pool.Exec(r.Context(),
				`UPDATE node_registration_tokens
				 SET spire_join_token = $1, spire_join_token_expires = $2
				 WHERE token = $3`,
				spireJoinToken, spireExpires, token,
			)
		}
	} else {
		slog.Warn("spire token generate failed — node will need manual SPIRE provisioning",
			"error", spireErr)
	}

	// Re-render dashboard with the token surfaced once.
	data, err := ps.buildDashboardData(r.Context(), claims)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	data.RegToken = token
	ps.renderTemplate(w, "dashboard.html", data)
}


// formatOptOutSyncStatus returns the per-node sync-status string for the UI.
// Uses last_heartbeat_at vs opt_out_updated_at as a heuristic: if the node
// has heartbeated since the version was bumped we treat that as confirmation.
func formatOptOutSyncStatus(version int, optOutUpdatedAt time.Time, lastHeartbeat *time.Time, now time.Time) string {
	if lastHeartbeat == nil || lastHeartbeat.Before(optOutUpdatedAt) {
		return fmt.Sprintf("Version %d · pending node confirmation", version)
	}
	return fmt.Sprintf("Version %d · last confirmed %s ago", version, formatShortDuration(now.Sub(*lastHeartbeat)))
}

// formatShortDuration renders a duration in compact form: "12s", "3m", "2h", "5d".
func formatShortDuration(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}

// handleOptOutPage renders /opt-out, listing every node owned by the
// authenticated participant with its current opt-out flags and printer toggles.
func (ps *PortalServer) handleOptOutPage(w http.ResponseWriter, r *http.Request) {
	claims, _ := ClaimsFromContext(r.Context())

	rows, err := ps.db.Pool.Query(r.Context(), `
		SELECT n.id, n.hostname,
		       n.opt_out_compute, n.opt_out_storage, n.opt_out_printing,
		       n.opt_out_version, n.opt_out_updated_at, n.last_heartbeat_at,
		       COALESCE(
		         jsonb_agg(
		           jsonb_build_object(
		             'printer_id', np.printer_id,
		             'printer_name', np.printer_name,
		             'enabled', np.enabled
		           )
		           ORDER BY np.printer_id
		         ) FILTER (WHERE np.printer_id IS NOT NULL),
		         '[]'::jsonb
		       ) AS printers
		FROM nodes n
		LEFT JOIN node_printers np ON np.node_id = n.id
		WHERE n.participant_id = $1
		GROUP BY n.id
		ORDER BY n.hostname
	`, claims.UserID)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	now := time.Now()
	var nodeRows []OptOutNodeRow
	for rows.Next() {
		var (
			row             OptOutNodeRow
			optOutUpdatedAt time.Time
			lastHeartbeat   *time.Time
			printersJSON    []byte
		)
		if err := rows.Scan(
			&row.ID, &row.Hostname,
			&row.OptOutCompute, &row.OptOutStorage, &row.OptOutPrinting,
			&row.Version, &optOutUpdatedAt, &lastHeartbeat,
			&printersJSON,
		); err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		var raw []optOutPrinterDTO
		if err := json.Unmarshal(printersJSON, &raw); err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		for _, p := range raw {
			row.Printers = append(row.Printers, PrinterRow{
				PrinterID:   p.PrinterID,
				DisplayName: p.Name,
				Enabled:     p.Enabled,
			})
		}

		row.SyncStatus = formatOptOutSyncStatus(row.Version, optOutUpdatedAt, lastHeartbeat, now)
		nodeRows = append(nodeRows, row)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	ps.renderTemplate(w, "opt_out.html", OptOutPageData{
		IsAuthenticated: true,
		Nodes:           nodeRows,
	})
}

// handleGetOptOut returns the current opt-out state for a single node owned by
// the authenticated participant. Returns 404 if the node does not exist OR is
// not owned by the caller — deliberately indistinguishable to avoid leaking
// node existence across accounts.
func (ps *PortalServer) handleGetOptOut(w http.ResponseWriter, r *http.Request) {
	claims, _ := ClaimsFromContext(r.Context())

	nodeID := strings.TrimSpace(r.URL.Query().Get("node_id"))
	if nodeID == "" {
		http.Error(w, "node_id required", http.StatusBadRequest)
		return
	}

	var (
		resp          optOutAPIResponse
		participantID string
		lastHeartbeat *time.Time
		printersJSON  []byte
	)
	err := ps.db.Pool.QueryRow(r.Context(), `
		SELECT n.participant_id, n.id,
		       n.opt_out_compute, n.opt_out_storage, n.opt_out_printing,
		       n.opt_out_version, n.opt_out_updated_at, n.last_heartbeat_at,
		       COALESCE(
		         jsonb_agg(
		           jsonb_build_object(
		             'printer_id', np.printer_id,
		             'printer_name', np.printer_name,
		             'enabled', np.enabled
		           )
		           ORDER BY np.printer_id
		         ) FILTER (WHERE np.printer_id IS NOT NULL),
		         '[]'::jsonb
		       ) AS printers
		FROM nodes n
		LEFT JOIN node_printers np ON np.node_id = n.id
		WHERE n.id = $1
		GROUP BY n.id
	`, nodeID).Scan(
		&participantID, &resp.NodeID,
		&resp.Compute, &resp.Storage, &resp.Printing,
		&resp.Version, &resp.UpdatedAt, &lastHeartbeat,
		&printersJSON,
	)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if participantID != claims.UserID {
		// Deliberate 404 (not 403): don't leak that this node exists under
		// another account. Differs from handleAddProfile which uses 403.
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	if err := json.Unmarshal(printersJSON, &resp.Printers); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	resp.LastHeartbeat = lastHeartbeat

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		return
	}
}

// handlePostOptOut writes new opt-out state for a single owned node. Bumps
// opt_out_version by 1, refreshes opt_out_updated_at, and updates the enabled
// flag on each printer in the request body. Printers absent from the body are
// left untouched. Version bump and printer updates are atomic (transaction).
func (ps *PortalServer) handlePostOptOut(w http.ResponseWriter, r *http.Request) {
	claims, _ := ClaimsFromContext(r.Context())

	var body optOutPostRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	body.NodeID = strings.TrimSpace(body.NodeID)
	if body.NodeID == "" {
		http.Error(w, "node_id required", http.StatusBadRequest)
		return
	}

	tx, err := ps.db.Pool.Begin(r.Context())
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(r.Context())

	// Ownership check — 404 on any failure or mismatch (don't leak existence).
	var participantID string
	err = tx.QueryRow(r.Context(),
		`SELECT participant_id FROM nodes WHERE id = $1`,
		body.NodeID,
	).Scan(&participantID)
	if err != nil || participantID != claims.UserID {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	// Bump version + write opt-out flags. RETURNING gives new version for response.
	var newVersion int
	err = tx.QueryRow(r.Context(), `
		UPDATE nodes
		SET opt_out_compute = $1,
		    opt_out_storage = $2,
		    opt_out_printing = $3,
		    opt_out_version = opt_out_version + 1,
		    opt_out_updated_at = NOW()
		WHERE id = $4
		RETURNING opt_out_version
	`, body.Compute, body.Storage, body.Printing, body.NodeID).Scan(&newVersion)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Apply printer enabled flags. Printers absent from the body are left as-is;
	// unknown printer_ids no-op silently — agent re-report on hash mismatch
	// reconciles any drift.
	for _, p := range body.Printers {
		if _, err := tx.Exec(r.Context(),
			`UPDATE node_printers SET enabled = $1 WHERE node_id = $2 AND printer_id = $3`,
			p.Enabled, body.NodeID, p.PrinterID,
		); err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
	}

	if err := tx.Commit(r.Context()); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"version": newVersion,
	})
}

// handleConsumerPickedUp transitions a print job from awaiting_pickup → picked_up.
// The consumer must own the job (jobs.participant_id == session UserID).
// Sets picked_up_at = NOW(). C5 print lifecycle.
func (ps *PortalServer) handleConsumerPickedUp(w http.ResponseWriter, r *http.Request) {
	claims, _ := ClaimsFromContext(r.Context())
	jobID := r.PathValue("id")

	var currentStatus string
	err := ps.db.Pool.QueryRow(r.Context(),
		`SELECT status::text FROM jobs WHERE id = $1 AND participant_id = $2`,
		jobID, claims.UserID,
	).Scan(&currentStatus)
	if err != nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	if currentStatus != "awaiting_pickup" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":          "wrong_status",
			"current_status": currentStatus,
		})
		return
	}

	tag, err := ps.db.Pool.Exec(r.Context(),
		`UPDATE jobs SET status = 'picked_up'::job_status, picked_up_at = NOW(), updated_at = NOW()
		 WHERE id = $1 AND status = 'awaiting_pickup'::job_status`,
		jobID,
	)
	if err != nil {
		slog.Error("handleConsumerPickedUp: update", "job_id", jobID, "error", err)
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}
	if tag.RowsAffected() == 0 {
		http.Error(w, "status changed concurrently", http.StatusConflict)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "picked_up"})
}

// handleConsumerDelivered transitions a print job from picked_up → delivered.
// The consumer must own the job (jobs.participant_id == session UserID).
// Sets delivered_at and completed_at (terminal, payout-eligible for C7). C5 print lifecycle.
func (ps *PortalServer) handleConsumerDelivered(w http.ResponseWriter, r *http.Request) {
	claims, _ := ClaimsFromContext(r.Context())
	jobID := r.PathValue("id")

	var currentStatus string
	err := ps.db.Pool.QueryRow(r.Context(),
		`SELECT status::text FROM jobs WHERE id = $1 AND participant_id = $2`,
		jobID, claims.UserID,
	).Scan(&currentStatus)
	if err != nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	if currentStatus != "picked_up" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":          "wrong_status",
			"current_status": currentStatus,
		})
		return
	}

	tag, err := ps.db.Pool.Exec(r.Context(),
		`UPDATE jobs SET status = 'delivered'::job_status,
			delivered_at = NOW(), completed_at = NOW(), updated_at = NOW()
		 WHERE id = $1 AND status = 'picked_up'::job_status`,
		jobID,
	)
	if err != nil {
		slog.Error("handleConsumerDelivered: update", "job_id", jobID, "error", err)
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}
	if tag.RowsAffected() == 0 {
		http.Error(w, "status changed concurrently", http.StatusConflict)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "delivered"})
}

// handleProviderNoShow flags an awaiting_pickup print job as failed after 7 days
// without consumer pickup. Authorization: caller must own the node assigned to
// the job (provider role, via JOIN). In C5 this is unilateral — no consumer
// dispute mechanism exists yet. C7 will add disputes; admin recourse is
// out-of-band until then.
func (ps *PortalServer) handleProviderNoShow(w http.ResponseWriter, r *http.Request) {
	claims, _ := ClaimsFromContext(r.Context())
	jobID := r.PathValue("id")

	var currentStatus string
	var awaitingPickupAt *time.Time
	err := ps.db.Pool.QueryRow(r.Context(),
		`SELECT j.status::text, j.awaiting_pickup_at
		 FROM jobs j JOIN nodes n ON n.id = j.node_id
		 WHERE j.id = $1 AND n.participant_id = $2`,
		jobID, claims.UserID,
	).Scan(&currentStatus, &awaitingPickupAt)
	if err != nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	if currentStatus != "awaiting_pickup" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":          "wrong_status",
			"current_status": currentStatus,
		})
		return
	}
	if awaitingPickupAt == nil || time.Since(*awaitingPickupAt) < 7*24*time.Hour {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "too_soon"})
		return
	}

	// Race-safe: 7-day check re-encoded in WHERE so a concurrent update between
	// the SELECT and this Exec cannot produce a premature no-show.
	tag, err := ps.db.Pool.Exec(r.Context(),
		`UPDATE jobs SET status = 'failed'::job_status,
			failure_cause = 'no_show_after_7d',
			completed_at = NOW(),
			updated_at = NOW()
		 WHERE id = $1
		   AND status = 'awaiting_pickup'::job_status
		   AND awaiting_pickup_at < NOW() - INTERVAL '7 days'`,
		jobID,
	)
	if err != nil {
		slog.Error("handleProviderNoShow: update", "job_id", jobID, "error", err)
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}
	if tag.RowsAffected() == 0 {
		http.Error(w, "status changed concurrently", http.StatusConflict)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "failed"})
}

// handleConsumerContestNoShow opens a dispute over a job the contributor flagged
// as a no-show (handleProviderNoShow). This is the C5→C7 recourse: the no-show
// is contributor-unilateral, so the consumer's counter is to contest it after
// the fact. The job stays 'failed'; the dispute is the recourse layer on top,
// resolved by an arbiter through the existing Dispute Terminal.
//
// Authorization: caller must own the job (jobs.participant_id == session
// UserID). A miss returns 404 (no-leak convention, consistent with the other
// C5 consumer endpoints). Guards: the job must be status='failed' with
// failure_cause='no_show_after_7d'; node_id must be non-NULL (disputes.node_id
// is NOT NULL, jobs.node_id is ON DELETE SET NULL); and the contest must land
// within 7 days of completion. One active dispute per job is enforced by
// idx_disputes_one_active_per_job (migration 026); a unique violation maps to
// 409 already_contested. C7 print lifecycle.
func (ps *PortalServer) handleConsumerContestNoShow(w http.ResponseWriter, r *http.Request) {
	claims, _ := ClaimsFromContext(r.Context())
	jobID := r.PathValue("id")

	var (
		currentStatus string
		failureCause  string
		nodeID        string
		completedAt   *time.Time
	)
	err := ps.db.Pool.QueryRow(r.Context(),
		`SELECT status::text, COALESCE(failure_cause, ''),
		        COALESCE(node_id::text, ''), completed_at
		 FROM jobs WHERE id = $1 AND participant_id = $2`,
		jobID, claims.UserID,
	).Scan(&currentStatus, &failureCause, &nodeID, &completedAt)
	if err != nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	if currentStatus != "failed" || failureCause != "no_show_after_7d" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":          "wrong_status",
			"current_status": currentStatus,
		})
		return
	}
	if nodeID == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "no_node"})
		return
	}
	if completedAt == nil || time.Since(*completedAt) >= 7*24*time.Hour {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "window_expired"})
		return
	}

	// Race-safe INSERT: the guards above are re-encoded in the WHERE so a
	// concurrent status change between the SELECT and this INSERT cannot open a
	// dispute on a job that no longer qualifies. The unique index enforces the
	// one-active-dispute-per-job invariant; a duplicate contest surfaces as a
	// 23505 unique violation translated to 409 below.
	var disputeID string
	err = ps.db.Pool.QueryRow(r.Context(),
		`INSERT INTO disputes (job_id, node_id, participant_id, payment_intent_id, reason, status)
		 SELECT id, node_id, participant_id, COALESCE(payment_intent_id, ''),
		        'contested no-show', 'open'::dispute_status
		 FROM jobs
		 WHERE id = $1
		   AND participant_id = $2
		   AND status = 'failed'::job_status
		   AND failure_cause = 'no_show_after_7d'
		   AND node_id IS NOT NULL
		   AND completed_at > NOW() - INTERVAL '7 days'
		 RETURNING id`,
		jobID, claims.UserID,
	).Scan(&disputeID)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "already_contested"})
			return
		}
		if errors.Is(err, pgx.ErrNoRows) {
			// A concurrent update changed the job out from under the guards.
			http.Error(w, "status changed concurrently", http.StatusConflict)
			return
		}
		slog.Error("handleConsumerContestNoShow: insert", "job_id", jobID, "error", err)
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":     "contested",
		"dispute_id": disputeID,
	})
}
