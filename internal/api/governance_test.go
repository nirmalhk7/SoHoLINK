package api

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/NTARI-RAND/sohocloud-protocol/fees"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/notify"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/operator"
)

// fakeGovRepo is an in-memory governanceRepo for handler unit tests.
type fakeGovRepo struct {
	published    []fees.FeeDeclaration
	current      *fees.FeeDeclaration
	revoked      []string
	disconnected []string
	opEmails     []string
	memEmails    []string
	failPublish  error
	failEmails   error
}

func (f *fakeGovRepo) PublishFeeDeclaration(_ context.Context, decl fees.FeeDeclaration) error {
	if f.failPublish != nil {
		return f.failPublish
	}
	// Enforce the same monotonicity the real repo does, so the handler mapping is
	// exercised realistically.
	if f.current != nil {
		if decl.Seq <= f.current.Seq {
			return operator.ErrFeeSeqNotMonotonic
		}
		if !decl.EffectiveAt.After(f.current.EffectiveAt) {
			return operator.ErrFeeEffectiveAtRetroactive
		}
	}
	f.published = append(f.published, decl)
	cp := decl
	f.current = &cp
	return nil
}

func (f *fakeGovRepo) CurrentFeeDeclaration(_ context.Context, _ string) (fees.FeeDeclaration, error) {
	if f.current == nil {
		return fees.FeeDeclaration{}, operator.ErrNoFeeDeclaration
	}
	return *f.current, nil
}

func (f *fakeGovRepo) Revoke(_ context.Context, id string) error {
	f.revoked = append(f.revoked, id)
	return nil
}

func (f *fakeGovRepo) Disconnect(_ context.Context, id string) error {
	f.disconnected = append(f.disconnected, id)
	return nil
}

func (f *fakeGovRepo) OperatorEmails(_ context.Context) ([]string, error) {
	if f.failEmails != nil {
		return nil, f.failEmails
	}
	return f.opEmails, nil
}

func (f *fakeGovRepo) MemberEmails(_ context.Context) ([]string, error) {
	if f.failEmails != nil {
		return nil, f.failEmails
	}
	return f.memEmails, nil
}

func newTestGovServer(t *testing.T, repo governanceRepo, n notify.Notifier) *GovernanceServer {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	g, err := NewGovernanceServer(repo, n, GovernanceConfig{
		Addr:           "127.0.0.1:0",
		CoordinatorID:  "soholink",
		CoordinatorKey: priv,
	})
	if err != nil {
		t.Fatalf("NewGovernanceServer: %v", err)
	}
	return g
}

// govMux builds the routed+loopback-guarded handler for direct httptest use.
func govMux(g *GovernanceServer) http.Handler {
	mux := http.NewServeMux()
	g.registerRoutes(mux)
	return g.loopbackOnly(mux)
}

func postGov(h http.Handler, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.RemoteAddr = "127.0.0.1:5555" // loopback source
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// A non-loopback bind is refused at construction.
func TestNewGovernanceServer_RejectsNonLoopback(t *testing.T) {
	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	for _, addr := range []string{"0.0.0.0:8090", ":8090", "203.0.113.5:8090"} {
		_, err := NewGovernanceServer(&fakeGovRepo{}, notify.NewLogNotifier(), GovernanceConfig{
			Addr:           addr,
			CoordinatorID:  "soholink",
			CoordinatorKey: priv,
		})
		if err != ErrGovernanceNotLoopback {
			t.Fatalf("addr %q: got %v, want ErrGovernanceNotLoopback", addr, err)
		}
	}
}

// A malformed coordinator key is refused at construction.
func TestNewGovernanceServer_RejectsBadKey(t *testing.T) {
	_, err := NewGovernanceServer(&fakeGovRepo{}, notify.NewLogNotifier(), GovernanceConfig{
		Addr:           "127.0.0.1:8090",
		CoordinatorID:  "soholink",
		CoordinatorKey: ed25519.PrivateKey([]byte("too short")),
	})
	if err != ErrGovernanceBadKey {
		t.Fatalf("got %v, want ErrGovernanceBadKey", err)
	}
}

// A non-loopback source is rejected 403 even on a correctly-bound server (SSRF
// defense-in-depth).
func TestGovernance_RejectsNonLoopbackSource(t *testing.T) {
	g := newTestGovServer(t, &fakeGovRepo{}, notify.NewLogNotifier())
	h := govMux(g)
	req := httptest.NewRequest(http.MethodGet, "/admin/fees/current", nil)
	req.RemoteAddr = "203.0.113.9:4444" // NOT loopback
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("non-loopback source: got %d, want 403", rec.Code)
	}
}

// Publishing fees signs with the coordinator key and the signature verifies
// against the coordinator public key over the canonical bytes.
func TestGovernance_PublishFees_SignsVerifiably(t *testing.T) {
	repo := &fakeGovRepo{}
	g := newTestGovServer(t, repo, notify.NewLogNotifier())
	h := govMux(g)

	body := `{"contributor_share_bps":6500,"platform_fee_bps":3500,"seq":1,"effective_at":"2026-08-01T00:00:00Z"}`
	rec := postGov(h, "/admin/fees", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("publish fees: got %d body=%s", rec.Code, rec.Body.String())
	}
	if len(repo.published) != 1 {
		t.Fatalf("expected 1 published declaration, got %d", len(repo.published))
	}
	decl := repo.published[0]
	pub := g.coordKey.Public().(ed25519.PublicKey)
	if !decl.Verify(pub) {
		t.Fatalf("published declaration signature does not verify against coordinator key")
	}
	if decl.CoordinatorID != "soholink" {
		t.Fatalf("coordinator id = %q, want soholink", decl.CoordinatorID)
	}

	var resp feeDeclarationResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !resp.Balanced {
		t.Fatalf("expected balanced=true for 6500+3500")
	}
}

func TestGovernance_PublishFees_RejectsUnrepresentableEffectiveTime(t *testing.T) {
	for _, effectiveAt := range []string{
		"0001-01-01T00:00:00Z",
		"1677-09-21T00:12:43.145224191Z",
		"2262-04-11T23:47:16.854775808Z",
	} {
		t.Run(effectiveAt, func(t *testing.T) {
			repo := &fakeGovRepo{}
			handler := govMux(newTestGovServer(t, repo, notify.NewLogNotifier()))
			body := `{"contributor_share_bps":6500,"platform_fee_bps":3500,"seq":1,"effective_at":"` + effectiveAt + `"}`
			rec := postGov(handler, "/admin/fees", body)
			if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "effective_at") {
				t.Errorf("publish fees: got %d body=%s, want 400 for effective_at", rec.Code, rec.Body)
			}
			if len(repo.published) != 0 || repo.current != nil {
				t.Fatal("invalid declaration reached publication")
			}
		})
	}
}

func TestGovernance_PublishFees_AcceptsInRangeEffectiveTime(t *testing.T) {
	repo := &fakeGovRepo{}
	g := newTestGovServer(t, repo, notify.NewLogNotifier())
	body := `{"contributor_share_bps":6500,"platform_fee_bps":3500,"seq":1,"effective_at":"2262-04-11T23:47:16.854775Z"}`
	rec := postGov(govMux(g), "/admin/fees", body)
	if rec.Code != http.StatusCreated || len(repo.published) != 1 {
		t.Fatalf("publish fees: got %d body=%s; published %d declarations", rec.Code, rec.Body, len(repo.published))
	}
	if !repo.published[0].Verify(g.coordKey.Public().(ed25519.PublicKey)) {
		t.Fatal("published declaration signature does not verify")
	}
}

// A non-monotonic Seq is rejected 409 (SPEC §5.3 legible/non-retroactive).
func TestGovernance_PublishFees_RejectsNonMonotonicSeq(t *testing.T) {
	repo := &fakeGovRepo{}
	g := newTestGovServer(t, repo, notify.NewLogNotifier())
	h := govMux(g)

	if rec := postGov(h, "/admin/fees",
		`{"contributor_share_bps":6500,"platform_fee_bps":3500,"seq":5,"effective_at":"2026-08-01T00:00:00Z"}`); rec.Code != http.StatusCreated {
		t.Fatalf("first publish: got %d", rec.Code)
	}
	// Same seq -> 409.
	if rec := postGov(h, "/admin/fees",
		`{"contributor_share_bps":7000,"platform_fee_bps":3000,"seq":5,"effective_at":"2026-09-01T00:00:00Z"}`); rec.Code != http.StatusConflict {
		t.Fatalf("non-monotonic seq: got %d, want 409", rec.Code)
	}
}

// A retroactive EffectiveAt is rejected 409.
func TestGovernance_PublishFees_RejectsRetroactive(t *testing.T) {
	repo := &fakeGovRepo{}
	g := newTestGovServer(t, repo, notify.NewLogNotifier())
	h := govMux(g)

	if rec := postGov(h, "/admin/fees",
		`{"contributor_share_bps":6500,"platform_fee_bps":3500,"seq":1,"effective_at":"2026-08-01T00:00:00Z"}`); rec.Code != http.StatusCreated {
		t.Fatalf("first publish: got %d", rec.Code)
	}
	// Higher seq but earlier EffectiveAt -> retroactive -> 409.
	if rec := postGov(h, "/admin/fees",
		`{"contributor_share_bps":7000,"platform_fee_bps":3000,"seq":2,"effective_at":"2026-07-01T00:00:00Z"}`); rec.Code != http.StatusConflict {
		t.Fatalf("retroactive effective_at: got %d, want 409", rec.Code)
	}
}

// Revoke and disconnect reach the repository.
func TestGovernance_RevokeAndDisconnect(t *testing.T) {
	repo := &fakeGovRepo{}
	g := newTestGovServer(t, repo, notify.NewLogNotifier())
	h := govMux(g)

	if rec := postGov(h, "/admin/operators/cloudy/revoke", ""); rec.Code != http.StatusOK {
		t.Fatalf("revoke: got %d", rec.Code)
	}
	if rec := postGov(h, "/admin/operators/fruitful/disconnect", ""); rec.Code != http.StatusOK {
		t.Fatalf("disconnect: got %d", rec.Code)
	}
	if len(repo.revoked) != 1 || repo.revoked[0] != "cloudy" {
		t.Fatalf("revoked = %v", repo.revoked)
	}
	if len(repo.disconnected) != 1 || repo.disconnected[0] != "fruitful" {
		t.Fatalf("disconnected = %v", repo.disconnected)
	}
}

// Messaging sends to both audiences, deduplicated, via the Notifier; the code is
// never in the body.
func TestGovernance_SendMessage_BothAudiencesDeduped(t *testing.T) {
	repo := &fakeGovRepo{
		opEmails:  []string{"ops@cloudy.example", "shared@example.com"},
		memEmails: []string{"member@example.com", "shared@example.com"}, // shared is dup
	}
	log := notify.NewLogNotifier()
	g := newTestGovServer(t, repo, log)
	h := govMux(g)

	rec := postGov(h, "/admin/messages",
		`{"subject":"Maintenance","body":"Scheduled downtime tonight.","operators":true,"members":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("send message: got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp sendMessageResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// 3 unique recipients (shared collapsed).
	if resp.Recipients != 3 || resp.Sent != 3 {
		t.Fatalf("recipients=%d sent=%d, want 3/3", resp.Recipients, resp.Sent)
	}
	if len(log.Sent()) != 3 {
		t.Fatalf("notifier got %d messages, want 3", len(log.Sent()))
	}
}

// Messaging with no audience selected is a 400.
func TestGovernance_SendMessage_RequiresAudience(t *testing.T) {
	g := newTestGovServer(t, &fakeGovRepo{}, notify.NewLogNotifier())
	h := govMux(g)
	rec := postGov(h, "/admin/messages",
		`{"subject":"x","body":"y","operators":false,"members":false}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("no audience: got %d, want 400", rec.Code)
	}
}

// Only operators selected: members are not queried/sent.
func TestGovernance_SendMessage_OperatorsOnly(t *testing.T) {
	repo := &fakeGovRepo{
		opEmails:  []string{"ops@cloudy.example"},
		memEmails: []string{"member@example.com"},
	}
	log := notify.NewLogNotifier()
	g := newTestGovServer(t, repo, log)
	h := govMux(g)

	rec := postGov(h, "/admin/messages",
		`{"subject":"Ops","body":"operator-only note","operators":true,"members":false}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("got %d", rec.Code)
	}
	sent := log.Sent()
	if len(sent) != 1 || sent[0].To != "ops@cloudy.example" {
		t.Fatalf("expected only operator recipient, got %+v", sent)
	}
}

// A signing self-test failure surfaces as ErrGovernanceBadKey. Build a 64-byte
// key whose second half does not match the seed-derived public key.
func TestNewGovernanceServer_RejectsMismatchedKeyHalves(t *testing.T) {
	_, good, _ := ed25519.GenerateKey(rand.Reader)
	bad := make(ed25519.PrivateKey, ed25519.PrivateKeySize)
	copy(bad, good)
	// Corrupt the public-key half (bytes 32..63).
	bad[40] ^= 0xFF
	_, err := NewGovernanceServer(&fakeGovRepo{}, notify.NewLogNotifier(), GovernanceConfig{
		Addr:           "127.0.0.1:8090",
		CoordinatorID:  "soholink",
		CoordinatorKey: bad,
	})
	if err != ErrGovernanceBadKey {
		t.Fatalf("mismatched key halves: got %v, want ErrGovernanceBadKey", err)
	}
	_ = time.Now
}
