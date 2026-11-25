//go:build integration

package api

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/NTARI-RAND/sohocloud-protocol/fees"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/notify"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/operator"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/orchestrator"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/protocoladapter"
)

func TestGovernance_FeeTimestampSurvivesDatabaseRoundTrip(t *testing.T) {
	db := connectAPITestDB(t)
	ctx := context.Background()
	const coordinatorID = "fee-precision-test"
	if _, err := db.Pool.Exec(ctx, "DELETE FROM fee_declarations WHERE coordinator_id = $1", coordinatorID); err != nil {
		t.Fatalf("reset fee declarations: %v", err)
	}
	repo := operator.NewRepository(db.Pool)
	g := newTestGovServer(t, repo, notify.NewLogNotifier())
	g.coordinatorID = coordinatorID

	admin := govMux(g)
	rec := postGov(admin, "/admin/fees",
		`{"contributor_share_bps":6500,"platform_fee_bps":3500,"seq":1,"effective_at":"2026-12-01T00:00:00.000000001Z"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("publish sub-microsecond time: got %d, want 400; body=%s", rec.Code, rec.Body)
	}
	if _, err := repo.CurrentFeeDeclaration(ctx, coordinatorID); !errors.Is(err, operator.ErrNoFeeDeclaration) {
		t.Fatalf("rejected declaration should not be stored: %v", err)
	}

	rec = postGov(admin, "/admin/fees",
		`{"contributor_share_bps":6500,"platform_fee_bps":3500,"seq":1,"effective_at":"2026-12-01T00:00:00.123456Z"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("publish microsecond time: got %d, want 201; body=%s", rec.Code, rec.Body)
	}
	adapter := protocoladapter.New(nil, orchestrator.NewNodeRegistry(), repo, coordinatorID, nil, nil)
	handler := protocoladapter.NewHandler(adapter, nil, true, nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v0/fees", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("read fees: got %d, want 200; body=%s", rec.Code, rec.Body)
	}

	var got fees.FeeDeclaration
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode stored fee declaration: %v", err)
	}
	want := time.Date(2026, time.December, 1, 0, 0, 0, 123456000, time.UTC)
	if !got.EffectiveAt.Equal(want) || !got.Verify(g.coordKey.Public().(ed25519.PublicKey)) {
		t.Fatalf("stored declaration did not preserve the signed timestamp %s: %+v", want, got)
	}
}
