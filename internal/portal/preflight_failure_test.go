package portal

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/orchclient"
)

const preflightContentType = "Content-Type"

// A successful recheck must not invite a duplicate after an ambiguous submit.
func TestJobCheckSubmissionFailureRequiresDashboardCheck(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusServiceUnavailable} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			server := httptest.NewServer(failedSubmissionAPI(status))
			defer server.Close()
			ps := newTestPortalServerWithOrch(t, nil, orchclient.New(server.URL))
			w := checkRequest(t, ps, "POST", "/consumer/job/check/submit", jobForm(), true)
			if w.Code != http.StatusConflict && w.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d", w.Code)
			}
			if !strings.Contains(w.Body.String(), "Check your dashboard before trying again.") {
				t.Errorf("missing dashboard warning: %s", w.Body.String())
			}
			if strings.Contains(w.Body.String(), "Submit job</button>") {
				t.Error("failed submission offered another Submit")
			}
		})
	}
}

// Unknown coordinator codes must remain visible rather than imply no nodes.
func TestJobCheckExplainsUnknownBlocker(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(preflightContentType, "application/json")
		_, _ = fmt.Fprint(w, `{"Ready":false,"HardwareChecked":true,"Blockers":[{"Code":"future_reason","Count":3}]}`)
	}))
	defer server.Close()
	ps := newTestPortalServerWithOrch(t, nil, orchclient.New(server.URL))
	w := checkRequest(t, ps, "POST", "/consumer/job/check", jobForm(), true)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "3: Nodes cannot meet another placement requirement.") || strings.Contains(w.Body.String(), "No eligible nodes are currently registered") {
		t.Fatalf("unknown blocker lost: %d %s", w.Code, w.Body.String())
	}
}

// Disconnected portal requests must cancel the real process-to-process check.
func TestJobCheckCancelsInternalRequest(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			return
		}
		close(started)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	ps := newTestPortalServerWithOrch(t, nil, orchclient.New(server.URL))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := httptest.NewRequest("POST", "/consumer/job/check", strings.NewReader(jobForm().Encode())).WithContext(ctx)
	r.Header.Set(preflightContentType, "application/x-www-form-urlencoded")
	r.AddCookie(&http.Cookie{Name: "session_token", Value: mustCreateToken(t, ps.sm, "buyer", "buyer@example.org")})
	done := make(chan struct{})
	go func() { defer close(done); ps.srv.Handler.ServeHTTP(httptest.NewRecorder(), r) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("internal request did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Error("portal check ignored request cancellation")
	}
}

func failedSubmissionAPI(checkStatus int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/jobs/submit" {
			http.Error(w, "unknown outcome", http.StatusServiceUnavailable)
			return
		}
		if checkStatus != http.StatusOK {
			http.Error(w, "unavailable", checkStatus)
			return
		}
		w.Header().Set(preflightContentType, "application/json")
		_, _ = fmt.Fprint(w, `{"Ready":true,"EligibleNodes":1,"RequiredNodes":1,"HardwareChecked":true}`)
	})
}
