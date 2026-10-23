package portal

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"html"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/store"
)

func TestJobHistoryRequiresLogin(t *testing.T) {
	ps := newTestPortalServer(t, nil)
	w := httptest.NewRecorder()
	ps.srv.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/consumer/jobs", nil))
	if w.Code != http.StatusFound || w.Header().Get("Location") != "/login" {
		t.Fatalf("unauthenticated history: status %d, location %q", w.Code, w.Header().Get("Location"))
	}
}

func seedHistoryJob(t *testing.T, db *store.DB, owner, status, workload string, created time.Time) string {
	t.Helper()
	var id string
	err := db.Pool.QueryRow(context.Background(), `INSERT INTO jobs (participant_id, workload_type, status, created_at) VALUES ($1,$2::workload_type,$3::job_status,$4) RETURNING id`, owner, workload, status, created).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func historyPage(t *testing.T, ps *PortalServer, owner, path string) *httptest.ResponseRecorder {
	t.Helper()
	r := authenticatedRequest(t, ps.sm, http.MethodGet, path, owner, "history@test.com")
	w := httptest.NewRecorder()
	ps.srv.Handler.ServeHTTP(w, r)
	return w
}

var historyJobLinks = regexp.MustCompile(`href="/consumer/job/([0-9a-f-]+)"`)
var historyPageLinks = regexp.MustCompile(`href="(/consumer/jobs\?[^"<>]+)"`)

func historyIDs(body string) []string {
	ids := []string{}
	for _, match := range historyJobLinks.FindAllStringSubmatch(body, -1) {
		ids = append(ids, match[1])
	}
	return ids
}
func nextHistoryPage(t *testing.T, body string) string {
	t.Helper()
	for _, match := range historyPageLinks.FindAllStringSubmatch(body, -1) {
		path := html.UnescapeString(match[1])
		u, err := url.Parse(path)
		if err != nil {
			t.Fatal(err)
		}
		if u.Query().Get("cursor") != "" {
			return path
		}
	}
	return ""
}

// A timestamp-only cursor or OFFSET pagination would skip/duplicate jobs in
// this fixture: rows tie on created_at, then a newer matching job is inserted.
func TestJobHistoryPaginationFiltersAndOwnership(t *testing.T) {
	db := setupTestDB(t)
	ps := newTestPortalServer(t, db)
	owner := seedParticipant(t, db, "history-owner@test.com", "password")
	other := seedParticipant(t, db, "history-other@test.com", "password")
	stamp := time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC)
	var want []string
	for i := 0; i < 30; i++ {
		want = append(want, seedHistoryJob(t, db, owner, "completed", "batch_compute", stamp))
	}
	excluded := []string{
		seedHistoryJob(t, db, owner, "failed", "batch_compute", stamp),
		seedHistoryJob(t, db, owner, "completed", "ai_inference", stamp),
		seedHistoryJob(t, db, other, "completed", "batch_compute", stamp),
	}
	sort.Sort(sort.Reverse(sort.StringSlice(want)))
	path := "/consumer/jobs?status=completed&workload_type=batch_compute&participant_id=" + other
	first := historyPage(t, ps, owner, path)
	if first.Code != 200 {
		t.Fatalf("first page: %d %s", first.Code, first.Body.String())
	}
	got := historyIDs(first.Body.String())
	if !slices.Equal(got, want[:25]) {
		t.Fatalf("first page got %v, want first 25 of %v", got, want)
	}
	next := nextHistoryPage(t, first.Body.String())
	if next == "" {
		t.Fatal("missing older-jobs link")
	}
	parsed, err := url.Parse(next)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Query().Get("status") != "completed" || parsed.Query().Get("workload_type") != "batch_compute" {
		t.Fatalf("pagination lost filters: %s", next)
	}
	// uuid.Parse accepts URN form; a cursor accepted by validation must not
	// become a database cast error. Canonicalize it before executing the query.
	cursorBytes, err := base64.RawURLEncoding.DecodeString(parsed.Query().Get("cursor"))
	if err != nil {
		t.Fatal(err)
	}
	var urnCursor map[string]any
	if err := json.Unmarshal(cursorBytes, &urnCursor); err != nil {
		t.Fatal(err)
	}
	urnCursor["id"] = "urn:uuid:" + urnCursor["id"].(string)
	cursorBytes, err = json.Marshal(urnCursor)
	if err != nil {
		t.Fatal(err)
	}
	urnPage := *parsed
	urnQuery := urnPage.Query()
	urnQuery.Set("cursor", base64.RawURLEncoding.EncodeToString(cursorBytes))
	urnPage.RawQuery = urnQuery.Encode()
	if page := historyPage(t, ps, owner, urnPage.String()); page.Code != http.StatusOK || !slices.Equal(historyIDs(page.Body.String()), want[25:]) {
		t.Fatalf("accepted UUID form failed pagination: status %d, body %s", page.Code, page.Body.String())
	}
	changedFilters := *parsed
	changedQuery := changedFilters.Query()
	changedQuery.Set("status", "failed")
	changedFilters.RawQuery = changedQuery.Encode()
	if w := historyPage(t, ps, owner, changedFilters.String()); w.Code != http.StatusBadRequest {
		t.Fatalf("cursor reused with changed filters: status %d", w.Code)
	}
	newer := seedHistoryJob(t, db, owner, "completed", "batch_compute", stamp.Add(time.Hour))
	second := historyPage(t, ps, owner, next)
	if second.Code != 200 {
		t.Fatalf("second page: %d %s", second.Code, second.Body.String())
	}
	got = append(got, historyIDs(second.Body.String())...)
	if !slices.Equal(got, want) {
		t.Fatalf("pagination skipped/duplicated jobs: got %v, want %v", got, want)
	}
	if nextHistoryPage(t, second.Body.String()) != "" {
		t.Fatal("unexpected extra page")
	}
	for _, id := range append(excluded, newer) {
		if slices.Contains(got, id) {
			t.Fatalf("unexpected job %s in filtered pages", id)
		}
	}
	// Reusing another user's cursor and supplying a participant_id cannot expose
	// that user's jobs; authorization comes exclusively from the session cookie.
	foreign := historyPage(t, ps, other, next)
	if foreign.Code != http.StatusOK {
		t.Fatalf("foreign cursor request: %d %s", foreign.Code, foreign.Body.String())
	}
	for _, id := range historyIDs(foreign.Body.String()) {
		if slices.Contains(want, id) || id == newer {
			t.Fatalf("foreign history leaked owner job %s", id)
		}
	}
	reset := historyPage(t, ps, owner, "/consumer/jobs?status=completed&workload_type=batch_compute")
	if ids := historyIDs(reset.Body.String()); len(ids) == 0 || ids[0] != newer {
		t.Fatal("first page did not include new matching job")
	}
}

func TestJobHistoryIndividualFiltersAndEmptyState(t *testing.T) {
	db := setupTestDB(t)
	ps := newTestPortalServer(t, db)
	owner := seedParticipant(t, db, "history-filters@test.com", "password")
	stamp := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	completed := seedHistoryJob(t, db, owner, "completed", "batch_compute", stamp)
	failed := seedHistoryJob(t, db, owner, "failed", "ai_inference", stamp.Add(time.Second))
	for _, tc := range []struct {
		query string
		want  []string
	}{
		{"", []string{failed, completed}},
		{"?status=failed", []string{failed}},
		{"?workload_type=batch_compute", []string{completed}},
		{"?status=failed&workload_type=batch_compute", []string{}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			page := historyPage(t, ps, owner, "/consumer/jobs"+tc.query)
			if page.Code != 200 {
				t.Fatalf("status %d: %s", page.Code, page.Body.String())
			}
			if got := historyIDs(page.Body.String()); !slices.Equal(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			if len(tc.want) == 0 && !strings.Contains(page.Body.String(), "No jobs match") {
				t.Fatal("missing filtered empty state")
			}
		})
	}
	emptyOwner := seedParticipant(t, db, "history-empty@test.com", "password")
	empty := historyPage(t, ps, emptyOwner, "/consumer/jobs")
	if empty.Code != 200 || len(historyIDs(empty.Body.String())) != 0 || !strings.Contains(empty.Body.String(), "No jobs") {
		t.Fatalf("bad empty history: %d %s", empty.Code, empty.Body.String())
	}
}

func TestJobHistoryRejectsInvalidFiltersAndCursor(t *testing.T) {
	db := setupTestDB(t)
	ps := newTestPortalServer(t, db)
	owner := seedParticipant(t, db, "history-invalid@test.com", "password")
	for _, query := range []string{"?status=not-a-status", "?workload_type=not-a-workload", "?cursor=garbage", "?cursor=e30", "?status=%27%20OR%201%3D1--"} {
		t.Run(query, func(t *testing.T) {
			w := historyPage(t, ps, owner, "/consumer/jobs"+query)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("invalid query: got %d, body %s", w.Code, w.Body.String())
			}
		})
	}
}
