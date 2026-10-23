package portal

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/types"
)

const jobHistoryPageSize = 25

type jobHistoryCursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	Workload  string    `json:"workload_type"`
}

type historyFilterOption struct{ Value, Label string }

type jobHistoryData struct {
	Email           string
	IsAuthenticated bool
	Jobs            []SubmittedJobRow
	Status          string
	Workload        string
	Statuses        []historyFilterOption
	Workloads       []historyFilterOption
	NextURL         string
	FirstURL        string
}

func historyURL(status, workload, cursor string) string {
	values := url.Values{}
	if status != "" {
		values.Set("status", status)
	}
	if workload != "" {
		values.Set("workload_type", workload)
	}
	if cursor != "" {
		values.Set("cursor", cursor)
	}
	if len(values) == 0 {
		return "/consumer/jobs"
	}
	return "/consumer/jobs?" + values.Encode()
}

func (ps *PortalServer) handleJobHistory(w http.ResponseWriter, r *http.Request) {
	claims, ok := ClaimsFromContext(r.Context())
	if !ok {
		http.Redirect(w, r, "/login", http.StatusFound)
		return
	}
	status, workload := r.URL.Query().Get("status"), r.URL.Query().Get("workload_type")
	if workload != "" {
		if _, err := types.ParseMarketplaceWorkloadType(workload); err != nil {
			http.Error(w, "invalid workload_type", http.StatusBadRequest)
			return
		}
	}
	var cursor jobHistoryCursor
	rawCursor := r.URL.Query().Get("cursor")
	if rawCursor != "" {
		encoded, err := base64.RawURLEncoding.DecodeString(rawCursor)
		if len(rawCursor) > 1024 || err != nil || json.Unmarshal(encoded, &cursor) != nil || cursor.CreatedAt.IsZero() {
			http.Error(w, "invalid cursor", http.StatusBadRequest)
			return
		}
		id, err := uuid.Parse(cursor.ID)
		if err != nil || cursor.Status != status || cursor.Workload != workload {
			http.Error(w, "cursor does not match filters", http.StatusBadRequest)
			return
		}
		cursor.ID = id.String()
	}
	// PostgreSQL's enum is authoritative, including the extended print lifecycle.
	statusRows, err := ps.db.Pool.Query(r.Context(), `SELECT unnest(enum_range(NULL::job_status))::text`)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	var statuses []string
	for statusRows.Next() {
		var value string
		if err := statusRows.Scan(&value); err != nil {
			statusRows.Close()
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		statuses = append(statuses, value)
	}
	err = statusRows.Err()
	statusRows.Close()
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if status != "" && !slices.Contains(statuses, status) {
		http.Error(w, "invalid status", http.StatusBadRequest)
		return
	}

	// Every page derives ownership from the session, never query parameters or
	// cursor data. The ID breaks timestamp ties; keyset pagination avoids OFFSET
	// shifts when newer jobs are submitted while someone browses older pages.
	query := `SELECT id, status, workload_type, created_at FROM jobs
  WHERE participant_id = $1
    AND ($2 = '' OR status::text = $2)
    AND ($3 = '' OR workload_type::text = $3)`
	args := []any{claims.UserID, status, workload}
	if rawCursor != "" {
		query += ` AND (created_at, id) < ($4::timestamptz, $5::uuid)`
		args = append(args, cursor.CreatedAt, cursor.ID)
	}
	query += fmt.Sprintf(` ORDER BY created_at DESC, id DESC LIMIT %d`, jobHistoryPageSize+1)
	rows, err := ps.db.Pool.Query(r.Context(), query, args...)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	data := jobHistoryData{Email: claims.Email, IsAuthenticated: true, Status: status, Workload: workload}
	for rows.Next() {
		var row SubmittedJobRow
		if err := rows.Scan(&row.JobID, &row.Status, &row.Workload, &row.CreatedAt); err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		data.Jobs = append(data.Jobs, row)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	for _, value := range statuses {
		data.Statuses = append(data.Statuses, historyFilterOption{value, strings.ReplaceAll(value, "_", " ")})
	}
	for _, value := range types.AllMarketplaceWorkloadTypes() {
		data.Workloads = append(data.Workloads, historyFilterOption{string(value), strings.ReplaceAll(string(value), "_", " ")})
	}
	if len(data.Jobs) > jobHistoryPageSize {
		data.Jobs = data.Jobs[:jobHistoryPageSize]
		last := data.Jobs[len(data.Jobs)-1]
		encoded, err := json.Marshal(jobHistoryCursor{CreatedAt: last.CreatedAt, ID: last.JobID, Status: status, Workload: workload})
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		data.NextURL = historyURL(status, workload, base64.RawURLEncoding.EncodeToString(encoded))
	}
	if rawCursor != "" {
		data.FirstURL = historyURL(status, workload, "")
	}
	ps.renderTemplate(w, "consumer_job_history.html", data)
}
