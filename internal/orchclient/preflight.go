package orchclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/orchestrator"
)

// PreflightJob checks the orchestrator's live registry over its private API.
// The client's existing timeout bounds the read-only request.
func (c *Client) PreflightJob(req orchestrator.SubmitJobRequest) (orchestrator.JobPreflight, error) {
	var result orchestrator.JobPreflight
	body, err := json.Marshal(req)
	if err != nil {
		return result, fmt.Errorf("orchclient: marshal preflight: %w", err)
	}
	resp, err := c.httpClient.Post(c.baseURL+"/internal/jobs/preflight", "application/json", bytes.NewReader(body))
	if err != nil {
		return result, fmt.Errorf("orchclient: preflight: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusBadRequest {
		return result, orchestrator.ErrInvalidPreflight
	}
	if resp.StatusCode != http.StatusOK {
		return result, fmt.Errorf("orchclient: preflight unavailable: status %d", resp.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&result); err != nil {
		return result, fmt.Errorf("orchclient: decode preflight: %w", err)
	}
	return result, nil
}
