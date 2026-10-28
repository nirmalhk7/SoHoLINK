package orchclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/orchestrator"
)

// PreflightJob checks the orchestrator's live registry over its private API.
// The client's existing timeout bounds the read-only request.
func (c *Client) PreflightJob(ctx context.Context, req orchestrator.SubmitJobRequest) (orchestrator.JobPreflight, error) {
	var result orchestrator.JobPreflight
	resp, err := c.preflightRequest(ctx, req)
	if err != nil {
		return result, err
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

func (c *Client) preflightRequest(ctx context.Context, req orchestrator.SubmitJobRequest) (*http.Response, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("orchclient: marshal preflight: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/internal/jobs/preflight", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("orchclient: build preflight: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	return c.httpClient.Do(httpReq)
}
