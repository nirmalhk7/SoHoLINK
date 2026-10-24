package protocoladapter

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"

	protoidentity "github.com/NTARI-RAND/sohocloud-protocol/identity"

	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/identity"
)

// maxBindBody caps how much request body the binding middleware buffers.
// Protocol messages are small; anything larger is rejected outright.
const maxBindBody = 1 << 20 // 1 MiB

// bindNodeID enforces the SPEC §2 SPIFFE↔NodeID binding at the HTTP layer,
// in front of the reference httpjson handler (which is deliberately not an
// authenticator). For the four signed POSTs it buffers the body, decodes the
// NodeID field — protocol structs carry no json tags, so Go field names are
// the wire form, matching the reference client — checks the binding, and
// restores the body for the inner handler. For GET /v0/jobs it binds the
// node_id query parameter. 401: no identity in context; 403: identity does
// not bind to the named node. The adapter re-checks the same binding per the
// Coordinator docstring — defense in depth.
func bindNodeID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && nodePostPath(r.URL.Path):
			nodeID, ok := readPostNode(w, r)
			if !ok || !authorize(w, r, nodeID) {
				return
			}
			next.ServeHTTP(w, r)

		case r.Method == http.MethodGet && r.URL.Path == "/v0/jobs":
			nodeID := r.URL.Query().Get("node_id")
			if nodeID == "" {
				http.Error(w, "missing node_id", http.StatusBadRequest)
				return
			}
			if !authorize(w, r, nodeID) {
				return
			}
			next.ServeHTTP(w, r)

		default:
			// Unknown /v0 paths and wrong methods fall through to the
			// reference handler's own 404/405 handling.
			next.ServeHTTP(w, r)
		}
	})
}

func nodePostPath(path string) bool {
	switch path {
	case "/v0/listing", "/v0/heartbeat", "/v0/decline", "/v0/report":
		return true
	default:
		return false
	}
}

func readPostNode(w http.ResponseWriter, r *http.Request) (string, bool) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBindBody+1))
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return "", false
	}
	if len(body) > maxBindBody {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return "", false
	}
	var probe struct{ NodeID string }
	if err := json.Unmarshal(body, &probe); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return "", false
	}
	if probe.NodeID == "" {
		http.Error(w, "missing NodeID", http.StatusBadRequest)
		return "", false
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	return probe.NodeID, true
}

// authorize writes 401/403 and returns false when the caller's SPIFFE
// identity does not bind to nodeID; returns true when it does.
func authorize(w http.ResponseWriter, r *http.Request, nodeID string) bool {
	spiffeID, ok := identity.SPIFFEIDFromContext(r.Context())
	if !ok {
		http.Error(w, "no SPIFFE identity in context", http.StatusUnauthorized)
		return false
	}
	if !protoidentity.BindsTo(spiffeID.Path(), protoidentity.NodeID(nodeID)) {
		http.Error(w, "SPIFFE identity does not match node", http.StatusForbidden)
		return false
	}
	return true
}
