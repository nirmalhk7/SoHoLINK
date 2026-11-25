package protocoladapter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/NTARI-RAND/sohocloud-protocol/fees"
	"github.com/NTARI-RAND/sohocloud-protocol/transport/httpjson"
	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/orchestrator"
)

// A strict reference client must accept the real handler's signed fees and
// reject a second document appended by a faulty intermediary.
func TestReferenceClientFeeFraming(t *testing.T) {
	pub, private := testKeypair(t)
	declaration := fees.FeeDeclaration{CoordinatorID: "soholink", Seq: 7, EffectiveAt: time.Unix(1700000000, 0).UTC()}
	declaration.Sign(private)
	adapter := New(nil, orchestrator.NewNodeRegistry(), stubFees{decl: declaration}, "soholink", nil, nil)
	handler := NewHandler(adapter, nil, true, nil)
	for _, suffix := range []string{" \t\r\n", " {}"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { handler.ServeHTTP(w, r); _, _ = w.Write([]byte(suffix)) }))
		client := &httpjson.Client{BaseURL: server.URL, HTTP: server.Client()}
		got, err := client.Fees(context.Background())
		server.Close()
		if suffix == " {}" {
			if err == nil {
				t.Fatal("reference client accepted a second response value")
			}
			if !reflect.DeepEqual(got, fees.FeeDeclaration{}) {
				t.Fatalf("framing error returned a partial fee declaration: %+v", got)
			}
		} else if err != nil || !got.Verify(pub) || got.Seq != 7 {
			t.Fatalf("fee=%v error=%v", got, err)
		}
	}
}
