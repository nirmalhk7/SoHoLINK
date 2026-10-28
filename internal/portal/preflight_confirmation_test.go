package portal

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"golang.org/x/net/html"
)

// Confirmation must post the checked snapshot, never the editable check form.
func TestJobCheckConfirmationFreezesCheckedValues(t *testing.T) {
	ps, _ := checkPortal(t)
	w := checkRequest(t, ps, "POST", "/consumer/job/check", jobForm(), true)
	checked := checkedConfirmationValues(t, w.Body.String())
	if checked.Encode() != jobForm().Encode() {
		t.Fatalf("checked values changed: %v", checked)
	}
	stub := &stubOrchestrator{}
	ps.orch = stub
	w = checkRequest(t, ps, "POST", "/consumer/job/check/submit", checked, true)
	if w.Code != http.StatusSeeOther || stub.lastReq.StorageGB != 20 || stub.lastReq.ContainerImage != checkImage || !stub.lastReq.GPURequired {
		t.Fatalf("checked snapshot not submitted: %d %+v", w.Code, stub.lastReq)
	}
}

func htmlAttribute(n *html.Node, key string) string {
	for _, a := range n.Attr {
		if a.Key == key {
			return a.Val
		}
	}
	return ""
}

func checkedConfirmationValues(t *testing.T, body string) url.Values {
	t.Helper()
	document, err := html.Parse(strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var confirmation *html.Node
	walkJobCheckHTML(document, func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "form" && htmlAttribute(n, "action") == "/consumer/job/check/submit" {
			confirmation = n
		}
	})
	if confirmation == nil {
		t.Fatal("no separate confirmation form for checked values")
	}
	checked := url.Values{}
	walkJobCheckHTML(confirmation, func(n *html.Node) {
		if n.Type != html.ElementNode {
			return
		}
		switch n.Data {
		case "input":
			if htmlAttribute(n, "type") != "hidden" {
				t.Error("confirmation contains editable requirements")
			}
			checked.Add(htmlAttribute(n, "name"), htmlAttribute(n, "value"))
		case "select", "textarea":
			t.Error("confirmation contains editable requirements")
		}
	})
	return checked
}

func walkJobCheckHTML(n *html.Node, visit func(*html.Node)) {
	visit(n)
	for child := n.FirstChild; child != nil; child = child.NextSibling {
		walkJobCheckHTML(child, visit)
	}
}
