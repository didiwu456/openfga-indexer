package admin_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yourorg/openfga-indexer/admin"
)

func TestAdmin_StatusEndpoint(t *testing.T) {
	h := admin.NewHandler(admin.Config{
		ClusterID: "cluster-1",
		Role:      "replica",
	}, nil, nil)

	req := httptest.NewRequest("GET", "/admin/status", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["role"] != "replica" {
		t.Fatalf("role = %v, want replica", resp["role"])
	}
}

func TestAdmin_PromoteEndpoint_RejectsWhenMasterAlive(t *testing.T) {
	h := admin.NewHandler(admin.Config{
		ClusterID: "cluster-1",
		Role:      "replica",
	}, nil, nil)

	req := httptest.NewRequest("POST", "/admin/promote", strings.NewReader(""))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	// Without a live Redis, the handler should return an appropriate error.
	if w.Code == http.StatusOK {
		t.Fatal("promote without Redis should not return 200")
	}
}
