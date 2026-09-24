package registry

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
)

// newTestServer spins up the registry HTTP handler over the in-memory store using
// httptest — no real network, no ports, instant.
func newTestServer() *httptest.Server {
	svc := NewService(NewMemoryStore(), nil)
	return httptest.NewServer(NewHandler(svc))
}

func TestHTTPCreateAndGet(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	f := sampleFeature("api_feature")
	body, _ := json.Marshal(f)

	// Create
	resp, err := http.Post(srv.URL+"/features", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST status = %d, want 201", resp.StatusCode)
	}
	resp.Body.Close()

	// Get
	resp, err = http.Get(srv.URL + "/features/api_feature")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", resp.StatusCode)
	}
	var got domain.Feature
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Name != "api_feature" {
		t.Fatalf("got name %q", got.Name)
	}
}

func TestHTTPCreateInvalidReturns400(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	bad := sampleFeature("x")
	bad.Owner = "" // invalid
	body, _ := json.Marshal(bad)
	resp, err := http.Post(srv.URL+"/features", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHTTPDuplicateReturns409(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	body, _ := json.Marshal(sampleFeature("dupe"))
	r1, err := http.Post(srv.URL+"/features", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("first POST: %v", err)
	}
	r1.Body.Close()
	r2, err := http.Post(srv.URL+"/features", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("second POST: %v", err)
	}
	defer r2.Body.Close()
	if r2.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409", r2.StatusCode)
	}
}

func TestHTTPGetMissingReturns404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/features/missing")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}
