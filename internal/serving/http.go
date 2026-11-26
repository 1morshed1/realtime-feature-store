package serving

import (
	"encoding/json"
	"net/http"

	"github.com/1morshed1/realtime-feature-store/internal/metrics"
)

// Handler is the HTTP/JSON serving API.
//
//	POST /getFeatures   body: GetRequest  -> GetResponse
//	GET  /healthz
//	GET  /metrics       Prometheus scrape endpoint
//
// DESIGN NOTE — why POST for a read?
// ------------------------------------
// A feature fetch is logically a read, but its inputs (a list of entity ids + a list of
// feature names) don't fit cleanly or safely in a URL — they can be large and are
// structured. Using POST with a JSON body is the pragmatic, widely-used choice for
// "batch lookup" style read APIs. (gRPC, the natural fit for this high-traffic path, is
// discussed in ADR-0004 and docs/learning/06-serving-and-metrics.md.)
type Handler struct {
	svc *Service
	mux *http.ServeMux
}

// NewHandler wires the serving routes. m is used to expose /metrics; pass the SAME Metrics
// instance the Service was built with so the exported numbers reflect real traffic.
func NewHandler(svc *Service, m *metrics.Metrics) *Handler {
	h := &Handler{svc: svc, mux: http.NewServeMux()}
	h.mux.HandleFunc("POST /getFeatures", h.getFeatures)
	h.mux.HandleFunc("GET /healthz", h.health)
	if m != nil {
		h.mux.Handle("GET /metrics", m.Handler())
	}
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.mux.ServeHTTP(w, r) }

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) getFeatures(w http.ResponseWriter, r *http.Request) {
	var req GetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if req.Entity == "" || len(req.EntityIDs) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "entity and entity_ids are required"})
		return
	}
	resp, err := h.svc.GetFeatures(r.Context(), req)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
