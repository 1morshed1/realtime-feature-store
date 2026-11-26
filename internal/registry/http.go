package registry

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/1morshed1/realtime-feature-store/internal/domain"
)

// Handler is the HTTP/JSON API for the registry. We expose a small REST surface:
//
//	POST   /features         register a new feature (body: Feature JSON)
//	GET    /features         list all features
//	GET    /features/{name}  fetch one feature
//	PUT    /features/{name}  update a feature
//	DELETE /features/{name}  delete a feature
//
// DESIGN NOTE — why REST/JSON here but gRPC for serving?
// --------------------------------------------------------
// The registry is a low-traffic control-plane API used by humans and CI (registering
// features, browsing the catalog). REST/JSON is the right ergonomic fit — curl-able,
// no codegen. The *serving* path is a high-traffic data-plane API with a tight latency
// budget, which is where a binary protocol like gRPC earns its keep. Matching the
// protocol to the traffic profile is a deliberate design choice, not an accident.
type Handler struct {
	svc *Service
	mux *http.ServeMux
}

// NewHandler wires the routes and returns an http.Handler.
func NewHandler(svc *Service) *Handler {
	h := &Handler{svc: svc, mux: http.NewServeMux()}
	// Go 1.22+ ServeMux supports method+path patterns and path wildcards, so we get a
	// tidy router with zero third-party dependencies.
	h.mux.HandleFunc("POST /features", h.create)
	h.mux.HandleFunc("GET /features", h.list)
	h.mux.HandleFunc("GET /features/{name}", h.get)
	h.mux.HandleFunc("PUT /features/{name}", h.update)
	h.mux.HandleFunc("DELETE /features/{name}", h.delete)
	h.mux.HandleFunc("GET /healthz", h.health)
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.mux.ServeHTTP(w, r) }

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	var f domain.Feature
	if err := json.NewDecoder(r.Body).Decode(&f); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	created, err := h.svc.Register(r.Context(), f)
	if err != nil {
		// Map domain errors to HTTP status codes. Validation and duplicates are client
		// errors (4xx); anything else is a server error (5xx).
		switch {
		case errors.Is(err, ErrAlreadyExists):
			writeError(w, http.StatusConflict, err.Error())
		case isValidationError(err):
			writeError(w, http.StatusBadRequest, err.Error())
		default:
			writeError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	features, err := h.svc.List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, features)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	f, err := h.svc.Get(r.Context(), name)
	if err != nil {
		writeNotFoundOr500(w, err)
		return
	}
	writeJSON(w, http.StatusOK, f)
}

func (h *Handler) update(w http.ResponseWriter, r *http.Request) {
	var f domain.Feature
	if err := json.NewDecoder(r.Body).Decode(&f); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	// The name in the URL is authoritative; ignore any name in the body to avoid
	// confusing "rename via PUT" semantics.
	f.Name = r.PathValue("name")
	updated, err := h.svc.Update(r.Context(), f)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		if isValidationError(err) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

func (h *Handler) delete(w http.ResponseWriter, r *http.Request) {
	if err := h.svc.Delete(r.Context(), r.PathValue("name")); err != nil {
		writeNotFoundOr500(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- response helpers ----------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeNotFoundOr500(w http.ResponseWriter, err error) {
	if errors.Is(err, ErrNotFound) {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeError(w, http.StatusInternalServerError, err.Error())
}

// isValidationError reports whether an error came from Feature.Validate. Validation
// errors are plain fmt.Errorf values (no sentinel), so we detect them by prefix. A
// larger system would use a typed ValidationError; this keeps the example readable.
func isValidationError(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "feature")
}
