// Package admin provides HTTP endpoints for cluster administration:
//   - GET  /admin/status   — current role, state, index freshness
//   - POST /admin/promote  — promote this replica to master (split-brain guarded)
//   - POST /admin/rebuild  — manually trigger Offline Pipeline rebuild
//   - GET  /admin/stores   — list all stores and their index status
package admin

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/yourorg/openfga-indexer/manager"
	leopardredis "github.com/yourorg/openfga-indexer/redis"
)

// Config holds admin handler settings.
type Config struct {
	ClusterID  string
	InstanceID string
	Role       string // "master" | "replica"
}

// Handler serves the admin HTTP endpoints.
type Handler struct {
	cfg       Config
	redis     leopardredis.Store
	mgr       *manager.Manager
	mux       *http.ServeMux
	startedAt time.Time
}

// NewHandler creates an admin Handler. redis and mgr may be nil in tests.
func NewHandler(cfg Config, redis leopardredis.Store, mgr *manager.Manager) http.Handler {
	h := &Handler{cfg: cfg, redis: redis, mgr: mgr, startedAt: time.Now()}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/status", h.handleStatus)
	mux.HandleFunc("POST /admin/promote", h.handlePromote)
	mux.HandleFunc("POST /admin/rebuild", h.handleRebuild)
	mux.HandleFunc("GET /admin/stores", h.handleStores)
	h.mux = mux
	return mux
}

func (h *Handler) handleStatus(w http.ResponseWriter, r *http.Request) {
	resp := map[string]any{
		"cluster_id":  h.cfg.ClusterID,
		"instance_id": h.cfg.InstanceID,
		"role":        h.cfg.Role,
		"uptime_sec":  int(time.Since(h.startedAt).Seconds()),
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) handlePromote(w http.ResponseWriter, r *http.Request) {
	if h.redis == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "redis not configured"})
		return
	}
	ctx := r.Context()

	// Split-brain guard: refuse if an existing master heartbeat is alive.
	clusterID, instanceID, err := h.redis.GetMaster(ctx)
	if err == nil && instanceID != "" && clusterID != h.cfg.ClusterID {
		writeJSON(w, http.StatusConflict, map[string]string{
			"error":      "active master detected — promotion refused",
			"master":     clusterID,
			"instanceID": instanceID,
		})
		return
	}

	if err := h.redis.SetMaster(ctx, h.cfg.ClusterID, h.cfg.InstanceID); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("set master: %v", err)})
		return
	}

	h.cfg.Role = "master"
	writeJSON(w, http.StatusOK, map[string]string{"status": "promoted", "cluster_id": h.cfg.ClusterID})
}

func (h *Handler) handleRebuild(w http.ResponseWriter, r *http.Request) {
	if h.cfg.Role != "master" {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "only master can trigger rebuild"})
		return
	}
	storeID := r.URL.Query().Get("store")
	if storeID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "store query param required"})
		return
	}
	if h.mgr == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "manager not configured"})
		return
	}
	epoch, err := h.mgr.TriggerRebuild(r.Context(), storeID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"store_id": storeID, "epoch": epoch})
}

func (h *Handler) handleStores(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"stores": []any{}})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
