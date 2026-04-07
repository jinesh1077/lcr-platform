package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/carrier-opt/ingestion/internal/adapters"
	"github.com/carrier-opt/ingestion/internal/config"
	"github.com/carrier-opt/ingestion/internal/db"
	"github.com/carrier-opt/ingestion/internal/kafka"
	"github.com/carrier-opt/ingestion/internal/models"
	"github.com/carrier-opt/ingestion/internal/redisstore"
	"github.com/carrier-opt/ingestion/internal/scheduler"
	"github.com/carrier-opt/ingestion/internal/trie"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
	cfg       config.Config
	store     *db.Store
	redis     *redisstore.Client
	producer  *kafka.Producer
	scheduler *scheduler.Scheduler
	builder   *trie.Builder
}

func NewServer(cfg config.Config, store *db.Store, redis *redisstore.Client, producer *kafka.Producer, sched *scheduler.Scheduler, builder *trie.Builder) *Server {
	return &Server{cfg: cfg, store: store, redis: redis, producer: producer, scheduler: sched, builder: builder}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /ready", s.handleReady)
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("POST /rates/upload", s.auth(s.handleRateUpload))
	mux.HandleFunc("GET /rates/{vendor}", s.handleGetRates)
	mux.HandleFunc("POST /invoices/upload", s.auth(s.handleInvoiceUpload))
	mux.HandleFunc("POST /admin/trie/rebuild", s.auth(s.handleTrieRebuild))
	mux.HandleFunc("DELETE /admin/blocklist/{carrier_id}", s.auth(s.handleClearBlocklist))
	mux.HandleFunc("GET /api/overview", s.handleOverview)
	return mux
}

func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-API-Key")
		if key != s.cfg.APIKey {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if err := s.redis.Ping(ctx); err != nil {
		http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ready"))
}

func (s *Server) handleRateUpload(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vendor := r.URL.Query().Get("vendor")
	if vendor == "" {
		vendor = r.FormValue("vendor")
	}
	if vendor == "" {
		http.Error(w, "vendor required", http.StatusBadRequest)
		return
	}

	effectiveStr := r.URL.Query().Get("effective_at")
	var effectiveAt time.Time
	if effectiveStr != "" {
		t, err := time.Parse(time.RFC3339, effectiveStr)
		if err != nil {
			http.Error(w, "invalid effective_at", http.StatusBadRequest)
			return
		}
		effectiveAt = t
	} else {
		effectiveAt = time.Now().UTC()
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}

	adapterType, err := s.store.GetVendorAdapterType(ctx, vendor)
	if err != nil {
		http.Error(w, "unknown vendor", http.StatusBadRequest)
		return
	}

	adapter := adapters.Get(adapterType)
	sheet, err := adapter.Parse(strings.NewReader(string(body)), vendor, effectiveAt)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	hash := db.HashContent(body)
	sheetID, err := s.store.InsertRateSheet(ctx, vendor, hash, sheet.EffectiveAt, sheet.Rates)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	// If immediately active, publish and rebuild
	if !sheet.EffectiveAt.After(time.Now().UTC()) {
		event := models.RatesActivatedEvent{
			RateSheetID: sheetID,
			EffectiveAt: sheet.EffectiveAt,
			RateCount:   len(sheet.Rates),
		}
		if err := s.scheduler.PublishNow(ctx, event); err != nil {
			slog.Warn("kafka publish failed", "error", err)
		}
		if err := s.builder.Rebuild(ctx); err != nil {
			slog.Warn("trie rebuild failed", "error", err)
		}
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"rate_sheet_id": sheetID,
		"rate_count":    len(sheet.Rates),
		"status":        "accepted",
	})
}

func (s *Server) handleGetRates(w http.ResponseWriter, r *http.Request) {
	vendor := r.PathValue("vendor")
	rates, err := s.store.GetRatesByVendor(r.Context(), vendor)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"vendor": vendor, "rates": rates})
}

func (s *Server) handleInvoiceUpload(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	carrierID := r.URL.Query().Get("carrier_id")
	if carrierID == "" {
		http.Error(w, "carrier_id required", http.StatusBadRequest)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	fileName := r.URL.Query().Get("file_name")
	if fileName == "" {
		fileName = "invoice.csv"
	}
	id, err := s.store.InsertInvoice(ctx, carrierID, fileName, string(body))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"invoice_id": id})
}

func (s *Server) handleTrieRebuild(w http.ResponseWriter, r *http.Request) {
	if err := s.builder.Rebuild(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "rebuilt"})
}

func (s *Server) handleClearBlocklist(w http.ResponseWriter, r *http.Request) {
	carrierID := r.PathValue("carrier_id")
	if err := s.redis.DeleteBlocklist(r.Context(), carrierID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "cleared", "carrier_id": carrierID})
}

func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	active, _ := s.redis.GetActiveBuffer(ctx)
	blocklist, _ := s.redis.ListBlocklist(ctx)
	rateCount, _ := s.store.CountActiveRates(ctx)
	carriers, _ := s.store.ListCarrierIDs(ctx)
	writeJSON(w, http.StatusOK, map[string]any{
		"trie_active_buffer": active,
		"blocklist":          blocklist,
		"active_rates":       rateCount,
		"carriers":           carriers,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
