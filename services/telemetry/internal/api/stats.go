package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/carrier-opt/telemetry/internal/quality"
	"github.com/carrier-opt/telemetry/internal/recent"
	"github.com/redis/go-redis/v9"
)

type StatsHandler struct {
	engine *quality.Engine
	rdb    *redis.Client
	recent *recent.Store
}

func NewStatsHandler(engine *quality.Engine, rdb *redis.Client, recentStore *recent.Store) *StatsHandler {
	return &StatsHandler{engine: engine, rdb: rdb, recent: recentStore}
}

func (h *StatsHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/stats", h.handleStats)
	mux.HandleFunc("GET /api/activity", h.handleActivity)
}

func (h *StatsHandler) handleStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	carriers := h.engine.Snapshot()
	blocklist, _ := h.scanBlocklist(ctx)
	health, _ := h.scanHealth(ctx)

	type carrierView struct {
		CarrierID    string  `json:"carrier_id"`
		ASR          float64 `json:"asr"`
		Attempts     int     `json:"attempts"`
		Answered     int     `json:"answered"`
		AvgDuration  float64 `json:"avg_duration_sec"`
		HealthPenalty float64 `json:"health_penalty"`
		Blocklisted  bool    `json:"blocklisted"`
	}

	ids := map[string]struct{}{}
	for id := range carriers {
		ids[id] = struct{}{}
	}
	for id := range blocklist {
		ids[id] = struct{}{}
	}
	for id := range health {
		ids[id] = struct{}{}
	}

	var out []carrierView
	for id := range ids {
		c := carriers[id]
		asr := 0.0
		avgDur := 0.0
		if c.Attempts > 0 {
			asr = float64(c.Answered) / float64(c.Attempts)
			if c.Answered > 0 {
				avgDur = float64(c.Duration) / float64(c.Answered)
			}
		}
		out = append(out, carrierView{
			CarrierID:     id,
			ASR:           asr,
			Attempts:      c.Attempts,
			Answered:      c.Answered,
			AvgDuration:   avgDur,
			HealthPenalty: health[id],
			Blocklisted:   blocklist[id],
		})
	}

	writeJSON(w, map[string]any{
		"carriers":         out,
		"blocklist_count":  len(blocklist),
		"asr_threshold":    h.engine.Threshold(),
	})
}

func (h *StatsHandler) handleActivity(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{
		"summary":      h.recent.Summary(),
		"recent_calls": h.recent.Recent(25),
	})
}

func (h *StatsHandler) scanBlocklist(ctx context.Context) (map[string]bool, error) {
	result := make(map[string]bool)
	iter := h.rdb.Scan(ctx, 0, "blocklist:*", 100).Iterator()
	for iter.Next(ctx) {
		id := strings.TrimPrefix(iter.Val(), "blocklist:")
		result[id] = true
	}
	return result, iter.Err()
}

func (h *StatsHandler) scanHealth(ctx context.Context) (map[string]float64, error) {
	result := make(map[string]float64)
	iter := h.rdb.Scan(ctx, 0, "health:*", 100).Iterator()
	for iter.Next(ctx) {
		id := strings.TrimPrefix(iter.Val(), "health:")
		val, err := h.rdb.Get(ctx, iter.Val()).Float64()
		if err == nil {
			result[id] = val
		}
	}
	return result, iter.Err()
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(v)
}
