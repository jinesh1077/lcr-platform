package main

import (
	"encoding/json"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
)

type config struct {
	CarrierID   string
	ASR         float64
	ACD         int
	FailureRate float64
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	cfg := config{
		CarrierID:   env("CARRIER_ID", "nexatel"),
		ASR:         parseFloat(env("MOCK_ASR", "0.95")),
		ACD:         parseInt(env("MOCK_ACD", "45")),
		FailureRate: parseFloat(env("MOCK_FAILURE_RATE", "0.05")),
	}

	var calls atomic.Int64
	var answered atomic.Int64

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /config", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(cfg)
	})
	mux.HandleFunc("POST /simulate", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		ok := rand.Float64() < cfg.ASR
		if ok {
			answered.Add(1)
		}
		duration := cfg.ACD
		if rand.Float64() < cfg.FailureRate {
			ok = false
			duration = 0
		}
		json.NewEncoder(w).Encode(map[string]any{
			"carrier_id":   cfg.CarrierID,
			"answered":     ok,
			"duration_sec": duration,
		})
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		c := calls.Load()
		a := answered.Load()
		asr := 0.0
		if c > 0 {
			asr = float64(a) / float64(c)
		}
		json.NewEncoder(w).Encode(map[string]any{"calls": c, "answered": a, "asr": asr})
	})

	slog.Info("mock carrier starting", "carrier", cfg.CarrierID, "port", "8083")
	http.ListenAndServe(":8083", mux)
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func parseFloat(s string) float64 {
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

func parseInt(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}
