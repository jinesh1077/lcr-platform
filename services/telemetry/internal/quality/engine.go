package quality

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/carrier-opt/telemetry/internal/models"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

var (
	asrGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "carrier_asr",
		Help: "Answer Seizure Ratio per carrier",
	}, []string{"carrier_id"})
	blocklistCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "blocklist_count",
		Help: "Number of blocklisted carriers",
	})
)

func init() {
	prometheus.MustRegister(asrGauge, blocklistCount)
}

type Window struct {
	Attempts  int
	Answered  int
	Duration  int
	StartedAt time.Time
}

type Engine struct {
	rdb           *redis.Client
	windows       map[string]*Window
	mu            sync.Mutex
	asrThreshold  float64
	blocklistTTL  time.Duration
	windowSize    time.Duration
	blocklistKeys map[string]struct{}
}

func New(rdb *redis.Client, asrThreshold float64, blocklistTTLSec int) *Engine {
	return &Engine{
		rdb:           rdb,
		windows:       make(map[string]*Window),
		asrThreshold:  asrThreshold,
		blocklistTTL:  time.Duration(blocklistTTLSec) * time.Second,
		windowSize:    5 * time.Minute,
		blocklistKeys: make(map[string]struct{}),
	}
}

func (e *Engine) Process(ctx context.Context, cdr models.CDREvent) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	w, ok := e.windows[cdr.CarrierID]
	if !ok || time.Since(w.StartedAt) >= e.windowSize {
		w = &Window{StartedAt: time.Now()}
		e.windows[cdr.CarrierID] = w
	}
	w.Attempts++
	if cdr.Answered {
		w.Answered++
		w.Duration += cdr.DurationSec
	}

	if time.Since(w.StartedAt) >= e.windowSize {
		return e.evaluate(ctx, cdr.CarrierID, w)
	}
	return nil
}

func (e *Engine) evaluate(ctx context.Context, carrierID string, w *Window) error {
	if w.Attempts == 0 {
		return nil
	}
	asr := float64(w.Answered) / float64(w.Attempts)
	asrGauge.WithLabelValues(carrierID).Set(asr)

	penalty := 0.0
	if asr < 1.0 {
		penalty = 1.0 - asr
	}
	if err := e.rdb.Set(ctx, fmt.Sprintf("health:%s", carrierID), fmt.Sprintf("%.4f", penalty), 0).Err(); err != nil {
		return err
	}

	if asr < e.asrThreshold {
		key := fmt.Sprintf("blocklist:%s", carrierID)
		if err := e.rdb.Set(ctx, key, "1", e.blocklistTTL).Err(); err != nil {
			return err
		}
		e.blocklistKeys[key] = struct{}{}
		blocklistCount.Set(float64(len(e.blocklistKeys)))
		slog.Warn("circuit breaker tripped", "carrier", carrierID, "asr", asr)
	}

	delete(e.windows, carrierID)
	return nil
}

func ParseThreshold(s string) float64 {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0.40
	}
	return v
}

type CarrierSnapshot struct {
	Attempts int
	Answered int
	Duration int
}

func (e *Engine) Threshold() float64 {
	return e.asrThreshold
}

func (e *Engine) Snapshot() map[string]CarrierSnapshot {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]CarrierSnapshot)
	for id, w := range e.windows {
		out[id] = CarrierSnapshot{
			Attempts: w.Attempts,
			Answered: w.Answered,
			Duration: w.Duration,
		}
	}
	return out
}
