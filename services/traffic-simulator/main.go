package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
)

type routeRequest struct {
	DialedNumber  string `json:"dialedNumber"`
	DefaultRegion string `json:"defaultRegion"`
}

type routeCandidate struct {
	CarrierID    string  `json:"carrierId"`
	CostPerMin   float64 `json:"costPerMin"`
	EffectiveCost float64 `json:"effectiveCost"`
}

type routeResponse struct {
	DialedNumber string           `json:"dialedNumber"`
	Candidates   []routeCandidate `json:"candidates"`
}

type cdrEvent struct {
	CallID          string    `json:"call_id"`
	DialedNumber    string    `json:"dialed_number"`
	CarrierID       string    `json:"carrier_id"`
	DurationSec     int       `json:"duration_sec"`
	Answered        bool      `json:"answered"`
	DisconnectReason string   `json:"disconnect_reason"`
	Timestamp       time.Time `json:"timestamp"`
	CostTheoretical float64   `json:"cost_theoretical"`
}

type trafficDest struct {
	DialedNumber  string  `json:"dialed_number"`
	DefaultRegion string  `json:"default_region"`
	Weight        float64 `json:"weight"`
}

type trafficProfile struct {
	Destinations []trafficDest `json:"destinations"`
}

func loadTrafficProfile(path string) ([]trafficDest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var p trafficProfile
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	if len(p.Destinations) == 0 {
		return nil, fmt.Errorf("empty profile")
	}
	return p.Destinations, nil
}

func pickDestination(dests []trafficDest) trafficDest {
	var total float64
	for _, d := range dests {
		total += d.Weight
	}
	r := rand.Float64() * total
	var acc float64
	for _, d := range dests {
		acc += d.Weight
		if r <= acc {
			return d
		}
	}
	return dests[len(dests)-1]
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	routingURL := env("ROUTING_URL", "http://localhost:8081/route")
	mockURL := env("MOCK_CARRIER_URL", "http://localhost:8083/simulate")
	kafkaBrokers := []string{env("KAFKA_BROKERS", "localhost:9092")}
	totalCalls, _ := strconv.Atoi(env("SIM_CALLS", "1000"))
	concurrency, _ := strconv.Atoi(env("SIM_CONCURRENCY", "50"))
	profilePath := env("TRAFFIC_PROFILE", "/scripts/seed/generated/traffic-profile.json")

	dests, err := loadTrafficProfile(profilePath)
	if err != nil {
		slog.Warn("traffic profile not loaded, using fallback", "error", err, "path", profilePath)
		dests = []trafficDest{
			{DialedNumber: "447700900123", DefaultRegion: "GB", Weight: 1},
			{DialedNumber: "44207123456", DefaultRegion: "GB", Weight: 1},
			{DialedNumber: "33123456789", DefaultRegion: "FR", Weight: 1},
			{DialedNumber: "4915123456789", DefaultRegion: "DE", Weight: 1},
		}
	} else {
		slog.Info("loaded traffic profile", "destinations", len(dests))
	}

	producer := mustProducer(kafkaBrokers)
	defer producer.Close()

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	var success, failed int64
	var mu sync.Mutex

	start := time.Now()
	for i := 0; i < totalCalls; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			dest := pickDestination(dests)
			route, err := callRoute(routingURL, dest.DialedNumber, dest.DefaultRegion)
			if err != nil || len(route.Candidates) == 0 {
				mu.Lock()
				failed++
				mu.Unlock()
				return
			}
			carrier := route.Candidates[0]
			sim := callMock(mockURL)
			cdr := cdrEvent{
				CallID:           uuid.New().String(),
				DialedNumber:     dest.DialedNumber,
				CarrierID:        carrier.CarrierID,
				DurationSec:      sim.Duration,
				Answered:         sim.Answered,
				DisconnectReason: "normal",
				Timestamp:        time.Now().UTC(),
				CostTheoretical:  carrier.EffectiveCost * float64(sim.Duration) / 60.0,
			}
			body, _ := json.Marshal(cdr)
			producer.SendMessage(&sarama.ProducerMessage{
				Topic: "cdr.events",
				Value: sarama.ByteEncoder(body),
			})
			mu.Lock()
			success++
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	slog.Info("simulation complete",
		"total", totalCalls, "success", success, "failed", failed,
		"duration", elapsed.String(),
		"rps", float64(success)/elapsed.Seconds())
}

func callRoute(url, number, region string) (*routeResponse, error) {
	if region == "" {
		region = "GB"
	}
	body, _ := json.Marshal(routeRequest{DialedNumber: number, DefaultRegion: region})
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var route routeResponse
	if err := json.Unmarshal(data, &route); err != nil {
		return nil, err
	}
	return &route, nil
}

type simResult struct {
	Answered bool `json:"answered"`
	Duration int  `json:"duration_sec"`
}

func callMock(url string) simResult {
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		return simResult{Answered: rand.Float64() > 0.1, Duration: 30 + rand.Intn(60)}
	}
	defer resp.Body.Close()
	var r simResult
	json.NewDecoder(resp.Body).Decode(&r)
	return r
}

func mustProducer(brokers []string) sarama.SyncProducer {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	var p sarama.SyncProducer
	var err error
	for attempt := 1; attempt <= 30; attempt++ {
		p, err = sarama.NewSyncProducer(brokers, cfg)
		if err == nil {
			return p
		}
		slog.Warn("kafka producer not ready, retrying", "attempt", attempt, "error", err)
		time.Sleep(2 * time.Second)
	}
	panic(fmt.Sprintf("kafka: %v", err))
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
