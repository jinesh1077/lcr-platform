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

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	routingURL := env("ROUTING_URL", "http://localhost:8081/route")
	mockURL := env("MOCK_CARRIER_URL", "http://localhost:8083/simulate")
	kafkaBrokers := []string{env("KAFKA_BROKERS", "localhost:9092")}
	totalCalls, _ := strconv.Atoi(env("SIM_CALLS", "1000"))
	concurrency, _ := strconv.Atoi(env("SIM_CONCURRENCY", "50"))

	numbers := []string{
		"447700900123", "447700900456", "33123456789", "4915123456789",
		"34612345678", "393331234567", "5511987654321", "81312345678",
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

			num := numbers[rand.Intn(len(numbers))]
			route, err := callRoute(routingURL, num)
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
				DialedNumber:     num,
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

func callRoute(url, number string) (*routeResponse, error) {
	body, _ := json.Marshal(routeRequest{DialedNumber: number, DefaultRegion: "GB"})
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
	p, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		panic(fmt.Sprintf("kafka: %v", err))
	}
	return p
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
