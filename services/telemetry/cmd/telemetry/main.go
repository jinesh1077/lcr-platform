package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/carrier-opt/telemetry/internal/api"
	"github.com/carrier-opt/telemetry/internal/clickhouse"
	"github.com/carrier-opt/telemetry/internal/models"
	"github.com/carrier-opt/telemetry/internal/quality"
	"github.com/carrier-opt/telemetry/internal/recent"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

const (
	topicCDR    = "cdr.events"
	topicDLQ    = "cdr.events.dlq"
	groupQuality = "telemetry-quality"
	groupLedger  = "telemetry-ledger"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	redisAddr := env("REDIS_ADDR", "localhost:6379")
	kafkaBrokers := []string{env("KAFKA_BROKERS", "localhost:9092")}
	chAddr := env("CLICKHOUSE_ADDR", "localhost:9000")
	chDB := env("CLICKHOUSE_DB", "carrier_opt")
	chPass := env("CLICKHOUSE_PASSWORD", "")
	asrThreshold := quality.ParseThreshold(env("ASR_THRESHOLD", "0.40"))
	blocklistTTL, _ := strconv.Atoi(env("CIRCUIT_BREAKER_TTL_SEC", "300"))

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	chWriter, err := clickhouse.Connect(chAddr, chDB, chPass)
	if err != nil {
		slog.Warn("clickhouse not ready", "error", err)
	}

	qEngine := quality.New(rdb, asrThreshold, blocklistTTL)
	callLog := recent.NewStore()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Write([]byte("ok"))
	})
	api.NewStatsHandler(qEngine, rdb, callLog).Register(mux)

	go func() {
		slog.Info("metrics server", "port", "8082")
		http.ListenAndServe(":8082", mux)
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		runConsumer(ctx, kafkaBrokers, groupQuality, func(msg *sarama.ConsumerMessage) error {
			var cdr models.CDREvent
			if err := json.Unmarshal(msg.Value, &cdr); err != nil {
				publishDLQ(kafkaBrokers, msg.Value)
				return nil
			}
			callLog.Add(cdr)
			return qEngine.Process(ctx, cdr)
		})
	}()

	go func() {
		defer wg.Done()
		if chWriter == nil {
			slog.Warn("clickhouse writer disabled")
			return
		}
		batch := make([]models.CDREvent, 0, 100)
		var mu sync.Mutex
		flush := func() {
			mu.Lock()
			if len(batch) == 0 {
				mu.Unlock()
				return
			}
			toFlush := batch
			batch = make([]models.CDREvent, 0, 100)
			mu.Unlock()
			if err := chWriter.InsertBatch(ctx, toFlush); err != nil {
				slog.Error("clickhouse insert failed", "error", err)
			}
		}

		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					flush()
				}
			}
		}()

		runConsumer(ctx, kafkaBrokers, groupLedger, func(msg *sarama.ConsumerMessage) error {
			var cdr models.CDREvent
			if err := json.Unmarshal(msg.Value, &cdr); err != nil {
				publishDLQ(kafkaBrokers, msg.Value)
				return nil
			}
			mu.Lock()
			batch = append(batch, cdr)
			shouldFlush := len(batch) >= 100
			mu.Unlock()
			if shouldFlush {
				flush()
			}
			return nil
		})
	}()

	wg.Wait()
}

func runConsumer(ctx context.Context, brokers []string, group string, handler func(*sarama.ConsumerMessage) error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	var consumer sarama.ConsumerGroup
	var err error
	for {
		consumer, err = sarama.NewConsumerGroup(brokers, group, cfg)
		if err == nil {
			break
		}
		slog.Warn("kafka consumer not ready, retrying", "group", group, "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
		}
	}
	defer consumer.Close()

	h := &consumerHandler{handler: handler}
	for {
		if err := consumer.Consume(ctx, []string{topicCDR}, h); err != nil {
			slog.Error("consume error", "group", group, "error", err)
		}
		if ctx.Err() != nil {
			return
		}
	}
}

type consumerHandler struct {
	handler func(*sarama.ConsumerMessage) error
}

func (h *consumerHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if err := h.handler(msg); err != nil {
			slog.Error("handler error", "error", err)
		}
		session.MarkMessage(msg, "")
	}
	return nil
}

func publishDLQ(brokers []string, payload []byte) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	p, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return
	}
	defer p.Close()
	p.SendMessage(&sarama.ProducerMessage{Topic: topicDLQ, Value: sarama.ByteEncoder(payload)})
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
