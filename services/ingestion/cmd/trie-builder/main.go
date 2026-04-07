package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/carrier-opt/ingestion/internal/config"
	"github.com/carrier-opt/ingestion/internal/db"
	"github.com/carrier-opt/ingestion/internal/kafka"
	"github.com/carrier-opt/ingestion/internal/models"
	"github.com/carrier-opt/ingestion/internal/redisstore"
	"github.com/carrier-opt/ingestion/internal/trie"
)

type handler struct {
	builder *trie.Builder
}

func (h *handler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *handler) Cleanup(_ sarama.ConsumerGroupSession) error  { return nil }

func (h *handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var event models.RatesActivatedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			slog.Error("invalid event", "error", err)
			session.MarkMessage(msg, "")
			continue
		}
		if err := h.builder.Rebuild(session.Context()); err != nil {
			slog.Error("trie rebuild failed", "error", err)
		} else {
			slog.Info("trie rebuilt from event", "sheet", event.RateSheetID)
		}
		session.MarkMessage(msg, "")
	}
	return nil
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	cfg := config.Load()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	store, err := connectWithRetry(ctx, cfg.PostgresDSN, 60*time.Second)
	if err != nil {
		slog.Error("postgres connect failed", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	redisClient := redisstore.New(cfg.RedisAddr)
	builder := trie.NewBuilder(store, redisClient)

	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, "trie-builder")
	if err != nil {
		slog.Error("kafka consumer failed", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	h := &handler{builder: builder}
	for {
		if err := consumer.Consume(ctx, []string{kafka.TopicRatesActivated}, h); err != nil {
			slog.Error("consume error", "error", err)
		}
		if ctx.Err() != nil {
			return
		}
	}
}

func connectWithRetry(ctx context.Context, dsn string, timeout time.Duration) (*db.Store, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		store, err := db.Connect(ctx, dsn)
		if err == nil {
			return store, nil
		}
		lastErr = err
		slog.Warn("postgres not ready, retrying", "error", err)
		time.Sleep(2 * time.Second)
	}
	return nil, lastErr
}
