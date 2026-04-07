package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/carrier-opt/ingestion/internal/api"
	"github.com/carrier-opt/ingestion/internal/config"
	"github.com/carrier-opt/ingestion/internal/db"
	"github.com/carrier-opt/ingestion/internal/kafka"
	"github.com/carrier-opt/ingestion/internal/redisstore"
	"github.com/carrier-opt/ingestion/internal/scheduler"
	"github.com/carrier-opt/ingestion/internal/trie"
)

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

	if err := store.RunMigrations(ctx, cfg.MigrationsPath); err != nil {
		slog.Error("migrations failed", "error", err)
		os.Exit(1)
	}

	redisClient := redisstore.New(cfg.RedisAddr)
	if err := redisClient.Ping(ctx); err != nil {
		slog.Warn("redis not ready yet", "error", err)
	}

	if err := kafka.EnsureTopics(cfg.KafkaBrokers); err != nil {
		slog.Warn("kafka topic setup", "error", err)
	}

	producer, err := kafka.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		slog.Error("kafka producer failed", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	sched := scheduler.New(store, producer, 30*time.Second)
	go sched.Run(ctx)

	builder := trie.NewBuilder(store, redisClient)
	srv := api.NewServer(cfg, store, redisClient, producer, sched, builder)

	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      srv.Routes(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	go func() {
		slog.Info("ingestion service starting", "port", cfg.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = server.Shutdown(shutdownCtx)
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
