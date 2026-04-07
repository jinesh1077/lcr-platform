package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/carrier-opt/ingestion/internal/db"
	"github.com/carrier-opt/ingestion/internal/kafka"
	"github.com/carrier-opt/ingestion/internal/models"
)

type Scheduler struct {
	store    *db.Store
	producer *kafka.Producer
	interval time.Duration
}

func New(store *db.Store, producer *kafka.Producer, interval time.Duration) *Scheduler {
	return &Scheduler{store: store, producer: producer, interval: interval}
}

func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.processPending(ctx); err != nil {
				slog.Error("scheduler error", "error", err)
			}
		}
	}
}

func (s *Scheduler) processPending(ctx context.Context) error {
	events, err := s.store.PendingActivations(ctx)
	if err != nil {
		return err
	}
	for _, e := range events {
		if err := s.store.ActivateRateSheet(ctx, e.RateSheetID); err != nil {
			slog.Error("activation failed", "sheet", e.RateSheetID, "error", err)
			continue
		}
		if err := s.producer.PublishRatesActivated(ctx, e); err != nil {
			slog.Error("kafka publish failed", "error", err)
			continue
		}
		slog.Info("rate sheet activated", "sheet", e.RateSheetID, "rates", e.RateCount)
	}
	return nil
}

func (s *Scheduler) PublishNow(ctx context.Context, event models.RatesActivatedEvent) error {
	return s.producer.PublishRatesActivated(ctx, event)
}
