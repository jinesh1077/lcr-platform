package trie

import (
	"context"
	"log/slog"

	"github.com/carrier-opt/ingestion/internal/db"
	"github.com/carrier-opt/ingestion/internal/redisstore"
)

type Builder struct {
	store *db.Store
	redis *redisstore.Client
}

func NewBuilder(store *db.Store, redis *redisstore.Client) *Builder {
	return &Builder{store: store, redis: redis}
}

func (b *Builder) Rebuild(ctx context.Context) error {
	rates, err := b.store.GetAllActiveRates(ctx)
	if err != nil {
		return err
	}
	if err := b.redis.BuildTrie(ctx, rates); err != nil {
		return err
	}
	slog.Info("trie rebuilt", "rate_count", len(rates))
	return nil
}
