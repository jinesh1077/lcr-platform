package redisstore

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/carrier-opt/ingestion/internal/models"
	"github.com/redis/go-redis/v9"
)

const (
	KeyActiveBuffer = "trie:active"
	BufferA         = "A"
	BufferB         = "B"
)

type Client struct {
	rdb *redis.Client
}

func New(addr string) *Client {
	return &Client{rdb: redis.NewClient(&redis.Options{Addr: addr})}
}

func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

func (c *Client) Close() error {
	return c.rdb.Close()
}

func (c *Client) DeleteBlocklist(ctx context.Context, carrierID string) error {
	return c.rdb.Del(ctx, fmt.Sprintf("blocklist:%s", carrierID)).Err()
}

func (c *Client) ListBlocklist(ctx context.Context) ([]string, error) {
	var ids []string
	iter := c.rdb.Scan(ctx, 0, "blocklist:*", 100).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		ids = append(ids, strings.TrimPrefix(key, "blocklist:"))
	}
	return ids, iter.Err()
}

func (c *Client) GetActiveBuffer(ctx context.Context) (string, error) {
	buf, err := c.rdb.Get(ctx, KeyActiveBuffer).Result()
	if err == redis.Nil {
		return BufferA, nil
	}
	return buf, err
}

func (c *Client) GetInactiveBuffer(ctx context.Context) (string, error) {
	active, err := c.GetActiveBuffer(ctx)
	if err != nil {
		return BufferB, err
	}
	if active == BufferA {
		return BufferB, nil
	}
	return BufferA, nil
}

type trieEntry struct {
	Carriers map[string]float64 `json:"carriers"`
}

// BuildTrie writes prefix→carrier rates into the inactive buffer, then swaps.
func (c *Client) BuildTrie(ctx context.Context, rates []models.CarrierRate) error {
	inactive, err := c.GetInactiveBuffer(ctx)
	if err != nil {
		return err
	}

	// Group rates by prefix
	prefixMap := make(map[string]map[string]float64)
	for _, r := range rates {
		if prefixMap[r.Prefix] == nil {
			prefixMap[r.Prefix] = make(map[string]float64)
		}
		// Keep cheapest rate per carrier per prefix
		if existing, ok := prefixMap[r.Prefix][r.CarrierID]; !ok || r.CostPerMin < existing {
			prefixMap[r.Prefix][r.CarrierID] = r.CostPerMin
		}
	}

	// Also build intermediate prefix nodes for LPM
	allPrefixes := make(map[string]map[string]float64)
	for prefix, carriers := range prefixMap {
		allPrefixes[prefix] = carriers
		for i := 1; i < len(prefix); i++ {
			sub := prefix[:i]
			if allPrefixes[sub] == nil {
				allPrefixes[sub] = make(map[string]float64)
			}
		}
	}

	pipe := c.rdb.Pipeline()
	// Clear inactive buffer keys
	pattern := fmt.Sprintf("trie:%s:*", inactive)
	iter := c.rdb.Scan(ctx, 0, pattern, 100).Iterator()
	for iter.Next(ctx) {
		pipe.Del(ctx, iter.Val())
	}

	for prefix, carriers := range allPrefixes {
		entry := trieEntry{Carriers: carriers}
		body, _ := json.Marshal(entry)
		key := fmt.Sprintf("trie:%s:%s", inactive, prefix)
		pipe.Set(ctx, key, body, 0)
	}

	version := fmt.Sprintf("trie:%s:version", inactive)
	pipe.Set(ctx, version, len(allPrefixes), 0)

	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	// Atomic swap
	return c.rdb.Set(ctx, KeyActiveBuffer, inactive, 0).Err()
}

// ForceRebuild triggers rebuild using all active rates from provided list.
func (c *Client) SwapBuffer(ctx context.Context) error {
	inactive, err := c.GetInactiveBuffer(ctx)
	if err != nil {
		return err
	}
	return c.rdb.Set(ctx, KeyActiveBuffer, inactive, 0).Err()
}

func SortPrefixes(prefixes []string) {
	sort.Strings(prefixes)
	for i, j := 0, len(prefixes)-1; i < j; i, j = i+1, j-1 {
		if len(prefixes[i]) > len(prefixes[j]) {
			prefixes[i], prefixes[j] = prefixes[j], prefixes[i]
		}
	}
	sort.Slice(prefixes, func(i, j int) bool {
		return len(prefixes[i]) > len(prefixes[j])
	})
}

func BufferKey(buffer, prefix string) string {
	return fmt.Sprintf("trie:%s:%s", buffer, prefix)
}

func BlocklistKey(carrierID string) string {
	return fmt.Sprintf("blocklist:%s", carrierID)
}

func HealthKey(carrierID string) string {
	return fmt.Sprintf("health:%s", carrierID)
}

func ParseTrieKey(key string) (buffer, prefix string, ok bool) {
	parts := strings.SplitN(key, ":", 3)
	if len(parts) != 3 || parts[0] != "trie" {
		return "", "", false
	}
	return parts[1], parts[2], true
}
