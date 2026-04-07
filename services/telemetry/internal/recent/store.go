package recent

import (
	"sync"
	"time"

	"github.com/carrier-opt/telemetry/internal/models"
)

const maxCalls = 100

type CallRecord struct {
	CallID          string    `json:"call_id"`
	DialedNumber    string    `json:"dialed_number"`
	CarrierID       string    `json:"carrier_id"`
	Answered        bool      `json:"answered"`
	DurationSec     int       `json:"duration_sec"`
	CostTheoretical float64   `json:"cost_theoretical"`
	DisconnectReason string   `json:"disconnect_reason"`
	Timestamp       time.Time `json:"timestamp"`
	ReceivedAt      time.Time `json:"received_at"`
}

type Summary struct {
	TotalCalls    int     `json:"total_calls"`
	AnsweredCalls int     `json:"answered_calls"`
	FailedCalls   int     `json:"failed_calls"`
	AnswerRate    float64 `json:"answer_rate"`
	TotalCost     float64 `json:"total_cost"`
	LastCallAt    *time.Time `json:"last_call_at,omitempty"`
}

type Store struct {
	mu      sync.RWMutex
	calls   []CallRecord
	total   int
	answered int
	failed  int
	totalCost float64
	lastAt  time.Time
}

func NewStore() *Store {
	return &Store{calls: make([]CallRecord, 0, maxCalls)}
}

func (s *Store) Add(cdr models.CDREvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec := CallRecord{
		CallID:           cdr.CallID,
		DialedNumber:     cdr.DialedNumber,
		CarrierID:        cdr.CarrierID,
		Answered:         cdr.Answered,
		DurationSec:      cdr.DurationSec,
		CostTheoretical:  cdr.CostTheoretical,
		DisconnectReason: cdr.DisconnectReason,
		Timestamp:        cdr.Timestamp,
		ReceivedAt:       time.Now().UTC(),
	}

	s.calls = append([]CallRecord{rec}, s.calls...)
	if len(s.calls) > maxCalls {
		s.calls = s.calls[:maxCalls]
	}

	s.total++
	s.totalCost += cdr.CostTheoretical
	if cdr.Answered {
		s.answered++
	} else {
		s.failed++
	}
	s.lastAt = rec.ReceivedAt
}

func (s *Store) Recent(limit int) []CallRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit <= 0 || limit > len(s.calls) {
		limit = len(s.calls)
	}
	out := make([]CallRecord, limit)
	copy(out, s.calls[:limit])
	return out
}

func (s *Store) Summary() Summary {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sum := Summary{
		TotalCalls:    s.total,
		AnsweredCalls: s.answered,
		FailedCalls:   s.failed,
		TotalCost:     s.totalCost,
	}
	if s.total > 0 {
		sum.AnswerRate = float64(s.answered) / float64(s.total)
	}
	if !s.lastAt.IsZero() {
		t := s.lastAt
		sum.LastCallAt = &t
	}
	return sum
}
