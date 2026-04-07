package models

import "time"

type CDREvent struct {
	CallID           string    `json:"call_id"`
	DialedNumber     string    `json:"dialed_number"`
	CarrierID        string    `json:"carrier_id"`
	DurationSec      int       `json:"duration_sec"`
	Answered         bool      `json:"answered"`
	DisconnectReason string    `json:"disconnect_reason"`
	Timestamp        time.Time `json:"timestamp"`
	CostTheoretical  float64   `json:"cost_theoretical"`
}
