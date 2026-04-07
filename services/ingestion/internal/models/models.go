package models

import (
	"time"

	"github.com/google/uuid"
)

type RateRow struct {
	Prefix     string  `json:"prefix"`
	CarrierID  string  `json:"carrier_id"`
	CostPerMin float64 `json:"cost_per_min"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
}

type NormalizedRateSheet struct {
	Vendor      string    `json:"vendor"`
	EffectiveAt time.Time `json:"effective_at"`
	Rates       []RateRow `json:"rates"`
}

type RateSheetRecord struct {
	ID          uuid.UUID
	VendorID    string
	SheetHash   string
	EffectiveAt time.Time
	Status      string
}

type RatesActivatedEvent struct {
	RateSheetID uuid.UUID `json:"rate_sheet_id"`
	EffectiveAt time.Time `json:"effective_at"`
	RateCount   int       `json:"rate_count"`
}

type CarrierRate struct {
	Prefix     string
	CarrierID  string
	CostPerMin float64
}
