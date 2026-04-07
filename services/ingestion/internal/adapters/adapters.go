package adapters

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/carrier-opt/ingestion/internal/e164"
	"github.com/carrier-opt/ingestion/internal/models"
)

type Adapter interface {
	Parse(r io.Reader, vendor string, effectiveAt time.Time) (*models.NormalizedRateSheet, error)
}

func Get(adapterType string) Adapter {
	switch adapterType {
	case "vendor_a":
		return &VendorA{}
	case "vendor_b":
		return &VendorB{}
	default:
		return &DefaultAdapter{}
	}
}

type DefaultAdapter struct{}

func (a *DefaultAdapter) Parse(r io.Reader, vendor string, effectiveAt time.Time) (*models.NormalizedRateSheet, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	trimmed := strings.TrimSpace(string(data))
	if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
		var sheet models.NormalizedRateSheet
		if err := json.Unmarshal(data, &sheet); err != nil {
			return nil, err
		}
		sheet.Vendor = vendor
		if sheet.EffectiveAt.IsZero() {
			sheet.EffectiveAt = effectiveAt
		}
		return normalizeSheet(&sheet)
	}
	return parseDefaultCSV(strings.NewReader(string(data)), vendor, effectiveAt)
}

func parseDefaultCSV(r io.Reader, vendor string, effectiveAt time.Time) (*models.NormalizedRateSheet, error) {
	reader := csv.NewReader(r)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	sheet := &models.NormalizedRateSheet{Vendor: vendor, EffectiveAt: effectiveAt}
	for i, row := range records {
		if i == 0 && strings.EqualFold(row[0], "prefix") {
			continue
		}
		if len(row) < 3 {
			continue
		}
		cost, err := strconv.ParseFloat(strings.TrimSpace(row[2]), 64)
		if err != nil {
			return nil, fmt.Errorf("row %d: invalid cost", i+1)
		}
		sheet.Rates = append(sheet.Rates, models.RateRow{
			Prefix:     strings.TrimSpace(row[0]),
			CarrierID:  strings.TrimSpace(row[1]),
			CostPerMin: cost,
		})
	}
	return normalizeSheet(sheet)
}

type VendorA struct{}

// Vendor A CSV: Destination,Carrier,Rate,Effective
func (a *VendorA) Parse(r io.Reader, vendor string, effectiveAt time.Time) (*models.NormalizedRateSheet, error) {
	reader := csv.NewReader(r)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	sheet := &models.NormalizedRateSheet{Vendor: vendor, EffectiveAt: effectiveAt}
	for i, row := range records {
		if i == 0 {
			continue
		}
		if len(row) < 3 {
			continue
		}
		dest := strings.TrimPrefix(strings.TrimSpace(row[0]), "+")
		cost, err := strconv.ParseFloat(strings.TrimSpace(row[2]), 64)
		if err != nil {
			return nil, err
		}
		sheet.Rates = append(sheet.Rates, models.RateRow{
			Prefix:     dest,
			CarrierID:  strings.TrimSpace(row[1]),
			CostPerMin: cost,
		})
	}
	return normalizeSheet(sheet)
}

type VendorB struct{}

// Vendor B JSON: { "vendor_code": "...", "rates": [{ "dest_prefix", "provider", "price" }] }
func (a *VendorB) Parse(r io.Reader, vendor string, effectiveAt time.Time) (*models.NormalizedRateSheet, error) {
	var input struct {
		Rates []struct {
			DestPrefix string  `json:"dest_prefix"`
			Provider   string  `json:"provider"`
			Price      float64 `json:"price"`
		} `json:"rates"`
	}
	if err := json.NewDecoder(r).Decode(&input); err != nil {
		return nil, err
	}
	sheet := &models.NormalizedRateSheet{Vendor: vendor, EffectiveAt: effectiveAt}
	for _, r := range input.Rates {
		sheet.Rates = append(sheet.Rates, models.RateRow{
			Prefix:     r.DestPrefix,
			CarrierID:  r.Provider,
			CostPerMin: r.Price,
		})
	}
	return normalizeSheet(sheet)
}

func normalizeSheet(sheet *models.NormalizedRateSheet) (*models.NormalizedRateSheet, error) {
	for i := range sheet.Rates {
		prefix, err := e164.NormalizePrefix(sheet.Rates[i].Prefix)
		if err != nil {
			return nil, err
		}
		sheet.Rates[i].Prefix = prefix
	}
	if len(sheet.Rates) == 0 {
		return nil, fmt.Errorf("no rates found")
	}
	return sheet, nil
}
