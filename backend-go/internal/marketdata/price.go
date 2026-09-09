// Package marketdata owns immutable, source-aware market observations. A
// provider close and an adjusted close are different facts and are never
// silently substituted for one another.
package marketdata

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
)

const PriceContractVersion = "market-price-observation-v1"

type PriceObservation struct {
	ID               string         `json:"id"`
	AssetID          string         `json:"asset_id"`
	Market           string         `json:"market"`
	Currency         string         `json:"currency"`
	SessionDate      time.Time      `json:"session_date"`
	ObservedAt       time.Time      `json:"observed_at"`
	AvailableAt      time.Time      `json:"available_at"`
	Price            float64        `json:"price"`
	PriceField       string         `json:"price_field"`
	TimePrecision    string         `json:"time_precision"`
	SourceName       string         `json:"source_name"`
	SourceDocumentID string         `json:"source_document_id"`
	SourceURL        string         `json:"source_url,omitempty"`
	Metadata         map[string]any `json:"metadata"`
	CreatedAt        time.Time      `json:"created_at,omitempty"`
}

func (value *PriceObservation) NormalizeAndValidate() error {
	value.AssetID = strings.TrimSpace(value.AssetID)
	value.Market = strings.ToUpper(strings.TrimSpace(value.Market))
	value.Currency = strings.ToUpper(strings.TrimSpace(value.Currency))
	value.PriceField = strings.ToLower(strings.TrimSpace(value.PriceField))
	value.TimePrecision = strings.ToLower(strings.TrimSpace(value.TimePrecision))
	value.SourceName = strings.TrimSpace(value.SourceName)
	value.SourceDocumentID = strings.TrimSpace(value.SourceDocumentID)
	value.SourceURL = strings.TrimSpace(value.SourceURL)
	if value.AssetID == "" || value.Market == "" || value.Currency == "" {
		return fmt.Errorf("market price observation requires asset_id, market and currency")
	}
	if value.ObservedAt.IsZero() || value.AvailableAt.IsZero() {
		return fmt.Errorf("market price observation requires observed_at and available_at")
	}
	value.ObservedAt = value.ObservedAt.UTC()
	value.AvailableAt = value.AvailableAt.UTC()
	if value.AvailableAt.Before(value.ObservedAt) {
		return fmt.Errorf("market price available_at precedes observed_at")
	}
	if value.SessionDate.IsZero() {
		value.SessionDate = dateUTC(value.ObservedAt)
	} else {
		value.SessionDate = dateUTC(value.SessionDate)
	}
	if value.Price <= 0 || math.IsNaN(value.Price) || math.IsInf(value.Price, 0) {
		return fmt.Errorf("market price must be finite and positive")
	}
	if value.PriceField != "close" && value.PriceField != "adjusted_close" {
		return fmt.Errorf("market price field must be close or adjusted_close")
	}
	if value.TimePrecision != "daily_close" && value.TimePrecision != "timestamped" {
		return fmt.Errorf("market price time_precision must be daily_close or timestamped")
	}
	if value.SourceName == "" || value.SourceDocumentID == "" {
		return fmt.Errorf("market price observation requires source_name and source_document_id")
	}
	if value.Metadata == nil {
		value.Metadata = map[string]any{}
	}
	value.Metadata["contract_version"] = PriceContractVersion
	if value.ID == "" {
		value.ID = DeterministicPriceID(*value)
	}
	return nil
}

func DeterministicPriceID(value PriceObservation) string {
	identity := struct {
		AssetID, SourceName, SourceDocumentID, PriceField string
		ObservedAt                                        time.Time
		Price                                             float64
	}{value.AssetID, value.SourceName, value.SourceDocumentID, value.PriceField, value.ObservedAt.UTC(), value.Price}
	body, _ := json.Marshal(identity)
	hash := sha256.Sum256(body)
	return "market-price-" + hex.EncodeToString(hash[:])[:48]
}

func dateUTC(value time.Time) time.Time {
	value = value.UTC()
	return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
}
