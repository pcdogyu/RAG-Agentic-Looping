package marketdata

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const SecurityUniverseContractVersion = "security-universe-pit-v1"

type SecurityUniverseSnapshot struct {
	ID                string         `json:"id"`
	UniverseID        string         `json:"universe_id"`
	Market            string         `json:"market"`
	Status            string         `json:"status"`
	ObservedAt        time.Time      `json:"observed_at"`
	AvailableAt       time.Time      `json:"available_at"`
	SourceName        string         `json:"source_name"`
	SourceDocumentID  string         `json:"source_document_id"`
	SourceURL         string         `json:"source_url,omitempty"`
	EligibilityPolicy map[string]any `json:"eligibility_policy"`
	AssetCount        int            `json:"asset_count"`
	IncludedCount     int            `json:"included_count"`
	ExcludedCount     int            `json:"excluded_count"`
	DelistedCount     int            `json:"delisted_count"`
	FailureDetail     string         `json:"failure_detail,omitempty"`
	Metadata          map[string]any `json:"metadata"`
	CreatedAt         time.Time      `json:"created_at"`
}

type SecurityUniverseMembership struct {
	SnapshotID       string         `json:"snapshot_id"`
	UniverseID       string         `json:"universe_id"`
	Market           string         `json:"market"`
	SnapshotStatus   string         `json:"snapshot_status"`
	AssetID          string         `json:"asset_id"`
	MembershipStatus string         `json:"membership_status"`
	EffectiveAt      time.Time      `json:"effective_at"`
	AvailableAt      time.Time      `json:"available_at"`
	ReasonCodes      []string       `json:"reason_codes"`
	SourceIdentity   map[string]any `json:"source_identity"`
	CreatedAt        time.Time      `json:"created_at"`
}

func (s *Store) ListUniverseSnapshots(ctx context.Context, universeID string, availableAsOf time.Time, limit int) ([]SecurityUniverseSnapshot, error) {
	universeID = strings.TrimSpace(universeID)
	if s.db == nil || universeID == "" || availableAsOf.IsZero() || limit < 1 || limit > 200 {
		return nil, fmt.Errorf("invalid security universe snapshot query")
	}
	rows, err := s.db.Query(ctx, `SELECT id,universe_id,market,status,observed_at,available_at,source_name,source_document_id,source_url,
        eligibility_policy::jsonb,asset_count,included_count,excluded_count,delisted_count,failure_detail,metadata::jsonb,created_at
        FROM security_universe_snapshots WHERE universe_id=$1 AND available_at<=$2
        ORDER BY available_at DESC,observed_at DESC,id DESC LIMIT $3`, universeID, availableAsOf.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("list security universe snapshots: %w", err)
	}
	defer rows.Close()
	items := []SecurityUniverseSnapshot{}
	for rows.Next() {
		var item SecurityUniverseSnapshot
		var policy, metadata any
		if err := rows.Scan(&item.ID, &item.UniverseID, &item.Market, &item.Status, &item.ObservedAt, &item.AvailableAt, &item.SourceName, &item.SourceDocumentID, &item.SourceURL,
			&policy, &item.AssetCount, &item.IncludedCount, &item.ExcludedCount, &item.DelistedCount, &item.FailureDetail, &metadata, &item.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan security universe snapshot: %w", err)
		}
		if err := decodeUniverseJSON(policy, &item.EligibilityPolicy); err != nil {
			return nil, err
		}
		if err := decodeUniverseJSON(metadata, &item.Metadata); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) ListAssetUniverseMemberships(ctx context.Context, assetID string, availableAsOf time.Time, limit int) ([]SecurityUniverseMembership, error) {
	assetID = strings.TrimSpace(assetID)
	if s.db == nil || assetID == "" || availableAsOf.IsZero() || limit < 1 || limit > 500 {
		return nil, fmt.Errorf("invalid security universe membership query")
	}
	rows, err := s.db.Query(ctx, `SELECT m.snapshot_id,s.universe_id,s.market,s.status,m.asset_id,m.membership_status,m.effective_at,m.available_at,
        m.reason_codes::jsonb,m.source_identity::jsonb,m.created_at FROM security_universe_memberships m
        JOIN security_universe_snapshots s ON s.id=m.snapshot_id
        WHERE m.asset_id=$1 AND m.available_at<=$2 ORDER BY m.available_at DESC,m.effective_at DESC,m.snapshot_id DESC LIMIT $3`, assetID, availableAsOf.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("list asset universe memberships: %w", err)
	}
	defer rows.Close()
	items := []SecurityUniverseMembership{}
	for rows.Next() {
		var item SecurityUniverseMembership
		var reasons, identity any
		if err := rows.Scan(&item.SnapshotID, &item.UniverseID, &item.Market, &item.SnapshotStatus, &item.AssetID, &item.MembershipStatus, &item.EffectiveAt, &item.AvailableAt,
			&reasons, &identity, &item.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan security universe membership: %w", err)
		}
		if err := decodeUniverseJSON(reasons, &item.ReasonCodes); err != nil {
			return nil, err
		}
		if err := decodeUniverseJSON(identity, &item.SourceIdentity); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func decodeUniverseJSON(raw, target any) error {
	body, err := json.Marshal(raw)
	if bytes, ok := raw.([]byte); ok {
		body = bytes
	} else if value, ok := raw.(string); ok {
		body = []byte(value)
	}
	if err != nil {
		return fmt.Errorf("encode security universe JSON: %w", err)
	}
	if err := json.Unmarshal(body, target); err != nil {
		return fmt.Errorf("decode security universe JSON: %w", err)
	}
	return nil
}
