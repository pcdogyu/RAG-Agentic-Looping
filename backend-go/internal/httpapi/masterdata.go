package httpapi

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
)

const assetJSON = `jsonb_build_object(
	'asset_id',id,'asset_class',asset_class,'market',market,'symbol',symbol,'name',name,
	'exchange_or_provider',exchange_or_provider,'currency',currency,'aliases',aliases::jsonb,
	'products',products::jsonb,'competitors',competitors::jsonb,'sector_id',sector_id,
	'industry_id',industry_id,'raw_sector',raw_sector,'raw_industry',raw_industry,
	'instrument_type',instrument_type,'market_cap',market_cap,'market_cap_rank',market_cap_rank,
	'association_tier',association_tier,'association_reason',association_reason,
	'last_synced_at',last_synced_at,'issuer_id',issuer_id,
	'primary_listing_asset_id',primary_listing_asset_id,'lot_size',lot_size,'active',active)`

func (s *Server) assetUniverse(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	offset, ok := intQuery(w, query, "offset", 0, 0, 1_000_000_000)
	if !ok {
		return
	}
	limit, ok := intQuery(w, query, "limit", 100, 1, 500)
	if !ok {
		return
	}
	where := []string{"1=1"}
	args := make([]any, 0, 8)
	add := func(clause string, value any) {
		args = append(args, value)
		where = append(where, fmt.Sprintf(clause, len(args)))
	}
	if value := strings.TrimSpace(query.Get("q")); value != "" {
		add(`(symbol ILIKE '%%'||$%[1]d||'%%' OR name ILIKE '%%'||$%[1]d||'%%' OR aliases::text ILIKE '%%'||$%[1]d||'%%')`, value)
	}
	if value := strings.TrimSpace(query.Get("market")); value != "" {
		add(`market=$%d`, strings.ToUpper(value))
	}
	if value := strings.TrimSpace(query.Get("sector_id")); value != "" {
		add(`sector_id=$%d`, value)
	}
	if value := strings.TrimSpace(query.Get("industry_id")); value != "" {
		add(`industry_id=$%d`, value)
	}
	if value := strings.TrimSpace(query.Get("association_tier")); value != "" {
		if value != "standard" && value != "exact_only" && value != "manual_only" {
			validationError(w, "association_tier", "Input should be 'standard', 'exact_only' or 'manual_only'")
			return
		}
		add(`association_tier=$%d`, value)
	}
	active := query.Get("active")
	if active == "" {
		active = "true"
	}
	if active != "null" {
		if active != "true" && active != "false" {
			validationError(w, "active", "Input should be a valid boolean")
			return
		}
		add(`active=$%d`, active == "true")
	}
	filter := strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRow(r.Context(), `SELECT count(*) FROM assets WHERE `+filter, args...).Scan(&total); err != nil {
		writeError(w, http.StatusInternalServerError, "asset universe query failed")
		return
	}
	args = append(args, offset, limit)
	rows, err := s.db.Query(r.Context(), `SELECT `+assetJSON+` FROM assets WHERE `+filter+fmt.Sprintf(` ORDER BY market,symbol OFFSET $%d LIMIT $%d`, len(args)-1, len(args)), args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "asset universe query failed")
		return
	}
	defer rows.Close()
	items := make([]any, 0)
	for rows.Next() {
		var body []byte
		if rows.Scan(&body) != nil {
			writeError(w, 500, "stored asset is invalid")
			return
		}
		item, _ := decodeDefault(body, map[string]any{}).(map[string]any)
		normalizeAsset(item)
		items = append(items, item)
	}
	if rows.Err() != nil {
		writeError(w, 500, "asset universe query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": total, "offset": offset, "limit": limit})
}

func (s *Server) industries(w http.ResponseWriter, r *http.Request) {
	market := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("market")))
	rows, err := s.db.Query(r.Context(), `
		SELECT i.id,i.parent_id,i.level,i.name_zh,i.name_en,i.aliases::jsonb,coalesce(a.asset_count,0)::int
		FROM industries i LEFT JOIN (
			SELECT industry_id,count(*)::int AS asset_count FROM assets
			WHERE active=true AND ($1='' OR market=$1) GROUP BY industry_id
		) a ON a.industry_id=i.id
		WHERE i.active=true
		ORDER BY i.level,i.parent_id,i.name_zh`, market)
	if err != nil {
		writeError(w, 500, "industry query failed")
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0)
	for rows.Next() {
		var id, nameZH, nameEN string
		var parent *string
		var level, count int
		var aliases []byte
		if rows.Scan(&id, &parent, &level, &nameZH, &nameEN, &aliases, &count) != nil {
			writeError(w, 500, "stored industry is invalid")
			return
		}
		items = append(items, map[string]any{"industry_id": id, "parent_id": parent, "level": level, "name_zh": nameZH, "name_en": nameEN, "aliases": decodeDefault(aliases, []any{}), "asset_count": count})
	}
	writeJSON(w, http.StatusOK, items)
}

func (s *Server) assetUniverseStatus(w http.ResponseWriter, r *http.Request) {
	counts := map[string][2]int{}
	tierCounts := map[string]map[string]int{}
	rows, err := s.db.Query(r.Context(), `SELECT market,count(*)::int,count(nullif(industry_id,''))::int FROM assets WHERE active=true GROUP BY market`)
	if err != nil {
		writeError(w, 500, "asset universe status query failed")
		return
	}
	for rows.Next() {
		var market string
		var total, classified int
		if rows.Scan(&market, &total, &classified) == nil {
			counts[market] = [2]int{total, classified}
		}
	}
	rows.Close()
	tierRows, err := s.db.Query(r.Context(), `SELECT market,association_tier,count(*)::int FROM assets WHERE active=true GROUP BY market,association_tier`)
	if err != nil {
		writeError(w, 500, "asset universe status query failed")
		return
	}
	for tierRows.Next() {
		var market, tier string
		var count int
		if tierRows.Scan(&market, &tier, &count) == nil {
			if tierCounts[market] == nil {
				tierCounts[market] = map[string]int{}
			}
			tierCounts[market][tier] = count
		}
	}
	tierRows.Close()
	syncRows, err := s.db.Query(r.Context(), `SELECT market,status,asset_count,industry_count,added_count,updated_count,deactivated_count,last_error,started_at,completed_at FROM asset_universe_sync ORDER BY market`)
	if err != nil {
		writeError(w, 500, "asset universe status query failed")
		return
	}
	defer syncRows.Close()
	markets := make([]map[string]any, 0)
	activeCounts := make(map[string]int, len(counts))
	for market, value := range counts {
		activeCounts[market] = value[0]
	}
	for syncRows.Next() {
		var market, status string
		var assetCount, industryCount, added, updated, deactivated int
		var lastError *string
		var started, completed *time.Time
		if syncRows.Scan(&market, &status, &assetCount, &industryCount, &added, &updated, &deactivated, &lastError, &started, &completed) != nil {
			continue
		}
		value := counts[market]
		rate := float64(0)
		if value[0] > 0 {
			rate = float64(value[1]) / float64(value[0])
		}
		marketTiers := tierCounts[market]
		if marketTiers == nil {
			marketTiers = map[string]int{}
		}
		activeCounts[market] = value[0]
		markets = append(markets, map[string]any{"market": market, "status": status, "asset_count": assetCount, "industry_count": industryCount,
			"added_count": added, "updated_count": updated, "deactivated_count": deactivated, "classified_count": value[1],
			"unclassified_count": value[0] - value[1], "classification_rate": round4(rate), "last_error": nullableStringPointer(lastError),
			"association_tier_counts": marketTiers, "started_at": isoTimeOrNil(started), "completed_at": isoTimeOrNil(completed)})
	}
	writeJSON(w, http.StatusOK, map[string]any{"markets": markets, "active_counts": activeCounts})
}

type assetEditInput struct {
	Aliases         *[]string `json:"aliases"`
	IndustryID      *string   `json:"industry_id"`
	Active          *bool     `json:"active"`
	AssociationTier *string   `json:"association_tier"`
}

func (s *Server) editAsset(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	var input assetEditInput
	if !decodeJSONBody(w, r, &input) {
		return
	}
	assetID := chi.URLParam(r, "assetID")
	tx, err := s.db.Begin(r.Context())
	if err != nil {
		writeError(w, 500, "asset update failed")
		return
	}
	defer tx.Rollback(r.Context()) //nolint:errcheck
	var exists bool
	if err := tx.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM assets WHERE id=$1)`, assetID).Scan(&exists); err != nil || !exists {
		writeError(w, 404, "asset not found")
		return
	}
	if input.Aliases != nil {
		aliases := uniqueTrimmed(*input.Aliases, 50)
		body, _ := json.Marshal(aliases)
		if _, err = tx.Exec(r.Context(), `UPDATE assets SET aliases=$2 WHERE id=$1`, assetID, body); err != nil {
			writeError(w, 500, "asset update failed")
			return
		}
	}
	if input.IndustryID != nil {
		var parent string
		if *input.IndustryID != "" {
			err = tx.QueryRow(r.Context(), `SELECT coalesce(parent_id,'') FROM industries WHERE id=$1`, *input.IndustryID).Scan(&parent)
			if errors.Is(err, pgx.ErrNoRows) {
				writeError(w, 422, "unknown industry_id")
				return
			}
			if err != nil {
				writeError(w, 500, "asset update failed")
				return
			}
		}
		if _, err = tx.Exec(r.Context(), `UPDATE assets SET manual_industry_id=$2,industry_id=$2,sector_id=$3 WHERE id=$1`, assetID, nullableString(*input.IndustryID), parent); err != nil {
			writeError(w, 500, "asset update failed")
			return
		}
	}
	if input.Active != nil {
		if _, err = tx.Exec(r.Context(), `UPDATE assets SET manual_active=$2,active=$2 WHERE id=$1`, assetID, *input.Active); err != nil {
			writeError(w, 500, "asset update failed")
			return
		}
	}
	if input.AssociationTier != nil {
		tier := strings.TrimSpace(*input.AssociationTier)
		if tier != "auto" && tier != "standard" && tier != "exact_only" && tier != "manual_only" {
			validationError(w, "association_tier", "Input should be 'auto', 'standard', 'exact_only' or 'manual_only'")
			return
		}
		if tier == "auto" {
			_, err = tx.Exec(r.Context(), `UPDATE assets SET manual_association_tier=NULL,association_tier=coalesce(provider_association_tier,'standard'),association_reason=coalesce(provider_association_reason,'provider_verified') WHERE id=$1`, assetID)
		} else {
			_, err = tx.Exec(r.Context(), `UPDATE assets SET manual_association_tier=$2,association_tier=$2,association_reason='manual_override' WHERE id=$1`, assetID, tier)
		}
		if err != nil {
			writeError(w, 500, "asset update failed")
			return
		}
	}
	if _, err = tx.Exec(r.Context(), `UPDATE assets SET last_synced_at=now() WHERE id=$1`, assetID); err != nil {
		writeError(w, 500, "asset update failed")
		return
	}
	var body []byte
	if err = tx.QueryRow(r.Context(), `SELECT `+assetJSON+` FROM assets WHERE id=$1`, assetID).Scan(&body); err != nil {
		writeError(w, 500, "asset update failed")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		writeError(w, 500, "asset update failed")
		return
	}
	item, _ := decodeDefault(body, map[string]any{}).(map[string]any)
	normalizeAsset(item)
	writeJSON(w, http.StatusOK, item)
}

func (s *Server) requireAdmin(w http.ResponseWriter, r *http.Request) bool {
	if s.cfg.AdminAPIToken == "" {
		writeError(w, http.StatusServiceUnavailable, "ADMIN_API_TOKEN is not configured")
		return false
	}
	provided := r.Header.Get("X-Admin-Token")
	if len(provided) != len(s.cfg.AdminAPIToken) || subtle.ConstantTimeCompare([]byte(provided), []byte(s.cfg.AdminAPIToken)) != 1 {
		writeError(w, http.StatusUnauthorized, "administrator token required")
		return false
	}
	return true
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, target any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(w, http.StatusUnprocessableEntity, "invalid request body")
		return false
	}
	return true
}

func uniqueTrimmed(values []string, limit int) []string {
	result := make([]string, 0, min(len(values), limit))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
		if len(result) == limit {
			break
		}
	}
	return result
}
func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
func round4(value float64) float64 { return float64(int(value*10000+0.5)) / 10000 }
