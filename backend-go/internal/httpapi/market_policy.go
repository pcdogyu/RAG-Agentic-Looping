package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/marketpolicy"
)

type assetPolicyRecord struct {
	AssetID    string
	AssetClass string
	Market     string
	Currency   string
	Policy     marketpolicy.Policy
}

func (s *Server) loadAssetPolicy(ctx context.Context, assetID string) (assetPolicyRecord, error) {
	item := assetPolicyRecord{AssetID: strings.TrimSpace(assetID)}
	if s.db == nil || item.AssetID == "" {
		return item, fmt.Errorf("asset store and asset_id are required")
	}
	if err := s.db.QueryRow(ctx, `SELECT asset_class,market,currency FROM assets WHERE id=$1 AND active=true`, item.AssetID).Scan(&item.AssetClass, &item.Market, &item.Currency); err != nil {
		return item, fmt.Errorf("active asset was not found")
	}
	item.Policy = marketpolicy.Resolve(item.AssetClass, item.Market)
	return item, nil
}

func (s *Server) marketPolicyForAsset(w http.ResponseWriter, r *http.Request) {
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	item, err := s.loadAssetPolicy(r.Context(), assetID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"asset_id": item.AssetID, "asset_class": item.AssetClass, "market": item.Market, "currency": item.Currency, "policy": item.Policy})
}

func validatePredictionPolicy(item assetPolicyRecord, declaredMarket string) error {
	if !item.Policy.PredictionSupported {
		return fmt.Errorf("prediction_not_supported_for_asset_market_policy")
	}
	if declaredMarket = strings.TrimSpace(declaredMarket); declaredMarket != "" && !strings.EqualFold(declaredMarket, item.Market) {
		return fmt.Errorf("declared market does not match the asset master")
	}
	return nil
}
