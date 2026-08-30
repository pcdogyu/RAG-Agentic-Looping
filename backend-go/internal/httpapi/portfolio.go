package httpapi

import (
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type portfolioPositionState struct {
	Asset          map[string]any
	Quantity, Cost float64
}

func (s *Server) portfolio(w http.ResponseWriter, r *http.Request) {
	snapshot, err := s.portfolioSnapshot(r)
	if err != nil {
		writeError(w, 500, "portfolio query failed")
		return
	}
	writeJSON(w, 200, snapshot)
}

func (s *Server) portfolioSnapshot(r *http.Request) (map[string]any, error) {
	rows, err := s.db.Query(r.Context(), `SELECT payload::jsonb FROM paper_orders ORDER BY executed_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	states := map[string]*portfolioPositionState{}
	order := make([]string, 0)
	cash := s.cfg.InitialCash
	for rows.Next() {
		var body []byte
		if rows.Scan(&body) != nil {
			continue
		}
		var item map[string]any
		if json.Unmarshal(body, &item) != nil {
			continue
		}
		asset, _ := item["asset"].(map[string]any)
		assetID := stringValue(asset["asset_id"])
		if assetID == "" {
			continue
		}
		state := states[assetID]
		if state == nil {
			state = &portfolioPositionState{Asset: asset}
			states[assetID] = state
			order = append(order, assetID)
		}
		quantity, price, fee := numberValue(item["quantity"]), numberValue(item["price"]), numberValue(item["fee"])
		fx := fxToUSD(stringValue(item["currency"]))
		signed := quantity
		if stringValue(item["side"]) == "sell" {
			signed = -quantity
		}
		state.Quantity += signed
		cash -= signed * price * fx
		cash -= fee * fx
		if signed > 0 {
			state.Cost += quantity*price*fx + fee*fx
		}
	}
	type rawPosition struct {
		Asset                         map[string]any
		Quantity, Price, Market, Cost float64
	}
	raw := make([]rawPosition, 0)
	marketTotal := float64(0)
	for _, id := range order {
		state := states[id]
		if state.Quantity <= 0 {
			continue
		}
		currency := stringValue(state.Asset["currency"])
		price := state.Cost / state.Quantity / fxToUSD(currency)
		market := state.Quantity * price * fxToUSD(currency)
		marketTotal += market
		raw = append(raw, rawPosition{state.Asset, state.Quantity, price, market, state.Cost})
	}
	nav := cash + marketTotal
	cryptoValue := float64(0)
	positions := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		if stringValue(item.Asset["asset_class"]) == "crypto" {
			cryptoValue += item.Market
		}
		weight := float64(0)
		if nav != 0 {
			weight = item.Market / nav
		}
		positions = append(positions, map[string]any{"asset": item.Asset, "quantity": item.Quantity, "average_cost": item.Cost / item.Quantity, "last_price": item.Price, "market_value_usd": item.Market, "unrealized_pnl_usd": item.Market - item.Cost, "weight": weight})
	}
	cryptoWeight := float64(0)
	if nav != 0 {
		cryptoWeight = cryptoValue / nav
	}
	return map[string]any{"cash_usd": cash, "nav_usd": nav, "crypto_weight": cryptoWeight, "positions": positions, "as_of": time.Now().UTC()}, rows.Err()
}

type paperOrderInput struct {
	RecommendationID string   `json:"recommendation_id"`
	Price            float64  `json:"price"`
	TargetWeight     *float64 `json:"target_weight"`
}

func (s *Server) paperOrder(w http.ResponseWriter, r *http.Request) {
	var input paperOrderInput
	if !decodeJSONBody(w, r, &input) {
		return
	}
	if _, err := uuid.Parse(input.RecommendationID); err != nil {
		writeError(w, 422, "recommendation_id must be a valid UUID")
		return
	}
	if input.Price <= 0 {
		writeError(w, 422, "price must be positive")
		return
	}
	if input.TargetWeight != nil && (*input.TargetWeight <= 0 || *input.TargetWeight > 0.15) {
		writeError(w, 422, "target_weight must be greater than 0 and at most 0.15")
		return
	}
	var body []byte
	err := s.db.QueryRow(r.Context(), `SELECT payload::jsonb FROM recommendations WHERE id=$1`, input.RecommendationID).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, 404, "recommendation not found")
		return
	}
	if err != nil {
		writeError(w, 500, "recommendation query failed")
		return
	}
	var recommendation map[string]any
	if json.Unmarshal(body, &recommendation) != nil {
		writeError(w, 500, "stored recommendation is invalid")
		return
	}
	asset, _ := recommendation["asset"].(map[string]any)
	if err = paperTradingGate(recommendation, asset); err != nil {
		writeError(w, 409, err.Error())
		return
	}
	snapshot, err := s.portfolioSnapshot(r)
	if err != nil {
		writeError(w, 500, "portfolio query failed")
		return
	}
	nav, cash := numberValue(snapshot["nav_usd"]), numberValue(snapshot["cash_usd"])
	assetClass := stringValue(asset["asset_class"])
	maxWeight := s.cfg.MaxEquityWeight
	if assetClass == "crypto" {
		maxWeight = s.cfg.MaxCryptoWeight
	}
	target := maxWeight
	if input.TargetWeight != nil {
		target = math.Min(*input.TargetWeight, maxWeight)
	}
	existingWeight := float64(0)
	for _, raw := range anySlice(snapshot["positions"]) {
		position, _ := raw.(map[string]any)
		positionAsset, _ := position["asset"].(map[string]any)
		if stringValue(positionAsset["asset_id"]) == stringValue(asset["asset_id"]) {
			existingWeight = numberValue(position["weight"])
			break
		}
	}
	increment := math.Max(0, target-existingWeight)
	if assetClass == "crypto" {
		remaining := s.cfg.MaxTotalCryptoWeight - numberValue(snapshot["crypto_weight"])
		increment = math.Min(increment, math.Max(0, remaining))
	}
	cashAvailable := math.Max(0, cash-nav*s.cfg.MinimumCashWeight)
	usdToUse := math.Min(nav*increment, cashAvailable)
	if usdToUse <= 0 {
		writeError(w, 409, "cash or asset-class risk limit reached")
		return
	}
	fx := fxToUSD(stringValue(asset["currency"]))
	rawQuantity := usdToUse / (input.Price * fx)
	quantity := roundOrderQuantity(asset, rawQuantity)
	if quantity <= 0 {
		writeError(w, 409, "target allocation is smaller than one exchange lot")
		return
	}
	bps := s.cfg.EquityCostBPS
	if assetClass == "crypto" {
		bps = s.cfg.CryptoCostBPS
	}
	fee := quantity * input.Price * float64(bps) / 10000
	now := time.Now().UTC()
	id := uuid.New().String()
	order := map[string]any{"id": id, "recommendation_id": input.RecommendationID, "asset": asset, "side": "buy", "quantity": quantity, "price": input.Price, "currency": asset["currency"], "fee": fee, "executed_at": now}
	encoded, _ := json.Marshal(order)
	_, err = s.db.Exec(r.Context(), `INSERT INTO paper_orders(id,recommendation_id,asset_id,side,quantity,price,currency,fee,executed_at,payload) VALUES($1,$2,$3,'buy',$4,$5,$6,$7,$8,$9)`, id, input.RecommendationID, stringValue(asset["asset_id"]), quantity, input.Price, stringValue(asset["currency"]), fee, now, encoded)
	if err != nil {
		writeError(w, 500, "paper order could not be saved")
		return
	}
	writeJSON(w, 200, order)
}

func paperTradingGate(recommendation, asset map[string]any) error {
	class := stringValue(asset["asset_class"])
	if class != "equity" && class != "crypto" {
		return errors.New("paper execution is not supported for commodity or FX assets")
	}
	version := stringValue(recommendation["scoring_version"])
	rating := stringValue(recommendation["rating"])
	confidence := numberValue(recommendation["confidence"])
	impact, _ := recommendation["impact"].(map[string]any)
	if version == "llm-direction-v3" {
		score := numberValue(recommendation["direction_score"])
		if score == 0 {
			score = numberValue(recommendation["score"])
		}
		if stringValue(recommendation["score_source"]) != "llm" || impact == nil || !boolValue(impact["execution_supported"]) || stringValue(impact["trade_status"]) != "tradeable" || score < 30 || !boolValue(recommendation["evidence_complete"]) {
			return errors.New("recommendation does not meet the v3 trading gate")
		}
	} else if version == "target-transmission-v2" {
		if impact == nil || !boolValue(impact["execution_supported"]) || stringValue(impact["trade_status"]) != "tradeable" || math.Abs(numberValue(impact["score"])) < 0.25 {
			return errors.New("target impact does not meet the v2 trading gate")
		}
	}
	if rating != "bullish" && rating != "strongly_bullish" {
		return errors.New("only bullish recommendations can open a position")
	}
	if confidence < 0.55 {
		return errors.New("recommendation confidence is below 55%")
	}
	return nil
}
func roundOrderQuantity(asset map[string]any, quantity float64) float64 {
	if stringValue(asset["asset_class"]) == "crypto" {
		return math.Floor(quantity*1e8) / 1e8
	}
	lot := int(numberValue(asset["lot_size"]))
	if stringValue(asset["market"]) == "CN" {
		lot = 100
	}
	if lot <= 0 {
		lot = 1
	}
	return math.Floor(quantity/float64(lot)) * float64(lot)
}
func fxToUSD(currency string) float64 {
	switch stringsUpper(currency) {
	case "CNY":
		return 0.14
	case "HKD":
		return 0.128
	default:
		return 1
	}
}
func stringsUpper(value string) string {
	bytes := []byte(value)
	for i, char := range bytes {
		if char >= 'a' && char <= 'z' {
			bytes[i] = char - 32
		}
	}
	return string(bytes)
}
