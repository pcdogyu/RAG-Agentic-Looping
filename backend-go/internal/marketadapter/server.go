package marketadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const maxRequestBytes = 1 << 20

type Asset struct {
	AssetID        string   `json:"asset_id"`
	AssetClass     string   `json:"asset_class"`
	Market         string   `json:"market"`
	Symbol         string   `json:"symbol"`
	Name           string   `json:"name"`
	Exchange       string   `json:"exchange_or_provider"`
	Currency       string   `json:"currency"`
	Aliases        []string `json:"aliases"`
	Products       []string `json:"products,omitempty"`
	Competitors    []string `json:"competitors,omitempty"`
	RawSector      string   `json:"raw_sector,omitempty"`
	RawIndustry    string   `json:"raw_industry,omitempty"`
	InstrumentType string   `json:"instrument_type,omitempty"`
	LotSize        int      `json:"lot_size"`
	Active         bool     `json:"active"`
}

type PriceRequest struct {
	Symbol string `json:"symbol"`
	Market string `json:"market"`
	Start  string `json:"start"`
	End    string `json:"end"`
}

type NewsRequest struct {
	Since string `json:"since"`
	Limit int    `json:"limit"`
}

type Provider interface {
	Universe(context.Context, string) ([]Asset, error)
	Prices(context.Context, PriceRequest) ([]map[string]any, error)
	Fundamentals(context.Context, string, string) ([]map[string]any, bool, error)
	News(context.Context, NewsRequest) ([]map[string]any, error)
}

type Server struct {
	provider Provider
}

func NewHandler(provider Provider) http.Handler {
	server := &Server{provider: provider}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", methodOnly(http.MethodGet, server.health))
	mux.HandleFunc("/v1/assets/resolve", methodOnly(http.MethodPost, server.resolveAssets))
	mux.HandleFunc("/v1/assets/universe", methodOnly(http.MethodPost, server.universe))
	mux.HandleFunc("/v1/prices", methodOnly(http.MethodPost, server.prices))
	mux.HandleFunc("/v1/fundamentals", methodOnly(http.MethodPost, server.fundamentals))
	mux.HandleFunc("/v1/news", methodOnly(http.MethodPost, server.news))
	return recoverer(mux)
}

func methodOnly(method string, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			w.Header().Set("Allow", method)
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		handler(w, r)
	}
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "market-adapter", "implementation": "go"})
}

func (s *Server) resolveAssets(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Query string `json:"query"`
		Limit int    `json:"limit"`
	}
	if !decodeRequest(w, r, &request) {
		return
	}
	query := strings.ToLower(strings.TrimSpace(request.Query))
	if query == "" {
		writeError(w, http.StatusUnprocessableEntity, "query is required")
		return
	}
	limit := request.Limit
	if limit == 0 {
		limit = 20
	}
	limit = min(max(limit, 1), 100)
	items, err := s.provider.Universe(r.Context(), "")
	if err != nil {
		writeProviderError(w, err)
		return
	}
	matches := make([]Asset, 0, min(limit, len(items)))
	for _, item := range items {
		if strings.Contains(strings.ToLower(item.Symbol+" "+item.Name), query) {
			item.Products = emptyStrings(item.Products)
			item.Competitors = emptyStrings(item.Competitors)
			matches = append(matches, item)
			if len(matches) == limit {
				break
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": matches})
}

func (s *Server) universe(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Market string `json:"market"`
	}
	if !decodeRequest(w, r, &request) {
		return
	}
	market := strings.ToUpper(strings.TrimSpace(request.Market))
	if market != "" && market != "CN" && market != "HK" {
		writeError(w, http.StatusUnprocessableEntity, "market must be CN or HK")
		return
	}
	items, err := s.provider.Universe(r.Context(), market)
	if err != nil {
		writeProviderError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) prices(w http.ResponseWriter, r *http.Request) {
	request := PriceRequest{}
	if !decodeRequest(w, r, &request) {
		return
	}
	request.Symbol = strings.TrimSpace(request.Symbol)
	request.Market = strings.ToUpper(strings.TrimSpace(request.Market))
	if request.Symbol == "" {
		writeError(w, http.StatusUnprocessableEntity, "symbol is required")
		return
	}
	if request.Market == "" {
		request.Market = "CN"
	}
	if request.Market != "CN" && request.Market != "HK" {
		writeError(w, http.StatusUnprocessableEntity, "market must be CN or HK")
		return
	}
	items, err := s.provider.Prices(r.Context(), request)
	if err != nil {
		writeProviderError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) fundamentals(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Symbol string `json:"symbol"`
		Market string `json:"market"`
	}
	if !decodeRequest(w, r, &request) {
		return
	}
	request.Symbol = strings.TrimSpace(request.Symbol)
	request.Market = strings.ToUpper(strings.TrimSpace(request.Market))
	if request.Symbol == "" {
		writeError(w, http.StatusUnprocessableEntity, "symbol is required")
		return
	}
	if request.Market == "" {
		request.Market = "CN"
	}
	items, unsupported, err := s.provider.Fundamentals(r.Context(), request.Symbol, request.Market)
	if err != nil {
		writeProviderError(w, err)
		return
	}
	response := map[string]any{"items": items}
	if unsupported {
		response["unsupported"] = true
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) news(w http.ResponseWriter, r *http.Request) {
	request := NewsRequest{}
	if !decodeRequest(w, r, &request) {
		return
	}
	if request.Limit == 0 {
		request.Limit = 40
	}
	request.Limit = min(max(request.Limit, 1), 200)
	items, err := s.provider.News(r.Context(), request)
	if err != nil {
		writeProviderError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func decodeRequest(w http.ResponseWriter, r *http.Request, target any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(target); err != nil && !errors.Is(err, io.EOF) {
		status := http.StatusUnprocessableEntity
		if strings.Contains(err.Error(), "request body too large") {
			status = http.StatusRequestEntityTooLarge
		}
		writeError(w, status, "invalid JSON request: "+err.Error())
		return false
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		writeError(w, http.StatusUnprocessableEntity, "request must contain one JSON object")
		return false
	}
	return true
}

func recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if value := recover(); value != nil {
				writeError(w, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func writeProviderError(w http.ResponseWriter, err error) {
	writeError(w, http.StatusBadGateway, fmt.Sprintf("%T: market provider failed", err))
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, detail string) {
	writeJSON(w, status, map[string]string{"detail": detail})
}

func emptyStrings(value []string) []string {
	if value == nil {
		return []string{}
	}
	return value
}
