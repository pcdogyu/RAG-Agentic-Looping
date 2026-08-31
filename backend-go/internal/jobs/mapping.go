package jobs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
	"github.com/redis/go-redis/v9"
)

const mappingTask = "market_loop.resolve_event_assets"

type mappingHint struct {
	AssetID       string   `json:"asset_id"`
	SourceMention string   `json:"source_mention"`
	Name          string   `json:"name"`
	Symbol        string   `json:"symbol"`
	Market        string   `json:"market"`
	AssetClass    string   `json:"asset_class"`
	Relationship  string   `json:"relationship"`
	Confidence    float64  `json:"confidence"`
	Rationale     string   `json:"rationale"`
	SearchQueries []string `json:"search_queries"`
}

type mappingOutput struct {
	Candidates    []mappingHint `json:"candidates"`
	IndustryIDs   []string      `json:"industry_ids"`
	NoAssetReason string        `json:"no_asset_reason"`
}

type mappingAsset struct {
	Data          map[string]any
	ID            string
	Class         string
	Market        string
	Symbol        string
	Name          string
	Aliases       []string
	Products      []string
	IndustryID    string
	Instrument    string
	MarketCap     float64
	IssuerID      string
	PrimaryID     string
	ShortlistRank int
}

type mappingResult struct {
	Candidates         []any
	IndustryIDs        []string
	ProposedCount      int
	MasterDerivedCount int
	RejectedCount      int
	NoAssetReason      string
	TechnicalWarning   string
}

func NewMappingHandlers(cfg config.Config, db *pgxpool.Pool, redisClient *redis.Client) map[string]Handler {
	runtime := &ExtractRuntime{cfg: cfg, db: db, redis: redisClient, client: &http.Client{Timeout: cfg.OllamaTimeout}}
	return map[string]Handler{mappingTask: runtime.resolveEventAssets}
}

func (runtime *ExtractRuntime) resolveEventAssets(ctx context.Context, job Job) (any, error) {
	envelope, err := decodeTaskEnvelope(job.Payload)
	if err != nil {
		return nil, err
	}
	if len(envelope.Args) == 0 {
		return nil, errors.New("resolve_event_assets requires event_id")
	}
	eventID, err := uuid.Parse(fmt.Sprint(envelope.Args[0]))
	if err != nil {
		return nil, err
	}
	taskID := job.ID.String()
	if runtime.modelTaskCancelled(ctx, taskID) {
		return map[string]any{"status": "cancelled", "event_id": eventID}, nil
	}
	event, err := runtime.loadEvent(ctx, eventID)
	if err != nil {
		runtime.updateTrackedTask(ctx, "assist", taskID, "failed", job.Attempt, eventID.String(), "股票映射任务", "", err.Error(), nil)
		return nil, err
	}
	runtime.updateTrackedTask(ctx, "assist", taskID, "running", job.Attempt, eventID.String(), stringValue(event["headline"]), stringValue(event["event_type"]), "", nil)
	forceMapping := boolValue(envelope.Kwargs["force_mapping"])
	refreshReport := boolValue(envelope.Kwargs["refresh_event_report"])
	forceWebSearch := boolValue(envelope.Kwargs["force_web_search"])
	instanceID := stringValue(envelope.Kwargs["model_instance_id"])
	result := mappingResult{Candidates: anySlice(event["candidates"]), IndustryIDs: stringSlice(event["industry_ids"])}
	if forceMapping || len(result.Candidates) == 0 {
		running := analysisStep("asset_mapping", "running", "ollama+provider-registry", fmt.Sprintf("%s 正在从原文提及中识别证券，并通过主数据验证代码。", runtime.cfg.AssistModel), map[string]any{})
		replaceAnalysisStep(event, running)
		if err := runtime.saveEvent(ctx, event); err != nil {
			return nil, runtime.mappingFailure(ctx, job, event, refreshReport, forceWebSearch, err)
		}
		result, err = runtime.mapEvent(ctx, event, instanceID)
		if err != nil {
			return nil, runtime.mappingFailure(ctx, job, event, refreshReport, forceWebSearch, err)
		}
		event["candidates"], event["industry_ids"] = result.Candidates, result.IndustryIDs
		status := "completed"
		if len(result.Candidates) == 0 {
			status = "unmapped"
		}
		if result.TechnicalWarning != "" {
			status = "fallback"
		}
		step := analysisStep("asset_mapping", status, "ollama+provider-registry", fmt.Sprintf("%s 提出 %d 个候选，主数据产品归属补全 %d 个，主数据验证通过 %d 个、拒绝 %d 个。", runtime.cfg.AssistModel, result.ProposedCount, result.MasterDerivedCount, len(result.Candidates), result.RejectedCount), map[string]any{
			"proposed_count": result.ProposedCount, "master_derived_count": result.MasterDerivedCount,
			"verified_count": len(result.Candidates), "rejected_count": result.RejectedCount,
			"provider_errors": []any{}, "no_asset_reason": result.NoAssetReason,
			"technical_warning": result.TechnicalWarning, "industry_ids": result.IndustryIDs,
			"industry_peer_count": countRelationship(result.Candidates, "industry_peer"),
		})
		replaceAnalysisStep(event, step)
		replaceAnalysisStep(event, analysisStep("asset_mapping_queue", "completed", "go-worker", fmt.Sprintf("%s 二次标的发现任务已完成。", runtime.cfg.AssistModel), map[string]any{}))
		if err := runtime.saveEvent(ctx, event); err != nil {
			return nil, runtime.mappingFailure(ctx, job, event, refreshReport, forceWebSearch, err)
		}
	}
	researchTaskID, runID, err := runtime.enqueueResearchAfterMapping(ctx, event, refreshReport, forceWebSearch)
	if err != nil {
		return nil, runtime.mappingFailure(ctx, job, event, refreshReport, forceWebSearch, err)
	}
	metrics := map[string]any{"proposed_count": result.ProposedCount, "verified_count": len(result.Candidates), "rejected_count": result.RejectedCount}
	runtime.updateTrackedTask(ctx, "assist", taskID, "completed", job.Attempt, eventID.String(), stringValue(event["headline"]), stringValue(event["event_type"]), "", metrics)
	return map[string]any{"status": "event_research_queued", "event_id": eventID, "event_research_run_id": runID, "task_id": researchTaskID, "verified_assets": len(result.Candidates)}, nil
}

func (runtime *ExtractRuntime) mappingFailure(ctx context.Context, job Job, event map[string]any, refreshReport, forceWebSearch bool, cause error) error {
	status, summary := "retrying", fmt.Sprintf("%s 标的发现暂时失败，等待重试（%T）。", runtime.cfg.AssistModel, cause)
	if job.Attempt >= job.MaxAttempts {
		status, summary = "failed", fmt.Sprintf("%s 标的发现最终失败（%T）。", runtime.cfg.AssistModel, cause)
	}
	replaceAnalysisStep(event, analysisStep("asset_mapping", status, "ollama+provider-registry", summary, map[string]any{}))
	_ = runtime.saveEvent(ctx, event)
	runtime.updateTrackedTask(ctx, "assist", job.ID.String(), status, job.Attempt, stringValue(event["id"]), stringValue(event["headline"]), stringValue(event["event_type"]), cause.Error(), nil)
	if job.Attempt >= job.MaxAttempts {
		_, _, queueErr := runtime.enqueueResearchAfterMapping(ctx, event, refreshReport, forceWebSearch)
		if queueErr != nil {
			return fmt.Errorf("asset mapping failed: %w; research queue failed: %v", cause, queueErr)
		}
		return nil
	}
	return cause
}

func (runtime *ExtractRuntime) mapEvent(ctx context.Context, event map[string]any, instanceID string) (mappingResult, error) {
	newsItems := make([]newsRecord, 0)
	for _, rawID := range stringSlice(event["news_item_ids"]) {
		newsID, err := uuid.Parse(rawID)
		if err != nil {
			continue
		}
		if item, err := runtime.loadNews(ctx, newsID); err == nil {
			newsItems = append(newsItems, item)
		}
	}
	sourceText := stringValue(event["headline"]) + "\n" + strings.Join(stringSlice(event["entities"]), "\n")
	for _, item := range newsItems {
		sourceText += "\n" + item.Title + "\n" + item.Summary
	}
	assets, err := runtime.loadMappingAssets(ctx)
	if err != nil {
		return mappingResult{}, err
	}
	shortlist := shortlistMappingAssets(sourceText, assets, 30)
	master := productOwnerCandidates(sourceText, assets)
	industries, mentionedIndustries, err := runtime.mappingIndustries(ctx, sourceText)
	if err != nil {
		return mappingResult{}, err
	}
	output, warning, err := runtime.generateMapping(ctx, event, newsItems, shortlist, industries, instanceID)
	if err != nil {
		return mappingResult{}, err
	}
	candidates := map[string]any{}
	for id, candidate := range master {
		candidates[id] = candidate
	}
	rejected := 0
	for _, hint := range output.Candidates {
		validated := validateMappingHint(hint, sourceText, newsItems, shortlist, assets)
		if len(validated) == 0 {
			rejected++
			continue
		}
		for _, candidate := range validated {
			id := stringValue(objectValue(objectValue(candidate)["asset"])["asset_id"])
			previous := objectValue(candidates[id])
			if previous == nil || numberValue(objectValue(candidate)["relevance"]) > numberValue(previous["relevance"]) {
				candidates[id] = candidate
			}
		}
	}
	allowedIndustries := map[string]bool{}
	for _, industry := range industries {
		allowedIndustries[stringValue(industry["industry_id"])] = true
	}
	industryIDs := append([]string{}, mentionedIndustries...)
	for _, id := range output.IndustryIDs {
		if allowedIndustries[id] && !containsString(industryIDs, id) {
			industryIDs = append(industryIDs, id)
		}
	}
	for _, peer := range industryRepresentatives(assets, industryIDs, max(0, 8-len(candidates))) {
		if candidates[peer.ID] == nil {
			candidates[peer.ID] = mappingCandidate(peer, "industry_peer", .4, .55, "新闻涉及该标的所属行业；公司未被新闻直接点名。", []string{"industry_taxonomy", peer.IndustryID})
		}
	}
	ranked := make([]any, 0, len(candidates))
	for _, candidate := range candidates {
		ranked = append(ranked, candidate)
	}
	sort.Slice(ranked, func(i, j int) bool {
		return numberValue(objectValue(ranked[i])["relevance"]) > numberValue(objectValue(ranked[j])["relevance"])
	})
	return mappingResult{Candidates: ranked, IndustryIDs: industryIDs, ProposedCount: len(output.Candidates), MasterDerivedCount: len(master), RejectedCount: rejected, NoAssetReason: ternaryString(len(ranked) == 0, output.NoAssetReason, ""), TechnicalWarning: warning}, nil
}

func (runtime *ExtractRuntime) generateMapping(ctx context.Context, event map[string]any, newsItems []newsRecord, shortlist []mappingAsset, industries []map[string]any, instanceID string) (mappingOutput, string, error) {
	newsPayload := make([]map[string]any, 0, len(newsItems))
	for _, item := range newsItems {
		newsPayload = append(newsPayload, map[string]any{"title": item.Title, "symbols": item.Symbols, "summary": truncateRunes(item.Summary, 2000)})
	}
	assetPayload := make([]map[string]any, 0, len(shortlist))
	for _, asset := range shortlist {
		assetPayload = append(assetPayload, map[string]any{"asset_id": asset.ID, "symbol": asset.Symbol, "name": asset.Name, "aliases": firstStrings(asset.Aliases, 5), "market": asset.Market, "industry_id": asset.IndustryID})
	}
	prompt := "从给定新闻中找出被明确提及、可交易且直接相关的股票或高流动性加密资产。只做名称到证券代码的消歧，不得推荐行业受益股、ETF、指数或新闻未提及的代理标的。source_mention 必须逐字来自新闻；有候选主数据时必须原样返回 asset_id。没有候选时填写 no_asset_reason。新闻只描述行业时填写 industry_ids。\n" +
		"事件：" + jsonString(map[string]any{"headline": event["headline"], "event_type": event["event_type"], "entities": event["entities"]}) + "\n候选证券主数据：" + jsonString(assetPayload) + "\n允许行业：" + jsonString(industries) + "\n新闻：" + truncateRunes(jsonString(newsPayload), 12000)
	hintProperties := map[string]any{
		"asset_id": map[string]any{"type": "string"}, "source_mention": map[string]any{"type": "string"},
		"name": map[string]any{"type": "string"}, "symbol": map[string]any{"type": "string"},
		"market": map[string]any{"type": "string"}, "asset_class": map[string]any{"type": "string"},
		"relationship":   map[string]any{"type": "string", "enum": []string{"direct", "entity", "product_owner"}},
		"confidence":     map[string]any{"type": "number", "minimum": 0, "maximum": 1},
		"rationale":      map[string]any{"type": "string"},
		"search_queries": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
	}
	schema := map[string]any{
		"type": "object", "required": []string{"candidates", "industry_ids", "no_asset_reason"},
		"properties": map[string]any{
			"candidates": map[string]any{"type": "array", "maxItems": 10, "items": map[string]any{
				"type": "object", "required": []string{"source_mention", "name", "relationship", "confidence", "rationale"}, "properties": hintProperties,
			}},
			"industry_ids":    map[string]any{"type": "array", "maxItems": 5, "items": map[string]any{"type": "string"}},
			"no_asset_reason": map[string]any{"type": "string"},
		},
	}
	messages := []map[string]string{{"role": "system", "content": "你是谨慎的跨市场证券主数据映射器。宁可说明没有标的，也不能创造证券或关系。"}, {"role": "user", "content": prompt + "\n只返回符合 format JSON Schema 的 JSON。"}}
	request := map[string]any{"model": runtime.cfg.AssistModel, "messages": messages, "format": schema, "stream": false, "keep_alive": ollamaKeepAliveValue(runtime.cfg.OllamaKeepAlive), "options": map[string]any{"temperature": 0, "num_ctx": runtime.cfg.MappingContextLength, "num_predict": runtime.cfg.MappingMaxOutput, "num_thread": runtime.cfg.OllamaAssistThreads}}
	logicalID := uuid.New()
	var lastErr error
	for attempt := 1; attempt <= 2; attempt++ {
		for index, baseURL := range preferredEndpoints(runtime.cfg.AssistURLs, instanceID, "assist") {
			started := time.Now().UTC()
			body, _ := json.Marshal(request)
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/api/chat", bytes.NewReader(body))
			if err != nil {
				lastErr = err
				continue
			}
			req.Header.Set("Content-Type", "application/json")
			response, err := runtime.client.Do(req)
			if err != nil {
				lastErr = err
				runtime.persistMappingAudit(ctx, logicalID, stringValue(event["id"]), attempt, "failed", started, messages, schema, "", nil, err.Error(), 0, 0, index)
				continue
			}
			payload, readErr := io.ReadAll(io.LimitReader(response.Body, 8<<20))
			_ = response.Body.Close()
			if readErr != nil || response.StatusCode < 200 || response.StatusCode >= 300 {
				lastErr = fmt.Errorf("ollama mapping returned %s", response.Status)
				runtime.persistMappingAudit(ctx, logicalID, stringValue(event["id"]), attempt, "failed", started, messages, schema, string(payload), nil, lastErr.Error(), 0, 0, index)
				continue
			}
			var modelResponse ollamaResponse
			if err := json.Unmarshal(payload, &modelResponse); err != nil {
				lastErr = err
				continue
			}
			var output mappingOutput
			if err := json.Unmarshal([]byte(modelResponse.Message.Content), &output); err != nil || (len(output.Candidates) == 0 && strings.TrimSpace(output.NoAssetReason) == "") {
				if err == nil {
					err = errors.New("no_asset_reason is required when candidates are empty")
				}
				lastErr = err
				runtime.persistMappingAudit(ctx, logicalID, stringValue(event["id"]), attempt, "failed", started, messages, schema, modelResponse.Message.Content, nil, err.Error(), modelResponse.PromptTokens, modelResponse.CompletionTokens, index)
				continue
			}
			runtime.persistMappingAudit(ctx, logicalID, stringValue(event["id"]), attempt, "completed", started, messages, schema, modelResponse.Message.Content, output, "", modelResponse.PromptTokens, modelResponse.CompletionTokens, index)
			return output, "", nil
		}
	}
	if ctx.Err() != nil {
		return mappingOutput{}, "", ctx.Err()
	}
	return mappingOutput{Candidates: []mappingHint{}, IndustryIDs: []string{}, NoAssetReason: "模型未返回合规结果，已使用主数据中的产品归属。"}, fmt.Sprintf("%T: asset mapping validation failed", lastErr), nil
}

func (runtime *ExtractRuntime) persistMappingAudit(ctx context.Context, logicalID uuid.UUID, eventID string, attempt int, status string, started time.Time, messages, schema any, raw string, parsed any, errorValue string, promptTokens, completionTokens, endpoint int) {
	messagesJSON, _ := json.Marshal(messages)
	schemaJSON, _ := json.Marshal(schema)
	parsedJSON, _ := json.Marshal(parsed)
	metrics, _ := json.Marshal(map[string]any{"endpoint": fmt.Sprintf("assist-%d", endpoint), "lane": "assist"})
	var parsedArgument, errorArgument any
	if parsed != nil {
		parsedArgument = parsedJSON
	}
	if errorValue != "" {
		errorArgument = errorValue
	}
	_, _ = runtime.db.Exec(ctx, `INSERT INTO model_call_audits(id,logical_call_id,provider,model,operation,entity_type,entity_id,attempt,status,fidelity,started_at,completed_at,duration_ms,prompt_tokens,completion_tokens,input_language,output_language,messages,schema_payload,raw_response,parsed_response,error,metrics) VALUES($1,$2,'ollama',$3,'asset_mapping','news_event',$4,$5,$6,'exact',$7,$8,$9,$10,$11,'other','other',$12,$13,$14,$15,$16,$17)`, uuid.New(), logicalID, runtime.cfg.AssistModel, eventID, attempt, status, started, time.Now().UTC(), time.Since(started).Milliseconds(), nullableInt(promptTokens), nullableInt(completionTokens), messagesJSON, schemaJSON, raw, parsedArgument, errorArgument, metrics)
}

func (runtime *ExtractRuntime) loadMappingAssets(ctx context.Context) ([]mappingAsset, error) {
	rows, err := runtime.db.Query(ctx, `SELECT id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases::jsonb,products::jsonb,competitors::jsonb,sector_id,industry_id,raw_sector,raw_industry,instrument_type,market_cap,market_cap_rank,last_synced_at,issuer_id,primary_listing_asset_id,lot_size,active FROM assets WHERE active=true`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	assets := make([]mappingAsset, 0)
	for rows.Next() {
		var id, class, market, symbol, name, exchange, currency, sector, industry, rawSector, rawIndustry, instrument string
		var aliasesJSON, productsJSON, competitorsJSON []byte
		var marketCap *float64
		var marketRank *int
		var synced *time.Time
		var issuer, primary *string
		var lot int
		var active bool
		if err := rows.Scan(&id, &class, &market, &symbol, &name, &exchange, &currency, &aliasesJSON, &productsJSON, &competitorsJSON, &sector, &industry, &rawSector, &rawIndustry, &instrument, &marketCap, &marketRank, &synced, &issuer, &primary, &lot, &active); err != nil {
			return nil, err
		}
		aliases, products, competitors := []string{}, []string{}, []string{}
		_ = json.Unmarshal(aliasesJSON, &aliases)
		_ = json.Unmarshal(productsJSON, &products)
		_ = json.Unmarshal(competitorsJSON, &competitors)
		asset := mappingAsset{ID: id, Class: class, Market: market, Symbol: symbol, Name: name, Aliases: aliases, Products: products, IndustryID: industry, Instrument: instrument, IssuerID: pointerString(issuer), PrimaryID: pointerString(primary)}
		if marketCap != nil {
			asset.MarketCap = *marketCap
		}
		asset.Data = map[string]any{"asset_id": id, "asset_class": class, "market": market, "symbol": symbol, "name": name, "exchange_or_provider": exchange, "currency": currency, "aliases": aliases, "products": products, "competitors": competitors, "sector_id": sector, "industry_id": industry, "raw_sector": rawSector, "raw_industry": rawIndustry, "instrument_type": instrument, "market_cap": marketCap, "market_cap_rank": marketRank, "last_synced_at": synced, "issuer_id": issuer, "primary_listing_asset_id": primary, "lot_size": max(1, lot), "active": active}
		assets = append(assets, asset)
	}
	return assets, rows.Err()
}

func shortlistMappingAssets(source string, assets []mappingAsset, limit int) []mappingAsset {
	values := make([]mappingAsset, 0)
	for _, asset := range assets {
		score := 0
		if explicitSymbol(source, asset.Symbol, false) {
			score = 100
		}
		for _, term := range append([]string{asset.Name}, asset.Aliases...) {
			if meaningfulTerm(term) && explicitTerm(source, term) && score < 90 {
				score = 90
			}
		}
		for _, product := range asset.Products {
			if meaningfulProduct(product) && explicitTerm(source, product) && score < 95 {
				score = 95
			}
		}
		if score > 0 {
			asset.ShortlistRank = score
			values = append(values, asset)
		}
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].ShortlistRank != values[j].ShortlistRank {
			return values[i].ShortlistRank > values[j].ShortlistRank
		}
		if values[i].MarketCap != values[j].MarketCap {
			return values[i].MarketCap > values[j].MarketCap
		}
		return values[i].ID < values[j].ID
	})
	if len(values) > limit {
		values = values[:limit]
	}
	return values
}

func productOwnerCandidates(source string, assets []mappingAsset) map[string]any {
	result := map[string]any{}
	for _, owner := range assets {
		for _, product := range owner.Products {
			if !meaningfulProduct(product) || !explicitTerm(source, product) {
				continue
			}
			for _, sibling := range assets {
				if sameMappingIssuer(owner, sibling) {
					result[sibling.ID] = mappingCandidate(sibling, "product_owner", .85, .99, fmt.Sprintf("主数据确认新闻中的 %s 归属于 %s", product, sibling.Name), []string{"source_product", "product_owner_master", product})
				}
			}
		}
	}
	return result
}

func validateMappingHint(hint mappingHint, source string, newsItems []newsRecord, shortlist, assets []mappingAsset) []any {
	if hint.Confidence < .6 || !meaningfulTerm(hint.SourceMention) || !explicitTerm(source, hint.SourceMention) || strings.Contains(strings.ToLower(hint.Name), "etf") || strings.Contains(hint.Name, "基金") {
		return nil
	}
	if hint.Relationship == "product_owner" {
		return mapProductHint(hint, assets)
	}
	resolved := make([]mappingAsset, 0)
	for _, asset := range shortlist {
		matches := hint.AssetID != "" && hint.AssetID == asset.ID
		matches = matches || (hint.Symbol != "" && strings.EqualFold(strings.TrimSpace(hint.Symbol), strings.TrimSpace(asset.Symbol)))
		matches = matches || explicitTerm(hint.SourceMention, asset.Name)
		for _, alias := range asset.Aliases {
			matches = matches || (meaningfulTerm(alias) && explicitTerm(hint.SourceMention, alias))
		}
		if !matches || (hint.Market != "" && hint.Market != asset.Market) || (hint.AssetClass != "" && hint.AssetClass != asset.Class) {
			continue
		}
		if hint.Symbol != "" && !strings.EqualFold(strings.TrimSpace(hint.Symbol), strings.TrimSpace(asset.Symbol)) {
			continue
		}
		sourceSymbol := explicitSymbol(source, asset.Symbol, false)
		for _, news := range newsItems {
			sourceSymbol = sourceSymbol || containsStringFold(news.Symbols, asset.Symbol)
		}
		issuerMention := explicitTerm(hint.SourceMention, asset.Name)
		for _, alias := range asset.Aliases {
			issuerMention = issuerMention || (meaningfulTerm(alias) && explicitTerm(hint.SourceMention, alias))
		}
		if !sourceSymbol && !issuerMention {
			continue
		}
		resolved = append(resolved, asset)
	}
	result := make([]any, 0, len(resolved))
	for _, asset := range resolved {
		relationship, relevance, confidence, basis := hint.Relationship, hint.Confidence, hint.Confidence, []string{"llm_source_mention", "provider_master"}
		if asset.PrimaryID != "" && !explicitSymbol(source, asset.Symbol, false) {
			relationship, relevance, confidence = "cross_listing_issuer", math.Min(hint.Confidence, .55), math.Min(hint.Confidence, .75)
			basis = append(basis, "explicit_primary_listing")
		}
		result = append(result, mappingCandidate(asset, relationship, relevance, confidence, hint.Rationale, basis))
	}
	return result
}

func mapProductHint(hint mappingHint, assets []mappingAsset) []any {
	result := make([]any, 0)
	for _, owner := range assets {
		for _, product := range owner.Products {
			if meaningfulProduct(product) && explicitTerm(hint.SourceMention, product) {
				for _, sibling := range assets {
					if sameMappingIssuer(owner, sibling) && (hint.AssetClass == "" || hint.AssetClass == sibling.Class) {
						result = append(result, mappingCandidate(sibling, "product_owner", hint.Confidence, hint.Confidence, hint.Rationale, []string{"llm_source_mention", "product_owner_master", product}))
					}
				}
			}
		}
	}
	return result
}

func (runtime *ExtractRuntime) mappingIndustries(ctx context.Context, source string) ([]map[string]any, []string, error) {
	rows, err := runtime.db.Query(ctx, `SELECT id,name_zh,name_en,aliases::jsonb FROM industries WHERE active=true AND level=2`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	values, mentioned := make([]map[string]any, 0), make([]string, 0)
	for rows.Next() {
		var id, zh, en string
		var aliasesJSON []byte
		if err := rows.Scan(&id, &zh, &en, &aliasesJSON); err != nil {
			return nil, nil, err
		}
		aliases := []string{}
		_ = json.Unmarshal(aliasesJSON, &aliases)
		values = append(values, map[string]any{"industry_id": id, "name_zh": zh, "name_en": en})
		for _, term := range append([]string{zh, en}, aliases...) {
			if meaningfulTerm(term) && explicitTerm(source, term) && !containsString(mentioned, id) {
				mentioned = append(mentioned, id)
			}
		}
	}
	return values, mentioned, rows.Err()
}

func industryRepresentatives(assets []mappingAsset, industryIDs []string, limit int) []mappingAsset {
	if limit <= 0 {
		return nil
	}
	allowed := map[string]bool{}
	for _, id := range industryIDs {
		if id != "industry:special_purpose" {
			allowed[id] = true
		}
	}
	values := make([]mappingAsset, 0)
	for _, asset := range assets {
		if allowed[asset.IndustryID] && asset.Class == "equity" && asset.Instrument != "shell_company" && (asset.Market == "CN" || asset.Market == "HK" || asset.Market == "US") {
			values = append(values, asset)
		}
	}
	sort.Slice(values, func(i, j int) bool { return values[i].MarketCap > values[j].MarketCap })
	if len(values) > limit {
		values = values[:limit]
	}
	return values
}

func (runtime *ExtractRuntime) enqueueResearchAfterMapping(ctx context.Context, event map[string]any, refresh, forceWebSearch bool) (string, string, error) {
	var runID uuid.UUID
	var body []byte
	err := runtime.db.QueryRow(ctx, `SELECT id,payload::jsonb FROM event_research_runs WHERE event_id=$1`, event["id"]).Scan(&runID, &body)
	if errors.Is(err, pgx.ErrNoRows) {
		queued, err := runtime.enqueueEventResearch(ctx, event)
		if err != nil {
			return "", "", err
		}
		if !queued {
			return "", "", nil
		}
		if err := runtime.db.QueryRow(ctx, `SELECT id,payload::jsonb FROM event_research_runs WHERE event_id=$1`, event["id"]).Scan(&runID, &body); err != nil {
			return "", "", err
		}
		var run map[string]any
		_ = json.Unmarshal(body, &run)
		return stringValue(run["celery_task_id"]), runID.String(), nil
	}
	if err != nil {
		return "", "", err
	}
	var run map[string]any
	if err := json.Unmarshal(body, &run); err != nil {
		return "", "", err
	}
	if !refresh {
		return "", runID.String(), nil
	}
	previousStatus, previousTask := stringValue(run["status"]), stringValue(run["celery_task_id"])
	if report := run["report"]; report != nil {
		history := anySlice(run["report_history"])
		if len(history) == 0 || jsonString(history[len(history)-1]) != jsonString(report) {
			run["report_history"] = append(history, report)
		}
	}
	taskID := uuid.NewString()
	instanceID := runtime.selectDownstreamInstance(ctx, "research", len(runtime.cfg.ResearchURLs))
	run["status"], run["as_of"], run["verification_round"] = "queued", iso(time.Now()), 0
	run["missing_requirements"], run["contradictions"], run["error"], run["retryable_reason"] = []any{}, []any{}, nil, nil
	run["celery_task_id"], run["model_instance_id"], run["updated_at"] = taskID, instanceID, iso(time.Now())
	appendAnalysisStep(run, analysisStep("forced_event_research_queue", "queued", "celery", "已保留当前事件研报，并创建完整事件重新调研任务。", map[string]any{"instance_id": instanceID, "priority": 1, "previous_status": previousStatus, "archived_report_count": len(anySlice(run["report_history"]))}))
	encoded, _ := json.Marshal(run)
	if _, err := runtime.db.Exec(ctx, `UPDATE event_research_runs SET status='queued',payload=$2,updated_at=now() WHERE id=$1`, runID, encoded); err != nil {
		return "", "", err
	}
	kwargs := map[string]any{"model_instance_id": instanceID}
	if forceWebSearch {
		kwargs["force_web_search"] = true
	}
	if err := publishCelery(ctx, runtime.redis, "market_loop.research_event", "research."+instanceID, taskID, []any{stringValue(event["id"]), runID.String()}, kwargs, 1); err != nil {
		run["status"], run["celery_task_id"], run["error"] = previousStatus, previousTask, "event research refresh queue failed"
		appendAnalysisStep(run, analysisStep("forced_event_research_queue", "failed", "celery", "事件重新调研入队失败，已保留原研报。", map[string]any{}))
		failed, _ := json.Marshal(run)
		_, _ = runtime.db.Exec(ctx, `UPDATE event_research_runs SET status=$2,payload=$3,updated_at=now() WHERE id=$1`, runID, previousStatus, failed)
		return "", "", err
	}
	return taskID, runID.String(), nil
}

func mappingCandidate(asset mappingAsset, relationship string, relevance, confidence float64, rationale string, basis []string) map[string]any {
	return map[string]any{"asset": asset.Data, "relationship": relationship, "relevance": relevance, "rationale": rationale, "mapping_confidence": confidence, "identity_basis": basis}
}

func sameMappingIssuer(left, right mappingAsset) bool {
	if left.ID == right.ID {
		return true
	}
	if left.IssuerID != "" && right.IssuerID != "" && strings.EqualFold(left.IssuerID, right.IssuerID) {
		return true
	}
	return left.PrimaryID == right.ID || right.PrimaryID == left.ID || (left.PrimaryID != "" && left.PrimaryID == right.PrimaryID)
}

func countRelationship(values []any, relationship string) int {
	count := 0
	for _, value := range values {
		if stringValue(objectValue(value)["relationship"]) == relationship {
			count++
		}
	}
	return count
}

func pointerString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func firstStrings(values []string, limit int) []string {
	if len(values) <= limit {
		return values
	}
	return values[:limit]
}

func jsonString(value any) string { encoded, _ := json.Marshal(value); return string(encoded) }

func ternaryString(condition bool, yes, no string) string {
	if condition {
		return yes
	}
	return no
}
