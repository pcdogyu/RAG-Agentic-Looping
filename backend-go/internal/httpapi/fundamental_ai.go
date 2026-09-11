package httpapi

import (
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/fundamentalai"
)

const fundamentalAIPrepareTaskType = "market_loop.prepare_fundamental_ai"

type fundamentalAIBatchInput struct {
	AssetIDs []string `json:"asset_ids"`
	Market   string   `json:"market"`
	Scope    string   `json:"scope"`
	Limit    int      `json:"limit"`
}

func (s *Server) startFundamentalAIPreparation(w http.ResponseWriter, r *http.Request) {
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" {
		writeError(w, http.StatusUnprocessableEntity, "Idempotency-Key header is required")
		return
	}
	run, created, err := s.enqueueFundamentalAIRun(r, assetID, nil, key)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"run_id": run.ID, "task_id": run.TaskID, "status": run.Status, "created": created})
}

func (s *Server) latestFundamentalAIPreparation(w http.ResponseWriter, r *http.Request) {
	assetID, err := fundamentalAssetID(chi.URLParam(r, "assetID"))
	if err != nil || assetID == "" {
		writeError(w, http.StatusUnprocessableEntity, "asset_id path is invalid")
		return
	}
	run, sources, candidates, err := fundamentalai.NewStore(s.db).Latest(r.Context(), assetID)
	if errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, http.StatusOK, map[string]any{"asset_id": assetID, "status": "not_run", "sources": []any{}, "candidates": []any{}})
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "AI preparation query failed")
		return
	}
	for index := range sources {
		sources[index].ContentText = ""
	}
	writeJSON(w, http.StatusOK, map[string]any{"run": run, "sources": sources, "candidates": candidates, "search_snippets_are_evidence": false, "automatic_model_release": false})
}

func (s *Server) startFundamentalAIBatch(w http.ResponseWriter, r *http.Request) {
	input := fundamentalAIBatchInput{Market: "US", Scope: "pending_predictions", Limit: 200}
	if !decodeJSONBody(w, r, &input) {
		return
	}
	input.Market = strings.ToUpper(strings.TrimSpace(input.Market))
	input.Scope = strings.ToLower(strings.TrimSpace(input.Scope))
	if input.Market == "" {
		input.Market = "US"
	}
	if input.Scope == "" {
		input.Scope = "pending_predictions"
	}
	if input.Limit == 0 {
		input.Limit = 200
	}
	if input.Market != "US" || input.Scope != "pending_predictions" || input.Limit < 1 || input.Limit > 200 {
		writeError(w, http.StatusUnprocessableEntity, "first release requires market=US, scope=pending_predictions and limit between 1 and 200")
		return
	}
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" {
		writeError(w, http.StatusUnprocessableEntity, "Idempotency-Key header is required")
		return
	}
	assetIDs, err := s.fundamentalAIBatchAssets(r, input)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	batchID := uuid.New()
	taskIDs := make([]uuid.UUID, len(assetIDs))
	for index := range taskIDs {
		taskIDs[index] = uuid.New()
	}
	batch, created, err := fundamentalai.NewStore(s.db).CreateBatch(r.Context(), fundamentalai.Batch{ID: batchID, Market: input.Market, Scope: input.Scope, RequestedCount: len(assetIDs), TaskIDs: taskIDs}, key)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "AI preparation batch could not be created persisted")
		return
	}
	if !created {
		writeJSON(w, http.StatusAccepted, map[string]any{"batch": batch, "created": false})
		return
	}
	queued := 0
	reused := 0
	actualTaskIDs := make([]uuid.UUID, 0, len(assetIDs))
	for index, assetID := range assetIDs {
		run, runCreated, enqueueErr := s.enqueueFundamentalAIRunWithTask(r, assetID, &batch.ID, key+"|"+assetID, taskIDs[index])
		if enqueueErr != nil {
			continue
		}
		actualTaskIDs = append(actualTaskIDs, run.TaskID)
		if runCreated {
			queued++
		} else {
			reused++
		}
	}
	if err := fundamentalai.NewStore(s.db).SetBatchTaskIDs(r.Context(), batch.ID, actualTaskIDs); err != nil {
		writeError(w, http.StatusInternalServerError, "AI preparation batch tasks could not be persisted")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"batch_id": batch.ID, "status": "queued", "requested": len(assetIDs), "queued": queued, "reused": reused, "task_ids": actualTaskIDs})
}

func (s *Server) getFundamentalAIBatch(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(strings.TrimSpace(chi.URLParam(r, "batchID")))
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, "batch_id path is invalid")
		return
	}
	batch, err := fundamentalai.NewStore(s.db).GetBatch(r.Context(), id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "AI preparation batch not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "AI preparation batch query failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"batch": batch})
}

func (s *Server) enqueueFundamentalAIRun(r *http.Request, assetID string, batchID *uuid.UUID, key string) (fundamentalai.Run, bool, error) {
	return s.enqueueFundamentalAIRunWithTask(r, assetID, batchID, key, uuid.New())
}

func (s *Server) enqueueFundamentalAIRunWithTask(r *http.Request, assetID string, batchID *uuid.UUID, key string, taskID uuid.UUID) (fundamentalai.Run, bool, error) {
	var name, symbol, market, assetClass string
	if err := s.db.QueryRow(r.Context(), `SELECT name,symbol,market,asset_class FROM assets WHERE id=$1 AND active=true`, assetID).Scan(&name, &symbol, &market, &assetClass); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fundamentalai.Run{}, false, errors.New("active asset was not found")
		}
		return fundamentalai.Run{}, false, err
	}
	if !strings.EqualFold(assetClass, "equity") || !strings.EqualFold(market, "US") {
		return fundamentalai.Run{}, false, errors.New("first release supports active US equities only")
	}
	runID := uuid.New()
	run, created, err := fundamentalai.NewStore(s.db).CreateRun(r.Context(), fundamentalai.Run{ID: runID, BatchID: batchID, AssetID: assetID, TaskID: taskID, ModelVersion: s.cfg.ResearchModel, AsOf: time.Now().UTC()}, key)
	if err != nil {
		return fundamentalai.Run{}, false, err
	}
	queuedID, err := s.enqueueGoModelJob(r.Context(), "research", run.TaskID.String(), fundamentalAIPrepareTaskType, []any{assetID, run.ID.String()}, map[string]any{"asset_id": assetID, "run_id": run.ID.String()}, 4, "fundamental-ai:"+assetID)
	if err != nil {
		_ = fundamentalai.NewStore(s.db).UpdateRun(r.Context(), run.ID, "failed", "queue_failed", map[string]any{}, []string{"research_queue_unavailable"})
		return fundamentalai.Run{}, false, err
	}
	if queuedID != run.TaskID.String() {
		activeTaskID, parseErr := uuid.Parse(queuedID)
		store := fundamentalai.NewStore(s.db)
		if parseErr == nil {
			_ = store.DeleteEmptyRun(r.Context(), run.ID)
			if active, readErr := store.GetByTaskID(r.Context(), activeTaskID); readErr == nil {
				return active, false, nil
			}
		}
		_ = store.UpdateRun(r.Context(), run.ID, "cancelled", "deduplicated", map[string]any{"active_task_id": queuedID}, []string{"active_asset_preparation_reused"})
		run.Status, run.TaskID = "cancelled", activeTaskID
		return run, false, nil
	}
	s.trackModelTask(r.Context(), "research", run.TaskID.String(), "fundamental_ai_prepare", assetID, name, symbol, "local-search+ollama", "")
	return run, created, nil
}

func (s *Server) fundamentalAIBatchAssets(r *http.Request, input fundamentalAIBatchInput) ([]string, error) {
	if len(input.AssetIDs) > 0 {
		if len(input.AssetIDs) > input.Limit {
			return nil, errors.New("asset_ids exceeds batch limit")
		}
		seen := map[string]bool{}
		values := []string{}
		for _, raw := range input.AssetIDs {
			assetID, err := fundamentalAssetID(raw)
			if err != nil || assetID == "" {
				return nil, errors.New("asset_ids contains an invalid canonical equity ID")
			}
			var valid bool
			if err := s.db.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM assets WHERE id=$1 AND active=true AND asset_class='equity' AND market='US')`, assetID).Scan(&valid); err != nil || !valid {
				return nil, errors.New("asset_ids contains a non-US or inactive equity")
			}
			if !seen[assetID] {
				seen[assetID] = true
				values = append(values, assetID)
			}
		}
		sort.Strings(values)
		return values, nil
	}
	rows, err := s.db.Query(r.Context(), `SELECT DISTINCT p.asset_id FROM prediction_runs p JOIN assets a ON a.id=p.asset_id
		WHERE p.asset_class='equity' AND a.market='US' AND a.active=true AND NOT EXISTS(SELECT 1 FROM outcome_records o WHERE o.prediction_run_id=p.id)
		ORDER BY p.asset_id LIMIT $1`, input.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := []string{}
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}
