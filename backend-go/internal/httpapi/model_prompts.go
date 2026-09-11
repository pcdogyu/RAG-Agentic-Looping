package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/jobs"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/modelprompt"
)

type modelPromptUpdateRequest struct {
	Prompt          string `json:"prompt"`
	ExpectedVersion int64  `json:"expected_version"`
}

type modelPromptResetRequest struct {
	ExpectedVersion int64 `json:"expected_version"`
}

func (s *Server) modelPrompts(w http.ResponseWriter, r *http.Request) {
	store := modelprompt.NewStore(s.db)
	items := make([]modelprompt.Value, 0, len(jobs.ModelPromptDefinitions()))
	for _, definition := range jobs.ModelPromptDefinitions() {
		value, err := store.Get(r.Context(), definition)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "model prompts query failed")
			return
		}
		items = append(items, value)
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "max_prompt_bytes": modelprompt.MaxPromptBytes})
}

func (s *Server) updateModelPrompt(w http.ResponseWriter, r *http.Request) {
	definition, ok := jobs.ModelPromptDefinition(chi.URLParam(r, "promptKey"))
	if !ok {
		writeError(w, http.StatusNotFound, "model prompt not found")
		return
	}
	var request modelPromptUpdateRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, modelprompt.MaxPromptBytes+4096))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusUnprocessableEntity, "invalid model prompt payload")
		return
	}
	value, err := modelprompt.NewStore(s.db).Update(r.Context(), definition, request.Prompt, "ui:model-prompts", request.ExpectedVersion)
	writeModelPromptResult(w, value, err)
}

func (s *Server) resetModelPrompt(w http.ResponseWriter, r *http.Request) {
	definition, ok := jobs.ModelPromptDefinition(chi.URLParam(r, "promptKey"))
	if !ok {
		writeError(w, http.StatusNotFound, "model prompt not found")
		return
	}
	var request modelPromptResetRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, 4096))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusUnprocessableEntity, "invalid model prompt payload")
		return
	}
	value, err := modelprompt.NewStore(s.db).Reset(r.Context(), definition, "ui:model-prompts", request.ExpectedVersion)
	writeModelPromptResult(w, value, err)
}

func writeModelPromptResult(w http.ResponseWriter, value modelprompt.Value, err error) {
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, value)
	case errors.Is(err, modelprompt.ErrVersionConflict):
		writeError(w, http.StatusConflict, "model prompt was updated by another request; reload and retry")
	case errors.Is(err, modelprompt.ErrInvalidPrompt):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "model prompt update failed")
	}
}
