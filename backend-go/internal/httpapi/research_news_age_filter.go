package httpapi

import (
	"net/http"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/jobs"
)

type researchNewsAgeFilterInput struct {
	Enabled *bool `json:"enabled"`
}

func (s *Server) researchNewsAgeFilter(w http.ResponseWriter, r *http.Request) {
	filter, err := jobs.LoadResearchNewsAgeFilter(r.Context(), s.db)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "research news age filter query failed")
		return
	}
	writeJSON(w, http.StatusOK, filter)
}

func (s *Server) updateResearchNewsAgeFilter(w http.ResponseWriter, r *http.Request) {
	var input researchNewsAgeFilterInput
	if !decodeJSONBody(w, r, &input) {
		return
	}
	enabled := true
	if input.Enabled != nil {
		enabled = *input.Enabled
	}
	filter, err := jobs.SaveResearchNewsAgeFilter(r.Context(), s.db, enabled)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "research news age filter update failed")
		return
	}
	discarded := 0
	if enabled {
		discarded, err = jobs.FilterExpiredAutomaticResearch(r.Context(), s.db, s.redis)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "expired research cleanup failed")
			return
		}
	}
	_ = s.redis.Del(r.Context(), modelQueueOverviewCacheKey).Err()
	writeJSON(w, http.StatusOK, map[string]any{"enabled": filter.Enabled, "max_age_hours": filter.MaxAgeHours, "discarded": discarded})
}
