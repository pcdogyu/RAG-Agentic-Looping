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
	if input.Enabled != nil && !*input.Enabled {
		writeError(w, http.StatusConflict, "48-hour news expiry is mandatory for all news queues")
		return
	}
	filter := jobs.DefaultResearchNewsAgeFilter()
	var discarded int64
	for {
		count, err := jobs.DiscardExpiredNewsJobs(r.Context(), s.db, 500)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "expired news cleanup failed")
			return
		}
		discarded += count
		if count < 500 {
			break
		}
	}
	if _, err := jobs.DiscardExpiredNewsOutbox(r.Context(), s.db); err != nil {
		writeError(w, http.StatusInternalServerError, "expired news outbox cleanup failed")
		return
	}
	_ = s.redis.Del(r.Context(), modelQueueOverviewCacheKey).Err()
	writeJSON(w, http.StatusOK, map[string]any{"enabled": filter.Enabled, "max_age_hours": filter.MaxAgeHours, "discarded": discarded})
}
