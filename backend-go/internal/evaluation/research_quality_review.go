package evaluation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ResearchQualityReviewInput struct {
	ResearchRunID       string `json:"research_run_id"`
	FactCorrect         *bool  `json:"fact_correct"`
	RelationshipCorrect *bool  `json:"relationship_correct"`
	CitationSupported   *bool  `json:"citation_supported"`
	RefusalAppropriate  *bool  `json:"refusal_appropriate"`
	Reviewer            string `json:"reviewer"`
	Note                string `json:"note"`
	IdempotencyKey      string `json:"-"`
}

type ResearchQualityReview struct {
	ID                  string    `json:"id"`
	ResearchRunID       string    `json:"research_run_id"`
	FactCorrect         *bool     `json:"fact_correct"`
	RelationshipCorrect *bool     `json:"relationship_correct"`
	CitationSupported   *bool     `json:"citation_supported"`
	RefusalAppropriate  *bool     `json:"refusal_appropriate"`
	Reviewer            string    `json:"reviewer"`
	Note                string    `json:"note"`
	CreatedAt           time.Time `json:"created_at"`
}

type ResearchQualityReviewStore struct{ db *pgxpool.Pool }

func NewResearchQualityReviewStore(db *pgxpool.Pool) *ResearchQualityReviewStore {
	return &ResearchQualityReviewStore{db: db}
}

func (s *ResearchQualityReviewStore) Create(ctx context.Context, input ResearchQualityReviewInput, now time.Time) (ResearchQualityReview, bool, error) {
	input.ResearchRunID, input.Reviewer, input.Note, input.IdempotencyKey = strings.TrimSpace(input.ResearchRunID), strings.TrimSpace(input.Reviewer), strings.TrimSpace(input.Note), strings.TrimSpace(input.IdempotencyKey)
	if s.db == nil || input.ResearchRunID == "" || input.Reviewer == "" || input.IdempotencyKey == "" {
		return ResearchQualityReview{}, false, fmt.Errorf("research_run_id, reviewer and idempotency key are required")
	}
	if input.FactCorrect == nil && input.RelationshipCorrect == nil && input.CitationSupported == nil && input.RefusalAppropriate == nil {
		return ResearchQualityReview{}, false, fmt.Errorf("at least one dimension-specific review label is required")
	}
	if err := s.db.QueryRow(ctx, `SELECT id FROM research_runs WHERE id=$1`, input.ResearchRunID).Scan(&input.ResearchRunID); err != nil {
		return ResearchQualityReview{}, false, fmt.Errorf("load research run: %w", err)
	}
	sum := sha256.Sum256([]byte(input.IdempotencyKey))
	item := ResearchQualityReview{ID: "research-review-" + hex.EncodeToString(sum[:])[:32], ResearchRunID: input.ResearchRunID,
		FactCorrect: input.FactCorrect, RelationshipCorrect: input.RelationshipCorrect, CitationSupported: input.CitationSupported,
		RefusalAppropriate: input.RefusalAppropriate, Reviewer: input.Reviewer, Note: input.Note, CreatedAt: now.UTC()}
	tag, err := s.db.Exec(ctx, `INSERT INTO research_quality_reviews(id,research_run_id,fact_correct,relationship_correct,citation_supported,
		refusal_appropriate,reviewer,note,idempotency_key,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(idempotency_key) DO NOTHING`,
		item.ID, item.ResearchRunID, item.FactCorrect, item.RelationshipCorrect, item.CitationSupported, item.RefusalAppropriate,
		item.Reviewer, item.Note, input.IdempotencyKey, item.CreatedAt)
	if err != nil {
		return item, false, err
	}
	if tag.RowsAffected() == 1 {
		return item, true, nil
	}
	existing, err := s.getByIdempotencyKey(ctx, input.IdempotencyKey)
	if err != nil {
		return item, false, err
	}
	if existing.ResearchRunID != item.ResearchRunID || boolPointerValue(existing.FactCorrect) != boolPointerValue(item.FactCorrect) ||
		boolPointerValue(existing.RelationshipCorrect) != boolPointerValue(item.RelationshipCorrect) ||
		boolPointerValue(existing.CitationSupported) != boolPointerValue(item.CitationSupported) ||
		boolPointerValue(existing.RefusalAppropriate) != boolPointerValue(item.RefusalAppropriate) || existing.Reviewer != item.Reviewer || existing.Note != item.Note {
		return item, false, fmt.Errorf("idempotency key is already bound to a different research quality review")
	}
	return existing, false, nil
}

func (s *ResearchQualityReviewStore) List(ctx context.Context, researchRunID string, limit int) ([]ResearchQualityReview, error) {
	if s.db == nil || limit < 1 || limit > 200 {
		return nil, fmt.Errorf("invalid research quality review query")
	}
	rows, err := s.db.Query(ctx, `SELECT id,research_run_id,fact_correct,relationship_correct,citation_supported,refusal_appropriate,reviewer,note,created_at
		FROM research_quality_reviews WHERE ($1='' OR research_run_id=$1) ORDER BY created_at DESC,id LIMIT $2`, strings.TrimSpace(researchRunID), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []ResearchQualityReview{}
	for rows.Next() {
		item, scanErr := scanResearchQualityReview(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *ResearchQualityReviewStore) getByIdempotencyKey(ctx context.Context, key string) (ResearchQualityReview, error) {
	return scanResearchQualityReview(s.db.QueryRow(ctx, `SELECT id,research_run_id,fact_correct,relationship_correct,citation_supported,refusal_appropriate,reviewer,note,created_at
		FROM research_quality_reviews WHERE idempotency_key=$1`, key))
}

func scanResearchQualityReview(row rowScanner) (ResearchQualityReview, error) {
	var item ResearchQualityReview
	err := row.Scan(&item.ID, &item.ResearchRunID, &item.FactCorrect, &item.RelationshipCorrect, &item.CitationSupported,
		&item.RefusalAppropriate, &item.Reviewer, &item.Note, &item.CreatedAt)
	return item, err
}

func boolPointerValue(value *bool) string {
	if value == nil {
		return "null"
	}
	if *value {
		return "true"
	}
	return "false"
}
