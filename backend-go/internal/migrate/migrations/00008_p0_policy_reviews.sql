-- One immutable human decision per shadow evaluation.  Reviews are separated
-- from the approval record so the 100-impact gate is independently auditable.
CREATE TABLE IF NOT EXISTS policy_impact_reviews (
    policy_evaluation_id varchar(36) PRIMARY KEY,
    reviewer varchar(160) NOT NULL,
    decision varchar(16) NOT NULL CHECK (decision IN ('accepted','rejected')),
    note text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_policy_impact_reviews_decision ON policy_impact_reviews(decision,created_at DESC);
