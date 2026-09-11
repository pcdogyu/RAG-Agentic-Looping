CREATE TABLE IF NOT EXISTS model_prompt_overrides (
    prompt_key varchar(80) PRIMARY KEY,
    prompt text,
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    updated_by varchar(160) NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (prompt IS NULL OR (length(btrim(prompt)) > 0 AND octet_length(prompt) <= 32768))
);

CREATE TABLE IF NOT EXISTS model_prompt_revisions (
    id uuid PRIMARY KEY,
    prompt_key varchar(80) NOT NULL,
    version bigint NOT NULL CHECK (version > 0),
    prompt text,
    action varchar(16) NOT NULL CHECK (action IN ('updated', 'reset')),
    updated_by varchar(160) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (prompt_key, version)
);

CREATE INDEX IF NOT EXISTS ix_model_prompt_revisions_key_created
    ON model_prompt_revisions(prompt_key, created_at DESC);
