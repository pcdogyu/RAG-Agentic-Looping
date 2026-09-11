package modelprompt

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const MaxPromptBytes = 32768

var (
	ErrInvalidPrompt   = errors.New("invalid model prompt")
	ErrVersionConflict = errors.New("model prompt version conflict")
)

type Definition struct {
	Key            string `json:"key"`
	Name           string `json:"name"`
	Description    string `json:"description"`
	ModelRole      string `json:"model_role"`
	DefaultPrompt  string `json:"default_prompt"`
	RequiredSuffix string `json:"required_suffix"`
}

type Value struct {
	Definition
	CustomPrompt    *string    `json:"custom_prompt"`
	EffectivePrompt string     `json:"effective_prompt"`
	HasOverride     bool       `json:"has_override"`
	Version         int64      `json:"version"`
	UpdatedBy       string     `json:"updated_by,omitempty"`
	UpdatedAt       *time.Time `json:"updated_at,omitempty"`
}

type Store struct{ db *pgxpool.Pool }

func NewStore(db *pgxpool.Pool) *Store { return &Store{db: db} }

func Compose(custom *string, definition Definition) string {
	prompt := definition.DefaultPrompt
	if custom != nil && strings.TrimSpace(*custom) != "" {
		prompt = strings.TrimSpace(*custom)
		if suffix := strings.TrimSpace(definition.RequiredSuffix); suffix != "" {
			prompt += "\n\n[系统固定安全边界]\n" + suffix
		}
	}
	return prompt
}

func (s *Store) Get(ctx context.Context, definition Definition) (Value, error) {
	value := Value{Definition: definition, EffectivePrompt: definition.DefaultPrompt}
	if s == nil || s.db == nil {
		return value, nil
	}
	var prompt *string
	var updatedAt time.Time
	err := s.db.QueryRow(ctx, `SELECT prompt,version,updated_by,updated_at FROM model_prompt_overrides WHERE prompt_key=$1`, definition.Key).Scan(&prompt, &value.Version, &value.UpdatedBy, &updatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return value, nil
	}
	if err != nil {
		return Value{}, fmt.Errorf("load model prompt %s: %w", definition.Key, err)
	}
	value.CustomPrompt = prompt
	value.HasOverride = prompt != nil
	value.UpdatedAt = &updatedAt
	value.EffectivePrompt = Compose(prompt, definition)
	return value, nil
}

func (s *Store) Resolve(ctx context.Context, definition Definition) (string, int64, error) {
	value, err := s.Get(ctx, definition)
	return value.EffectivePrompt, value.Version, err
}

func (s *Store) Update(ctx context.Context, definition Definition, prompt, actor string, expectedVersion int64) (Value, error) {
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return Value{}, fmt.Errorf("%w: prompt must not be empty", ErrInvalidPrompt)
	}
	if len([]byte(prompt)) > MaxPromptBytes {
		return Value{}, fmt.Errorf("%w: prompt exceeds %d bytes", ErrInvalidPrompt, MaxPromptBytes)
	}
	return s.write(ctx, definition, &prompt, actor, expectedVersion, "updated")
}

func (s *Store) Reset(ctx context.Context, definition Definition, actor string, expectedVersion int64) (Value, error) {
	return s.write(ctx, definition, nil, actor, expectedVersion, "reset")
}

func (s *Store) write(ctx context.Context, definition Definition, prompt *string, actor string, expectedVersion int64, action string) (Value, error) {
	if s == nil || s.db == nil {
		return Value{}, errors.New("model prompt store is unavailable")
	}
	actor = strings.TrimSpace(actor)
	if actor == "" {
		actor = "ui:model-prompts"
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return Value{}, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, definition.Key); err != nil {
		return Value{}, err
	}
	var current int64
	err = tx.QueryRow(ctx, `SELECT version FROM model_prompt_overrides WHERE prompt_key=$1 FOR UPDATE`, definition.Key).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		current = 0
	} else if err != nil {
		return Value{}, err
	}
	if current != expectedVersion {
		return Value{}, ErrVersionConflict
	}
	next := current + 1
	_, err = tx.Exec(ctx, `INSERT INTO model_prompt_overrides(prompt_key,prompt,version,updated_by,updated_at)
		VALUES($1,$2,$3,$4,now()) ON CONFLICT(prompt_key) DO UPDATE SET prompt=EXCLUDED.prompt,version=EXCLUDED.version,updated_by=EXCLUDED.updated_by,updated_at=EXCLUDED.updated_at`, definition.Key, prompt, next, actor)
	if err != nil {
		return Value{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO model_prompt_revisions(id,prompt_key,version,prompt,action,updated_by) VALUES($1,$2,$3,$4,$5,$6)`, uuid.New(), definition.Key, next, prompt, action, actor)
	if err != nil {
		return Value{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Value{}, err
	}
	return s.Get(ctx, definition)
}
