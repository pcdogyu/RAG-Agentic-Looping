# Python-to-Go database schema audit

Audit baseline: commit `a255f08`, the last revision before first-party Python
runtime removal. The executable contract is
`backend-go/internal/migrate/migrate_integration_test.go`; this document records
the human-readable inventory and the migration decision.

## First-party SQLAlchemy inventory

SQLAlchemy declared 20 tables. It declared no foreign-key or check
constraints. Every table has the primary key shown below. `UNIQUE` entries are
the only additional constraints; PostgreSQL creates a backing unique index for
each one.

| Table | Primary key | Additional unique key | SQLAlchemy indexes |
| --- | --- | --- | --- |
| `assets` | `id` | — | `asset_class`, `market`, `symbol`, `name`, `sector_id`, `industry_id`, `instrument_type`, `market_cap`, `market_cap_rank`, `association_tier`, `last_synced_at`, `manual_industry_id`, `manual_association_tier` |
| `industries` | `id` | — | `parent_id`, `level`, `name_zh`, `name_en` |
| `asset_universe_sync` | `market` | — | `status` |
| `news_items` | `id` | `content_hash` | `source`, `published_at`, `as_of`, `content_hash` |
| `news_source_states` | `source` | — | `status`, `watermark_at`, `last_attempt_at`, `last_success_at`, `updated_at` |
| `news_processing` | `news_id` | — | `status`, `scan_task_id`, `celery_task_id`, `heartbeat_at`, `updated_at` |
| `news_processing_outbox` | `id` | `news_id` | `news_id`, `status`, `available_at`, `updated_at` |
| `news_filter_logs` | `id` | `content_hash` | `content_hash`, `source`, `matched_keyword`, `published_at`, `last_filtered_at` |
| `news_events` | `id` | — | `event_type`, `priority`, `published_at`, `as_of` |
| `research_runs` | `id` | — | `event_id`, `asset_id`, `status` |
| `event_research_runs` | `id` | `event_id` | `event_id`, `status` |
| `evidence` | `id` | — | `run_id`, `source_quality`, `published_at`, `observed_at`, `as_of` |
| `document_chunks` | `id` | `evidence_id` | `evidence_id`, `run_id`, `asset_id`, `source_quality`, `published_at`, `observed_at`, `as_of` |
| `recommendations` | `id` | `run_id` | `run_id`, `asset_id`, `rating`, `as_of` |
| `paper_orders` | `id` | — | `recommendation_id`, `asset_id` |
| `outcomes` | `id` | — | `recommendation_id`, `horizon_days` |
| `evolution_candidates` | `id` | `branch` | — |
| `model_call_audits` | `id` | `source_key` | `logical_call_id`, `provider`, `model`, `operation`, `entity_type`, `entity_id`, `status`, `fidelity`, `started_at`, `completed_at`, `input_language`, `output_language` |
| `mcp_sources` | `id` | `name` | `name`, `priority`, `enabled` |
| `integration_settings` | `key` | — | — |

SQLAlchemy's conventional explicit index names are `ix_<table>_<column>`.
The Go integration contract asserts every one of these names, primary keys,
unique keys, exact column order, and `document_chunks.embedding` as
`vector(384)`.

## Python dependency and deployed compatibility tables

The removed research runtime called `langgraph-checkpoint-postgres` setup code.
That dependency created four additional tables:

| Table | Primary key | Other constraints | Indexes |
| --- | --- | --- | --- |
| `checkpoint_migrations` | `v` | — | primary-key index |
| `checkpoints` | `thread_id, checkpoint_ns, checkpoint_id` | — | `checkpoints_thread_id_idx` |
| `checkpoint_blobs` | `thread_id, checkpoint_ns, channel, version` | — | `checkpoint_blobs_thread_id_idx` |
| `checkpoint_writes` | `thread_id, checkpoint_ns, checkpoint_id, task_id, idx` | — | `checkpoint_writes_thread_id_idx` |

The deployed development database also contained `asset_relationships`. It was
not declared in the first-party SQLAlchemy file at the audit baseline, so it is
classified as a deployed compatibility table rather than an ORM table. Its
primary key is `(source_asset_id, target_asset_id, relationship_type)` and its
indexes are:

- `ix_asset_relationships_source_asset_id`
- `ix_asset_relationships_target_asset_id`
- `ix_asset_relationships_relationship_type`
- `ix_asset_relationships_active`

Go no longer writes the five compatibility tables, but migrations own their
observed schema so that restoring or upgrading an old database never requires
Python startup DDL.

## Go-owned runtime tables

The Go job migration adds four tables:

| Table | Primary/unique constraints | Foreign/check constraints | Indexes |
| --- | --- | --- | --- |
| `go_jobs` | PK `id`; partial unique `(queue, dedupe_key)` for active jobs | `parent_job_id -> go_jobs(id) ON DELETE SET NULL`; priority 0–9; `max_attempts > 0` | claim, lease, recent-execution indexes |
| `go_job_dependencies` | PK `(job_id, depends_on_job_id)` | both columns reference `go_jobs(id) ON DELETE CASCADE`; jobs must differ | primary-key index |
| `go_worker_instances` | PK `id` | — | primary-key index |
| `go_workflow_checkpoints` | PK `(workflow_id, sequence)` | — | primary-key index |

## Comparison and remediation

Before this audit, `00000_core_schema.sql` owned 19 of the 20 first-party ORM
tables. `infra/postgres/init.sql` created only the `vector` and `pg_trgm`
extensions, which meant Go still relied on container initialization for vector
support.

| Gap | Resolution |
| --- | --- |
| `document_chunks` absent from Go migrations | Added idempotent table DDL, seven indexes, unique `evidence_id`, and `vector(384)` contract |
| `vector` extension only in `infra/postgres` | Go migration now runs `CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public` |
| Legacy `assets` tables were not upgraded | Added `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` for every identity/taxonomy/manual override field |
| Nine later `assets` indexes absent | Added idempotent index DDL |
| LangGraph checkpoint tables depended on Python package setup | Added their observed table, default, primary-key and index definitions |
| Deployed `asset_relationships` had no current owner | Added compatibility DDL matching the observed schema |
| No automatic schema parity gate | Added fresh-schema, legacy-upgrade, idempotence, key/index/type, and restored-data fingerprint tests |

All DDL in `00004_legacy_orm_parity.sql` is additive and idempotent. It does not
drop, truncate, rename, or rebuild any legacy table.

## Restore verification

The running local development database was 191 MB and contained 36,822 assets,
3,577 news items, 2,266 events, 5,818 evidence rows, 203 document chunks, 503
research runs, 1,650 event research runs, and existing checkpoint data. It was
dumped and restored into a disposable database. The migration was executed
twice against that restore. For 24 legacy business, evidence, integration, and
checkpoint tables, row counts and content fingerprints were identical before
and after migration.

A temporary Go API instance connected only to the restored copy. `/health`,
`/api/v1/assets`, `/api/v1/news`, `/api/v1/events`, `/api/v1/research-runs`, and
`/api/v1/event-research-runs` all returned HTTP 200 and existing rows. The
temporary API, restored database, and dump were then removed.

This is evidence for the local deployed-data shape, not a production-copy
certification. No production snapshot, production database credential, or
configured remote deployment workflow is present in this workspace. The same
opt-in restored-data test must be run with `TEST_EXISTING_DATABASE_URL` against
a disposable production restore before Python database code can be considered
safe to delete under the production gate.
