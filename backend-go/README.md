# Go backend migration

This module is the contract-preserving Go replacement for the public API and
business workers. It currently runs only through the `go-shadow` Compose
profile and is not the web upstream.

## Migration batches

- Batch 1 audit found that the original implementation owned 18 operations,
  while the frozen contract actually contained 78 rather than the documented
  72. The coverage gate now derives its count from the registered native route
  manifest and verifies every operation ID against the frozen OpenAPI file.
- Batch 2 brought ownership to 52 of 78 operations. It includes durable read APIs, asset master
  data, source/MCP configuration, operational queue views, the news board,
  analysis logs, paper portfolio APIs, and the SSE snapshot stream.
- Batch 3 owns all 78 operations. The remaining command, retry, queue-control,
  admin search, scan/research, and target-change APIs are native Go. Go publishes
  Kombu-compatible Celery messages to the existing Python execution workers, so
  the API cutover does not require an unsafe simultaneous worker rewrite. The
  Python API remains the rollback target until the upstream is switched.
- Batch 4 migrates Python model workers one lane at a time, in the fixed order
  `extract` -> `mapping` -> `research` -> `evolution`. The aliases are part of
  the contract: mapping is Python model lane/Go queue `assist`, while evolution
  is Python model lane/Go queue `code`.
- Batch 5 adds `discovery`: Go now schedules and runs news discovery, persists
  provider watermarks and health, stages news and extraction intents in one
  transaction, and dispatches the durable outbox to the Go extract queue.
- Batch 6 adds `recovery`: Go now schedules and runs orphaned-news/downstream
  recovery, durable research and mapping lease reconciliation, and model-call
  audit retention cleanup. Python Beat keeps only the not-yet-migrated universe,
  outcome, market-factor, evolution-dispatch, and system-monitor schedules.
  `/go/migration-status` exposes the machine-readable lane plan and current
  next lane.

## Safety gates

- The frozen contract contains 78 HTTP operations. `/go/migration-status`
  reports native coverage and the exact native operation IDs.
- `APP_ENV=production` refuses to start until every frozen operation is native.
- `GO_ALLOW_LEGACY_PROXY=true` is accepted only outside production. It lets
  differential tests exercise the whole frontend while endpoints are ported.
- Database migrations are additive so the Python release remains a rollback
  target during the shadow period.

## Local verification

```text
docker compose --profile go-shadow up -d --build go-api
cd backend-go
go test ./...
go run ./cmd/contract-diff
```

The PostgreSQL-backed job integration test runs when `DATABASE_URL` or
`TEST_DATABASE_URL` is present. The shadow image normalizes the existing
SQLAlchemy `postgresql+psycopg://` URL for pgx.

## API cutover and rollback

The Web container reads its API upstream when it starts. The Compose default is
the production-mode Go API; the Python API remains running as the rollback
target. Switch either direction without restarting an API or worker:

```text
./ops/switch-web-api.sh go
./ops/switch-web-api.sh python
```

The script recreates only `web`, then requires `/health` to succeed and checks
the `X-API-Backend` response header. It never starts `go-worker`.

## Worker migration gates

`go-worker` is intentionally fail-closed. It requires one explicit
`GO_WORKER_LANE`, refuses a lane that skips the fixed order, and will not open a
database connection until every task handler for that lane is registered. The
current batch definition therefore cannot consume production work by accident.

Each lane is replaced using the same sequence:

1. Implement every registered task type and golden-test its durable database,
   Redis state, retries, cancellation, and downstream dispatch against Python.
2. Replay copied payloads in isolation; then shadow with duplicated input and
   side effects redirected away from production state.
3. Drain the Python lane, switch only that publisher/consumer pair to Go, and
   set `GO_WORKER_COMPLETED_LANES` to the completed prefix.
4. Verify backlog, failure rate, leases, and output parity before moving to the
   next lane. Roll back by routing that lane to Celery and restarting only its
   Python worker.

The lane task boundary is:

- `extract`: news extraction, event re-extraction, news retry, and extraction
  finalization.
- `mapping`: event-to-asset resolution.
- `research`: event research and asset research.
- `evolution`: outcome evolution, manual evolution, and candidate execution.
- `discovery`: FMP/RSS/MCP/AkShare news scanning and durable extraction-outbox
  dispatch. AkShare stays behind the narrow Python market-adapter HTTP boundary.
- `recovery`: orphaned news and downstream follow-up recovery, research/mapping
  lease reconciliation, and model-call audit retention cleanup.

### Recovery lane cutover

Set
`GO_WORKER_COMPLETED_LANES=extract,mapping,research,evolution,discovery,recovery`,
start `go-recovery-worker`, and recreate `go-scheduler`, the Python `scheduler`,
and `io-worker`. The completed-lane flag removes only the four migrated recovery
schedules from Celery Beat; asset-universe refresh, outcome evaluation,
market-factor refresh, evolution dispatch, and system monitoring remain on
Python until their later batches.

The Go scheduler uses Redis interval leases, so scheduler restarts do not create
duplicate recovery jobs. The worker stages recovered news in the existing
transactional outbox, restores recent events that never received mapping or
research work, and republishes queued research only when no active durable Go
job exists. Roll back by removing `recovery` from the completed prefix, stopping
`go-recovery-worker`, and recreating both schedulers; durable rows and job
history are retained.

### Extract lane cutover

The extract lane has native handlers for all four registered task types. During
cutover, set `GO_WORKER_COMPLETED_LANES=extract`, start `go-worker` (the Compose
service pins this worker to the extract lane), and stop only the Python
`extract-worker` after its Celery queues have drained. Python IO discovery keeps
the transactional news outbox, but publishes new extraction jobs to `go_jobs`;
the Go API does the same for manual news retry and complete event refresh.

The Go worker preserves `news_processing`, event/report payloads, model queue
tracking, retries, cancellation, and model-call audits. Until the mapping and
research lanes migrate, it publishes their work to the instance-specific Celery
queues. Rollback clears `GO_WORKER_COMPLETED_LANES`, stops `go-worker`, and
restarts the Python `extract-worker`; durable failed work remains available to
the existing retry API.

If the legacy Celery lane cannot drain within the maintenance window, stop the
IO/scheduler publishers and the Python extract worker, then preview and apply
the idempotent queue bridge:

```text
python -m backend.app.extract_queue_cutover
python -m backend.app.extract_queue_cutover --apply
```

The bridge accepts only `retry_news_item`, verifies every durable
`news_processing` row, preserves task IDs/priorities, commits every `go_job`
before moving the Redis list, and keeps the original list under
`market-loop:archive:extract-cutover:*` for rollback inspection.

### Mapping lane cutover

The mapping lane owns `market_loop.resolve_event_assets` in Go. It preserves
the 7B structured mapping call, verified product ownership, issuer listing
expansion, strict source-mention checks, industry representatives, event/model
audit state, retries, cancellation, and downstream event-research dispatch.

Set `GO_WORKER_COMPLETED_LANES=extract,mapping`, keep `go-worker` running for
extract, start the independent `go-mapping-worker`, then stop only the Python
`mapping-worker`. Python and Go extraction publishers will route new mapping
work to durable `go_jobs(queue=assist)` while research remains on its existing
instance-specific Celery queues.

For a non-empty legacy mapping queue, freeze publishers and stop the Python
mapping worker before previewing and applying the bridge:

```text
python -m backend.app.mapping_queue_cutover
python -m backend.app.mapping_queue_cutover --apply
```

The bridge accepts only `resolve_event_assets`, validates every event, keeps
task IDs, kwargs, and priorities, persists every Go job first, then archives
the original Redis lists under `market-loop:archive:mapping-cutover:*`.

### Research lane cutover

The research lane owns `market_loop.research_event` and
`market_loop.research_asset` in Go. Event jobs keep priority `1`, asset jobs
keep priority `3`, and the independent `go-research-worker` runs at concurrency
`3`, matching the three configured 7B endpoints. Asset execution uses the
34-minute cooperative limit and a 35-minute durable hard-limit reconciliation;
timeouts are written back as retryable `research_time_limit` failures.

Set `GO_WORKER_COMPLETED_LANES=extract,mapping,research`, keep the extract and
mapping Go workers running, start `go-research-worker`, and stop the three
Python research workers. Python and Go publishers then write all new research
work to `go_jobs(queue=research)`.

For a non-empty legacy research queue, freeze publishers and stop the Python
research workers before previewing and applying the idempotent bridge:

```text
python -m backend.app.research_queue_cutover
python -m backend.app.research_queue_cutover --apply
```

The bridge accepts only the two research task types, validates their durable
run rows, preserves task IDs, kwargs and event-before-asset priorities, then
archives the original Redis lists under
`market-loop:archive:research-cutover:*`.

### Evolution lane cutover

The evolution lane owns `market_loop.evolve_from_outcomes`,
`market_loop.evolve_failures`, and `market_loop.execute_evolution` in Go. The
dedicated `go-evolution-worker` keeps concurrency at the configured code-model
capacity and uses a purpose-built image containing the Go worker plus Git,
Python/Go test toolchains, and Docker Compose. Candidate execution retains the
clean-worktree gate, secret and protected-path checks, full validation suite,
isolated candidate branch, optional merge/deploy verification, and rollback.
The base branch defaults to `golang` through `EVOLUTION_BASE_BRANCH`.

Set `GO_WORKER_COMPLETED_LANES=extract,mapping,research,evolution`, start
`go-evolution-worker`, then stop the Python `evolution-worker`. Periodic IO
dispatch, the Go and Python compatibility APIs, and queue retries will publish
new work to `go_jobs(queue=code)`.

If legacy evolution messages remain after the Python worker is stopped, first
preview and then apply the idempotent bridge:

```text
python -m backend.app.evolution_queue_cutover
python -m backend.app.evolution_queue_cutover --apply
```

The bridge accepts only the three registered evolution task types, validates
candidate IDs for execution jobs, preserves task IDs, kwargs, and priorities,
persists each Go job before archiving the original Redis lists under
`market-loop:archive:evolution-cutover:*`.

### Discovery lane cutover

The discovery lane owns `market_loop.scan_news` and
`market_loop.dispatch_news_processing_outbox`. It preserves the Redis singleton
scan gate and pause state, per-source overlap watermarks, source filter log,
provider health, transactional `news_processing`/outbox staging, deterministic
extraction IDs, and retry backoff. Database-backed FMP, China-news, RSS, search,
and MCP source settings are reloaded for every scan.

Set
`GO_WORKER_COMPLETED_LANES=extract,mapping,research,evolution,discovery`, start
`market-adapter` and `go-discovery-worker`, and recreate `go-scheduler` and the
Python `scheduler`/`io-worker`. The completed-lane flag disables the Python
scan beat and Python recovery outbox publisher; unrelated Python IO tasks remain
available. An already queued legacy scan may finish once, while the shared scan
gate prevents overlapping Go work. Rollback removes only `discovery` from the
completed prefix, stops `go-discovery-worker`, and recreates the two Python
services so Celery resumes scheduling and outbox dispatch.
