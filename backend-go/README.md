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
