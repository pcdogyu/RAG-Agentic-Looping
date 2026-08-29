# Go backend migration

This module is the contract-preserving Go replacement for the public API and
business workers. It currently runs only through the `go-shadow` Compose
profile and is not the web upstream.

## Safety gates

- The frozen contract contains 72 HTTP operations. `/go/migration-status`
  reports native coverage.
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
