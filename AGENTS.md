# Project Delivery Memory

## Version delivery policy

- The canonical delivery branch is `golang` on the `origin` remote (`pcdogyu/RAG-Agentic-Looping`).
- After every requested version change, run the relevant automated checks, commit the task-specific changes, and push the resulting commit to `origin/golang` without waiting for a separate push request.
- Never stage or commit unrelated pre-existing working-tree changes. In particular, the current deletion of `README.md` is user-owned and must remain unstaged unless the user explicitly asks to include it.
- After a successful push, trigger the repository's configured remote deployment/upgrade workflow and verify both the API health endpoint and the web application before reporting completion.
- If deployment credentials or a remote deployment workflow are not configured, report that deployment is blocked; never claim that a server was upgraded when only GitHub CI ran.
- Keep credentials in GitHub Actions secrets or the server environment. Never write tokens, SSH keys, passwords, or production `.env` contents into tracked files or logs.

## Required delivery checks

- Go backend (`backend-go/**`): require clean `gofmt` output, `go vet ./...`, and the relevant Go tests; use `go test ./...` for cross-cutting Go changes and `go test -race ./...` in CI.
- Go persistence (`backend-go/internal/jobs/**` or `backend-go/internal/migrate/**`): additionally run the relevant PostgreSQL integration tests.
- The first-party runtime is Go-only. Reintroducing Python source, Python package manifests, or Python-based first-party images requires an explicit architecture decision from the user.
- Frontend (`frontend/**`): run Vitest and a production TypeScript/Vite build.
- Compose, Dockerfile, workflow, and documentation-only changes: validate the affected configuration; do not run unrelated language test suites.
- Deployment: confirm the target containers/services are healthy, `GET /health` succeeds, and the web root returns HTTP 200.
