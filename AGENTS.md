# Project Delivery Memory

## Version delivery policy

- The canonical delivery branch is `golang` on the `origin` remote (`pcdogyu/RAG-Agentic-Looping`).
- After every requested version change, run the relevant automated checks, commit the task-specific changes, and push the resulting commit to `origin/golang` without waiting for a separate push request.
- Never stage or commit unrelated pre-existing working-tree changes. In particular, the current deletion of `README.md` is user-owned and must remain unstaged unless the user explicitly asks to include it.
- After a successful push, trigger the repository's configured remote deployment/upgrade workflow and verify both the API health endpoint and the web application before reporting completion.
- If deployment credentials or a remote deployment workflow are not configured, report that deployment is blocked; never claim that a server was upgraded when only GitHub CI ran.
- Keep credentials in GitHub Actions secrets or the server environment. Never write tokens, SSH keys, passwords, or production `.env` contents into tracked files or logs.

## Required delivery checks

- Backend: run Ruff and the relevant pytest suite; use the full suite for cross-cutting changes.
- Frontend: run Vitest and a production TypeScript/Vite build when frontend code changes.
- Deployment: confirm the target containers/services are healthy, `GET /health` succeeds, and the web root returns HTTP 200.
