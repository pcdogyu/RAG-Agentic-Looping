# Go Runtime

`backend-go` 是系统唯一的第一方后端运行时。

## 命令

| 入口 | 用途 |
| --- | --- |
| `cmd/api` | REST/SSE API 和数据库迁移 |
| `cmd/worker` | 按 `GO_WORKER_LANE` 消费持久任务 |
| `cmd/scheduler` | 周期调度 |
| `cmd/maintenance` | 维护工具 |
| `cmd/market-adapter` | A/H 股市场适配器 |
| `cmd/search-mcp` | SearXNG Streamable HTTP MCP 网关 |
| `cmd/evaluation` | 网络隔离的冻结评估门禁 |
| `cmd/freeze-contract` | API 契约冻结工具 |

## 数据库

`internal/migrate/migrations/00000_core_schema.sql` 定义业务基线 schema，后续迁移定义 Go 队列和执行指标。迁移使用幂等 SQL，每个 API、worker 和 scheduler 进程启动时都会安全执行。

数据库 URL 使用标准 pgx 形式：

```text
postgresql://user:password@host:5432/database
```

## Worker

worker lane 与队列一一对应：`extract`、`mapping`、`research`、`evolution`、`discovery`、`recovery`、`outcomes`、`masterdata`、`operations`、`backfill` 和 `maintenance`。

任务使用 PostgreSQL `FOR UPDATE SKIP LOCKED` 领取，支持租约、心跳、重试、取消、依赖和执行指标。Redis 用于跨实例协调和实时状态。

## Search MCP

`internal/searchmcp` 实现 MCP `initialize`、`ping`、`tools/list` 和 `tools/call`，公开 `web_search` 工具。它只返回带有效 HTTP(S) 原始链接的结果，并限制查询和响应大小。

## 检查

```powershell
gofmt -l .
go vet ./...
go test ./...
go run ./cmd/evaluation fixed-evidence --root ..
```

涉及并发或持久队列的变更还应运行 `go test -race ./...` 和相关 PostgreSQL 集成测试。
