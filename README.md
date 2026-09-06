# RAG Agentic Looping

面向跨市场新闻的证据优先研究系统。第一方 API、调度器、任务 worker、市场适配器、搜索 MCP 和离线评估门禁均使用 Go；Web 使用 React/Vite。

## 运行架构

| 服务 | 实现 | 作用 |
| --- | --- | --- |
| `go-api` | Go | REST、SSE、健康检查、任务与数据源管理 |
| `go-scheduler` | Go | 新闻发现、维护、结果评估和演进调度 |
| `go-*-worker` | Go | 抽取、映射、研究、发现、恢复、结果、主数据、演进等队列 |
| `market-adapter` | Go | A/H 股行情、资产主数据和新闻适配 |
| `search-mcp` | Go | 将 SearXNG 暴露为 Streamable HTTP MCP 工具 |
| `web` | React + Nginx | 管理和研究界面 |
| `postgres` | PostgreSQL/pgvector | 业务数据和持久任务状态 |
| `redis` | Redis | 调度锁、缓存和实时协调 |
| `searxng` | 第三方镜像 | 聚合网页搜索上游 |
| `fmp-mcp` | 第三方 Node.js 服务，可选 | FMP MCP 工具集 |

仓库不再需要第一方 Python 运行环境，也不包含 FastAPI、Celery、pytest 或 Python 包清单。

## 快速启动

1. 从示例创建本地环境文件：

   ```powershell
   Copy-Item .env.example .env
   ```

2. 根据机器上的 Ollama 实例修改 `.env`。默认模型分工：

   - 抽取：`qwen2.5:3b`
   - 映射：`qwen2.5:7b`
   - 研究：`qwen2.5:7b`
   - 代码演进：`qwen2.5-coder:7b`

3. 构建并启动完整 Go 运行环境：

   ```powershell
   docker compose --profile go-shadow up -d --build
   ```

4. 验证：

   ```powershell
   Invoke-RestMethod http://127.0.0.1:8081/health
   Invoke-WebRequest -UseBasicParsing http://127.0.0.1/
   docker compose --profile go-shadow ps
   ```

Web 默认位于 <http://127.0.0.1/>，Go API 默认位于 <http://127.0.0.1:8081/>。

## 核心目录

```text
backend-go/
  cmd/                  API、worker、scheduler、适配器和离线工具入口
  internal/httpapi/     REST/SSE API
  internal/jobs/        持久队列和业务任务
  internal/migrate/     Go 管理的完整数据库 schema
  internal/searchmcp/   SearXNG MCP 网关
frontend/               React/Vite Web
infra/                  PostgreSQL、SearXNG、Ollama 与第三方 MCP 配置
evals/                  冻结评估数据和基线
```

## Go 队列

`GO_WORKER_LANE` 决定 worker 消费的任务域：

- `extract`
- `mapping`
- `research`
- `evolution`
- `discovery`
- `recovery`
- `outcomes`
- `masterdata`
- `operations`
- `backfill`
- `maintenance`

所有任务状态均存储在 PostgreSQL 的 `go_jobs` 及相关表中。队列卡片展示排队、运行、重试、完成/失败和最近执行耗时。

## 搜索与 MCP

内置 `search-mcp` 通过 `http://search-mcp:8080/mcp` 提供 `web_search` 工具，并转发到 SearXNG。Go API 也支持在“数据源”页面添加其他 Streamable HTTP MCP 来源。

FMP MCP 是可选服务：

```powershell
docker compose --profile mcp up -d --build fmp-mcp
```

FMP MCP 不可用时，Go provider 会按配置使用 Stable REST 接口。

## 代码演进

演进 worker 使用纯 Go 镜像，包含 Go、Git、Docker CLI 和 Compose 插件。候选补丁只能修改 `backend-go/`，并依次执行：

- gofmt
- `go vet ./...`
- `go test ./...`
- 固定证据、时间前推和概率校准评估
- `go mod verify`
- 相关镜像构建

冻结评估可手动执行：

```powershell
Set-Location backend-go
go run ./cmd/evaluation fixed-evidence --root ..
go run ./cmd/evaluation chronological_holdout --root ..
go run ./cmd/evaluation probability-calibration --root ..
go run ./cmd/evaluation compare-models --root ..
```

## 开发检查

Go：

```powershell
Set-Location backend-go
gofmt -l .
go vet ./...
go test ./...
```

前端：

```powershell
Set-Location frontend
npm install
npm test
npm run build
```

Compose：

```powershell
docker compose --profile go-shadow config --quiet
docker compose --profile go-shadow build go-api search-mcp market-adapter go-evolution-worker web
```

## 安全

- 凭据只放在未跟踪的 `.env`、服务器环境或 GitHub Actions Secrets 中。
- 不要把 Token 放进 URL、提交记录、数据库明文或日志。
- MCP 凭据由 `MCP_SECRET_KEY` 加密后保存。
- 演进自动合并和自动回滚由 `EVOLUTION_ENABLED`、`EVOLUTION_AUTO_MERGE` 控制。

## 交付

规范交付分支为 `origin/golang`。CI 包括 Go 竞态测试、冻结评估、前端测试/构建和 Compose 镜像构建。远程服务器升级需要单独配置部署 workflow 与服务器凭据。
