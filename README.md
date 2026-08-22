# Market Loop Agent

一个本地优先、证据驱动的跨市场投资研究循环。系统每 20 分钟发现新闻，将事件映射到 A 股、港股、美股与高流动性加密资产，生成带来源的 1–6 个月研究建议，并用 10 万美元模拟组合持续验证结果。

> 本项目只做研究与模拟交易，不连接券商，不构成投资建议。

## 已实现

- FastAPI + SSE、React 看板、Celery 调度、Redis、PostgreSQL/pgvector 和 Docker Compose。
- FMP MCP 静态白名单服务，以及自动回退的 FMP Stable REST 客户端。
- FMP、SEC EDGAR、RSS、AkShare/CNInfo、CoinGecko、DefiLlama、CCXT 数据适配器；统一 `AssetRef` 标识。
- 元数据过滤 + BM25 + CPU 多语言向量 + RRF 混合检索；历史回放禁止调用只提供“当前视图”的实时 Provider。
- 3B 新闻抽取、通用 7B 深研、Coder 7B 演进的 Ollama 模型网关；跨进程 GPU 单并发。
- LangGraph `收集 → 撰写 → 验证 → 修订 → 定稿` 状态图和 PostgreSQL checkpoint。
- 时间边界、来源独立性、引用 ID 和报告完整性门禁；低于 0.55 置信度强制“观察”，高影响结论须经过第二轮本地验证和可选云复核。
- 均衡型模拟组合、交易成本、资产上限、结果观察与 Telegram 推送。
- 默认关闭的整仓库自动演进：分支、补丁、测试、评测、合并标签与回滚。

## 快速启动

1. 启动 Docker Desktop 和 Ollama，并准备三个模型：

   ```powershell
   ollama pull qwen2.5:3b
   ollama pull qwen2.5:7b
   ollama pull qwen2.5-coder:7b
   ```

2. 复制环境文件并填入轮换后的 FMP Key；不要重用曾发布到聊天、日志或 Git 的密钥。若启用 SEC 直连，也请按 SEC 要求填写真实联系身份：

   ```powershell
   Copy-Item .env.example .env
   ```

3. 启动全部核心服务和自托管 FMP MCP：

   ```powershell
   docker compose --profile mcp up --build -d
   docker compose exec api celery -A backend.app.worker call market_loop.seed_assets
   ```

4. 打开：

   - Web 看板：<http://localhost:5173>
   - API 文档：<http://localhost:8000/docs>
   - 健康检查：<http://localhost:8000/health>
   - FMP MCP：<http://localhost:8081/mcp>

如果暂时不启用 MCP，可清空 `FMP_MCP_URL` 并执行 `docker compose up --build -d`；系统会使用 FMP REST。

CPU 嵌入模型会在第一次研究时下载。Ollama 请求设置 `keep_alive=0`，三个本地模型通过 Redis 全局锁串行加载，适合 8 GB 显存环境。

## 常用操作

```powershell
# 手动触发一次发现循环
Invoke-RestMethod -Method Post http://localhost:8000/api/v1/scan `
  -ContentType application/json -Body '{"background":true}'

# 用上一步返回的 task_id 查看扫描进度；重复触发会复用正在运行的任务
Invoke-RestMethod http://localhost:8000/api/v1/tasks/<task_id>

# 为 Apple 启动深研
Invoke-RestMethod -Method Post http://localhost:8000/api/v1/research `
  -ContentType application/json `
  -Body '{"asset_id":"equity:XNAS:AAPL","background":true}'

# 查看 Worker
docker compose logs -f io-worker llm-worker scheduler
```

运行测试：

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m pip_audit
```

## 自动演进（实验性）

先将仓库提交到干净的 `main`，再设置 `EVOLUTION_ENABLED=true`；只有明确接受自动合并风险时才设置 `EVOLUTION_AUTO_MERGE=true`。启动带 Docker socket 权限的独立演进 Worker：

```powershell
docker compose --profile mcp --profile evolution up --build -d
```

候选补丁使用独立 `evolve/*` 分支，并依次执行编译、全量测试、时间穿越测试、固定证据集、walk-forward 切分、密钥扫描、依赖审计、容器构建和 API 健康检查。目标指标需提升至少 2%，任一共同指标不得下降超过 1%；部署失败会回到 `last-known-good`。演进 Worker 挂载 Docker socket，拥有非常高的主机权限。

## 数据与安全边界

- FMP Key、Telegram Token 和可选云模型密钥只通过 `.env`/运行时环境注入。请求使用 `apikey` Header，日志和数据库不保存密钥。
- 新闻默认保存标题、摘要、URL、时间、来源和哈希，不批量复制受版权保护的正文。
- 所有资料都有 `published_at`、`observed_at`、`as_of`；历史回放只能读取当时已观察到的数据。
- `AUTO_PAPER_TRADE=false`、`EVOLUTION_ENABLED=false`、`EVOLUTION_AUTO_MERGE=false` 是安全默认值。
- 用户选择允许 Agent 修改整个仓库且不设置外部守门器。因此自动演进无法成为可信安全边界：Agent 理论上可以同时修改实现和测试。即使启用，也不得向系统提供券商密钥。
- FMP 数据如需展示或再分发，必须另行确认相应套餐的数据许可。

## 借鉴项目

- [TradingAgents](https://github.com/TauricResearch/TradingAgents)：LangGraph 多角色研究与复盘模式。
- [FinRobot](https://github.com/AI4Finance-Foundation/FinRobot)：FMP/SEC 与财务研报结构。
- [AI Hedge Fund](https://github.com/virattt/ai-hedge-fund)：组合约束、策略与回测模式。
- [OpenBB](https://github.com/OpenBB-finance/OpenBB)：金融数据适配层设计。
- [FMP MCP Server](https://github.com/imbenrabi/Financial-Modeling-Prep-MCP-Server)：Apache-2.0、自托管静态工具集。

系统没有 fork 上述任一项目；代码从空仓库按领域边界实现。

## API

主要端点：

- `GET /api/v1/assets|news|events`
- `POST /api/v1/scan`
- `GET /api/v1/tasks/{task_id}`
- `POST /api/v1/research`
- `GET /api/v1/research-runs|recommendations`
- `GET /api/v1/portfolio|outcomes|evolution`
- `POST /api/v1/paper-orders`
- `GET /api/v1/stream`（SSE）

## 当前限制

- A/H 股已接入 AkShare/CNInfo 公告适配，但公开源稳定性低于 SEC/FMP；生产使用仍应配置交易所/公司 IR 官方 Feed 和有授权的中文新闻源。
- 港股 board lot 支持证券主数据字段，但动态标的和跨币种汇率仍使用保守默认值；提交模拟订单前应补齐正式 board lot 与 FX 数据。
- 当前黄金集只有 3 条可执行回归样例；达到计划中的 100 条人工标注后，才能把映射 precision/recall 当作有统计意义的验收指标。
