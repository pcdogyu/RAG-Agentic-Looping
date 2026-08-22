# Market Loop Agent

本地优先、证据驱动的跨市场投资研究循环。系统持续发现新闻，将事件映射到 A 股、港股、美股和高流动性加密资产，完成差异化深度研究、证据验证、建议生成、模拟组合与结果评估。

> [!WARNING]
> 本项目只用于研究和模拟交易，不连接实盘券商，不构成投资建议。自动演进默认关闭，任何真实密钥都不得提交到 Git。

## 目录

- [项目定位](#项目定位)
- [系统架构](#系统架构)
- [组件说明](#组件说明)
- [运行流程](#运行流程)
- [模型与硬件要求](#模型与硬件要求)
- [数据源与市场覆盖](#数据源与市场覆盖)
- [快速启动](#快速启动)
- [配置说明](#配置说明)
- [API 与日常操作](#api-与日常操作)
- [研究、验证与组合规则](#研究验证与组合规则)
- [自动演进](#自动演进)
- [开发与测试](#开发与测试)
- [Windows 常见问题](#windows-常见问题)
- [安全边界与当前限制](#安全边界与当前限制)

## 项目定位

Market Loop Agent 面向 1–6 个月的中线研究，通过有限、可恢复、可验证的循环代替无限制的“自由思考”：

```text
每 10 分钟发现新闻
  → 去重与事件抽取
  → 映射股票/加密标的
  → 收集财务、公告、历史新闻和链上指标
  → 两轮证据验证
  → 五级研究建议
  → 模拟组合
  → 1/5/20/60/120 日结果观察
  → 评测与可选自动演进
```

当前实现包括：

- A 股、港股、美股，以及 BTC、ETH 和动态市值前 20 的非稳定币加密资产。
- FMP MCP 白名单服务与 FMP Stable REST 自动降级客户端。
- SEC EDGAR、RSS、AkShare/CNInfo、CoinGecko、DefiLlama 和 CCXT 数据适配器。
- 结构化财务数据、全文检索、pgvector 向量检索和 RRF 融合重排。
- Ollama 本地三模型分工、Redis 跨进程 GPU 单并发和及时卸载。
- LangGraph `收集 → 撰写 → 验证 → 修订 → 定稿` 状态图与 PostgreSQL checkpoint。
- FastAPI、SSE 实时事件流、React Web 看板、Celery 调度和 Telegram 可选通知。
- 10 万美元初始模拟组合、市场交易规则、成本约束和多周期结果评估。
- 默认关闭的 Coder 7B 整仓库演进、测试门禁、自动合并与回滚流程。

## 系统架构

```mermaid
flowchart LR
    USER[用户] --> WEB[React Web<br/>localhost:5173]
    WEB -->|REST / SSE| API[FastAPI<br/>localhost:8000]

    SCHED[Celery Beat<br/>定时调度] --> REDIS[(Redis<br/>队列、缓存、锁)]
    API --> REDIS
    API --> PG[(PostgreSQL + pgvector<br/>业务数据、证据、checkpoint)]

    REDIS --> IO[I/O Worker<br/>新闻、数据源、事件抽取]
    REDIS --> LLM[LLM Worker<br/>单并发深研]

    IO --> PROVIDERS[数据提供器]
    PROVIDERS --> FMP[FMP MCP / REST]
    PROVIDERS --> SEC[SEC EDGAR]
    PROVIDERS --> CN[AkShare / CNInfo / RSS]
    PROVIDERS --> CRYPTO[CoinGecko / DefiLlama / CCXT]

    IO -->|qwen2.5:3b| OLLAMA[宿主机 Ollama<br/>Redis GPU 全局锁]
    LLM --> GRAPH[LangGraph<br/>研究与两轮验证]
    GRAPH -->|qwen2.5:7b| OLLAMA
    GRAPH --> PG
    GRAPH --> REPORTS[Markdown 报告<br/>./reports]

    PG --> API
    API -->|snapshot SSE| WEB

    EVOLVE[可选 Evolution Worker] -->|qwen2.5-coder:7b| OLLAMA
    EVOLVE --> GIT[Git 分支、测试、构建、回滚]
    REDIS -. evolution queue .-> EVOLVE
```

### 关键设计

- **事件驱动而非无限循环**：扫描、深研、验证、观察和演进都有明确终止条件。
- **可恢复**：任务结果存入 Redis，研究状态由 LangGraph checkpoint 保存到 PostgreSQL；扫描任务防重复，并能恢复“新闻已保存但事件尚未生成”的记录。
- **证据优先**：重要结论绑定证据 ID、来源 URL 和时间边界；官方披露优先于聚合内容。
- **时间安全**：数据携带 `published_at`、`observed_at`、`as_of`，历史回放不得读取当时尚未观察到的内容。
- **GPU 单并发**：所有 Ollama 请求共用 Redis 分布式锁；研究 Worker 本身并发为 1，避免三个模型同时占用显存。
- **安全降级**：数据源故障时使用缓存或备用 Provider；证据不足、低置信度或未完成高影响复核时输出“观察”。

## 组件说明

### Docker Compose 服务

| 服务 | 技术/并发 | 对外端口 | Profile | 职责 |
|---|---|---:|---|---|
| `web` | React + Vite + Nginx | `5173` | 核心 | 展示事件、建议、研究轨迹、模拟组合和健康状态 |
| `api` | FastAPI + SSE | `8000` | 核心 | REST 接口、任务提交、健康检查和实时快照流 |
| `scheduler` | Celery Beat | 无 | 核心 | 每 10 分钟扫描新闻、每 6 小时刷新加密资产、每日评估结果 |
| `io-worker` | Celery，I/O 并发 4 | 无 | 核心 | 新闻发现、去重、事件抽取、资产映射和外部数据请求 |
| `llm-worker` | Celery，LLM 并发 1 | 无 | 核心 | LangGraph 深研、证据验证、报告和建议生成 |
| `postgres` | PostgreSQL 16 + pgvector | `5432` | 核心 | 结构化数据、全文/向量投影、证据、组合和 checkpoint |
| `redis` | Redis 7 | `6379` | 核心 | Celery broker/result、Provider 缓存、扫描单例锁和 GPU 锁 |
| `fmp-mcp` | Node.js MCP Server | `8081` → 容器 `8080` | `mcp` | 暴露受白名单约束的 FMP 工具；失败时后端回退到 REST |
| `evolution-worker` | Celery + Coder 7B | 无 | `evolution` | 创建演进分支、修改仓库、执行门禁、合并或回滚 |

> PostgreSQL 和 Redis 端口仅为本机开发方便而暴露，不应直接开放到公网。

### 应用模块

| 模块 | 作用 |
|---|---|
| `backend/app/main.py` | FastAPI 路由、SSE、健康检查和任务入口 |
| `backend/app/worker.py` | Celery 队列、周期任务、扫描单例和研究任务 |
| `backend/app/domain.py` | `AssetRef`、`NewsEvent`、`Evidence`、`ResearchRun`、`Recommendation` 等核心类型 |
| `backend/app/providers/` | FMP、SEC、A/H 股、RSS 和加密数据适配层 |
| `backend/app/services/events.py` | 新闻事件抽取、转载聚类和资产候选映射 |
| `backend/app/services/research.py` | LangGraph 深研、验证、建议和 Markdown 报告 |
| `backend/app/services/retrieval.py` | 元数据过滤、BM25、向量召回和 RRF 融合 |
| `backend/app/services/portfolio.py` | 模拟订单、交易单位、成本和仓位上限 |
| `backend/app/services/outcomes.py` | 多周期收益、基准超额、方向命中和 Brier 评分 |
| `backend/app/services/evolution.py` | 演进假设、代码补丁、测试、合并和回滚 |
| `frontend/src/App.tsx` | 单页研究看板和扫描任务进度交互 |

### 目录结构

```text
RAG-Agentic-Looping/
├─ backend/
│  ├─ app/                 # API、Worker、领域模型、Provider 和研究服务
│  ├─ tests/               # 单元与集成测试
│  ├─ Dockerfile           # 核心 Python 3.12 镜像
│  └─ Dockerfile.evolution # 具备仓库修改能力的演进镜像
├─ frontend/               # React Web 看板
├─ infra/
│  ├─ fmp-mcp/             # 固定提交版本的 FMP MCP 构建上下文
│  └─ postgres/            # PostgreSQL/pgvector 初始化脚本
├─ evals/                  # 固定证据集与评测基线
├─ reports/                # 运行时生成的研究报告，不提交敏感内容
├─ docker-compose.yml
├─ .env.example
└─ README.md
```

## 运行流程

### 1. 新闻发现与事件抽取

1. Scheduler 默认每 10 分钟提交 `market_loop.scan_news`。
2. FMP、RSS 和 AkShare Provider 拉取最近窗口内的新闻，按内容哈希去重。
3. `qwen2.5:3b` 抽取事件类型、主体、影响方向、时间范围和搜索词。
4. 系统结合证券主数据、别名、产品和 Provider 搜索结果生成候选资产。
5. 重复点击扫描会复用同一个任务 ID；页面通过任务接口展示抽取进度。

### 2. 股票与加密资产深研

- **股票**：历史同类事件、营收增长、利润与现金流、产品、竞争对手、估值、催化剂、风险和失效条件。
- **加密资产**：供给与代币信息、市场数据、社区和开发活跃、DefiLlama 协议数据、交易所价格交叉验证、流动性、安全与监管风险。
- 财务数字直接来自结构化 Provider 数据或原始披露，不从向量文本猜测。

### 3. 检索、验证与建议

```mermaid
stateDiagram-v2
    [*] --> 收集
    收集 --> 撰写
    撰写 --> 验证
    验证 --> 修订: 缺失、冲突或高影响且轮次未满
    修订 --> 验证
    验证 --> 定稿: 通过或已达到两轮上限
    定稿 --> [*]
```

- 检索同时使用元数据过滤、BM25、CPU 向量和 RRF 重排。
- 一个官方原始来源，或两个独立来源，才能满足重大事实的来源门槛。
- 验证检查章节完整性、引用 ID、来源独立性、未知引用和时间穿越。
- 两轮后仍不合格会生成低置信度“观察”建议，不产生方向性仓位。
- `|score| ≥ 60` 属于高影响建议；未配置可选云端复核时，即使本地证据完整也降级为“观察”。

### 4. 模拟组合与结果观察

- 初始资金：`100,000 USD`。
- 单只股票上限：8%；单一加密资产上限：5%；加密总仓上限：15%。
- 至少保留 10% 现金。
- 股票成本默认每边 15 bps；加密默认每边 25 bps。
- A 股按手数、港股按 `lot_size`、美股按整股、加密按小数精度处理。
- 在 1、5、20、60、120 日评估原始收益、基准收益、Alpha、方向命中、最大回撤和 Brier 校准。
- 默认基准：美股 `SPY`、A 股 `000300`、港股 `HSI`、加密资产 `BTC`；Provider 无法提供基准时保守返回 0。

## 模型与硬件要求

### 本地模型

| 模型 | 默认配置变量 | 作用 | 本机 Ollama 显示大小 | 是否必需 |
|---|---|---|---:|---|
| `qwen2.5:3b` | `OLLAMA_EXTRACT_MODEL` | 新闻抽取、分类、实体识别和初筛 | 约 1.9 GB | 是 |
| `qwen2.5:7b` | `OLLAMA_RESEARCH_MODEL` | 研究综合、矛盾检查、两轮验证和中文报告 | 约 4.7 GB | 是 |
| `qwen2.5-coder:7b` | `OLLAMA_CODE_MODEL` | 演进假设、补丁和测试生成 | 约 4.7 GB | 仅启用演进时 |
| `intfloat/multilingual-e5-small` | `EMBEDDING_MODEL` | CPU 多语言向量检索，384 维 | 首次研究自动下载 | 是 |

模型文件大小会随 Ollama 版本和量化格式变化，表中数值仅用于估算磁盘空间。

### 推理与显存策略

- Ollama 运行在宿主机，容器通过 `host.docker.internal:11434` 调用。
- 每个请求设置 `keep_alive=0`，完成后卸载当前模型，再切换下一个模型。
- Redis 锁 `market-loop:gpu` 将 API 和所有 Worker 的 Ollama 调用限制为全局单并发。
- `llm-worker` 并发固定为 1；`io-worker` 可并发处理网络 I/O，但事件抽取仍受同一 GPU 锁约束。
- CPU 推理可以运行，但完整扫描与 7B 深研会明显变慢，不作为性能目标。

### 推荐运行环境

| 资源 | 推荐值 | 说明 |
|---|---|---|
| 操作系统 | Windows 10/11 + WSL2，或现代 Linux | Windows 使用 Docker Desktop Linux containers |
| 内存 | 16 GB 或更多 | PostgreSQL、Worker、Docker 和 Ollama 共同占用 |
| GPU | NVIDIA/兼容 GPU，8 GB 或更多显存 | 系统按单模型串行运行；无 GPU 时回退 CPU |
| 磁盘 | 至少 25 GB 可用空间 | 包含三个模型、Docker 镜像、数据库和缓存 |
| CPU | 8 核或更多 | CPU 嵌入、Provider 处理和容器运行 |
| 网络 | 可访问 Docker Hub、Ollama 模型源和金融数据源 | 首次构建与模型下载需要较稳定网络 |

宿主机需要安装：

- Docker Desktop（含 Docker Compose）或 Linux Docker Engine。
- Ollama。
- Git。
- 仅在宿主机运行测试时需要 Python `>=3.12,<3.14`；容器固定使用 Python 3.12。

## 数据源与市场覆盖

| 市场/数据 | 主要来源 | 验证或补充来源 | 降级行为 |
|---|---|---|---|
| 美股新闻与财务 | FMP MCP | FMP Stable REST | MCP 请求失败时自动调用 REST；缓存并执行限流 |
| 美股官方披露 | SEC EDGAR / EdgarTools | FMP filings | 官方 SEC 链接优先，聚合披露作为补充 |
| A 股新闻与行情 | AkShare / 东方财富 | RSS、公司 IR 或授权 Feed | 公共接口失败时保留其他来源结果，不阻塞整批扫描 |
| A 股公告 | CNInfo/AkShare | 官方 RSS 或 IR Feed | 无可靠公告时研究会降低证据完整度 |
| 港股新闻与行情 | AkShare | RSS、HKEX/公司 IR Feed | 公共接口失败时输出证据不足，不猜测数据 |
| 加密资产目录与行情 | CoinGecko | FMP crypto quote、CCXT Kraken | 交易所价格偏差超过阈值时标记交叉验证失败 |
| 加密协议指标 | DefiLlama | CoinGecko 社区/开发数据 | 协议不存在或接口失败时保留可用市场指标 |
| 通用与自有新闻 | FMP General News、RSS | `OFFICIAL_RSS_FEED_URLS` | 单个 Feed 超时不影响其他 Provider |

统一资产标识示例：

```text
equity:XNAS:AAPL
equity:XSHG:600519
equity:XHKG:00700
crypto:coingecko:bitcoin
```

新闻默认只保存标题、摘要、URL、来源、时间、内容哈希和抽取事实，不批量复制受版权保护的原文。

## 快速启动

以下命令以 Windows PowerShell 为主。

### 1. 启动并检查 Docker Desktop

```powershell
docker desktop start
docker desktop status
docker info
```

`docker info` 必须同时显示 `Client` 和 `Server`。推荐使用 Linux 容器上下文：

```powershell
docker context use desktop-linux
```

### 2. 准备 Ollama 模型

核心研究需要 3B 和通用 7B；Coder 7B 只在自动演进时使用：

```powershell
ollama pull qwen2.5:3b
ollama pull qwen2.5:7b
ollama pull qwen2.5-coder:7b
ollama list
```

确认宿主机 API 可访问：

```powershell
Invoke-RestMethod http://localhost:11434/api/tags
```

### 3. 创建配置文件

```powershell
Copy-Item .env.example .env
notepad .env
```

至少检查以下内容：

```dotenv
FMP_ACCESS_TOKEN=
SEC_IDENTITY=
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
```

- 使用 FMP 时，在 `FMP_ACCESS_TOKEN=` 后填入新生成的 Key。
- `SEC_IDENTITY` 建议填写 SEC 要求的真实联系人，例如 `Your Name your@email.example`。
- Telegram 不使用时保持为空。
- 曾经发送到聊天、截图、日志或 Git 的 Key 必须先轮换，不能继续使用。

### 4. 启动核心服务和 FMP MCP

```powershell
docker compose --profile mcp up --build -d
docker compose ps
```

首次构建会下载 Python 市场数据依赖、前端依赖和 FMP MCP 源码，耗时取决于网络。后续仅修改业务源码时会复用 Docker 依赖层缓存。

不启用 MCP 时，先把 `.env` 中的 `FMP_MCP_URL` 清空，再启动核心服务：

```powershell
docker compose up --build -d
```

后端会继续使用 FMP Stable REST。

### 5. 验证健康状态

```powershell
Invoke-RestMethod http://localhost:8000/health
Invoke-RestMethod http://localhost:8081/healthz
docker compose ps
```

核心健康响应应至少满足：

```text
status = ok
database = true
redis = true
ollama = true
```

打开：

- Web 看板：<http://localhost:5173>
- API 文档：<http://localhost:8000/docs>
- 健康检查：<http://localhost:8000/health>
- FMP MCP 健康检查：<http://localhost:8081/healthz>
- FMP MCP 端点：<http://localhost:8081/mcp>

### 6. 执行首次扫描

可以在 Web 点击“立即扫描”，也可以使用 API：

```powershell
$scan = Invoke-RestMethod -Method Post http://localhost:8000/api/v1/scan `
  -ContentType application/json `
  -Body '{"background":true}'

$scan
Invoke-RestMethod "http://localhost:8000/api/v1/tasks/$($scan.task_id)"
```

状态依次可能为 `PENDING`、`STARTED`、`PROGRESS`、`SUCCESS` 或 `FAILURE`。重复提交扫描会返回正在运行的同一任务 ID，不会启动多个重复扫描。

首次调用需要加载本地模型，第一次深研还会下载 CPU 嵌入模型，因此通常比后续增量任务慢。页面通过 SSE 自动刷新；必要时使用 `Ctrl + F5` 刷新静态资源。

### Bash 等价命令

```bash
cp .env.example .env
ollama pull qwen2.5:3b
ollama pull qwen2.5:7b
docker compose --profile mcp up --build -d
curl http://localhost:8000/health
```

## 配置说明

所有运行时配置从 `.env` 注入。`.env` 已被 `.gitignore` 排除，`.env.example` 只保存空占位符和安全默认值。

### 数据源与密钥

| 变量 | 默认/是否必需 | 说明 |
|---|---|---|
| `FMP_ACCESS_TOKEN` | 使用 FMP 时必需，默认空 | FMP MCP 和 REST 请求凭据；不写入 URL、数据库或提示词 |
| `FMP_MCP_URL` | `http://fmp-mcp:8080/mcp` | 容器内部 MCP 地址；不用 MCP 时清空 |
| `FMP_RATE_LIMIT_PER_MINUTE` | `240` | FMP REST 客户端限流上限 |
| `SEC_IDENTITY` | 可选，默认空 | SEC EDGAR 联系身份 |
| `RSS_FEED_URLS` | 可选，逗号分隔 | 有授权的专业新闻 Feed |
| `OFFICIAL_RSS_FEED_URLS` | 可选，逗号分隔 | 交易所、监管机构或公司 IR 官方 Feed |
| `TELEGRAM_BOT_TOKEN` | 可选，默认空 | Telegram 通知 Token |
| `TELEGRAM_CHAT_ID` | 可选，默认空 | Telegram 目标会话 |

### 服务与模型

| 变量 | 默认值 | 说明 |
|---|---|---|
| `DATABASE_URL` | `postgresql+psycopg://agent:agent@postgres:5432/agent` | PostgreSQL 连接 |
| `REDIS_URL` | `redis://redis:6379/0` | Celery、缓存和锁 |
| `OLLAMA_BASE_URL` | `http://host.docker.internal:11434` | 容器访问宿主机 Ollama 的地址 |
| `OLLAMA_EXTRACT_MODEL` | `qwen2.5:3b` | 新闻事件抽取模型 |
| `OLLAMA_RESEARCH_MODEL` | `qwen2.5:7b` | 深研和验证模型 |
| `OLLAMA_CODE_MODEL` | `qwen2.5-coder:7b` | 自动演进模型 |
| `EMBEDDING_MODEL` | `intfloat/multilingual-e5-small` | CPU 嵌入模型 |
| `EMBEDDING_DIMENSIONS` | `384` | pgvector 投影维度，修改后需同步数据库设计 |

### 循环与功能开关

| 变量 | 默认值 | 说明 |
|---|---:|---|
| `SCAN_INTERVAL_MINUTES` | `20` | 新闻发现周期，允许范围 5–1440 分钟 |
| `SCAN_BATCH_SIZE` | `40` | 单次最多处理新闻数，允许范围 1–200 |
| `AUTO_RESEARCH` | `true` | 为高优先级、高相关候选自动排队深研 |
| `AUTO_PAPER_TRADE` | `false` | 是否依据已验证看多建议自动创建模拟订单 |
| `EVOLUTION_ENABLED` | `false` | 是否启用演进任务和监控 |
| `EVOLUTION_AUTO_MERGE` | `false` | 门禁通过后是否自动合并；风险极高 |
| `CLOUD_LLM_BASE_URL` | 空 | 可选 OpenAI-compatible 高影响复核服务 |
| `CLOUD_LLM_API_KEY` | 空 | 可选云复核密钥 |
| `CLOUD_LLM_MODEL` | 空 | 可选云复核模型 |

云端复核只有三个变量同时非空时才启用；系统没有云端配置时不会静默发送本地研究内容。

## API 与日常操作

### 主要接口

| 方法 | 路径 | 作用 |
|---|---|---|
| `GET` | `/health` | 数据库、Redis、Ollama、模型、数据新鲜度和功能开关 |
| `GET` | `/api/v1/assets` | 统一资产目录 |
| `GET` | `/api/v1/news` | 新闻元数据，支持 `limit` 和 `as_of` |
| `GET` | `/api/v1/events` | 结构化事件与候选标的 |
| `POST` | `/api/v1/scan` | 提交后台扫描或执行同步扫描 |
| `GET` | `/api/v1/tasks/{task_id}` | 查看 Celery 任务状态和扫描进度 |
| `POST` | `/api/v1/research` | 按资产和可选事件启动深研 |
| `GET` | `/api/v1/research-runs` | 研究轨迹列表 |
| `GET` | `/api/v1/research-runs/{run_id}` | 单次研究、证据和验证结果 |
| `GET` | `/api/v1/recommendations` | 五级建议、概率、置信度和论文 |
| `GET` | `/api/v1/portfolio` | 模拟现金、净值、持仓和加密权重 |
| `POST` | `/api/v1/paper-orders` | 从已验证看多建议创建模拟订单 |
| `GET` | `/api/v1/outcomes` | 多周期评估结果 |
| `GET/POST` | `/api/v1/evolution` | 查看或提出演进候选 |
| `POST` | `/api/v1/evolution/{candidate_id}/execute` | 执行已存在的演进候选 |
| `GET` | `/api/v1/stream` | Web 使用的 SSE 快照流 |

交互式请求和响应 Schema 以 <http://localhost:8000/docs> 为准。

### 手动启动深研

```powershell
$research = Invoke-RestMethod -Method Post http://localhost:8000/api/v1/research `
  -ContentType application/json `
  -Body '{"asset_id":"equity:XNAS:AAPL","background":true}'

$research
Invoke-RestMethod "http://localhost:8000/api/v1/tasks/$($research.task_id)"
```

也可以在 Web 的事件卡片中点击候选资产。

### 查看日志

```powershell
# 实时查看核心任务
docker compose logs -f io-worker llm-worker scheduler

# 数据源与 MCP
docker compose logs -f api fmp-mcp

# 最近 200 行
docker compose logs --tail 200 api io-worker llm-worker
```

日志和 Telegram 通知只包含任务状态、报告摘要和已脱敏错误，不应包含 API Key、完整受版权保护正文或模型内部思维过程。

### 数据与报告位置

- PostgreSQL 数据保存在 Docker volume `postgres-data`，普通容器重建不会删除。
- Redis 保存队列、任务结果、缓存和分布式锁，不是长期研究记录的唯一来源。
- Markdown 研究报告写入宿主机 `./reports`。
- `docker compose down` 保留数据库 volume；`docker compose down -v` 会删除数据库，除非明确要重置，否则不要使用 `-v`。

## 研究、验证与组合规则

### 建议分级

| 分数 | 基础评级 |
|---:|---|
| `60…100` | 强烈看多 |
| `20…59` | 看多 |
| `-19…19` | 观察 |
| `-59…-20` | 看空 |
| `-100…-60` | 强烈看空 |

以下任一条件会强制降级为“观察”：

- 证据完整性检查未通过。
- 置信度低于 0.55。
- 本地生成高影响方向，但未通过配置的云端二次复核。

每条建议还包括 bull/base/bear 概率、估值区间、1–6 月催化剂、风险、失效条件和引用证据。

### 核心数据类型

- `AssetRef`：统一资产标识、市场、币种、别名、产品和交易单位。
- `NewsEvent`：事件类型、主体、直接影响、候选标的、优先级和时间边界。
- `Evidence`：可验证主张、来源、URL、时间、独立来源分组和可选数字。
- `ResearchRun`：可回放研究状态、证据、缺失项、冲突和最终建议。
- `Recommendation`：方向分数、评级、置信度、概率和投资论文。
- `PaperOrder`：模拟订单、数量、价格、币种和成本。
- `Outcome`：收益、Alpha、方向命中、Brier 分数和最大回撤。
- `EvolutionCandidate`：演进假设、分支、补丁、指标和门禁报告。

## 自动演进

自动演进是实验性功能，默认完全关闭。启用前必须确保仓库已经提交到干净的 `main`，且明确接受 Evolution Worker 拥有仓库写权限和 Docker socket 权限。

```dotenv
EVOLUTION_ENABLED=true
EVOLUTION_AUTO_MERGE=false
```

启动演进 Profile：

```powershell
docker compose --profile mcp --profile evolution up --build -d
```

流程：

1. 从方向错误或 Alpha 为负的历史结果生成“假设—修改—预期指标”。
2. 创建 `evolve/<timestamp>-<hypothesis>` 分支。
3. 由 `qwen2.5-coder:7b` 生成补丁和测试。
4. 执行编译、全量测试、时间穿越检查、固定证据集、walk-forward、密钥扫描、依赖审计、容器构建和健康检查。
5. 只有全部通过、目标指标至少改善 2%、共同指标下降不超过 1% 才可接受。
6. 开启 `EVOLUTION_AUTO_MERGE=true` 后才会打 `last-known-good` 标签并自动合并；部署失败或健康门禁触发时回滚。

> [!CAUTION]
> 系统没有 Agent 无权修改的外部守门器。Evolution Worker 理论上可以同时修改实现和测试，因此这些门禁不是可信安全边界。即使启用演进，也不得向系统提供券商密钥。

## 开发与测试

### 本机 Python 环境

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m pip_audit
```

Python 3.13 也在 `pyproject.toml` 支持范围内。如果只通过 Docker 运行系统，不需要在宿主机安装 Python 依赖。

### Compose 与构建验证

```powershell
docker compose --profile mcp --profile evolution config
docker compose build api web
docker compose ps
Invoke-RestMethod http://localhost:8000/health
```

### 当前测试覆盖

- 事件抽取、重复转载聚类和中断恢复。
- 资产映射与统一标识。
- 时间边界与历史回放。
- FMP MCP/REST 传输与降级。
- 证据验证、评级门控和研究状态。
- 模拟仓位、交易成本和风险上限。
- 检索与向量投影。
- 自动演进门禁和失败补丁拒绝。

## Windows 常见问题

### Docker Desktop 显示 running，但连接不到 daemon

典型错误：

```text
failed to connect to the docker API at npipe:////./pipe/dockerDesktopLinuxEngine
```

依次执行：

```powershell
docker desktop start
docker desktop status
docker context use desktop-linux
docker info
```

只有 `docker info` 同时显示 Server 信息后再运行 Compose。若仍失败，完全退出并重启 Docker Desktop，确认 WSL2 和 Linux containers 已启用。

### Docker Hub DNS、代理或 443 超时

典型错误包含：

```text
lookup registry-1.docker.io: no such host
connectex: A connection attempt failed
Docker Desktop has no HTTPS proxy
```

检查：

```powershell
Resolve-DnsName registry-1.docker.io
docker pull redis:7-alpine
docker pull pgvector/pgvector:pg16
```

- 公司网络或代理环境应在 Docker Desktop 设置中配置 HTTP/HTTPS Proxy；只在 PowerShell 设置代理不一定会传给 Docker daemon。
- 修改代理或 DNS 后重启 Docker Desktop再试。
- 不要把搜索结果中的临时 IP 硬编码到 hosts；CDN 地址会变化。

### FMP MCP 构建出现 `failed to evaluate path "https::"`

当前仓库已使用本地 `infra/fmp-mcp/Dockerfile` 拉取固定的上游 commit，不再把 Git URL 当作 Windows Compose build context。若仍看到该错误，先确认已经拉取最新 `main`：

```powershell
git pull --ff-only origin main
docker compose --profile mcp build fmp-mcp
```

### Ollama 显示离线或缺少模型

```powershell
ollama list
Invoke-RestMethod http://localhost:11434/api/tags
docker compose exec api python -c "import httpx; print(httpx.get('http://host.docker.internal:11434/api/tags').status_code)"
```

- 缺少模型时重新执行对应的 `ollama pull`。
- 确保 Ollama 正在宿主机监听 `11434`。
- 容器内不要把 `OLLAMA_BASE_URL` 配成 `localhost`，因为那会指向容器自身。

### 页面正常但事件和建议都是 0

1. 查看 <http://localhost:8000/health>，确认数据库、Redis 和 Ollama 正常。
2. 点击“立即扫描”或调用 `/api/v1/scan`。
3. 使用返回的 `task_id` 查询 `/api/v1/tasks/{task_id}`。
4. 查看 `docker compose logs -f io-worker llm-worker`。
5. 首次扫描和首次嵌入模型下载较慢；事件先出现，建议会在 7B 深研完成后逐步增加。

### FMP 返回 403、429 或 MCP 调用失败

- `403`：检查 Key 是否有效、套餐是否包含对应端点，以及是否已轮换曾泄露的 Key。
- `429`：降低 `FMP_RATE_LIMIT_PER_MINUTE`，等待缓存生效后重试。
- MCP 健康检查：`Invoke-RestMethod http://localhost:8081/healthz`。
- MCP 工具失败时后端会尝试 Stable REST；单个 Provider 失败不会阻止其他来源入库。
- Key 通过请求头或 MCP 运行时环境传递，不应放入 URL 进行测试。

### 常用恢复命令

```powershell
# 查看服务状态和最近错误
docker compose ps
docker compose logs --tail 200 api io-worker llm-worker fmp-mcp

# 保留数据库并重启应用
docker compose --profile mcp up -d

# 重建业务镜像，仍保留 postgres-data
docker compose --profile mcp up --build -d
```

## 安全边界与当前限制

### 密钥与数据安全

- FMP Key、Telegram Token 和云模型 Key 只通过 `.env` 或运行时 Secret 注入。
- `.env` 不得提交；提交前运行仓库密钥扫描并检查 `git diff`。
- 通知服务会脱敏已配置 Token，仍不应把密钥写入提示词、URL 或日志命令。
- 原始新闻只在许可允许时保存；默认保留元数据、摘要、哈希和抽取事实。
- FMP 或其他数据如需展示、再分发或商业使用，必须另行确认对应套餐与来源许可。

### 权限边界

- 系统只研究和模拟交易，不包含券商连接器，也不应持有券商密钥。
- `AUTO_PAPER_TRADE=false`、`EVOLUTION_ENABLED=false`、`EVOLUTION_AUTO_MERGE=false` 是安全默认值。
- `evolution-worker` 挂载整个仓库和 Docker socket，只有在理解主机权限风险后才能启用。
- Telegram 只用于高优先级事件、报告完成、模拟仓位变化、数据源故障和演进结果通知。

### 当前限制

- A/H 股公共数据源稳定性低于 SEC/FMP；生产使用仍应配置交易所、公司 IR 和有授权的中文新闻 Feed。
- 港股动态 `board lot` 与跨币种 FX 仍使用保守数据或默认值，模拟下单前应补齐正式主数据。
- 加密协议并不都能在 DefiLlama 找到，CCXT 交叉验证也可能受交易所地域或网络限制。
- CPU 嵌入模型首次运行需要下载；容器缓存被清理后可能再次下载。
- 当前黄金集仍较小；达到至少 100 条人工事件—标的样本后，映射 precision/recall 才具有稳定统计意义。
- 云端复核默认关闭，因此绝对分数达到 60 的高影响本地结论会安全降级为“观察”。

## 借鉴项目

- [TradingAgents](https://github.com/TauricResearch/TradingAgents)：LangGraph 多角色研究与复盘模式。
- [FinnewsHunter](https://github.com/DemonDamon/FinnewsHunter)：中文新闻与 A 股事件映射思路。
- [FinRobot](https://github.com/AI4Finance-Foundation/FinRobot)：FMP/SEC 和财务研报结构。
- [StockAI](https://github.com/hyhmrright/StockAI)：A/H/美股、本地模型、引用和回测模式。
- [AI Hedge Fund](https://github.com/virattt/ai-hedge-fund)：组合约束、策略和回测模式。
- [OpenBB](https://github.com/OpenBB-finance/OpenBB)：金融数据适配层设计。
- [FMP MCP Server](https://github.com/imbenrabi/Financial-Modeling-Prep-MCP-Server)：Apache-2.0、自托管静态工具集。

系统没有 fork 上述任一项目；代码从空仓库按领域边界实现。
