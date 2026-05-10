# 🚢 智能船弦号识别系统

基于 **LangChain + YOLO + Qwen VLM** 的船弦号识别系统，支持 Agent 对话检索与视频实时处理。

## 核心能力

| 模块 | 功能 |
|------|------|
| **Agent 检索** | 精确弦号匹配 O(1) + RAG 语义搜索，LangChain ReAct Agent 编排 |
| **Pipeline 视频** | YOLO 检测 → ByteTrack 跟踪 → VLM 识别，级联/并发双模式 |
| **Web 管理** | FastAPI REST API + 前端页面，CRUD + 图片上传自动识别 |

## 架构

```
                        ┌─────────────────────────────────────┐
                        │           Web (FastAPI)             │
                        │   REST API + Jinja2 前端页面        │
                        └──────────────┬──────────────────────┘
                                       │
                        ┌──────────────▼──────────────────────┐
                        │        ShipDatabase (统一接口)       │
                        │   精确查找(dict) + 语义检索(Embedding) │
                        └──────┬───────────────┬──────────────┘
                               │               │
                     ┌─────────▼──┐    ┌───────▼────────┐
                     │  CSV 后端   │    │  SQLite 后端    │
                     └────────────┘    └────────────────┘

  视频流 ──▶ YOLO 检测 ──▶ ByteTrack 跟踪 ──▶ 裁剪 ──▶ VLM 识别
                                                          │
                                            ┌─────────────▼─────────────┐
                                            │  use_agent=false (默认)   │
                                            │  硬编码: VLM→查库→语义检索  │
                                            │                           │
                                            │  use_agent=true           │
                                            │  Agent: lookup→retrieve   │
                                            └───────────────────────────┘
```

## 快速开始

### 安装

```bash
git clone https://github.com/hyshhh/SQL-boat-v1.git
cd SQL-boat-v1
pip install -e .
```

### 启动 LLM 服务

```bash
# 对话模型（Qwen3-VL）
CUDA_VISIBLE_DEVICES=0 vllm serve /path/to/Qwen3.5-2B-AWQ \
  --api-key abc123 \
  --served-model-name Qwen/Qwen3-VL-4B-AWQ \
  --max-model-len 10240 --port 7890 \
  --gpu-memory-utilization 0.15 --max-num-seqs 10 \
  --enable-auto-tool-choice --tool-call-parser qwen3_xml

# Embedding 模型
CUDA_VISIBLE_DEVICES=1 python -m vllm.entrypoints.openai.api_server \
  --model ./models/Qwen3-Embedding-0.6B \
  --api-key abc123 \
  --served-model-name Qwen3-Embedding-0.6B \
  --convert embed --gpu-memory-utilization 0.08 \
  --max-model-len 2048 --port 7891
```

### 启动 Web 服务

```bash
uvicorn web.app:app --host 0.0.0.0 --port 8000 --reload
# 或
python -m web
```

浏览器访问 `http://localhost:8000`

### 视频处理

```bash
# 基本用法
python -m pipeline.cli video.mp4 --demo --output result.mp4

# 并发模式（更快）
python -m pipeline.cli video.mp4 --demo -c --max-concurrent 8 -o result.mp4

# Agent 模式
python -m pipeline.cli video.mp4 --agent --demo -o result.mp4

# 摄像头 / RTSP
python -m pipeline.cli 0 --demo --display
python -m pipeline.cli rtsp://192.168.1.100/stream --demo --display
```

## 项目结构

```
├── config.py / config.yaml     # 配置中心
├── database/                   # 可插拔数据层
│   ├── base.py                 # 抽象基类 ShipDataSource
│   ├── csv_source.py           # CSV 后端（向后兼容）
│   └── sql_source.py           # SQLite 后端（支持 CRUD）
├── web/                        # FastAPI Web 服务
│   ├── app.py                  # 应用入口 + lifespan
│   ├── models/schemas.py       # Pydantic 请求/响应模型
│   ├── services/ship_service.py # 业务逻辑服务层
│   ├── routes/
│   │   ├── pages.py            # 页面路由（Jinja2）
│   │   └── api.py              # REST API 路由
│   ├── templates/index.html    # Jinja2 模板
│   └── static/{css,js}/        # 分离的静态资源
├── agent/                      # LangChain ReAct Agent
├── tools/                      # LangChain 工具定义
├── pipeline/                   # 视频处理流水线
│   ├── pipeline.py             # 主编排（级联/并发双模式）
│   ├── detector.py             # YOLO 检测 + ByteTrack
│   ├── tracker.py              # 跟踪状态管理
│   └── agent_inference.py      # Qwen VLM 推理
└── cli/                        # Rich CLI
```

## Web API

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/ships` | 获取所有船只 |
| `GET` | `/api/ships/{hn}` | 查询单条 |
| `POST` | `/api/ships` | 新增 |
| `PUT` | `/api/ships/{hn}` | 更新 |
| `DELETE` | `/api/ships/{hn}` | 删除 |
| `POST` | `/api/ships/bulk` | 批量导入 |
| `POST` | `/api/ships/recognize` | 图片 → VLM 识别（不入库） |
| `POST` | `/api/ships/recognize-and-add` | 图片 → 识别 → 自动入库 |
| `GET` | `/api/ships/search?q=` | 关键词搜索 |
| `GET` | `/api/ships/stats` | 统计信息 |

## 数据后端

通过 `config.yaml` 切换：

```yaml
database:
  backend: "csv"      # CSV 文件（默认，向后兼容）
  # backend: "sqlite" # SQLite 数据库（支持 Web CRUD）
```

迁移工具：`python migrate_csv_to_sqlite.py --csv ./data/ships.csv --db ./data/ships.db`

## 推理模式

| | 硬编码模式 (默认) | Agent 模式 |
|---|---|---|
| 调用 | 直接 VLM → 查库 → 语义检索 | LangChain Agent 编排工具链 |
| 优势 | 快速、可控、无额外 LLM 调用 | 灵活、可扩展、自动决策跳步 |
| 配置 | `use_agent: false` | `use_agent: true` |
| CLI | `--no-agent` (默认) | `--agent` |

## 配置说明

完整配置见 `config.yaml`，关键项：

```yaml
llm:                          # 对话模型
  model: "Qwen/Qwen3-VL-4B-AWQ"
  base_url: "http://localhost:7890/v1"

embed:                        # Embedding 模型
  model: "Qwen3-Embedding-0.6B"
  base_url: "http://localhost:7891/v1"

retrieval:                    # RAG 检索
  top_k: 3
  score_threshold: 0.5

pipeline:
  concurrent_mode: true       # 并发模式
  use_agent: false            # Agent/硬编码切换
  enable_refresh: false       # 定时刷新
  process_every_n_frames: 15  # 推理频率

web:
  host: "0.0.0.0"
  port: 8000
```

## License

MIT
