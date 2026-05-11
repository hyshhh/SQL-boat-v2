# 🚢 SQL-boat-v2 — 智能船只舷号识别系统

[![Python](https://img.shields.io/badge/Python-≥3.10-blue?logo=python)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104+-009688?logo=fastapi)](https://fastapi.tiangolo.com)
[![YOLO](https://img.shields.io/badge/YOLO-v8-FF6F00?logo=ultralytics)](https://ultralytics.com)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

基于 **LangChain + YOLO + Qwen VLM** 的智能船只舷号识别与管理系统。支持从图片/视频/摄像头实时识别船体舷号，自动入库，并提供完整的 Web 管理界面。

---

## ✨ 核心功能

| 功能 | 说明 |
|------|------|
| 🖼️ **图片识别** | 上传船只照片，VLM 自动识别舷号和描述，一键入库 |
| 🎬 **视频处理** | 上传视频 → YOLO 检测船只 → VLM 逐帧识别 → 输出标注结果视频 |
| 📷 **摄像头实时识别** | 接入本地摄像头或 RTSP 流，实时检测与识别 |
| 🗄️ **数据库管理** | 支持 CSV / SQLite 双后端，CRUD + 批量导入 + 关键词搜索 |
| 🧠 **语义检索** | 基于 Embedding 向量的语义搜索，描述模糊匹配 |
| 🌐 **Web 界面** | 全功能 Web 管理面板，三个 Tab 覆盖全部操作 |

---

## 🏗️ 系统架构

```
┌─────────────────────────────────────────────────────────┐
│                     Web UI (FastAPI + Jinja2)            │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────────┐   │
│  │ 数据库管理 │  │ 视频 Demo │  │ 摄像头 Demo          │   │
│  └────┬─────┘  └────┬─────┘  └──────────┬───────────┘   │
│       │              │                    │               │
├───────┼──────────────┼────────────────────┼───────────────┤
│       ▼              ▼                    ▼               │
│  ┌─────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │ShipService│  │Pipeline API │  │Camera API   │          │
│  └────┬─────┘  └──────┬──────┘  └──────┬──────┘          │
│       │               │                │                  │
│  ┌────▼─────┐  ┌──────▼──────┐  ┌──────▼──────┐          │
│  │ShipDatabase│ │ YOLO + VLM  │  │ YOLO + VLM  │          │
│  │(CSV/SQLite)│ │ (异步流水线)  │  │ (实时流处理)  │          │
│  └────┬─────┘  └──────┬──────┘  └──────┬──────┘          │
│       │               │                │                  │
│  ┌────▼───────────────▼────────────────▼──────┐           │
│  │         Qwen VLM (视觉语言模型)              │           │
│  │    Qwen3-VL-4B-AWQ via OpenAI-compatible    │           │
│  └────────────────────────────────────────────┘           │
└─────────────────────────────────────────────────────────┘
```

---

## 📁 项目结构

```
SQL-boat-v2/
├── config.py                # 配置加载（唯一配置源）
├── config.yaml              # 全局配置文件
├── pyproject.toml           # 项目元数据与依赖
├── data/
│   └── ships.csv            # 示例船只数据
├── database/
│   ├── __init__.py          # ShipDatabase 核心类（双通道检索）
│   ├── base.py              # 数据源抽象基类
│   ├── csv_source.py        # CSV 数据源实现
│   └── sql_source.py        # SQLite 数据源实现
├── agent/                   # Agent 模块（待扩展）
├── cli/                     # CLI 模块（待扩展）
├── pipeline/                # 视频处理流水线（待扩展）
├── tools/                   # 工具模块（待扩展）
└── web/
    ├── app.py               # FastAPI 应用入口
    ├── models/
    │   └── schemas.py       # Pydantic 请求/响应模型
    ├── routes/
    │   ├── api.py           # REST API（船只 CRUD + VLM 识别）
    │   ├── pages.py         # 页面路由
    │   └── pipeline_api.py  # Pipeline API（视频/摄像头控制）
    ├── services/
    │   └── ship_service.py  # 业务逻辑服务层
    ├── static/
    │   ├── css/style.css    # 样式文件
    │   └── js/
    │       ├── app.js       # 数据库管理前端逻辑
    │       └── pipeline.js  # Pipeline 前端逻辑
    └── templates/
        └── index.html       # 主页模板
```

---

## 🚀 快速开始

### 1. 环境要求

- **Python ≥ 3.10**
- **Qwen VLM 服务**（OpenAI-compatible API）
- **Embedding 服务**（可选，用于语义检索）

### 2. 安装依赖

```bash
pip install -e .
# 或手动安装
pip install fastapi uvicorn jinja2 python-multipart pyyaml \
    langchain-core langchain-openai langgraph httpx \
    opencv-python numpy ultralytics
```

### 3. 配置

编辑 `config.yaml`，至少配置 VLM 服务地址：

```yaml
llm:
  model: "Qwen/Qwen3-VL-4B-AWQ"
  api_key: "your-api-key"
  base_url: "http://your-vlm-server:7890/v1"

# 数据库后端（csv 或 sqlite）
database:
  backend: "sqlite"
  sqlite_path: "./data/ships.db"

# Web 服务
web:
  host: "0.0.0.0"
  port: 8000
```

> 完整配置项说明见 [config.yaml](config.yaml) 注释。

### 4. 启动 Web 服务

```bash
# 方式一：python -m 启动
python -m web

# 方式二：uvicorn 启动（支持热重载）
uvicorn web.app:app --host 0.0.0.0 --port 8000 --reload
```

浏览器访问 `http://localhost:8000` 即可使用。

---

## 📖 使用指南

### 图片识别

1. 打开 Web 界面，点击 **📷 上传图片识别**
2. 选择或拖拽船只图片（支持 JPG/PNG/BMP/WebP，最大 20MB）
3. 点击 **🔍 识别**，VLM 自动分析图片
4. 确认识别结果（可手动修改），点击 **✅ 确认添加** 入库

### 数据库管理

- **新增**：点击 **+ 新增船只**，输入舷号和描述
- **编辑**：点击行右侧的 **编辑** 按钮
- **删除**：点击行右侧的 **删除** 按钮
- **批量导入**：点击 **批量导入**，输入 JSON 格式数据
- **搜索**：在搜索框输入关键词，实时过滤

### 视频 Demo

1. 切换到 **🎬 视频 Demo** 标签页
2. 上传视频文件（支持 MP4/AVI/MKV/MOV/FLV/WebM，最大 500MB）
3. 从列表中选择视频，配置选项：
   - **Agent 模式**：启用 LangChain Agent 增强识别
   - **并发模式**：多帧并行处理，提升速度
4. 点击 **▶ 开始处理**，等待 Pipeline 完成
5. 处理完成后可对比播放原始视频和结果视频

### 摄像头 Demo

1. 切换到 **📷 摄像头 Demo** 标签页
2. 选择输入源：
   - **本地摄像头 (0)**：使用设备默认摄像头
   - **RTSP 流**：填入 RTSP 地址
   - **自定义**：任意 OpenCV 支持的输入源
3. 配置处理选项，点击 **▶ 启动摄像头识别**

---

## 🔌 API 参考

### 船只管理 API

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/ships` | 获取所有船只列表 |
| `GET` | `/api/ships/{hull_number}` | 查询单条船只 |
| `POST` | `/api/ships` | 新增船只 |
| `PUT` | `/api/ships/{hull_number}` | 更新船只描述 |
| `DELETE` | `/api/ships/{hull_number}` | 删除船只 |
| `POST` | `/api/ships/bulk` | 批量添加船只 |
| `GET` | `/api/ships/search?q=关键词` | 按描述搜索 |
| `GET` | `/api/ships/stats` | 数据库统计 |
| `POST` | `/api/ships/recognize` | 上传图片识别（不入库） |
| `POST` | `/api/ships/recognize-and-add` | 上传图片识别并自动入库 |

### Pipeline API

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/pipeline/videos` | 获取 Demo 视频列表 |
| `POST` | `/api/pipeline/videos/upload` | 上传视频 |
| `DELETE` | `/api/pipeline/videos/{filename}` | 删除视频 |
| `POST` | `/api/pipeline/start` | 启动 Pipeline 处理 |
| `GET` | `/api/pipeline/status` | 获取所有任务状态 |
| `GET` | `/api/pipeline/status/{task_id}` | 获取单个任务状态 |
| `POST` | `/api/pipeline/stop/{task_id}` | 停止任务 |
| `GET` | `/api/pipeline/outputs` | 获取输出视频列表 |
| `GET` | `/api/pipeline/outputs/{filename}` | 下载输出视频 |
| `DELETE` | `/api/pipeline/tasks/clear` | 清除历史任务 |

> 详细的 API 文档启动后访问 `http://localhost:8000/docs`（Swagger UI）。

---

## ⚙️ 配置说明

所有配置集中在 `config.yaml`，支持以下模块：

| 配置块 | 说明 |
|--------|------|
| `llm` | VLM 对话模型（用于图片识别） |
| `embed` | Embedding 模型（用于语义检索） |
| `retrieval` | RAG 检索参数（top_k、阈值） |
| `vector_store` | 向量存储路径 |
| `database` | 数据库后端（csv/sqlite）及路径 |
| `web` | Web 服务 host/port |
| `demo_video` | 视频 Demo 目录与限制 |
| `pipeline` | 视频处理流水线参数（YOLO、追踪器、并发等） |

配置优先级：`config.yaml` > 内置默认值。支持深层合并，只需覆盖需要修改的字段。

---

## 🛠️ 技术栈

- **后端框架**：FastAPI + Uvicorn
- **模板引擎**：Jinja2
- **视觉模型**：Qwen3-VL-4B-AWQ（OpenAI-compatible API）
- **目标检测**：YOLOv8（Ultralytics）
- **追踪算法**：ByteTrack
- **Embedding**：Qwen3-Embedding-0.6B
- **向量检索**：余弦相似度（SQLite 存储）
- **LLM 编排**：LangChain + LangGraph
- **前端**：原生 HTML/CSS/JS（无框架依赖）

---

## 📄 License

MIT License
