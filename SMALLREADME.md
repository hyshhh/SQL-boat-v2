# QUICKSTART — 命令速查

## 一键启动

```bash
# 安装
pip install -e .

# 启动 Web 管理界面
uvicorn web.app:app --host 0.0.0.0 --port 8000 --reload
# 浏览器打开 http://localhost:8000
```

---

## 启动后端服务

### LLM 推理（Qwen3-VL）

```bash
CUDA_VISIBLE_DEVICES=0 vllm serve /media/ddc/新加卷/hys/hysnew/Qwen3.5-2B-AWQ \
  --api-key abc123 \
  --served-model-name Qwen/Qwen3-VL-4B-AWQ \
  --max-model-len 10240 \
  --port 7890 \
  --gpu-memory-utilization 0.15 \
  --max-num-seqs 10 \
  --enable-auto-tool-choice \
  --tool-call-parser qwen3_xml
```

### Embedding（Qwen3-Embedding-0.6B）

```bash
CUDA_VISIBLE_DEVICES=1 python -m vllm.entrypoints.openai.api_server \
  --model ./models/Qwen3-Embedding-0.6B \
  --api-key abc123 \
  --served-model-name Qwen3-Embedding-0.6B \
  --convert embed \
  --gpu-memory-utilization 0.08 \
  --max-model-len 2048 \
  --port 7891
```

---

## 视频处理

```bash
# 处理视频 + 输出结果（默认硬编码模式）
python -m pipeline.cli /media/ddc/新加卷/hys/hysnew2/学习/1.mp4 \
  --demo \
  --output /media/ddc/新加卷/hys/hysnew2/学习/result.mp4

# 开启定时刷新（每150帧重新识别已跟踪船只）
python -m pipeline.cli /media/ddc/新加卷/hys/hysnew2/学习/1.mp4 \
  --demo --enable-refresh --output result.mp4

# 定时刷新 + 自定义间隔（每100帧刷新一次）
python -m pipeline.cli /media/ddc/新加卷/hys/hysnew2/学习/1.mp4 \
  --demo --enable-refresh --gap-num 100 --output result.mp4

# 并发模式（快）
python -m pipeline.cli /media/ddc/新加卷/hys/hysnew2/学习/1.mp4 \
  --demo -c --max-concurrent 8 --output result.mp4

# Agent 模式（LangChain 工具链）
python -m pipeline.cli /media/ddc/新加卷/hys/hysnew2/学习/1.mp4 \
  --agent --demo --output result.mp4

# 实时弹窗
python -m pipeline.cli /media/ddc/新加卷/hys/hysnew2/学习/1.mp4 \
  --demo --display -v

# 快速测试（只跑 50 帧）
python -m pipeline.cli /media/ddc/新加卷/hys/hysnew2/学习/1.mp4 \
  --demo --max-frames 50 -v

# 摄像头 / RTSP
python -m pipeline.cli 0 --demo --display
python -m pipeline.cli rtsp://192.168.1.100/stream --demo --display
```

---

## CLI 查询

```bash
# 单次查询
ship-hull "帮我查一下弦号0014是什么船"

# 交互模式
ship-hull --interactive

# 详细调用链
ship-hull --verbose "我看到一艘灰色军舰"
```

---

## 参数速查

| 参数 | 说明 | 默认 |
|------|------|------|
| `--demo` | 画检测框 | 沿用 config |
| `--display` | 弹窗实时显示 | 关 |
| `--output` / `-o` | 输出视频路径 | 无 |
| `-c` | 并发模式 | 沿用 config |
| `--max-concurrent N` | 并发 Agent 数 | 4 |
| `--agent` | Agent 模式 | 关（硬编码） |
| `--enable-refresh` | 定时刷新 | 关 |
| `--gap-num N` | 刷新间隔帧数 | 150 |
| `--max-frames N` | 最大帧数（0=不限） | 0 |
| `--process-every N` | 每 N 帧推理 | 30 |
| `--prompt-mode` | `detailed` / `brief` | detailed |
| `--yolo-model` | YOLO 模型 | yolov8n.pt |
| `--device` | `cpu` / `0` | 自动 |
| `--conf` | 检测置信度 | 0.25 |
| `--detect-every N` | 每 N 帧检测 | 1 |
| `-v` | 详细日志 | 关 |

---

## config.yaml 关键配置

```yaml
# 推理模式
pipeline:
  use_agent: false        # true=Agent模式 false=硬编码
  concurrent_mode: true   # true=并发 false=级联
  enable_refresh: false   # 定时重新识别
  gap_num: 150            # 刷新间隔帧数

# 数据后端
database:
  backend: "sqlite"       # csv 或 sqlite

# Web 服务
web:
  host: "0.0.0.0"
  port: 8000
```

---

## 快捷键（--display 模式）

| 键 | 功能 |
|----|------|
| `q` | 退出 |
| `d` | 切换 detailed / brief |
| `p` | 暂停 / 继续 |
| `s` | 截图 |

---

## 模式对比

| | 硬编码 (默认) | Agent |
|---|---|---|
| 流程 | VLM → 查库 → 语义检索 | Agent 编排 lookup → retrieve |
| 速度 | 快（无额外 LLM 调用） | 慢（多一轮 Agent 决策） |
| 灵活性 | 固定流程 | 可扩展、自动跳步 |
| 开启 | `--no-agent` (默认) | `--agent` |
