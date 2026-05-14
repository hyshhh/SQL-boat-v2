# 并发模型文档

本项目在 **Web 服务层**（FastAPI + asyncio）和 **Pipeline 计算层**（threading + queue）分别使用了不同并发模型，
通过 `queue.Queue`（线程安全）和 `os.pipe`（fd 重定向）桥接两层。

---

## 架构总览

```
浏览器
  │
  ▼ WebSocket / HTTP
┌─────────────────────────────────────────────────────┐
│  FastAPI (asyncio 事件循环)                          │
│                                                     │
│  asyncio.Semaphore ── 限制并行 pipeline 数量         │
│  asyncio.Lock ────── 保护全局状态字典                │
│  asyncio.create_task ─ 异步子任务（H.264 编码/推流） │
│                                                     │
│  ┌─── asyncio.subprocess ──┐  ┌── threading.Thread ─┐
│  │  pipeline 进程 (stdout)  │  │  pipeline 线程      │
│  │  ffmpeg 编码进程         │  │  (浏览器摄像头模式)  │
│  └──────────────────────────┘  └────────────────────┘
│              │                        │
│              ▼                        ▼ os.pipe
│         queue.Queue ◄──── VirtualCamera 消费帧
└─────────────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────┐
│  Pipeline 计算层 (threading)                         │
│                                                     │
│  threading.Thread × N ── VLM 推理工作线程池          │
│  queue.Queue ─────────── 帧队列 / 结果队列           │
│  threading.Event ────── 停止信号 / 背压信号          │
│  threading.Lock ─────── 保护 tracker 跟踪数据        │
└─────────────────────────────────────────────────────┘
```

---

## 一、Web 服务层（asyncio）

文件：`web/routes/pipeline_api.py`

### 1.1 asyncio.Semaphore — 并行 Pipeline 数量限制

```python
_pipeline_semaphore: asyncio.Semaphore | None = None
_MAX_PARALLEL_PIPELINES = 2  # 默认上限
```

- **作用**：防止多人同时访问时 GPU/CPU 被打爆
- **获取**：`_get_semaphore()` 延迟初始化，从 `config.yaml` 读取上限
- **抢占**：`await sem.acquire()` 在 pipeline 启动时获取
- **释放**：`sem.release()` 在 `_wait_pipeline` 的 `finally` 块中释放
- **快速拒绝**：`sem.locked()` 检查，已满时直接返回 HTTP 429

### 1.2 asyncio.Lock — 全局状态互斥

```python
_state_lock = asyncio.Lock()
```

- **保护对象**：`_running_processes`、`_task_status`、`_h264_streams`、`_stop_signals`
- **使用模式**：`async with _state_lock:` 包裹所有读写操作（全文件 16 处）
- **注意**：只保护 dict/set 的读写，不保护耗时操作（如网络 I/O）

### 1.3 asyncio.create_task — 异步子任务

| 任务 | 用途 |
|---|---|
| `_start_h264_reader` | 从 pipeline stdout 读 raw BGR 帧 → ffmpeg → fMP4 推流 |
| `_wait_pipeline` | 读取 pipeline stderr 日志，等待进程结束并更新状态 |
| `_viewer_sender` | 每个 WebSocket 观众独立的发送任务，慢观众自动丢帧 |
| `_delayed_cleanup` | 摄像头断开后 10 秒延迟清理 |

### 1.4 asyncio.gather — 并行协程

```python
await asyncio.gather(feed_frames(), read_ffmpeg_output(), drain_ffmpeg_stderr())
```

在 `_start_h264_reader` 中同时运行三个协程：
- `feed_frames()`：pipeline stdout → ffmpeg stdin（喂帧）
- `read_ffmpeg_output()`：ffmpeg stdout → WebSocket 广播（读编码结果）
- `drain_ffmpeg_stderr()`：消费 ffmpeg stderr 防止 pipe 阻塞

### 1.5 asyncio.Queue — 观众推流队列

```python
q: asyncio.Queue = asyncio.Queue(maxsize=30)  # 每观众独立队列
```

- **广播**：`_broadcast_h264()` 向所有观众队列 `put_nowait`
- **消费**：`_viewer_sender()` 从队列取数据发送给 WebSocket
- **丢帧策略**：队列满时丢掉最旧帧，保证观众看到最新画面

### 1.6 asyncio.subprocess — 子进程管理

三个异步子进程：

| 进程 | 输入 | 输出 |
|---|---|---|
| Pipeline (`python -m pipeline`) | 视频文件 / 摄像头 | raw BGR 帧 (stdout) + 日志 (stderr) |
| ffmpeg 编码器 | raw BGR (stdin) | H.264 fMP4 (stdout) |
| ffmpeg 解码器 | H.264 流 (stdin) | raw BGR (stdout) |

全部使用 `asyncio.create_subprocess_exec` 创建，stdin/stdout/stderr 均为 PIPE。

### 1.7 asyncio.wait_for — 超时保护

全文件 11 处使用，防止异步操作永久挂起：

| 场景 | 超时 |
|---|---|
| 视频上传 chunk 读取 | 300s |
| Pipeline stderr 读行 | 300s |
| SIGTERM 后等待进程退出 | 3.0s |
| SIGKILL 后等待进程退出 | 2.0s |
| 观众队列取数据 | 3.0s |
| WebSocket 发送数据 | 5.0s |
| WebRTC 关闭连接 | 3.0s |
| WebSocket 首条消息 | 10.0s |
| WebRTC 视频帧接收 | 5.0s |

### 1.8 asyncio.to_thread / run_in_executor — 线程池卸载

将阻塞 I/O 卸载到线程池，避免阻塞事件循环：

- `to_thread`：文件写入（上传）、视频转码、JPEG 解码/写入
- `run_in_executor`：文件 stat、文件读取、pipe fd 阻塞读取

---

## 二、asyncio → threading 桥接

文件：`web/routes/pipeline_api.py`

### 2.1 threading.Thread + os.pipe — 浏览器摄像头模式

```python
pipe_r, pipe_w = os.pipe()

# Pipeline 线程内：fd 重定向
saved_stdout_fd = os.dup(1)
os.dup2(pipe_w, 1)         # stdout → pipe
# ... pipeline 运行，写入 fd 1 → pipe_w ...
os.dup2(saved_stdout_fd, 1)  # 恢复 stdout
```

**流程**：
1. 主 asyncio 循环创建 `os.pipe()`，得到读端 `pipe_r` 和写端 `pipe_w`
2. 创建 `_FakeProcess` 对象包装 `pipe_r`，模拟 `asyncio.subprocess.Process` 接口
3. 启动 `threading.Thread` 运行同步 pipeline
4. 线程内通过 `os.dup2(pipe_w, 1)` 将 stdout fd 重定向到 pipe
5. Pipeline 的 `open("/dev/stdout", "wb")` 写入的数据全部进入 pipe
6. asyncio 侧的 `_PipeReader(pipe_r)` 从 pipe 读取，喂给 ffmpeg 编码

### 2.2 asyncio.run_coroutine_threadsafe — 线程→事件循环

```python
_main_loop = asyncio.get_event_loop()

def _run_pipeline():
    # ... 在线程中运行 ...
    async def _finish():
        # 清理工作，需要在事件循环中执行
        ...
    asyncio.run_coroutine_threadsafe(_finish(), _main_loop)
```

Pipeline 线程结束时，通过 `run_coroutine_threadsafe` 将清理协程调度回主事件循环执行。

### 2.3 queue.Queue — 跨线程帧传递

```python
frame_queue: queue.Queue = queue.Queue(maxsize=30)
```

- **生产者**：WebSocket 帧接收协程解码 JPEG → `queue.Queue`（线程安全）
- **消费者**：`VirtualCamera.read()` 在 pipeline 线程中从队列取帧
- **满时策略**：`_queue_put_latest()` 丢掉最旧帧，放入最新帧

---

## 三、Pipeline 计算层（threading）

文件：`pipeline/pipeline.py`、`pipeline/tracker.py`

### 3.1 VLM 推理线程池

```python
self._task_queue: queue.Queue(maxsize=...)    # 帧 → 工作线程
self._result_queue: queue.Queue(maxsize=...)  # 工作线程 → 主循环
self._workers: list[threading.Thread]          # 工作线程引用
self._stop_event: threading.Event()            # 停止信号
```

- **启动**：`_start_workers()` 创建 N 个 daemon 线程，每个运行 `_worker_loop`
- **工作循环**：从 `_task_queue` 取帧 → VLM 推理 → 结果放入 `_result_queue`
- **停止**：`_stop_event.set()` 通知所有工作线程退出
- **背压**：队列满时主循环等待（`can_write.wait(timeout=0.5)`）

### 3.2 RawStdoutWriter — 带背压的帧输出

```python
self._lock: threading.Lock()        # 保护 _queue
self._stop: threading.Event()       # 停止信号
self._can_write: threading.Event()  # 背压信号
self._thread: threading.Thread      # 后台写入线程
```

- `write()`：主循环调用，`_can_write.wait(timeout=0.5)` 阻塞等待队列有空位
- `_run()`：后台线程，从队列取帧 → 写入 `sys.stdout.buffer` → 设置 `_can_write`

### 3.3 ScreenshotSaver — 后台截图写入

```python
self._lock: threading.Lock()    # 保护 _queue（max 1 帧）
self._stop: threading.Event()   # 停止信号
self._thread: threading.Thread  # JPEG 写磁盘线程
```

队列最大 1 帧，满时新帧替换旧帧（只保留最新的截图）。

### 3.4 TrackInfoTracker — 跟踪数据保护

```python
self._lock = threading.Lock()  # 保护 _tracks 字典
```

所有方法通过 `with self._lock:` 串行访问跟踪数据，防止主循环和工作线程并发修改。

### 3.5 Agent Trace 日志保护

```python
self._trace_lock = threading.Lock()  # 保护 _agent_trace 列表
```

工作线程写入 trace 记录和主循环读取 trace 时加锁。

---

## 四、数据流全景

```
┌──────────┐    getUserMedia     ┌──────────────────┐
│ 浏览器    │ ──────────────────► │ WebSocket 推帧    │
│ 摄像头    │                    │ (MJPEG/WebRTC)    │
└──────────┘                    └────────┬─────────┘
                                         │
                                    queue.Queue
                                    (maxsize=30)
                                         │
                                         ▼
┌──────────────────────────────────────────────────────┐
│  Pipeline 线程                                        │
│                                                      │
│  VirtualCamera.read()                                │
│       │                                              │
│       ▼                                              │
│  Detector (YOLO) ──► queue.Queue ──► VLM Workers ×N  │
│       │                                │             │
│       │                           queue.Queue        │
│       │                                │             │
│       ▼                                ▼             │
│  Tracker ──► DemoRenderer.render() ──► RawStdoutWriter│
│       │                                   │          │
│  threading.Lock                        threading.Event│
│  (保护 _tracks)                       (背压)         │
└───────────────────────────────────────┬──────────────┘
                                        │
                                   os.pipe / stdout
                                        │
                                        ▼
┌──────────────────────────────────────────────────────┐
│  ffmpeg 编码进程 (asyncio.subprocess)                 │
│  stdin: raw BGR  ──►  stdout: H.264 fMP4             │
└───────────────────────────┬──────────────────────────┘
                            │
                       asyncio.gather
                       (3 个协程并行)
                            │
                            ▼
┌──────────────────────────────────────────────────────┐
│  _broadcast_h264() ──► asyncio.Queue × N 观众        │
│                            │                         │
│                       _viewer_sender                 │
│                       (每观众独立任务)                 │
│                            │                         │
└────────────────────────────┼─────────────────────────┘
                             │
                        WebSocket
                             │
                             ▼
                      ┌────────────┐
                      │  浏览器 MSE │
                      │  H.264 播放│
                      └────────────┘
```

---

## 五、并发安全要点

| 风险点 | 防护机制 |
|---|---|
| 多 pipeline 同时运行抢占 GPU | `asyncio.Semaphore(2)` 限制并行数 |
| 全局状态字典并发读写 | `asyncio.Lock` 保护，16 处加锁 |
| Tracker 跟踪数据被多线程访问 | `threading.Lock` 保护 |
| VLM 推理阻塞主循环 | 独立 `threading.Thread` 工作池 |
| Pipeline 输出帧速度快于编码 | `RawStdoutWriter._can_write` 背压 |
| 浏览器摄像头帧堆积 | `queue.Queue(maxsize=30)` + 丢旧帧 |
| 观众 WebSocket 慢导致积压 | `asyncio.Queue(maxsize=30)` + 丢旧帧 |
| WebSocket 永久挂起 | 11 处 `asyncio.wait_for` 超时保护 |
| Pipeline 线程结束无法通知事件循环 | `asyncio.run_coroutine_threadsafe` |
| 阻塞 I/O 卡死事件循环 | `asyncio.to_thread` / `run_in_executor` |
