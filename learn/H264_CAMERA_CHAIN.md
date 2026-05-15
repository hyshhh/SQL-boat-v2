# 摄像头 Demo H264 全链路

## 链路总览

```
浏览器摄像头
  ↓ getUserMedia(stream)
setupH264CameraWs()                         [pipeline.js]
  ↓ 首条文本消息 {"codec":"h264"}
  ↓ MediaRecorder(stream, avc1.42E01E, 2Mbps) → start(200ms)
  ↓ ondataavailable → arrayBuffer → ws.send(binary)
────────────────── WebSocket /ws/camera/{task_id} ──────────────────
  ↓ first_msg 是 text → 走 H264 分支
_receive_h264_camera_frames(codec="h264")   [pipeline_api.py:2056]
  ↓ "h264" in codec → ffmpeg -f mp4 -i pipe:0 -f rawvideo bgr24 pipe:1
  ↓ 解码帧 → _queue_put_latest(frame_queue)
────────────────── 内存队列 ──────────────────
Pipeline 线程                                [pipeline.py]
  ↓ 从队列取帧 → YOLO推理 → 标注 → raw BGR 写 stdout/pipe
_start_h264_reader()                         [pipeline_api.py:1040]
  ↓ pipe → ffmpeg libx264 ultrafast → fMP4
  ↓ 解析 moof+mdat box → _broadcast_h264()
────────────────── WebSocket /ws/h264/{task_id} ──────────────────
  ↓ send_bytes(init_segment / media_segment)
connectCameraH264()                          [pipeline.js:1197]
  ↓ MediaSource + SourceBuffer (avc1.42C01F)
  ↓ MSE appendBuffer → <video> 播放
```

## 三种摄像头编码模式对比

| | MJPEG | H264 | WebRTC |
|---|---|---|---|
| 前端编码 | Canvas toBlob(JPEG) | MediaRecorder(H264) | 原生 WebRTC track |
| 传输协议 | WebSocket binary | WebSocket binary | RTCPeerConnection |
| 后端解码 | cv2.imdecode | ffmpeg -f mp4 | aiortc |
| 结果推流 | H264 fMP4 / MJPEG | H264 fMP4 | H264 fMP4 |
| 带宽占用 | 高（逐帧JPEG） | 低（连续编码） | 低 |
| 延迟 | 低 | 中（编码缓冲） | 最低 |
| 兼容性 | 最好 | 需浏览器支持 | 需HTTPS |

## 关键代码位置

### 前端

| 文件 | 行号 | 功能 |
|------|------|------|
| `web/static/js/pipeline.js` | 733-737 | 摄像头变量声明 |
| `web/static/js/pipeline.js` | 827-836 | startBrowserCamera 三分支路由 |
| `web/static/js/pipeline.js` | 889-965 | setupH264CameraWs() — H264 推流 |
| `web/static/js/pipeline.js` | 847-886 | setupMjpegCameraWs() — MJPEG 推流 |
| `web/static/js/pipeline.js` | 968+ | setupWebRTCCamera() — WebRTC 推流 |
| `web/static/js/pipeline.js` | 1099-1128 | stopFrameCapture() — 统一清理 |
| `web/static/js/pipeline.js` | 1197-1339 | connectCameraH264() — H264 结果 MSE 播放 |

### 后端

| 文件 | 行号 | 功能 |
|------|------|------|
| `web/routes/pipeline_api.py` | 1542 | POST /start-browser-camera — 启动浏览器摄像头 |
| `web/routes/pipeline_api.py` | 1650 | _start_h264_reader() 无条件启动（结果编码） |
| `web/routes/pipeline_api.py` | 1769 | WS /ws/camera/{task_id} — 接收摄像头帧 |
| `web/routes/pipeline_api.py` | 1798-1817 | 首条消息模式判断（text=H264, binary=MJPEG） |
| `web/routes/pipeline_api.py` | 2056 | _receive_h264_camera_frames() — H264 解码 |
| `web/routes/pipeline_api.py` | 2077 | ffmpeg 命令构建（h264→-f mp4, 其他→自动探测） |
| `web/routes/pipeline_api.py` | 1040 | _start_h264_reader() — raw BGR → ffmpeg H264 |
| `web/routes/pipeline_api.py` | 1259 | _broadcast_h264() — 广播 fMP4 到所有观众 |
| `web/routes/pipeline_api.py` | 1308 | WS /ws/h264/{task_id} — H264 结果推流 |
| `web/routes/pipeline_api.py` | 1890 | WebRTC 帧接收（aiortc） |

### Pipeline

| 文件 | 行号 | 功能 |
|------|------|------|
| `pipeline/pipeline.py` | 341-384 | _FrameWriter — MJPEG 帧写入磁盘 |
| `pipeline/pipeline.py` | 388-448 | _RawStdoutWriter — raw BGR 写 stdout（供 H264） |
| `pipeline/virtual_camera.py` | 全文 | VirtualCamera — 从内存队列读帧 |

## 前端 H264 编码细节

```javascript
// codec 检测优先级
'avc1.42E01E'  // H264 Baseline 3.1 — Chrome/Edge 支持
'vp8'          // VP8 fallback — Firefox 支持

// MediaRecorder 配置
{
  mimeType: useMime,
  videoBitsPerSecond: 2_000_000,  // 2 Mbps
}
recorder.start(200);  // 每 200ms 产出一个 chunk
```

## 后端 ffmpeg 解码参数

```bash
ffmpeg -hide_banner -loglevel info \
  -fflags +nobuffer+discardcorrupt \
  -flags +low_delay \
  -f mp4 \                    # h264 codec 时添加
  -i pipe:0 \                 # stdin: MediaRecorder chunks
  -vf scale=640:480 \         # 强制输出分辨率
  -f rawvideo \               # 输出格式
  -pix_fmt bgr24 \            # OpenCV 兼容
  pipe:1                      # stdout: raw BGR 帧
```

## 后端 H264 编码参数（结果推流）

```bash
ffmpeg -hide_banner -loglevel error \
  -fflags +nobuffer \
  -flags +low_delay \
  -f rawvideo -pix_fmt bgr24 -video_size {w}x{h} \
  -r {fps} \
  -i pipe:0 \
  -c:v libx264 \
  -preset ultrafast -tune zerolatency \
  -profile:v baseline -level 3.1 \
  -bf 0 \                    # 无 B 帧
  -g {fps} \                 # GOP = fps（每秒一个关键帧）
  -threads 2 \
  -pix_fmt yuv420p \
  -movflags +frag_keyframe+empty_moov+default_base_moof+faststart \
  -frag_duration 250000 \    # 0.25s 一个 fragment
  -flush_packets 1 \
  -f mp4 \
  pipe:1
```
