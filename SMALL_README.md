# 快速启动

## 安装依赖

```bash
pip install -e .
```

## 启动 Web 服务

```bash
# 必须加 --ws-ping-interval 0，否则推流时 WebSocket 会崩溃
uvicorn web.app:app --ws-ping-interval 0 --host 0.0.0.0 --port 9000 --reload
```

浏览器打开 http://localhost:9000

## 功能说明

- **数据库管理**：增删改查船只舷号
- **视频 Demo**：上传视频 → YOLO 检测 + VLM 识别 → H.264 实时推流
- **摄像头 Demo**：浏览器摄像头 / 服务器摄像头 / RTSP → 实时识别推流

## 参数说明

| 参数 | 说明 |
|------|------|
| `--ws-ping-interval 0` | 禁用 websockets 内置心跳，防止推流 TCP 缓冲满时连接崩溃 |
| `target_fps` | 目标帧率，低于源帧率时自动跳帧减少计算量 |
| `capture_fps` | 摄像头模式：浏览器推送到后端的帧率 |
| `process_every` | VLM 推理间隔（每 N 帧推理一次） |
| `detect_every` | YOLO 检测间隔（每 N 帧检测一次） |
