"""Pipeline API 路由 — 视频上传、Demo 播放、摄像头流控制"""

from __future__ import annotations

import asyncio
import logging
import sys
import uuid
from pathlib import Path
from typing import Any

from fastapi import APIRouter, File, HTTPException, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, Response, StreamingResponse
from pydantic import BaseModel

from config import load_config

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])

# ── 全局状态 ──
_running_processes: dict[str, asyncio.subprocess.Process] = {}
_task_status: dict[str, dict[str, Any]] = {}


def _get_demo_config() -> dict:
    config = load_config()
    return config.get("demo_video", {})


def _get_demo_dir() -> Path:
    cfg = _get_demo_config()
    return Path(cfg.get("dir", "./demovid"))


def _get_output_dir() -> Path:
    cfg = _get_demo_config()
    return Path(cfg.get("output_dir", "./demo_output"))


def _ensure_dirs():
    _get_demo_dir().mkdir(parents=True, exist_ok=True)
    _get_output_dir().mkdir(parents=True, exist_ok=True)


def _get_allowed_extensions() -> set[str]:
    cfg = _get_demo_config()
    return set(cfg.get("allowed_extensions", [".mp4", ".avi", ".mkv", ".mov", ".flv", ".wmv", ".webm"]))


def _get_stream_dir(task_id: str) -> Path:
    """获取摄像头帧共享目录"""
    d = Path("./_camera_frames") / task_id
    d.mkdir(parents=True, exist_ok=True)
    return d


# ── 请求/响应模型 ──

class PipelineStartRequest(BaseModel):
    video_filename: str
    use_agent: bool = False
    concurrent_mode: bool = True
    display: bool = False


class BrowserCameraStartRequest(BaseModel):
    use_agent: bool = False
    concurrent_mode: bool = True


class PipelineStartResponse(BaseModel):
    success: bool
    message: str
    task_id: str | None = None
    output_filename: str | None = None


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    progress: str | None = None
    output_filename: str | None = None
    error: str | None = None


class VideoListResponse(BaseModel):
    videos: list[dict[str, Any]]


class PipelineStatusResponse(BaseModel):
    running: bool
    active_tasks: int
    tasks: list[dict[str, Any]]


def _safe_filename(filename: str) -> str:
    """安全校验文件名，防止目录遍历"""
    import re
    name = Path(filename).name
    name = re.sub(r'[^\w\-.]', '_', name)
    if not name or name.startswith('.') or '..' in name:
        raise HTTPException(status_code=400, detail="无效的文件名")
    return name


def _is_camera_input(video_filename: str) -> bool:
    """判断是否为摄像头/RTSP 输入"""
    return (
        video_filename.startswith("__camera__")
        or video_filename.startswith("rtsp://")
        or video_filename.startswith("rtmp://")
        or video_filename.startswith("http://")
        or video_filename.startswith("https://")
    )


def _get_video_path(video_filename: str) -> Path | None:
    """获取视频文件路径，摄像头输入返回 None"""
    if _is_camera_input(video_filename):
        return None
    demo_dir = _get_demo_dir()
    return demo_dir / _safe_filename(video_filename)


# ── 视频管理 ──

@router.get("/videos", response_model=VideoListResponse)
async def list_videos():
    """获取 demo 视频列表"""
    _ensure_dirs()
    demo_dir = _get_demo_dir()
    allowed = _get_allowed_extensions()
    videos = []
    for f in sorted(demo_dir.iterdir()):
        if f.is_file() and f.suffix.lower() in allowed:
            stat = f.stat()
            videos.append({
                "filename": f.name,
                "size_mb": round(stat.st_size / (1024 * 1024), 2),
                "modified": stat.st_mtime,
            })
    return VideoListResponse(videos=videos)


@router.post("/videos/upload")
async def upload_video(file: UploadFile = File(...)):
    """上传视频到 demovid 目录（流式写入，带大小预检和超时保护）"""
    _ensure_dirs()
    cfg = _get_demo_config()
    max_size = cfg.get("max_file_size_mb", 500) * 1024 * 1024
    allowed = _get_allowed_extensions()

    filename = file.filename or "upload.mp4"
    filename = Path(filename).name
    if not filename or filename.startswith('.'):
        filename = "upload.mp4"
    ext = Path(filename).suffix.lower()
    if ext not in allowed:
        raise HTTPException(status_code=400, detail=f"不支持的视频格式: {ext}，支持: {', '.join(sorted(allowed))}")

    # ── Content-Length 预检（拒绝明显过大的请求）──
    content_length = file.size  # Starlette 从 Content-Length header 读取
    if content_length and content_length > max_size:
        raise HTTPException(
            status_code=413,
            detail=f"文件过大 ({content_length / 1024 / 1024:.0f}MB)，最大 {cfg.get('max_file_size_mb', 500)}MB",
        )

    demo_dir = _get_demo_dir()
    save_path = demo_dir / filename
    if save_path.exists():
        stem = save_path.stem
        suffix = save_path.suffix
        counter = 1
        while save_path.exists():
            save_path = demo_dir / f"{stem}_{counter}{suffix}"
            counter += 1

    total_bytes = 0
    chunk_size = 1024 * 1024  # 1MB
    last_activity = asyncio.get_event_loop().time()
    timeout_seconds = 120  # 2 分钟无数据则超时

    try:
        with open(save_path, "wb") as f:
            while True:
                try:
                    chunk = await asyncio.wait_for(
                        file.read(chunk_size),
                        timeout=timeout_seconds,
                    )
                except asyncio.TimeoutError:
                    raise HTTPException(
                        status_code=408,
                        detail=f"上传超时（{timeout_seconds} 秒无数据）",
                    )

                if not chunk:
                    break

                total_bytes += len(chunk)
                if total_bytes > max_size:
                    raise HTTPException(
                        status_code=413,
                        detail=f"文件过大，最大 {cfg.get('max_file_size_mb', 500)}MB",
                    )

                # 异步写入，避免阻塞事件循环
                await asyncio.to_thread(f.write, chunk)
                last_activity = asyncio.get_event_loop().time()

    except HTTPException:
        # 清理已写入的部分文件（句柄已由 with 关闭）
        save_path.unlink(missing_ok=True)
        raise
    except Exception:
        save_path.unlink(missing_ok=True)
        raise

    logger.info("视频已上传: %s (%.2f MB)", save_path.name, total_bytes / (1024 * 1024))

    return {
        "success": True,
        "message": f"视频已上传: {save_path.name}",
        "filename": save_path.name,
        "size_mb": round(total_bytes / (1024 * 1024), 2),
    }


@router.delete("/videos/{filename}")
async def delete_video(filename: str):
    """删除 demo 视频"""
    filename = _safe_filename(filename)
    demo_dir = _get_demo_dir()
    video_path = demo_dir / filename
    if not video_path.exists():
        raise HTTPException(status_code=404, detail=f"视频不存在: {filename}")
    video_path.unlink()
    return {"success": True, "message": f"已删除: {filename}"}


# ── Pipeline 控制 ──

@router.post("/start", response_model=PipelineStartResponse)
async def start_pipeline(req: PipelineStartRequest):
    """启动视频处理 Pipeline（支持文件和摄像头/RTSP 输入）"""
    _ensure_dirs()

    is_camera = _is_camera_input(req.video_filename)
    task_id = str(uuid.uuid4())[:8]
    output_dir = _get_output_dir()

    if is_camera:
        video_source = req.video_filename
        if video_source.startswith("__camera__"):
            cam_id = video_source.replace("__camera__", "")
            video_source = cam_id
        output_filename = f"camera_{task_id}.mp4"
    else:
        video_path = _get_video_path(req.video_filename)
        if video_path is None or not video_path.exists():
            raise HTTPException(status_code=404, detail=f"视频不存在: {req.video_filename}")
        video_source = str(video_path)
        stem = Path(req.video_filename).stem
        output_filename = f"{stem}_result_{task_id}.mp4"

    output_path = output_dir / output_filename

    # 构建 pipeline 命令
    config = load_config()
    pipeline_cfg = config.get("pipeline", {})

    cmd = [
        sys.executable, "-m", "pipeline",
        video_source,
        "--output", str(output_path),
    ]
    if req.use_agent:
        cmd.append("--agent")
    if req.concurrent_mode:
        cmd.extend(["-c", "--max-concurrent", str(pipeline_cfg.get("max_concurrent", 4))])
    if is_camera:
        cmd.append("--camera")
        # 摄像头模式：通过 stream-dir 实现 MJPEG 流，不需要 --display
        stream_dir = _get_stream_dir(task_id)
        cmd.extend(["--stream-dir", str(stream_dir)])
    else:
        # 文件模式：实时预览 + 输出视频
        stream_dir = _get_stream_dir(task_id)
        cmd.extend(["--stream-dir", str(stream_dir)])
        if req.display:
            cmd.append("--display")
    cmd.append("--demo")

    logger.info("启动 Pipeline: %s (camera=%s)", " ".join(cmd), is_camera)

    _task_status[task_id] = {
        "task_id": task_id,
        "status": "running",
        "video_filename": req.video_filename,
        "output_filename": output_filename,
        "output_path": str(output_path),
        "progress": "处理中...",
        "error": None,
        "is_camera": is_camera,
    }

    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(Path.cwd()),
        )
        _running_processes[task_id] = process
        asyncio.create_task(_wait_pipeline(task_id, process, output_filename))

    except FileNotFoundError:
        _task_status[task_id]["status"] = "failed"
        _task_status[task_id]["error"] = "pipeline 模块不存在，请确认 pipeline 目录已实现"
        raise HTTPException(status_code=500, detail="pipeline 模块不存在")

    return PipelineStartResponse(
        success=True,
        message=f"Pipeline 已启动，任务 ID: {task_id}",
        task_id=task_id,
        output_filename=output_filename,
    )


async def _read_stderr(process: asyncio.subprocess.Process) -> bytes:
    """并发读取 stderr，避免 stdout/stderr 缓冲区死锁"""
    return await process.stderr.read()


async def _wait_pipeline(task_id: str, process: asyncio.subprocess.Process, output_filename: str):
    """异步等待 pipeline 完成，实时解析进度"""
    # 启动并发 stderr 读取，避免缓冲区满导致死锁
    stderr_task = asyncio.create_task(_read_stderr(process))
    try:
        while True:
            line = await process.stdout.readline()
            if not line:
                break
            text = line.decode("utf-8", errors="replace").strip()
            if not text:
                continue

            if "进度" in text or "progress" in text.lower() or "%" in text or "处理帧" in text:
                _task_status[task_id]["progress"] = text

            if text.startswith("__PIPELINE_SUMMARY__:"):
                try:
                    import json
                    summary = json.loads(text.replace("__PIPELINE_SUMMARY__:", ""))
                    _task_status[task_id]["summary"] = summary
                except Exception:
                    pass

            logger.info("[%s] %s", task_id, text)

        await process.wait()
        stderr = await stderr_task

        if process.returncode == 0:
            _task_status[task_id]["status"] = "completed"
            _task_status[task_id]["progress"] = "处理完成"
            logger.info("Pipeline 完成: %s", task_id)
        else:
            _task_status[task_id]["status"] = "failed"
            error_msg = stderr.decode("utf-8", errors="replace")[-500:] if stderr else "未知错误"
            _task_status[task_id]["error"] = error_msg
            logger.error("Pipeline 失败 [%s]: %s", task_id, error_msg)
    except Exception as e:
        _task_status[task_id]["status"] = "failed"
        _task_status[task_id]["error"] = str(e)
        logger.error("Pipeline 异常 [%s]: %s", task_id, e)
    finally:
        _running_processes.pop(task_id, None)
        # 浏览器摄像头的帧目录由 WebSocket 断开后的 _delayed_cleanup 管理，
        # 这里不要提前清理，否则 MJPEG 流和 WebSocket 写入会因目录消失而中断
        is_browser_cam = _task_status.get(task_id, {}).get("is_browser_camera", False)
        if not is_browser_cam:
            _cleanup_stream_dir(task_id)


@router.get("/status", response_model=PipelineStatusResponse)
async def get_pipeline_status():
    """获取所有 Pipeline 任务状态"""
    tasks = list(_task_status.values())
    running = sum(1 for t in tasks if t["status"] == "running")
    return PipelineStatusResponse(running=running > 0, active_tasks=running, tasks=tasks)


@router.get("/status/{task_id}", response_model=TaskStatusResponse)
async def get_task_status(task_id: str):
    """获取单个任务状态"""
    if task_id not in _task_status:
        raise HTTPException(status_code=404, detail=f"任务不存在: {task_id}")
    t = _task_status[task_id]
    return TaskStatusResponse(**t)


@router.post("/stop/{task_id}")
async def stop_pipeline(task_id: str):
    """停止正在运行的 Pipeline"""
    if task_id not in _running_processes:
        raise HTTPException(status_code=404, detail=f"任务不存在或已结束: {task_id}")
    process = _running_processes[task_id]
    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=5.0)
    except asyncio.TimeoutError:
        process.kill()
    _task_status[task_id]["status"] = "failed"
    _task_status[task_id]["error"] = "用户手动停止"
    _running_processes.pop(task_id, None)
    # 清理帧目录
    _cleanup_stream_dir(task_id)
    return {"success": True, "message": f"已停止任务: {task_id}"}


def _cleanup_stream_dir(task_id: str):
    """清理帧共享目录"""
    import shutil
    d = Path("./_camera_frames") / task_id
    if d.exists():
        shutil.rmtree(d, ignore_errors=True)
    # 同时清理浏览器帧目录
    d2 = Path("./_browser_frames") / task_id
    if d2.exists():
        shutil.rmtree(d2, ignore_errors=True)


async def _delayed_cleanup(task_id: str, delay: int = 10):
    """延迟清理：等待 pipeline 自然结束后再清理目录"""
    await asyncio.sleep(delay)
    # 如果 pipeline 还在运行，终止它
    if task_id in _running_processes:
        try:
            _running_processes[task_id].terminate()
            await asyncio.wait_for(_running_processes[task_id].wait(), timeout=5)
        except Exception:
            pass
        _running_processes.pop(task_id, None)
    # 更新状态
    if task_id in _task_status and _task_status[task_id]["status"] == "running":
        _task_status[task_id]["status"] = "completed"
        _task_status[task_id]["progress"] = "处理完成（摄像头已断开）"
    # 清理帧目录
    _cleanup_stream_dir(task_id)


def _get_browser_frames_dir(task_id: str) -> Path:
    """获取浏览器摄像头帧目录"""
    d = Path("./_browser_frames") / task_id
    d.mkdir(parents=True, exist_ok=True)
    return d


# ── 摄像头 MJPEG 流 ──

@router.get("/stream/{task_id}")
async def camera_stream(task_id: str):
    """MJPEG 实时流 — 从 pipeline 写入的 latest.jpg 读取帧"""
    if task_id not in _task_status:
        raise HTTPException(status_code=404, detail=f"任务不存在: {task_id}")

    stream_dir = _get_stream_dir(task_id)
    frame_file = stream_dir / "latest.jpg"

    async def generate():
        boundary = "--frame"
        # 发送一个 1x1 黑色 JPEG 作为初始帧，确保 img 标签立即开始渲染
        _black_jpeg = (
            b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00'
            b'\xff\xdb\x00C\x00\x08\x06\x06\x07\x06\x05\x08\x07\x07\x07\t\t'
            b'\x08\n\x0c\x14\r\x0c\x0b\x0b\x0c\x19\x12\x13\x0f\x14\x1d\x1a'
            b'\x1f\x1e\x1d\x1a\x1c\x1c $.\' ",#\x1c\x1c(7),01444\x1f\'9=82<.342'
            b'\xff\xc0\x00\x0b\x08\x00\x01\x00\x01\x01\x01\x11\x00'
            b'\xff\xc4\x00\x1f\x00\x00\x01\x05\x01\x01\x01\x01\x01\x01\x00\x00'
            b'\x00\x00\x00\x00\x00\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b'
            b'\xff\xda\x00\x08\x01\x01\x00\x00?\x00T\xdb\x9e\xb7\xa7\x93\x95'
            b'\xff\xd9'
        )
        while task_id in _task_status and _task_status[task_id]["status"] == "running":
            if frame_file.exists():
                try:
                    frame_data = frame_file.read_bytes()
                    if frame_data:
                        yield (
                            f"{boundary}\r\n"
                            f"Content-Type: image/jpeg\r\n\r\n"
                        ).encode() + frame_data + b"\r\n"
                    else:
                        await asyncio.sleep(0.05)
                        continue
                except (OSError, FileNotFoundError):
                    await asyncio.sleep(0.05)
                    continue
            else:
                # 发送黑色占位图而非 text/plain，img 标签可以正常渲染
                yield (
                    f"{boundary}\r\n"
                    f"Content-Type: image/jpeg\r\n\r\n"
                ).encode() + _black_jpeg + b"\r\n"
            await asyncio.sleep(0.05)  # ~20fps

    return StreamingResponse(
        generate(),
        media_type="multipart/x-mixed-replace; boundary=frame",
    )


# ── 浏览器摄像头 ──

@router.post("/start-browser-camera", response_model=PipelineStartResponse)
async def start_browser_camera(req: BrowserCameraStartRequest):
    """启动浏览器摄像头 Pipeline（等待 WebSocket 推流）"""
    _ensure_dirs()
    config = load_config()
    pipeline_cfg = config.get("pipeline", {})

    task_id = str(uuid.uuid4())[:8]
    output_dir = _get_output_dir()
    output_filename = f"browser_cam_{task_id}.mp4"
    output_path = output_dir / output_filename
    frames_dir = _get_browser_frames_dir(task_id)
    stream_dir = _get_stream_dir(task_id)

    cmd = [
        sys.executable, "-m", "pipeline",
        f"browser_camera_{task_id}",
        "--frames-dir", str(frames_dir),
        "--virtual-fps", "15",
        "--output", str(output_path),
        "--stream-dir", str(stream_dir),
        "--camera",
        "--demo",
    ]
    if req.use_agent:
        cmd.append("--agent")
    if req.concurrent_mode:
        cmd.extend(["-c", "--max-concurrent", str(pipeline_cfg.get("max_concurrent", 4))])

    logger.info("启动浏览器摄像头 Pipeline: %s", " ".join(cmd))

    _task_status[task_id] = {
        "task_id": task_id,
        "status": "running",
        "video_filename": f"浏览器摄像头 ({task_id})",
        "output_filename": output_filename,
        "output_path": str(output_path),
        "progress": "等待摄像头连接...",
        "error": None,
        "is_camera": True,
        "is_browser_camera": True,
    }

    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(Path.cwd()),
        )
        _running_processes[task_id] = process
        asyncio.create_task(_wait_pipeline(task_id, process, output_filename))
    except FileNotFoundError:
        _task_status[task_id]["status"] = "failed"
        _task_status[task_id]["error"] = "pipeline 模块不存在"
        raise HTTPException(status_code=500, detail="pipeline 模块不存在")

    return PipelineStartResponse(
        success=True,
        message=f"浏览器摄像头 Pipeline 已启动，请连接 WebSocket 推流",
        task_id=task_id,
        output_filename=output_filename,
    )


@router.websocket("/ws/camera/{task_id}")
async def browser_camera_ws(websocket: WebSocket, task_id: str):
    """WebSocket 端点 — 接收浏览器摄像头 JPEG 帧"""
    if task_id not in _task_status:
        await websocket.close(code=4004, reason="任务不存在")
        return

    frames_dir = _get_browser_frames_dir(task_id)
    await websocket.accept()
    logger.info("浏览器摄像头 WebSocket 已连接: %s", task_id)

    _task_status[task_id]["progress"] = "摄像头已连接，推流中..."

    frame_count = 0
    try:
        while _task_status.get(task_id, {}).get("status") == "running":
            data = await websocket.receive_bytes()

            # 验证 JPEG 魔数
            if len(data) < 3 or data[:2] != b'\xff\xd8':
                await websocket.send_json({"ok": False, "error": "非 JPEG 数据"})
                continue

            # 原子写入（用 asyncio.to_thread 避免阻塞事件循环）
            frame_path = frames_dir / "latest.jpg"
            tmp_path = frames_dir / "latest.jpg.tmp"

            def _write_frame():
                tmp_path.write_bytes(data)
                tmp_path.rename(frame_path)

            await asyncio.to_thread(_write_frame)

            frame_count += 1
            if frame_count % 30 == 0:
                logger.debug("浏览器摄像头帧计数: %d", frame_count)
                # 每 30 帧发一次 ack（减少网络开销）
                try:
                    await websocket.send_json({"ok": True, "frame": frame_count})
                except Exception:
                    pass

    except WebSocketDisconnect:
        logger.info("浏览器摄像头 WebSocket 断开: %s (共 %d 帧)", task_id, frame_count)
    except Exception as e:
        logger.error("浏览器摄像头 WebSocket 异常: %s", e)
    finally:
        # WebSocket 断开 → 标记断开，但不立即终止 pipeline
        # pipeline 会因帧目录被删/无新帧而自动退出
        logger.info("浏览器摄像头推流结束: %s (共 %d 帧)", task_id, frame_count)
        if task_id in _task_status and _task_status[task_id]["status"] == "running":
            _task_status[task_id]["progress"] = f"摄像头已断开（共接收 {frame_count} 帧），等待 pipeline 结束..."
        # 延迟清理：给 pipeline 时间写完输出视频
        asyncio.create_task(_delayed_cleanup(task_id, delay=10))


# ── 结果视频 ──

@router.get("/outputs")
async def list_outputs():
    """获取已完成的 Demo 输出视频列表"""
    _ensure_dirs()
    output_dir = _get_output_dir()
    allowed = _get_allowed_extensions()
    outputs = []
    for f in sorted(output_dir.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True):
        if f.is_file() and f.suffix.lower() in allowed:
            stat = f.stat()
            outputs.append({
                "filename": f.name,
                "size_mb": round(stat.st_size / (1024 * 1024), 2),
                "modified": stat.st_mtime,
            })
    return {"outputs": outputs}


@router.get("/outputs/{filename}")
async def get_output_video(filename: str):
    """下载/播放输出视频"""
    filename = _safe_filename(filename)
    output_dir = _get_output_dir()
    video_path = output_dir / filename
    if not video_path.exists():
        raise HTTPException(status_code=404, detail=f"视频不存在: {filename}")

    ext = video_path.suffix.lower()
    mime_map = {
        ".mp4": "video/mp4", ".avi": "video/x-msvideo",
        ".mkv": "video/x-matroska", ".mov": "video/quicktime",
        ".flv": "video/x-flv", ".wmv": "video/x-ms-wmv", ".webm": "video/webm",
    }
    return FileResponse(path=str(video_path), media_type=mime_map.get(ext, "video/mp4"), filename=filename)


@router.get("/video/{filename}")
async def get_source_video(filename: str):
    """获取源视频用于播放"""
    filename = _safe_filename(filename)
    demo_dir = _get_demo_dir()
    video_path = demo_dir / filename
    if not video_path.exists():
        raise HTTPException(status_code=404, detail=f"视频不存在: {filename}")

    ext = video_path.suffix.lower()
    mime_map = {
        ".mp4": "video/mp4", ".avi": "video/x-msvideo",
        ".mkv": "video/x-matroska", ".mov": "video/quicktime",
        ".flv": "video/x-flv", ".wmv": "video/x-ms-wmv", ".webm": "video/webm",
    }
    return FileResponse(path=str(video_path), media_type=mime_map.get(ext, "video/mp4"), filename=filename)


# ── 清理历史 ──

@router.delete("/tasks/clear")
async def clear_finished_tasks():
    """清除已完成/失败的任务记录"""
    global _task_status
    before = len(_task_status)
    _task_status = {k: v for k, v in _task_status.items() if v["status"] == "running"}
    cleared = before - len(_task_status)
    return {"success": True, "message": f"已清除 {cleared} 条历史记录"}
