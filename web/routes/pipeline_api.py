"""Pipeline API 路由 — 视频上传、Demo 播放、摄像头流控制"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import queue
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from fastapi import APIRouter, File, HTTPException, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, Response, StreamingResponse
from pydantic import BaseModel

from config import load_config

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])

# ── 并发控制 ──
# 限制同时运行的 pipeline 数量，防止多人访问时 GPU/CPU 被打爆
_MAX_PARALLEL_PIPELINES = 2
_pipeline_semaphore: asyncio.Semaphore | None = None
_state_lock = asyncio.Lock()


def _get_semaphore() -> asyncio.Semaphore:
    """延迟初始化信号量，从 config 读取最大并发数"""
    global _pipeline_semaphore, _MAX_PARALLEL_PIPELINES
    if _pipeline_semaphore is None:
        try:
            config = load_config()
            _MAX_PARALLEL_PIPELINES = config.get("pipeline", {}).get("max_parallel_pipelines", 2)
        except Exception:
            pass
        _pipeline_semaphore = asyncio.Semaphore(_MAX_PARALLEL_PIPELINES)
    return _pipeline_semaphore


# ── 全局状态 ──
_running_processes: dict[str, asyncio.subprocess.Process] = {}
_task_status: dict[str, dict[str, Any]] = {}
_stop_signals: set[str] = set()  # 已发送停止信号的任务

# ── H.264 推流状态 ──
# 每个 task 的 ffmpeg 进程和 WebSocket 观众
_h264_streams: dict[str, dict[str, Any]] = {}  # task_id → {ffmpeg, viewers, init_segment, ...}

# ── Pipeline 日志缓冲 ──
_pipeline_logs: dict[str, list[dict]] = {}  # task_id → [{time, line}, ...]
_log_start: dict[str, int] = {}             # task_id → logs[0] 的全局索引
_MAX_LOG_LINES = 10  # 每个任务最大日志条数，超出后 FIFO 清理


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


def _probe_camera_resolution(source: str) -> tuple[int, int] | None:
    """快速探测摄像头/RTSP 分辨率，用于 H.264 编码器初始化。"""
    import cv2
    try:
        cap_source = int(source) if source.isdigit() else source
        cap = cv2.VideoCapture(cap_source)
        if not cap.isOpened():
            return None
        w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        if w <= 0 or h <= 0:
            ret, frame = cap.read()
            if ret and frame is not None:
                h, w = frame.shape[:2]
        cap.release()
        return (w, h) if w > 0 and h > 0 else None
    except Exception:
        return None


def _get_stream_dir(task_id: str) -> Path:
    """获取摄像头帧共享目录"""
    d = Path("./_camera_frames") / task_id
    d.mkdir(parents=True, exist_ok=True)
    return d


# ── 请求/响应模型 ──

class PipelineStartRequest(BaseModel):
    video_filename: str
    concurrent_mode: bool = True
    display: bool = False
    # ── 核心检测参数 ──
    conf_threshold: float = 0.25
    iou_threshold: float = 0.45
    process_every: int = 15
    detect_every: int = 2
    target_fps: float = 0
    capture_fps: int = 15  # 摄像头推帧帧率
    pipe_scale: float = 0.5  # pipe 输出缩放系数 (0.1-1.0)
    # ── 高级参数 ──
    max_frames: int = 0
    device: str = ""
    yolo_model: str = ""
    prompt_mode: str = "detailed"
    enable_refresh: bool = True
    gap_num: int = 150
    max_concurrent: int = 4


class BrowserCameraStartRequest(BaseModel):
    concurrent_mode: bool = True
    stream_mode: str = "mjpeg"  # "mjpeg" 或 "h264"
    # ── 核心检测参数 ──
    conf_threshold: float = 0.25
    iou_threshold: float = 0.45
    process_every: int = 15
    detect_every: int = 2
    target_fps: float = 0
    capture_fps: int = 15  # 浏览器推帧帧率
    pipe_scale: float = 0.5  # pipe 输出缩放系数 (0.1-1.0)
    # ── 高级参数 ──
    max_frames: int = 0
    device: str = ""
    yolo_model: str = ""
    prompt_mode: str = "detailed"
    enable_refresh: bool = True
    gap_num: int = 150
    max_concurrent: int = 4


class PipelineStartResponse(BaseModel):
    success: bool
    message: str
    task_id: str | None = None
    output_filename: str | None = None
    capture_fps: int | None = None


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


# ── 视频编码检测与转码 ──

_BROWSER_COMPATIBLE_CODECS = {"h264", "vp8", "vp9", "av1", "mpeg4part10"}

def _find_binary(name: str) -> str | None:
    """查找二进制文件，支持多种路径。"""
    import shutil
    # 1. shutil.which (最可靠，检查 PATH + 可执行权限)
    found = shutil.which(name)
    if found:
        return found
    # 2. 常见绝对路径
    for path in [f"/usr/bin/{name}", f"/usr/local/bin/{name}", f"/snap/bin/{name}"]:
        if os.path.isfile(path) and os.access(path, os.X_OK):
            return path
    return None

_FFMPEG: str | None = None
_FFPROBE: str | None = None

def _ensure_ffmpeg():
    """延迟查找 ffmpeg/ffprobe，首次调用时检测。"""
    global _FFMPEG, _FFPROBE
    if _FFMPEG is None:
        _FFMPEG = _find_binary("ffmpeg") or ""
        logger.info("ffmpeg 查找结果: %s", _FFMPEG or "(未找到)")
    if _FFPROBE is None:
        _FFPROBE = _find_binary("ffprobe") or ""
        logger.info("ffprobe 查找结果: %s", _FFPROBE or "(未找到)")

def _probe_codec(video_path: str) -> str | None:
    """用 ffprobe 检测视频编码。"""
    _ensure_ffmpeg()
    if not _FFPROBE:
        logger.warning("ffprobe 不可用，跳过编码检测")
        return None
    try:
        ret = subprocess.run(
            [_FFPROBE, "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=codec_name",
             "-of", "default=noprint_wrappers=1:nokey=1", video_path],
            capture_output=True, text=True, timeout=30,
        )
        if ret.returncode == 0:
            codec = ret.stdout.strip().lower()
            if codec:
                return codec
            logger.warning("ffprobe 输出为空 (rc=%d): %s | stderr: %s", ret.returncode, video_path, ret.stderr.strip()[:200])
        else:
            logger.warning("ffprobe 失败 (rc=%d): %s | stderr: %s", ret.returncode, video_path, ret.stderr.strip()[:200])
    except Exception as e:
        logger.warning("ffprobe 异常: %s → %s", video_path, e)
    return None


def _probe_video_size(video_path: str) -> tuple[int, int] | None:
    """用 ffprobe 检测视频分辨率。"""
    _ensure_ffmpeg()
    if not _FFPROBE:
        return None
    try:
        ret = subprocess.run(
            [_FFPROBE, "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=width,height",
             "-of", "default=noprint_wrappers=1:nokey=1", video_path],
            capture_output=True, text=True, timeout=30,
        )
        if ret.returncode == 0:
            lines = ret.stdout.strip().split("\n")
            if len(lines) >= 2:
                w, h = int(lines[0]), int(lines[1])
                if w > 0 and h > 0:
                    return w, h
    except Exception as e:
        logger.warning("ffprobe 分辨率检测失败: %s → %s", video_path, e)
    return None


def _is_browser_compatible(video_path: str) -> bool:
    """检测视频是否被浏览器原生兼容。"""
    codec = _probe_codec(video_path)
    if codec is None:
        return True  # 检测失败时假设兼容，避免不必要的转码
    return codec in _BROWSER_COMPATIBLE_CODECS


def _ensure_h264(video_path: Path) -> Path:
    """
    确保视频为浏览器兼容的 H264 编码。
    如果不是，自动转码并缓存到 _transcoded/ 目录。
    返回可播放的视频路径。
    """
    codec = _probe_codec(str(video_path))
    logger.info("视频编码检测: %s → codec=%s", video_path.name, codec)

    if codec is None:
        # ffprobe 检测失败 — 跳过转码，直接返回原文件让浏览器尝试
        logger.warning("编码检测失败，跳过转码: %s", video_path.name)
        return video_path

    if codec in _BROWSER_COMPATIBLE_CODECS:
        return video_path  # 已兼容，直接返回

    # 需要转码 — 先检查 ffmpeg 是否可用
    _ensure_ffmpeg()
    if not _FFMPEG:
        logger.error("ffmpeg 不可用，无法转码 %s (codec=%s)", video_path.name, codec)
        return video_path

    # 转码
    transcoded_dir = video_path.parent / "_transcoded"
    transcoded_dir.mkdir(parents=True, exist_ok=True)
    transcoded_path = transcoded_dir / video_path.name

    # 如果已转码过且比源文件新，直接使用
    if transcoded_path.exists() and transcoded_path.stat().st_mtime >= video_path.stat().st_mtime:
        logger.info("使用已缓存的转码文件: %s", transcoded_path)
        return transcoded_path

    logger.info("视频编码 %s 不兼容浏览器，转码为 H264: %s → %s", codec, video_path.name, transcoded_path)
    try:
        ret = subprocess.run(
            [_FFMPEG, "-y", "-i", str(video_path),
             "-c:v", "libx264", "-preset", "fast", "-crf", "23",
             "-pix_fmt", "yuv420p",
             "-c:a", "aac", "-b:a", "128k",
             "-movflags", "+faststart",
             str(transcoded_path)],
            capture_output=True, timeout=600,
        )
        if ret.returncode == 0 and transcoded_path.exists() and transcoded_path.stat().st_size > 0:
            logger.info("转码成功: %s (%.1f MB)", transcoded_path.name, transcoded_path.stat().st_size / 1024 / 1024)
            return transcoded_path
        logger.error("转码失败: %s", ret.stderr.decode()[-300:] if ret.stderr else "未知错误")
    except Exception as e:
        logger.error("转码异常: %s", e)

    # 转码失败，返回原文件（浏览器可能无法播放）
    return video_path


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
    """删除 demo 视频（同时清理转码缓存）"""
    filename = _safe_filename(filename)
    demo_dir = _get_demo_dir()
    video_path = demo_dir / filename
    if not video_path.exists():
        raise HTTPException(status_code=404, detail=f"视频不存在: {filename}")
    video_path.unlink()
    # 清理转码缓存
    transcoded = demo_dir / "_transcoded" / filename
    if transcoded.exists():
        transcoded.unlink()
        logger.info("已清理转码缓存: %s", transcoded)
    return {"success": True, "message": f"已删除: {filename}"}


# ── 视频编码检测 ──

@router.get("/debug/ffmpeg")
async def debug_ffmpeg():
    """诊断 ffmpeg/ffprobe 可用性"""
    _ensure_ffmpeg()
    result = {
        "ffmpeg_path": _FFMPEG,
        "ffprobe_path": _FFPROBE,
        "ffmpeg_exists": os.path.isfile(_FFMPEG) if _FFMPEG else False,
        "ffprobe_exists": os.path.isfile(_FFPROBE) if _FFPROBE else False,
    }

    # 测试 ffprobe 执行
    if _FFPROBE:
        try:
            ret = subprocess.run(
                [_FFPROBE, "-version"],
                capture_output=True, text=True, timeout=10,
            )
            result["ffprobe_version"] = ret.stdout.strip()[:200]
            result["ffprobe_rc"] = ret.returncode
            if ret.returncode != 0:
                result["ffprobe_stderr"] = ret.stderr.strip()[:200]
        except Exception as e:
            result["ffprobe_error"] = str(e)

    # 测试 ffmpeg 执行
    if _FFMPEG:
        try:
            ret = subprocess.run(
                [_FFMPEG, "-version"],
                capture_output=True, text=True, timeout=10,
            )
            result["ffmpeg_version"] = ret.stdout.strip()[:200]
            result["ffmpeg_rc"] = ret.returncode
            if ret.returncode != 0:
                result["ffmpeg_stderr"] = ret.stderr.strip()[:200]
        except Exception as e:
            result["ffmpeg_error"] = str(e)

    return result

@router.get("/videos/{filename}/codec")
async def check_video_codec(filename: str):
    """检测视频编码格式及浏览器兼容性"""
    filename = _safe_filename(filename)
    demo_dir = _get_demo_dir()
    video_path = demo_dir / filename
    if not video_path.exists():
        raise HTTPException(status_code=404, detail=f"视频不存在: {filename}")

    codec = _probe_codec(str(video_path))
    compatible = codec in _BROWSER_COMPATIBLE_CODECS if codec else True

    # 检查是否有已转码的缓存
    transcoded_path = video_path.parent / "_transcoded" / video_path.name
    has_transcoded = transcoded_path.exists() and transcoded_path.stat().st_mtime >= video_path.stat().st_mtime

    # ffmpeg 可用性
    _ensure_ffmpeg()

    return {
        "filename": filename,
        "codec": codec,
        "browser_compatible": compatible,
        "has_transcoded_cache": has_transcoded,
        "ffmpeg_available": bool(_FFMPEG),
        "ffprobe_available": bool(_FFPROBE),
    }


@router.post("/videos/{filename}/transcode")
async def transcode_video(filename: str):
    """手动触发视频转码为 H264（浏览器兼容）"""
    filename = _safe_filename(filename)
    demo_dir = _get_demo_dir()
    video_path = demo_dir / filename
    if not video_path.exists():
        raise HTTPException(status_code=404, detail=f"视频不存在: {filename}")

    codec = _probe_codec(str(video_path))
    if codec and codec in _BROWSER_COMPATIBLE_CODECS:
        return {"success": True, "message": f"视频已是浏览器兼容格式 ({codec})，无需转码", "codec": codec}

    # 异步转码
    result_path = await asyncio.to_thread(_ensure_h264, video_path)
    if result_path == video_path:
        raise HTTPException(status_code=500, detail="转码失败，请检查 ffmpeg 是否可用")

    new_codec = _probe_codec(str(result_path))
    return {
        "success": True,
        "message": f"转码完成: {codec} → {new_codec}",
        "original_codec": codec,
        "new_codec": new_codec,
        "cached_path": str(result_path),
    }


# ── Pipeline 控制 ──

@router.post("/start", response_model=PipelineStartResponse)
async def start_pipeline(req: PipelineStartRequest):
    """启动视频处理 Pipeline（支持文件和摄像头/RTSP 输入）"""
    _ensure_dirs()

    # 并发控制：检查是否已达上限
    sem = _get_semaphore()
    running_count = sum(1 for t in _task_status.values() if t["status"] == "running")
    if running_count >= _MAX_PARALLEL_PIPELINES:
        raise HTTPException(
            status_code=429,
            detail=f"已有 {running_count} 个 Pipeline 在运行（上限 {_MAX_PARALLEL_PIPELINES}），请等待完成后再试",
        )

    is_camera = _is_camera_input(req.video_filename)
    task_id = str(uuid.uuid4())[:8]

    if is_camera:
        video_source = req.video_filename
        if video_source.startswith("__camera__"):
            cam_id = video_source.replace("__camera__", "")
            video_source = cam_id
    else:
        video_path = _get_video_path(req.video_filename)
        if video_path is None or not video_path.exists():
            raise HTTPException(status_code=404, detail=f"视频不存在: {req.video_filename}")
        video_source = str(video_path)

    # 探测视频分辨率（H.264 编码需要知道帧尺寸）
    video_w, video_h = 640, 480  # 默认值
    if is_camera:
        # 摄像头/RTSP：提前探测分辨率
        cam_source = video_source if not video_source.startswith("__camera__") else video_source.replace("__camera__", "")
        detected = _probe_camera_resolution(cam_source)
        if detected:
            video_w, video_h = detected
            logger.info("摄像头分辨率: %dx%d", video_w, video_h)
        else:
            logger.warning("摄像头分辨率探测失败，使用默认: %dx%d", video_w, video_h)
    elif video_path:
        detected = _probe_video_size(str(video_path))
        if detected:
            video_w, video_h = detected
            logger.info("视频分辨率: %dx%d", video_w, video_h)

    # 构建 pipeline 命令
    config = load_config()
    pipeline_cfg = config.get("pipeline", {})

    cmd = [
        sys.executable, "-m", "pipeline",
        video_source,
        "--no-output",  # 不保存输出视频，仅实时推流
        "--no-screenshots",  # 摄像头模式不保存截图，减少 I/O 开销
    ]
    if req.concurrent_mode:
        cmd.extend(["-c", "--max-concurrent", str(req.max_concurrent or pipeline_cfg.get("max_concurrent", 4))])

    # ── 核心检测参数 ──
    cmd.extend(["--conf", str(req.conf_threshold)])
    cmd.extend(["--iou", str(req.iou_threshold)])
    cmd.extend(["--process-every", str(req.process_every)])
    cmd.extend(["--detect-every", str(req.detect_every)])

    # ── 帧率控制 ──
    if req.target_fps > 0:
        cmd.extend(["--target-fps", str(req.target_fps)])

    # ── 高级参数 ──
    if req.max_frames > 0:
        cmd.extend(["--max-frames", str(req.max_frames)])
    if req.device:
        cmd.extend(["--device", req.device])
    if req.yolo_model:
        cmd.extend(["--yolo-model", req.yolo_model])
    if req.prompt_mode:
        cmd.extend(["--prompt-mode", req.prompt_mode])
    if req.enable_refresh:
        cmd.append("--enable-refresh")
        cmd.extend(["--gap-num", str(req.gap_num)])

    # pipe 输出缩放
    if 0.1 <= req.pipe_scale < 1.0:
        cmd.extend(["--pipe-scale", str(req.pipe_scale)])
        pipe_w = max(16, int(video_w * req.pipe_scale))
        pipe_h = max(16, int(video_h * req.pipe_scale))
    else:
        pipe_w, pipe_h = video_w, video_h

    if is_camera:
        cmd.append("--camera")
        # 摄像头也走 raw stdout → H.264 推流（和视频文件一样）
        cmd.append("--raw-stdout")
        cmd.extend(["--output-size", f"{video_w}x{video_h}"])
        # 停止信号文件（替代原来 --stream-dir 的 __STOP__ 机制）
        stream_dir = _get_stream_dir(task_id)
        cmd.extend(["--stop-file", str(stream_dir / "__STOP__")])
    else:
        # 文件模式：raw stdout 输出（H.264 编码用），不写磁盘
        cmd.append("--raw-stdout")
        # 统一输出尺寸，确保 ffmpeg 读取的帧大小与 pipeline 输出一致
        cmd.extend(["--output-size", f"{video_w}x{video_h}"])
        if req.display:
            cmd.append("--display")
    cmd.append("--demo")

    logger.info("启动 Pipeline: %s (camera=%s)", " ".join(cmd), is_camera)

    async with _state_lock:
        _task_status[task_id] = {
            "task_id": task_id,
            "status": "running",
            "video_filename": req.video_filename,
            "output_filename": None,
            "output_path": None,
            "progress": "处理中...",
            "error": None,
            "is_camera": is_camera,
        }
    _pipeline_logs[task_id] = []
    _log_start[task_id] = 0

    try:
        # 获取信号量（限制并发 pipeline 数量）
        await sem.acquire()
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,  # raw BGR 帧输出
            stderr=asyncio.subprocess.PIPE,  # 日志和进度
            cwd=str(Path.cwd()),
            preexec_fn=os.setsid if hasattr(os, 'setsid') else None,
        )
        async with _state_lock:
            _running_processes[task_id] = process

        # 启动 H.264 编码器（从 pipeline stdout 读 raw 帧 → ffmpeg → fMP4）
        h264_fps = int(req.target_fps) if req.target_fps > 0 else 15
        asyncio.create_task(_start_h264_reader(task_id, process, pipe_w, pipe_h, fps=h264_fps))

        asyncio.create_task(_wait_pipeline(task_id, process, sem))

    except FileNotFoundError:
        sem.release()
        async with _state_lock:
            _task_status[task_id]["status"] = "failed"
            _task_status[task_id]["error"] = "pipeline 模块不存在，请确认 pipeline 目录已实现"
        raise HTTPException(status_code=500, detail="pipeline 模块不存在")

    return PipelineStartResponse(
        success=True,
        message=f"Pipeline 已启动，任务 ID: {task_id}",
        task_id=task_id,
    )


async def _wait_pipeline(task_id: str, process: asyncio.subprocess.Process, sem: asyncio.Semaphore):
    """异步等待 pipeline 完成，从 stderr 读取进度（stdout 用于 raw 帧输出）"""
    try:
        while True:
            try:
                line = await asyncio.wait_for(process.stderr.readline(), timeout=300)
            except asyncio.TimeoutError:
                if process.returncode is not None:
                    break
                if task_id in _stop_signals:
                    logger.warning("Pipeline 超时且收到停止信号，强制终止: %s", task_id)
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                    break
                logger.warning("Pipeline 5分钟无输出，继续等待: %s", task_id)
                continue

            if not line:
                break
            text = line.decode("utf-8", errors="replace").strip()
            if not text:
                continue

            if "进度" in text or "progress" in text.lower() or "%" in text or "处理帧" in text:
                async with _state_lock:
                    _task_status[task_id]["progress"] = text

            # 捕获识别日志（解析 Track ID + Step1/Step3，格式化为 [Track X] 弦号+匹配结果）
            if "Step1" in text and "Step3" in text:
                logs = _pipeline_logs.get(task_id)
                if logs is not None:
                    import time, re
                    m_track = re.search(r'\[Track\s+(\d+)\]', text)
                    track_id_str = m_track.group(1) if m_track else "?"
                    m_id = re.search(r'Step1\(VLM\):\s*弦号=(\S+)', text)
                    hull = m_id.group(1) if m_id else "?"
                    m_match = re.search(r'匹配=(\w+)', text)
                    m_cand = re.search(r"语义候选=(\[.*?\])", text)
                    match_type = m_match.group(1) if m_match else "none"
                    candidates = m_cand.group(1) if m_cand else "[]"
                    if match_type == "exact":
                        line = f"[Track {track_id_str}] 弦号：{hull}，精确匹配"
                        level = "exact"
                    elif match_type == "semantic":
                        line = f"[Track {track_id_str}] 弦号：{hull}，相似：{candidates}"
                        level = "semantic"
                    else:
                        line = f"[Track {track_id_str}] 弦号：{hull}，未命中"
                        level = "miss"
                    logs.append({"time": time.strftime("%H:%M:%S"), "line": line, "level": level, "track": track_id_str})
                    # FIFO 清理
                    if len(logs) > _MAX_LOG_LINES:
                        overflow = len(logs) - _MAX_LOG_LINES
                        del logs[:overflow]
                        _log_start[task_id] = _log_start.get(task_id, 0) + overflow

            if text.startswith("__PIPELINE_SUMMARY__:"):
                try:
                    import json
                    summary = json.loads(text.replace("__PIPELINE_SUMMARY__:", ""))
                    async with _state_lock:
                        _task_status[task_id]["summary"] = summary
                except Exception:
                    pass

            logger.info("[%s] %s", task_id, text)

        await process.wait()

        async with _state_lock:
            if process.returncode == 0:
                _task_status[task_id]["status"] = "completed"
                _task_status[task_id]["progress"] = "处理完成"
                logger.info("Pipeline 完成: %s", task_id)
            else:
                _task_status[task_id]["status"] = "failed"
                _task_status[task_id]["error"] = "Pipeline 进程异常退出"
                logger.error("Pipeline 失败 [%s]: rc=%d", task_id, process.returncode)
    except Exception as e:
        async with _state_lock:
            _task_status[task_id]["status"] = "failed"
            _task_status[task_id]["error"] = str(e)
        logger.error("Pipeline 异常 [%s]: %s", task_id, e)
    finally:
        sem.release()
        # 停止 H.264 推流
        await _stop_h264_stream(task_id)
        async with _state_lock:
            _running_processes.pop(task_id, None)
            _stop_signals.discard(task_id)
            is_browser_cam = _task_status.get(task_id, {}).get("is_browser_camera", False)
        _pipeline_logs.pop(task_id, None)
        _log_start.pop(task_id, None)
        if not is_browser_cam:
            _cleanup_stream_dir(task_id)
        _cleanup_old_tasks()


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


@router.get("/logs/{task_id}")
async def get_pipeline_logs(task_id: str, since: int = 0):
    """获取 Pipeline 识别日志（since 指定起始索引，用于增量拉取）"""
    logs = _pipeline_logs.get(task_id)
    if logs is None:
        return {"logs": [], "total": 0}
    start = _log_start.get(task_id, 0)
    total = start + len(logs)
    if since < start:
        # 索引已过期（FIFO 删除），返回全部当前日志 + log_start 让前端重置
        return {"logs": logs, "total": total, "log_start": start}
    offset = since - start
    return {"logs": logs[offset:], "total": total}


@router.post("/stop/{task_id}")
async def stop_pipeline(task_id: str):
    """停止正在运行的 Pipeline"""
    async with _state_lock:
        if task_id not in _running_processes:
            if task_id in _task_status and _task_status[task_id]["status"] != "running":
                return {"success": True, "message": f"任务已结束: {task_id}"}
            raise HTTPException(status_code=404, detail=f"任务不存在或已结束: {task_id}")

        if task_id in _stop_signals:
            return {"success": True, "message": f"任务正在停止中: {task_id}"}
        _stop_signals.add(task_id)
        process = _running_processes[task_id]

    # 关闭 WebRTC PeerConnection（如果有）
    pc = _webrtc_pcs.pop(task_id, None)
    if pc:
        try:
            await pc.close()
            logger.info("WebRTC PeerConnection 已关闭: %s", task_id)
        except Exception:
            pass

    # 写入停止信号文件（pipeline 可以检测到）
    stream_dir = _get_stream_dir(task_id)
    stop_file = stream_dir / "__STOP__"
    try:
        stop_file.write_text("stop")
    except Exception:
        pass

    # 浏览器摄像头模式：注入 None 唤醒 pipeline 线程
    if process.pid == -1:
        fq = _frame_queues.get(task_id)
        if fq:
            try:
                fq.put_nowait(None)
            except Exception:
                pass
        try:
            await asyncio.wait_for(process.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            process.kill()
        async with _state_lock:
            _task_status[task_id]["status"] = "failed"
            _task_status[task_id]["error"] = "用户手动停止"
            _running_processes.pop(task_id, None)
            _stop_signals.discard(task_id)
        await _stop_h264_stream(task_id)
        _pipeline_logs.pop(task_id, None)
        _log_start.pop(task_id, None)
        _cleanup_stream_dir(task_id)
        return {"success": True, "message": f"已停止任务: {task_id}"}

    import signal

    # 用户主动停止：先尝试 SIGTERM，短暂等待后直接 SIGKILL
    try:
        pgid = os.getpgid(process.pid)
        os.killpg(pgid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        try:
            process.terminate()
        except ProcessLookupError:
            pass

    try:
        await asyncio.wait_for(process.wait(), timeout=1.0)
    except asyncio.TimeoutError:
        try:
            pgid = os.getpgid(process.pid)
            os.killpg(pgid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            try:
                process.kill()
            except ProcessLookupError:
                pass
        try:
            await asyncio.wait_for(process.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            pass

    # 更新状态
    async with _state_lock:
        _task_status[task_id]["status"] = "failed"
        _task_status[task_id]["error"] = "用户手动停止"
        _running_processes.pop(task_id, None)
        _stop_signals.discard(task_id)

    # 清理日志缓冲和帧目录
    _pipeline_logs.pop(task_id, None)
    _log_start.pop(task_id, None)
    _cleanup_stream_dir(task_id)

    return {"success": True, "message": f"已停止任务: {task_id}"}


def _cleanup_old_tasks():
    """清理已完成/失败的旧任务记录，防止内存泄漏（需在 _state_lock 下调用或单独使用）"""
    global _task_status
    if len(_task_status) > 100:
        running = {k: v for k, v in _task_status.items() if v["status"] == "running"}
        finished = sorted(
            [(k, v) for k, v in _task_status.items() if v["status"] != "running"],
            key=lambda x: x[1].get("task_id", ""),
            reverse=True,
        )[:50]
        _task_status = {**running, **dict(finished)}
        logger.info("自动清理旧任务记录，保留 %d 条", len(_task_status))


def _cleanup_stream_dir(task_id: str):
    """清理帧共享目录、内存队列和停止信号文件"""
    import shutil
    d = Path("./_camera_frames") / task_id
    if d.exists():
        shutil.rmtree(d, ignore_errors=True)
    # 同时清理浏览器帧目录
    d2 = Path("./_browser_frames") / task_id
    if d2.exists():
        shutil.rmtree(d2, ignore_errors=True)
    # 清理内存帧队列
    _frame_queues.pop(task_id, None)


async def _delayed_cleanup(task_id: str, delay: int = 10):
    """延迟清理：等待 pipeline 自然结束后再清理目录"""
    await asyncio.sleep(delay)
    async with _state_lock:
        if task_id in _running_processes:
            try:
                _running_processes[task_id].kill()
                await asyncio.wait_for(_running_processes[task_id].wait(), timeout=5)
            except Exception:
                pass
            _running_processes.pop(task_id, None)
        if task_id in _task_status and _task_status[task_id]["status"] == "running":
            _task_status[task_id]["status"] = "completed"
            _task_status[task_id]["progress"] = "处理完成（摄像头已断开）"
        _stop_signals.discard(task_id)
    _pipeline_logs.pop(task_id, None)
    _log_start.pop(task_id, None)
    _cleanup_stream_dir(task_id)


# ── 浏览器摄像头帧队列（内存直传，零磁盘 I/O）──
_frame_queues: dict[str, queue.Queue] = {}  # task_id → Queue(numpy BGR frames)


def _queue_put_latest(q: queue.Queue, item) -> None:
    """向队列放入最新帧：满时丢掉最旧的，保证消费者总是拿到最新帧。"""
    try:
        q.put_nowait(item)
    except queue.Full:
        try:
            q.get_nowait()  # 丢弃最旧帧
        except queue.Empty:
            pass
        try:
            q.put_nowait(item)
        except queue.Full:
            pass  # 极端情况：仍然满，丢弃此帧


def _get_browser_frames_dir(task_id: str) -> Path:
    """获取浏览器摄像头帧目录（仅作为 fallback）"""
    d = Path("./_browser_frames") / task_id
    d.mkdir(parents=True, exist_ok=True)
    return d


# ── H.264 推流管理 ──

async def _start_h264_reader(task_id: str, process: asyncio.subprocess.Process, w: int = 640, h: int = 480, fps: int = 15):
    """后台任务：从 pipeline stdout 读取 raw BGR 帧，启动 ffmpeg 编码为 H.264 fMP4"""

    # 读取编码器配置（默认 GPU h264_nvenc，fallback CPU libx264）
    config = load_config()
    encoder = config.get("pipeline", {}).get("ffmpeg_encoder", "h264_nvenc")

    # ffmpeg 命令：stdin raw BGR → H.264 fMP4
    ffmpeg_cmd = _find_binary("ffmpeg") or "ffmpeg"
    gop = max(1, fps)  # GOP = 帧率，确保每秒至少一个关键帧（低帧率时不需等太久）

    # 公共输入参数
    input_args = [
        ffmpeg_cmd, "-hide_banner", "-loglevel", "error",
        "-fflags", "+nobuffer",   # 减少输入缓冲
        "-flags", "+low_delay",   # 低延迟模式
        "-f", "rawvideo", "-pix_fmt", "bgr24", "-video_size", f"{w}x{h}",
        "-r", str(fps),  # 时间基准帧率（匹配目标 FPS）
        "-i", "pipe:0",
    ]

    # 公共输出参数（fMP4 容器）
    output_args = [
        "-pix_fmt", "yuv420p",
        "-movflags", "+frag_keyframe+empty_moov+default_base_moof+faststart",
        "-frag_duration", "250000",  # 0.25 秒一个 fragment（更低延迟）
        "-flush_packets", "1",
        "-f", "mp4",
        "pipe:1",
    ]

    # 根据编码器选择参数
    if encoder == "h264_nvenc":
        # NVIDIA GPU 编码 — 低延迟模式
        logger.info("H264 编码器: h264_nvenc (GPU)")
        encoder_args = [
            "-c:v", "h264_nvenc",
            "-preset", "p1",         # 最快预设
            "-tune", "ull",          # ultra low latency
            "-rc", "vbr",            # 可变码率
            "-cq", "23",             # 质量（越低越好，23 是平衡点）
            "-bf", "0",              # 无 B 帧
            "-g", str(gop),          # GOP = fps
            "-delay", "0",           # 零延迟
        ]
    else:
        # CPU 编码 — fallback
        logger.info("H264 编码器: libx264 (CPU)")
        encoder_args = [
            "-c:v", "libx264",
            "-preset", "ultrafast", "-tune", "zerolatency",
            "-profile:v", "baseline", "-level", "3.1",
            "-bf", "0",        # 无 B 帧（降低延迟）
            "-g", str(gop),    # GOP = fps（每秒一个关键帧，低帧率时减少首次关键帧等待）
            "-threads", "2",   # 限制编码线程，减少延迟
        ]

    ffmpeg_args = input_args + encoder_args + output_args

    # 状态初始化
    async with _state_lock:
        _h264_streams[task_id] = {
            "ffmpeg": None,
            "viewers": set(),
            "viewer_queues": {},    # ws → asyncio.Queue（每观众独立队列）
            "viewer_tasks": {},     # ws → asyncio.Task（每观众独立发送任务）
            "init_segment": None,
            "latest_segments": [],
            "max_segments": 30,
            "reader_task": None,
            "frames_fed": 0,      # 已喂给 ffmpeg 的帧数（供背压检测）
        }

    ffmpeg_proc = None
    init_sent = False
    box_buffer = b""
    _pending_moof = None

    try:
        # 从 pipeline stdout 读取 raw 帧，喂给 ffmpeg
        loop = asyncio.get_event_loop()

        # 启动 ffmpeg
        ffmpeg_proc = await asyncio.create_subprocess_exec(
            *ffmpeg_args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        async with _state_lock:
            _h264_streams[task_id]["ffmpeg"] = ffmpeg_proc

        frame_size = w * h * 3
        running = True

        async def drain_ffmpeg_stderr():
            """消费 ffmpeg stderr，防止 pipe buffer 满导致阻塞"""
            try:
                while running:
                    line = await ffmpeg_proc.stderr.readline()
                    if not line:
                        break
                    text = line.decode("utf-8", errors="replace").strip()
                    if text:
                        logger.debug("[ffmpeg %s] %s", task_id, text)
            except (asyncio.IncompleteReadError, OSError):
                pass

        async def feed_frames():
            """从 pipeline stdout 读 raw 帧 → ffmpeg stdin

            readexactly 一次读完整帧（2.7MB@640x480），避免多次 read 拼接
            带来的异步调度延迟。进程被杀时 IncompleteReadError 由 except 捕获。
            """
            nonlocal running
            try:
                while running:
                    data = await process.stdout.readexactly(frame_size)
                    if ffmpeg_proc.stdin and not ffmpeg_proc.stdin.is_closing():
                        ffmpeg_proc.stdin.write(data)
                        await ffmpeg_proc.stdin.drain()
                        async with _state_lock:
                            stream = _h264_streams.get(task_id)
                            if stream:
                                stream["frames_fed"] += 1
                    else:
                        break
            except (asyncio.IncompleteReadError, BrokenPipeError, OSError):
                pass
            finally:
                running = False
                if ffmpeg_proc.stdin and not ffmpeg_proc.stdin.is_closing():
                    ffmpeg_proc.stdin.close()

        async def read_ffmpeg_output():
            """从 ffmpeg stdout 读取 fMP4 数据，解析 box 并广播"""
            nonlocal box_buffer, init_sent, running
            try:
                while running:
                    chunk = await ffmpeg_proc.stdout.read(262144)  # 256KB buffer
                    if not chunk:
                        break
                    box_buffer += chunk
                    # 解析 fMP4 box，合并 moof+mdat 为完整 fragment
                    while len(box_buffer) >= 8:
                        box_size = int.from_bytes(box_buffer[:4], "big")
                        if box_size < 8 or len(box_buffer) < box_size:
                            break
                        box_data = box_buffer[:box_size]
                        box_buffer = box_buffer[box_size:]
                        box_type = box_data[4:8]

                        if box_type == b"moov":
                            # 初始化段（codec info）
                            async with _state_lock:
                                _h264_streams[task_id]["init_segment"] = box_data
                            await _broadcast_h264(task_id, b"\x01" + len(box_data).to_bytes(4, "big") + box_data)
                        elif box_type == b"moof":
                            # 媒体段头部，等 mdat 拼接后一起发
                            _pending_moof = box_data
                        elif box_type == b"mdat":
                            # 媒体段数据，和 moof 合并为完整 fragment
                            fragment = (_pending_moof or b"") + box_data
                            _pending_moof = None
                            async with _state_lock:
                                stream = _h264_streams[task_id]
                                stream["latest_segments"].append(fragment)
                                if len(stream["latest_segments"]) > stream["max_segments"]:
                                    stream["latest_segments"] = stream["latest_segments"][-stream["max_segments"]:]
                            await _broadcast_h264(task_id, b"\x02" + len(fragment).to_bytes(4, "big") + fragment)
            except (BrokenPipeError, OSError):
                pass
            finally:
                running = False

        # 并行运行 frame feeder + output reader + stderr drainer
        await asyncio.gather(feed_frames(), read_ffmpeg_output(), drain_ffmpeg_stderr())

    except Exception as e:
        logger.error("H.264 推流异常 [%s]: %s", task_id, e)
    finally:
        # 清理 ffmpeg
        if ffmpeg_proc:
            try:
                if ffmpeg_proc.stdin and not ffmpeg_proc.stdin.is_closing():
                    ffmpeg_proc.stdin.close()
            except Exception:
                pass
            try:
                ffmpeg_proc.kill()
            except ProcessLookupError:
                pass
        async with _state_lock:
            _h264_streams.pop(task_id, None)
        logger.info("H.264 推流结束: %s", task_id)


async def _viewer_sender(ws: WebSocket, queue: asyncio.Queue, task_id: str):
    """每观众独立发送任务：从队列取数据发送，慢观众自动丢帧

    不对 send_bytes 使用 asyncio.wait_for 超时（会 cancel 协程并破坏
    websockets 连接状态）。TCP 缓冲满时 send_bytes 自然阻塞实现背压。
    每次发送前先清空队列只取最新帧，避免发送积压旧数据。
    """
    try:
        while True:
            try:
                data = await asyncio.wait_for(queue.get(), timeout=3.0)
            except asyncio.TimeoutError:
                # 队列空闲 — 检查任务状态，发送心跳保活
                async with _state_lock:
                    task = _task_status.get(task_id)
                if not task or task["status"] != "running":
                    try:
                        await ws.send_json({"type": "done"})
                    except Exception:
                        pass
                    break
                try:
                    await ws.send_json({"type": "heartbeat"})
                except Exception:
                    break
                continue

            # 清空队列积压，只取最新帧
            while not queue.empty():
                try:
                    data = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

            # 直接发送，不使用 wait_for 超时
            # TCP 缓冲满时自然阻塞（背压），队列满时 _broadcast_h264 自动丢旧帧
            await ws.send_bytes(data)
    except asyncio.CancelledError:
        pass
    except Exception:
        pass
    finally:
        async with _state_lock:
            stream = _h264_streams.get(task_id)
            if stream:
                stream["viewers"].discard(ws)
                stream["viewer_queues"].pop(ws, None)
                stream["viewer_tasks"].pop(ws, None)
        try:
            await ws.close()
        except Exception:
            pass


async def _broadcast_h264(task_id: str, data: bytes):
    """向所有观众广播 fMP4 数据（非阻塞，每观众独立队列，慢观众自动丢帧）"""
    async with _state_lock:
        stream = _h264_streams.get(task_id)
        if not stream:
            return
        queues = list(stream["viewer_queues"].values())

    for q in queues:
        try:
            q.put_nowait(data)
        except asyncio.QueueFull:
            # 队列满了，丢掉最旧的帧
            try:
                q.get_nowait()
            except asyncio.QueueEmpty:
                pass
            try:
                q.put_nowait(data)
            except asyncio.QueueFull:
                pass


async def _stop_h264_stream(task_id: str):
    """停止 H.264 推流"""
    async with _state_lock:
        stream = _h264_streams.pop(task_id, None)
    if not stream:
        return
    # 取消所有观众发送任务
    for task in list(stream.get("viewer_tasks", {}).values()):
        task.cancel()
    # 通知所有客户端
    for ws in list(stream.get("viewers", [])):
        try:
            await ws.send_json({"type": "done"})
        except Exception:
            pass
    # 杀掉 ffmpeg
    ffmpeg = stream.get("ffmpeg")
    if ffmpeg:
        try:
            ffmpeg.kill()
        except ProcessLookupError:
            pass


# ── WebSocket H.264 推流端点 ──

@router.websocket("/ws/h264/{task_id}")
async def ws_h264_stream(websocket: WebSocket, task_id: str):
    """WebSocket H.264 推流 — fMP4 over WebSocket，前端用 MSE 播放"""
    async with _state_lock:
        if task_id not in _task_status:
            await websocket.close(code=4004, reason="任务不存在")
            return

    await websocket.accept()

    async with _state_lock:
        stream = _h264_streams.get(task_id)
        if not stream:
            await websocket.close(code=4004, reason="推流未就绪")
            return
        stream["viewers"].add(websocket)
        # 创建此观众的独立队列和发送任务
        q: asyncio.Queue = asyncio.Queue(maxsize=10)
        stream["viewer_queues"][websocket] = q
        sender_task = asyncio.create_task(_viewer_sender(websocket, q, task_id))
        stream["viewer_tasks"][websocket] = sender_task

    try:
        # 发送初始化段（如果已有）
        init_seg = stream.get("init_segment")
        if init_seg:
            msg = b"\x01" + len(init_seg).to_bytes(4, "big") + init_seg
            try:
                q.put_nowait(msg)
            except asyncio.QueueFull:
                pass

        # 发送最近的媒体段（避免新客户端黑屏）
        for seg in stream.get("latest_segments", []):
            msg = b"\x02" + len(seg).to_bytes(4, "big") + seg
            try:
                q.put_nowait(msg)
            except asyncio.QueueFull:
                pass

        # 保持连接，等待任务结束
        while True:
            async with _state_lock:
                task = _task_status.get(task_id)
                if not task or task["status"] != "running":
                    break
            await asyncio.sleep(1)

    except (WebSocketDisconnect, AssertionError):
        pass
    except Exception as e:
        logger.debug("H.264 WebSocket 异常 [%s]: %s", task_id, e)
    finally:
        # 清理：取消发送任务，移除队列
        st = None
        async with _state_lock:
            stream = _h264_streams.get(task_id)
            if stream:
                stream["viewers"].discard(websocket)
                st = stream["viewer_tasks"].pop(websocket, None)
                stream["viewer_queues"].pop(websocket, None)
        if st:
            st.cancel()


# ── WebSocket JPEG 推流（兼容） ──

# 每个 task 的 WebSocket 观众集合
_ws_viewers: dict[str, set[WebSocket]] = {}


@router.websocket("/ws/stream/{task_id}")
async def ws_stream(websocket: WebSocket, task_id: str):
    """WebSocket 实时推流 — 比 MJPEG 效率更高，支持多客户端、跳帧、低延迟"""
    async with _state_lock:
        if task_id not in _task_status:
            await websocket.close(code=4004, reason="任务不存在")
            return

    await websocket.accept()

    # 注册观众
    async with _state_lock:
        _ws_viewers.setdefault(task_id, set()).add(websocket)

    stream_dir = _get_stream_dir(task_id)
    frame_file = stream_dir / "latest.jpg"
    loop = asyncio.get_event_loop()
    last_mtime = 0.0
    target_interval = 0.033  # ~30fps
    no_frame_count = 0

    try:
        while True:
            # 检查任务是否还在运行
            async with _state_lock:
                task = _task_status.get(task_id)
                if not task or task["status"] != "running":
                    # 发送结束信号
                    try:
                        await websocket.send_json({"type": "done"})
                    except Exception:
                        pass
                    break

            t0 = loop.time()

            if frame_file.exists():
                try:
                    stat = await loop.run_in_executor(None, frame_file.stat)
                    mtime = stat.st_mtime

                    if mtime != last_mtime:
                        last_mtime = mtime
                        no_frame_count = 0
                        frame_data = await loop.run_in_executor(None, frame_file.read_bytes)
                        if frame_data:
                            # 二进制帧：直接发 JPEG bytes，零额外开销
                            await websocket.send_bytes(frame_data)
                    else:
                        no_frame_count += 1
                        # 超过 3 秒无新帧，发 ping 保活
                        if no_frame_count > 90:
                            no_frame_count = 0
                            try:
                                await websocket.send_json({"type": "heartbeat"})
                            except Exception:
                                break
                except (OSError, FileNotFoundError):
                    await asyncio.sleep(0.01)
                    continue
            else:
                # 无帧文件，等待
                await asyncio.sleep(0.05)

            # 动态 sleep，保持目标帧率
            elapsed = loop.time() - t0
            sleep_time = max(0.005, target_interval - elapsed)
            await asyncio.sleep(sleep_time)

    except (WebSocketDisconnect, AssertionError):
        pass
    except Exception as e:
        logger.debug("WebSocket 推流异常 [%s]: %s", task_id, e)
    finally:
        # 注销观众
        async with _state_lock:
            viewers = _ws_viewers.get(task_id, set())
            viewers.discard(websocket)
            if not viewers:
                _ws_viewers.pop(task_id, None)


# ── 保留旧 MJPEG 端点兼容（摄像头 Demo 仍用 img 标签）──

@router.get("/stream/{task_id}")
async def camera_stream(task_id: str):
    """MJPEG 兼容端点 — 供摄像头 Demo 的 img 标签使用"""
    async with _state_lock:
        if task_id not in _task_status:
            raise HTTPException(status_code=404, detail=f"任务不存在: {task_id}")

    stream_dir = _get_stream_dir(task_id)
    frame_file = stream_dir / "latest.jpg"

    async def generate():
        boundary = "--frame"
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
        last_mtime = 0.0
        target_interval = 0.04
        loop = asyncio.get_event_loop()
        no_frame_count = 0

        while True:
            async with _state_lock:
                task = _task_status.get(task_id)
                if not task or task["status"] != "running":
                    break

            t0 = loop.time()

            if frame_file.exists():
                try:
                    stat = await loop.run_in_executor(None, frame_file.stat)
                    mtime = stat.st_mtime

                    if mtime != last_mtime:
                        last_mtime = mtime
                        no_frame_count = 0
                        frame_data = await loop.run_in_executor(None, frame_file.read_bytes)
                        if frame_data:
                            yield (
                                f"{boundary}\r\n"
                                f"Content-Type: image/jpeg\r\n\r\n"
                            ).encode() + frame_data + b"\r\n"
                    else:
                        no_frame_count += 1
                        if no_frame_count > 125:
                            no_frame_count = 0
                            yield (
                                f"{boundary}\r\n"
                                f"Content-Type: image/jpeg\r\n\r\n"
                            ).encode() + _black_jpeg + b"\r\n"
                except (OSError, FileNotFoundError):
                    await asyncio.sleep(0.01)
                    continue
            else:
                yield (
                    f"{boundary}\r\n"
                    f"Content-Type: image/jpeg\r\n\r\n"
                ).encode() + _black_jpeg + b"\r\n"

            elapsed = loop.time() - t0
            sleep_time = max(0.005, target_interval - elapsed)
            await asyncio.sleep(sleep_time)

    return StreamingResponse(
        generate(),
        media_type="multipart/x-mixed-replace; boundary=frame",
    )


# ── 浏览器摄像头 ──

@router.post("/start-browser-camera", response_model=PipelineStartResponse)
async def start_browser_camera(req: BrowserCameraStartRequest):
    """启动浏览器摄像头 Pipeline（内存队列模式，零磁盘 I/O）

    架构：
    - 浏览器 JPEG → WebSocket → 服务端解码 numpy → 内存队列
    - Pipeline 线程从队列消费 numpy 帧（无磁盘 I/O）
    - Pipeline stdout 通过 fd 重定向到 pipe → ffmpeg H.264 编码
    - H.264 fMP4 → WebSocket → 浏览器 MSE 播放
    """
    _ensure_dirs()

    # 并发控制
    sem = _get_semaphore()
    running_count = sum(1 for t in _task_status.values() if t["status"] == "running")
    if running_count >= _MAX_PARALLEL_PIPELINES:
        raise HTTPException(
            status_code=429,
            detail=f"已有 {running_count} 个 Pipeline 在运行（上限 {_MAX_PARALLEL_PIPELINES}），请等待完成后再试",
        )

    config = load_config()
    pipeline_cfg = config.get("pipeline", {})

    task_id = str(uuid.uuid4())[:8]
    stream_dir = _get_stream_dir(task_id)

    # 创建内存帧队列（WebSocket 解码后直送 numpy 帧）
    frame_queue: queue.Queue = queue.Queue(maxsize=30)
    _frame_queues[task_id] = frame_queue

    # 创建 pipe：pipeline 线程写 raw BGR → H.264 读取器读
    pipe_r, pipe_w = os.pipe()

    logger.info("启动浏览器摄像头 Pipeline (内存队列模式, task=%s)", task_id)

    async with _state_lock:
        _task_status[task_id] = {
            "task_id": task_id,
            "status": "running",
            "video_filename": f"浏览器摄像头 ({task_id})",
            "output_filename": None,
            "output_path": None,
            "progress": "等待摄像头连接...",
            "error": None,
            "is_camera": True,
            "is_browser_camera": True,
            "stream_mode": req.stream_mode,
        }
    _pipeline_logs[task_id] = []
    _log_start[task_id] = 0

    try:
        await sem.acquire()

        # ── H.264 编码器：从 pipe 读 raw BGR → ffmpeg → fMP4 ──
        class _PipeReader:
            """从 pipe fd 读取 raw bytes，给 asyncio 用"""
            def __init__(self, fd: int):
                self._fd = fd
                self._loop = asyncio.get_event_loop()
            async def readexactly(self, n: int) -> bytes:
                return await self._loop.run_in_executor(None, self._blocking_read, n)
            def _blocking_read(self, n: int) -> bytes:
                buf = b""
                while len(buf) < n:
                    chunk = os.read(self._fd, n - len(buf))
                    if not chunk:
                        break
                    buf += chunk
                return buf if len(buf) == n else b""

        class _NullStderr:
            """模拟 process.stderr，pipeline 完成前阻塞"""
            def __init__(self):
                self._done = threading.Event()
            async def readline(self) -> bytes:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self._done.wait, 300)
                return b""
            def signal_done(self):
                self._done.set()

        class _FakeProcess:
            """模拟 asyncio.subprocess.Process 接口"""
            def __init__(self, pipe_fd: int):
                self.stdout = _PipeReader(pipe_fd)
                self.stderr = _NullStderr()
                self.returncode = None
                self.pid = -1  # 标记非真实进程
                self._finished = asyncio.Event()
            async def wait(self):
                await self._finished.wait()
            def kill(self):
                self._finished.set()
                try:
                    os.close(pipe_r)
                except OSError:
                    pass

        fake_proc = _FakeProcess(pipe_r)
        cam_fps = int(req.target_fps) if req.target_fps > 0 else 15
        # pipe 输出缩放
        if 0.1 <= req.pipe_scale < 1.0:
            pipe_out_w = max(16, int(640 * req.pipe_scale))
            pipe_out_h = max(16, int(480 * req.pipe_scale))
        else:
            pipe_out_w, pipe_out_h = 640, 480
        asyncio.create_task(_start_h264_reader(task_id, fake_proc, pipe_out_w, pipe_out_h, fps=cam_fps))

        # ── Pipeline 线程 ──
        pipe_cfg = dict(pipeline_cfg)
        pipe_cfg.update({
            "concurrent_mode": req.concurrent_mode,
            "conf_threshold": req.conf_threshold,
            "iou_threshold": req.iou_threshold,
            "process_every_n_frames": req.process_every,
            "detect_every_n_frames": req.detect_every,
            "target_fps": req.target_fps,
            "max_concurrent": req.max_concurrent or pipeline_cfg.get("max_concurrent", 4),
            "demo": True,
            "no_output": True,
            "save_screenshots": False,
            "raw_stdout": True,
            "output_size": [640, 480],
            "stop_file": str(stream_dir / "__STOP__"),
        })
        if 0.1 <= req.pipe_scale < 1.0:
            pipe_cfg["pipe_output_size"] = [pipe_out_w, pipe_out_h]
        if req.max_frames > 0:
            pipe_cfg["max_frames"] = req.max_frames
        if req.device:
            pipe_cfg["device"] = req.device
        if req.yolo_model:
            pipe_cfg["yolo_model"] = req.yolo_model
        if req.prompt_mode:
            pipe_cfg["prompt_mode"] = req.prompt_mode
        if req.enable_refresh:
            pipe_cfg["enable_refresh"] = True
            pipe_cfg["gap_num"] = req.gap_num
        config["pipeline"] = pipe_cfg

        def _run_pipeline():
            """在独立线程中运行 pipeline，stdout fd 重定向到 pipe"""
            import cv2
            from pipeline.pipeline import ShipPipeline
            from pipeline.virtual_camera import VirtualCamera

            # fd 重定向：把 stdout (fd=1) 指向 pipe_w
            # 这样 pipeline 的 open("/dev/stdout","wb") 写入的数据全部进入 pipe
            saved_stdout_fd = os.dup(1)
            os.dup2(pipe_w, 1)

            try:
                vc = VirtualCamera(frame_queue=frame_queue, fps=15.0)
                pipeline = ShipPipeline(config=config)
                stats = pipeline.process(source=vc, display=False, max_frames=req.max_frames)

                # 输出摘要到 stderr（不经过 fd 1）
                summary_json = json.dumps(stats, ensure_ascii=False)
                os.write(saved_stdout_fd, f"\n__PIPELINE_SUMMARY__:{summary_json}\n".encode())

            except Exception as e:
                logger.error("Pipeline 线程异常: %s", e)
                import traceback
                traceback.print_exc()
                stats = {"error": str(e)}
            finally:
                # 恢复 stdout fd
                os.dup2(saved_stdout_fd, 1)
                os.close(saved_stdout_fd)
                os.close(pipe_w)

            # 通知主循环完成（线程中无 event loop，需用主循环引用）
            async def _finish():
                async with _state_lock:
                    if "error" in stats:
                        _task_status[task_id]["status"] = "failed"
                        _task_status[task_id]["error"] = stats["error"]
                    else:
                        _task_status[task_id]["status"] = "completed"
                        _task_status[task_id]["progress"] = "处理完成"
                # 关闭 WebRTC PC，让帧接收器退出
                pc = _webrtc_pcs.pop(task_id, None)
                if pc:
                    try:
                        await asyncio.wait_for(pc.close(), timeout=3.0)
                    except Exception:
                        pass
                await _stop_h264_stream(task_id)
                fake_proc.stderr.signal_done()
                fake_proc._finished.set()
                sem.release()
                _pipeline_logs.pop(task_id, None)
                _log_start.pop(task_id, None)
                _cleanup_stream_dir(task_id)

            asyncio.run_coroutine_threadsafe(_finish(), _main_loop)

        # 捕获主循环引用，供线程内使用
        _main_loop = asyncio.get_event_loop()

        thread = threading.Thread(target=_run_pipeline, name=f"pipeline-{task_id}", daemon=True)
        thread.start()

    except Exception as e:
        sem.release()
        try:
            fake_proc._finished.set()
        except Exception:
            pass
        os.close(pipe_r)
        os.close(pipe_w)
        _frame_queues.pop(task_id, None)
        async with _state_lock:
            _task_status[task_id]["status"] = "failed"
            _task_status[task_id]["error"] = str(e)
        raise HTTPException(status_code=500, detail=str(e))

    return PipelineStartResponse(
        success=True,
        message=f"浏览器摄像头 Pipeline 已启动 (内存队列模式)，请连接 WebSocket 推流",
        task_id=task_id,
        capture_fps=req.capture_fps,
    )


@router.websocket("/ws/camera/{task_id}")
async def browser_camera_ws(websocket: WebSocket, task_id: str):
    """WebSocket 端点 — 接收浏览器摄像头视频流（支持 MJPEG 和 H264 两种模式）"""
    async with _state_lock:
        if task_id not in _task_status:
            await websocket.close(code=4004, reason="任务不存在")
            return

    await websocket.accept()
    logger.info("浏览器摄像头 WebSocket 已连接: %s", task_id)

    # 获取内存队列（pipeline 线程直接消费）
    frame_queue = _frame_queues.get(task_id)
    use_queue = frame_queue is not None

    # fallback：无队列时写磁盘
    frames_dir = _get_browser_frames_dir(task_id) if not use_queue else None

    async with _state_lock:
        _task_status[task_id]["progress"] = "摄像头已连接，等待推流..."

    # ── 根据第一条消息自动判断模式 ──
    try:
        first_msg = await asyncio.wait_for(websocket.receive(), timeout=10.0)
    except asyncio.TimeoutError:
        logger.warning("摄像头 WebSocket 10秒内无数据: %s", task_id)
        await websocket.close(code=4008, reason="超时")
        return

    # 判断模式：文本消息 = H264 模式（含 codec 配置），二进制 = MJPEG 模式
    if "text" in first_msg:
        # ── H264 模式 ──
        try:
            cfg = json.loads(first_msg["text"])
            codec = cfg.get("codec", "vp8")
        except (json.JSONDecodeError, KeyError):
            codec = "vp8"
        logger.info("摄像头 H264 模式: codec=%s, task=%s", codec, task_id)
        await _receive_h264_camera_frames(
            websocket, task_id, frame_queue, use_queue, frames_dir, codec
        )
    elif "bytes" in first_msg:
        # ── MJPEG 模式 ──
        logger.info("摄像头 MJPEG 模式: task=%s", task_id)
        await _receive_mjpeg_camera_frames(
            websocket, task_id, frame_queue, use_queue, frames_dir, first_msg["bytes"]
        )
    else:
        await websocket.close(code=4001, reason="未知消息格式")


async def _receive_mjpeg_camera_frames(
    websocket: WebSocket, task_id: str,
    frame_queue: queue.Queue | None, use_queue: bool,
    frames_dir: Path | None, first_frame: bytes,
):
    """MJPEG 模式：逐帧接收 JPEG，解码后送队列"""
    import cv2
    import numpy as np

    frame_count = 0

    # 处理已经收到的第一帧
    async def _process_jpeg(data: bytes):
        nonlocal frame_count
        if len(data) < 3 or data[:2] != b'\xff\xd8':
            await websocket.send_json({"ok": False, "error": "非 JPEG 数据"})
            return

        if use_queue:
            def _decode_and_enqueue():
                nparr = np.frombuffer(data, np.uint8)
                frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                if frame is not None:
                    _queue_put_latest(frame_queue, frame)
            await asyncio.to_thread(_decode_and_enqueue)
        else:
            frame_path = frames_dir / "latest.jpg"
            tmp_path = frames_dir / "latest.jpg.tmp"
            def _write_frame():
                tmp_path.write_bytes(data)
                tmp_path.rename(frame_path)
            await asyncio.to_thread(_write_frame)

        frame_count += 1
        if frame_count % 30 == 0:
            logger.debug("MJPEG 帧计数: %d", frame_count)
            try:
                await websocket.send_json({"ok": True, "frame": frame_count})
            except Exception:
                pass

    try:
        # 处理第一帧
        await _process_jpeg(first_frame)

        while _task_status.get(task_id, {}).get("status") == "running":
            data = await websocket.receive_bytes()
            await _process_jpeg(data)

    except WebSocketDisconnect:
        logger.info("MJPEG WebSocket 断开: %s (共 %d 帧)", task_id, frame_count)
    except AssertionError:
        pass
    except Exception as e:
        logger.error("MJPEG WebSocket 异常: %s", e)
    finally:
        if use_queue and frame_queue:
            try:
                frame_queue.put_nowait(None)
            except queue.Full:
                pass
        logger.info("MJPEG 推流结束: %s (共 %d 帧)", task_id, frame_count)
        if task_id in _task_status and _task_status[task_id]["status"] == "running":
            _task_status[task_id]["progress"] = f"摄像头已断开（共接收 {frame_count} 帧），等待 pipeline 结束..."
        asyncio.create_task(_delayed_cleanup(task_id, delay=10))


# ── WebRTC 摄像头 ──

# 每个 task 对应一个 PeerConnection，停止时需要关闭
_webrtc_pcs: dict[str, Any] = {}


async def _receive_webrtc_camera_frames(
    pc: Any,
    task_id: str,
    frame_queue: queue.Queue | None,
    use_queue: bool,
    frames_dir: Path | None,
):
    """WebRTC 模式：从 RTCPeerConnection 的视频 track 读取帧 → pipeline 队列"""
    import cv2
    import numpy as np

    frame_count = 0

    async with _state_lock:
        _task_status[task_id]["progress"] = "WebRTC 已连接，等待视频帧..."

    # 获取远程视频 track
    video_track = None
    for t in pc.getReceivers():
        if t.track and t.track.kind == "video":
            video_track = t.track
            break

    if video_track is None:
        logger.error("WebRTC 无视频 track: %s", task_id)
        _webrtc_pcs.pop(task_id, None)
        await pc.close()
        return

    logger.info("WebRTC 视频 track 已就绪: %s", task_id)

    try:
        # 限流：按目标帧率控制消费速度（默认 15fps）
        target_fps = 15
        try:
            config = load_config()
            target_fps = config.get("pipeline", {}).get("target_fps", 15) or 15
        except Exception:
            pass
        frame_interval = 1.0 / target_fps
        last_frame_time = 0.0

        while True:
            # 带超时的 recv，防止连接静默断开时永久挂起
            try:
                frame = await asyncio.wait_for(video_track.recv(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("WebRTC 5秒未收到帧，断开: %s", task_id)
                break
            except Exception as e:
                logger.debug("WebRTC track recv 结束: %s", e)
                break

            # 帧率控制：跳过过早到达的帧
            now = time.monotonic()
            if now - last_frame_time < frame_interval:
                continue
            last_frame_time = now

            # aiortc VideoFrame → numpy BGR
            img = frame.to_ndarray(format="bgr24")

            # 缩放到 640x480
            if img.shape[1] != 640 or img.shape[0] != 480:
                img = cv2.resize(img, (640, 480))

            if use_queue:
                _queue_put_latest(frame_queue, img)
            else:
                _, buf = cv2.imencode('.jpg', img, [cv2.IMWRITE_JPEG_QUALITY, 80])
                if frames_dir:
                    frame_path = frames_dir / "latest.jpg"
                    tmp_path = frames_dir / "latest.jpg.tmp"
                    tmp_path.write_bytes(buf.tobytes())
                    tmp_path.rename(frame_path)

            frame_count += 1
            if frame_count % 30 == 0:
                logger.debug("WebRTC 解码帧计数: %d", frame_count)

    except Exception as e:
        logger.error("WebRTC 帧接收异常 [%s]: %s", task_id, e)
    finally:
        logger.info("WebRTC 帧接收结束: %s (共 %d 帧)", task_id, frame_count)
        _webrtc_pcs.pop(task_id, None)
        try:
            await asyncio.wait_for(pc.close(), timeout=3.0)
        except Exception:
            pass
        if use_queue and frame_queue:
            try:
                frame_queue.put_nowait(None)
            except queue.Full:
                pass
        # 仅在 pipeline 仍在运行时才触发延迟清理（pipeline 已结束则无需干预）
        task_still_running = task_id in _task_status and _task_status[task_id]["status"] == "running"
        if task_still_running:
            _task_status[task_id]["progress"] = f"WebRTC 已断开（共 {frame_count} 帧），等待 pipeline 结束..."
            asyncio.create_task(_delayed_cleanup(task_id, delay=10))


class WebRTCOfferRequest(BaseModel):
    sdp: str
    type: str = "offer"


@router.post("/webrtc/offer/{task_id}")
async def webrtc_offer(task_id: str, req: WebRTCOfferRequest):
    """WebRTC 信令端点：接收 SDP offer，返回 SDP answer"""
    from aiortc import RTCPeerConnection, RTCSessionDescription

    async with _state_lock:
        if task_id not in _task_status:
            raise HTTPException(status_code=404, detail="任务不存在")
        if _task_status[task_id]["status"] != "running":
            raise HTTPException(status_code=400, detail="任务未运行")

    # 关闭该 task 已有的 PeerConnection（防止重复连接）
    old_pc = _webrtc_pcs.pop(task_id, None)
    if old_pc:
        try:
            await old_pc.close()
        except Exception:
            pass

    frame_queue = _frame_queues.get(task_id)
    use_queue = frame_queue is not None
    frames_dir = _get_browser_frames_dir(task_id) if not use_queue else None

    # 创建 PeerConnection
    pc = RTCPeerConnection()
    _webrtc_pcs[task_id] = pc

    @pc.on("connectionstatechange")
    async def on_connectionstatechange():
        logger.info("WebRTC 连接状态: %s, task=%s", pc.connectionState, task_id)
        # 只在确定失败时关闭，disconnected 是临时状态可能恢复
        if pc.connectionState in ("failed", "closed"):
            _webrtc_pcs.pop(task_id, None)
            try:
                await asyncio.wait_for(pc.close(), timeout=3.0)
            except Exception:
                pass

    # 设置远程描述（浏览器的 offer）
    offer = RTCSessionDescription(sdp=req.sdp, type=req.type)
    await pc.setRemoteDescription(offer)

    # 创建 answer
    answer = await pc.createAnswer()
    await pc.setLocalDescription(answer)

    # 启动帧接收（后台任务）
    asyncio.create_task(_receive_webrtc_camera_frames(
        pc, task_id, frame_queue, use_queue, frames_dir
    ))

    return {
        "sdp": pc.localDescription.sdp,
        "type": pc.localDescription.type,
    }


async def _receive_h264_camera_frames(
    websocket: WebSocket, task_id: str,
    frame_queue: queue.Queue | None, use_queue: bool,
    frames_dir: Path | None, codec: str,
):
    """H264 模式：接收 MediaRecorder 编码的视频流，ffmpeg 解码后送队列

    流程：前端 MediaRecorder 产出 WebM/MP4 chunk → WebSocket binary → ffmpeg stdin 解码
          → raw BGR 帧 → pipeline 队列
    """
    import cv2
    import numpy as np

    # MediaRecorder 即使指定 video/mp4，大多数浏览器实际输出 WebM 容器
    # 所以不强制指定输入格式，让 ffmpeg 自动探测（同时增加 probesize 加速识别）
    ffmpeg_bin = _find_binary("ffmpeg") or "ffmpeg"
    ffmpeg_cmd = [
        ffmpeg_bin, "-hide_banner", "-loglevel", "warning",
        "-fflags", "+nobuffer+discardcorrupt+fastseek",
        "-flags", "+low_delay",
        "-probesize", "16384",        # 16KB 探测（更小更快）
        "-analyzeduration", "200000",  # 200ms 分析时间
        "-max_delay", "0",            # 无延迟缓冲
        "-i", "pipe:0",
        "-vf", "scale=640:480",  # 强制输出目标分辨率
        "-f", "rawvideo",
        "-pix_fmt", "bgr24",
        "pipe:1",
    ]

    async with _state_lock:
        _task_status[task_id]["progress"] = f"H264 推流中 (codec={codec})..."

    ffmpeg_proc = None
    frame_count = 0
    frame_size = 640 * 480 * 3
    stderr_lines: list[str] = []

    async def _feed_and_decode():
        """主循环：从 WebSocket 接收数据 → 喂 ffmpeg → 读解码帧 → 送队列"""
        nonlocal frame_count

        async def _drain_stderr():
            """后台消费 ffmpeg stderr，防止 pipe 满阻塞"""
            try:
                while True:
                    line = await ffmpeg_proc.stderr.readline()
                    if not line:
                        break
                    text = line.decode("utf-8", errors="replace").strip()
                    if text:
                        stderr_lines.append(text)
                        logger.debug("[ffmpeg h264-cam %s] %s", task_id, text)
            except (asyncio.IncompleteReadError, OSError):
                pass

        # 启动 stderr 消费
        stderr_task = asyncio.create_task(_drain_stderr())

        try:
            # ── 第一阶段：接收 WebSocket 数据，喂给 ffmpeg stdin ──
            # 同时从 stdout 读解码帧（两个操作并行）
            async def _feed_stdin():
                """从 WebSocket 读数据块 → ffmpeg stdin"""
                try:
                    while _task_status.get(task_id, {}).get("status") == "running":
                        msg = await websocket.receive()
                        if "bytes" in msg:
                            chunk = msg["bytes"]
                            if chunk and ffmpeg_proc.stdin and not ffmpeg_proc.stdin.is_closing():
                                ffmpeg_proc.stdin.write(chunk)
                                await ffmpeg_proc.stdin.drain()
                        elif "text" in msg:
                            pass  # 忽略控制消息
                except WebSocketDisconnect:
                    logger.info("H264 WebSocket 断开: %s", task_id)
                except (BrokenPipeError, OSError) as e:
                    logger.debug("H264 stdin 喂入结束: %s", e)
                finally:
                    # 关闭 stdin，通知 ffmpeg 输入结束
                    if ffmpeg_proc.stdin and not ffmpeg_proc.stdin.is_closing():
                        try:
                            ffmpeg_proc.stdin.close()
                        except Exception:
                            pass

            async def _read_stdout():
                """从 ffmpeg stdout 读取解码后的 BGR 帧 → pipeline 队列"""
                nonlocal frame_count
                read_buf = bytearray()
                try:
                    while True:
                        chunk = await ffmpeg_proc.stdout.read(65536)
                        if not chunk:
                            break
                        read_buf.extend(chunk)
                        while len(read_buf) >= frame_size:
                            frame_data = bytes(read_buf[:frame_size])
                            del read_buf[:frame_size]

                            frame = np.frombuffer(frame_data, np.uint8).reshape(480, 640, 3).copy()

                            if use_queue:
                                _queue_put_latest(frame_queue, frame)
                                if frame_count % 30 == 0:
                                    logger.info("[H264 Cam] 帧已入队: %d, 队列大小: %d", frame_count, frame_queue.qsize())
                            else:
                                _, jpg_buf = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
                                if frames_dir:
                                    frame_path = frames_dir / "latest.jpg"
                                    tmp_path = frames_dir / "latest.jpg.tmp"
                                    tmp_path.write_bytes(jpg_buf.tobytes())
                                    tmp_path.rename(frame_path)

                            frame_count += 1
                            if frame_count % 30 == 0:
                                logger.debug("H264 解码帧计数: %d", frame_count)
                                try:
                                    await websocket.send_json({"ok": True, "frame": frame_count})
                                except Exception:
                                    pass
                except asyncio.IncompleteReadError:
                    logger.debug("H264 stdout 结束 (IncompleteRead), 已解码 %d 帧", frame_count)
                except OSError:
                    pass

            # 并行：喂数据 + 读帧
            await asyncio.gather(_feed_stdin(), _read_stdout())

        except Exception as e:
            logger.error("H264 解码异常 [%s]: %s", task_id, e)
        finally:
            stderr_task.cancel()
            # 如果 ffmpeg 还在运行，等待它退出并检查错误
            if ffmpeg_proc and ffmpeg_proc.returncode is None:
                try:
                    # 关闭 stdin 触发 EOF
                    if ffmpeg_proc.stdin and not ffmpeg_proc.stdin.is_closing():
                        ffmpeg_proc.stdin.close()
                except Exception:
                    pass
                try:
                    await asyncio.wait_for(ffmpeg_proc.wait(), timeout=3.0)
                except asyncio.TimeoutError:
                    ffmpeg_proc.kill()
            # 记录 ffmpeg 退出码
            if ffmpeg_proc:
                rc = ffmpeg_proc.returncode
                if rc != 0 and frame_count == 0:
                    logger.error("ffmpeg 退出码 %d，未解码任何帧。stderr:\n%s",
                                 rc, "\n".join(stderr_lines[-20:]))
                elif rc != 0:
                    logger.warning("ffmpeg 退出码 %d (已解码 %d 帧)", rc, frame_count)

    try:
        # 启动 ffmpeg 解码进程
        ffmpeg_proc = await asyncio.create_subprocess_exec(
            *ffmpeg_cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        logger.info("H264 解码器已启动: codec=%s, cmd=%s, task=%s",
                     codec, " ".join(ffmpeg_cmd), task_id)

        await _feed_and_decode()

    except Exception as e:
        logger.error("H264 摄像头推流异常 [%s]: %s", task_id, e)
    finally:
        if ffmpeg_proc:
            try:
                if ffmpeg_proc.stdin and not ffmpeg_proc.stdin.is_closing():
                    ffmpeg_proc.stdin.close()
            except Exception:
                pass
            try:
                ffmpeg_proc.kill()
            except ProcessLookupError:
                pass
        if use_queue and frame_queue:
            try:
                frame_queue.put_nowait(None)
            except queue.Full:
                pass
        logger.info("H264 推流结束: %s (共解码 %d 帧)", task_id, frame_count)
        if task_id in _task_status and _task_status[task_id]["status"] == "running":
            _task_status[task_id]["progress"] = f"摄像头已断开（共解码 {frame_count} 帧），等待 pipeline 结束..."
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
async def get_output_video(request: Request, filename: str):
    """下载/播放输出视频（自动转码 + Range 支持）"""
    filename = _safe_filename(filename)
    output_dir = _get_output_dir()
    video_path = output_dir / filename
    # 如果文件不存在，尝试补 .mp4 后缀
    if not video_path.exists() and not filename.endswith('.mp4'):
        video_path = output_dir / f"{filename}.mp4"
    if not video_path.exists():
        raise HTTPException(status_code=404, detail=f"视频不存在: {filename}")

    # 自动转码不兼容的编码为 H264
    video_path = await asyncio.to_thread(_ensure_h264, video_path)

    ext = video_path.suffix.lower()
    mime_map = {
        ".mp4": "video/mp4", ".avi": "video/x-msvideo",
        ".mkv": "video/x-matroska", ".mov": "video/quicktime",
        ".flv": "video/x-flv", ".wmv": "video/x-ms-wmv", ".webm": "video/webm",
    }
    media_type = mime_map.get(ext, "video/mp4")

    # 支持 Range 请求
    file_size = video_path.stat().st_size
    range_header = request.headers.get("range")

    if range_header:
        try:
            ranges = range_header.replace("bytes=", "").split("-")
            start = int(ranges[0]) if ranges[0] else 0
            end = int(ranges[1]) if ranges[1] else file_size - 1
            end = min(end, file_size - 1)

            if start >= file_size:
                raise HTTPException(status_code=416, detail="Range 不满足")

            content_length = end - start + 1

            def ranged_file():
                with open(video_path, "rb") as f:
                    f.seek(start)
                    remaining = content_length
                    while remaining > 0:
                        chunk_size = min(65536, remaining)
                        chunk = f.read(chunk_size)
                        if not chunk:
                            break
                        remaining -= len(chunk)
                        yield chunk

            return StreamingResponse(
                ranged_file(),
                status_code=206,
                media_type=media_type,
                headers={
                    "Content-Range": f"bytes {start}-{end}/{file_size}",
                    "Accept-Ranges": "bytes",
                    "Content-Length": str(content_length),
                    "Content-Disposition": f'inline; filename="{filename}"',
                },
            )
        except (ValueError, IndexError):
            pass

    return FileResponse(
        path=str(video_path),
        media_type=media_type,
        filename=filename,
        headers={"Accept-Ranges": "bytes"},
    )


@router.get("/video/{filename}")
async def get_source_video(request: Request, filename: str):
    """获取源视频用于播放（自动转码 HEVC → H264，支持 Range 请求）"""
    filename = _safe_filename(filename)
    demo_dir = _get_demo_dir()
    video_path = demo_dir / filename
    if not video_path.exists():
        raise HTTPException(status_code=404, detail=f"视频不存在: {filename}")

    # 自动转码不兼容的编码（如 H265/HEVC）为 H264
    video_path = await asyncio.to_thread(_ensure_h264, video_path)

    ext = video_path.suffix.lower()
    mime_map = {
        ".mp4": "video/mp4", ".avi": "video/x-msvideo",
        ".mkv": "video/x-matroska", ".mov": "video/quicktime",
        ".flv": "video/x-flv", ".wmv": "video/x-ms-wmv", ".webm": "video/webm",
    }
    media_type = mime_map.get(ext, "video/mp4")

    # 支持 Range 请求（视频拖拽/seek）
    file_size = video_path.stat().st_size
    range_header = request.headers.get("range")

    if range_header:
        # 解析 Range: bytes=start-end
        try:
            ranges = range_header.replace("bytes=", "").split("-")
            start = int(ranges[0]) if ranges[0] else 0
            end = int(ranges[1]) if ranges[1] else file_size - 1
            end = min(end, file_size - 1)

            if start >= file_size:
                raise HTTPException(status_code=416, detail="Range 不满足")

            content_length = end - start + 1

            def ranged_file():
                with open(video_path, "rb") as f:
                    f.seek(start)
                    remaining = content_length
                    while remaining > 0:
                        chunk_size = min(65536, remaining)
                        chunk = f.read(chunk_size)
                        if not chunk:
                            break
                        remaining -= len(chunk)
                        yield chunk

            return StreamingResponse(
                ranged_file(),
                status_code=206,
                media_type=media_type,
                headers={
                    "Content-Range": f"bytes {start}-{end}/{file_size}",
                    "Accept-Ranges": "bytes",
                    "Content-Length": str(content_length),
                    "Content-Disposition": f'inline; filename="{filename}"',
                },
            )
        except (ValueError, IndexError):
            pass

    # 无 Range — 返回完整文件
    return FileResponse(
        path=str(video_path),
        media_type=media_type,
        filename=filename,
        headers={"Accept-Ranges": "bytes"},
    )


# ── 清理历史 ──

@router.delete("/tasks/clear")
async def clear_finished_tasks():
    """清除已完成/失败的任务记录"""
    global _task_status
    before = len(_task_status)
    _task_status = {k: v for k, v in _task_status.items() if v["status"] == "running"}
    cleared = before - len(_task_status)
    return {"success": True, "message": f"已清除 {cleared} 条历史记录"}
