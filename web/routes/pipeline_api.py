"""Pipeline API 路由 — 视频上传、Demo 播放、摄像头流控制"""

from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import sys
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
    concurrent_mode: bool = True
    display: bool = False
    # ── 核心检测参数 ──
    conf_threshold: float = 0.25
    iou_threshold: float = 0.45
    process_every: int = 15
    detect_every: int = 2
    # ── 高级参数 ──
    max_frames: int = 0
    device: str = ""
    yolo_model: str = ""
    prompt_mode: str = "detailed"
    enable_refresh: bool = False
    gap_num: int = 150
    max_concurrent: int = 4


class BrowserCameraStartRequest(BaseModel):
    concurrent_mode: bool = True
    # ── 核心检测参数 ──
    conf_threshold: float = 0.25
    iou_threshold: float = 0.45
    process_every: int = 15
    detect_every: int = 2
    # ── 高级参数 ──
    max_frames: int = 0
    device: str = ""
    yolo_model: str = ""
    prompt_mode: str = "detailed"
    enable_refresh: bool = False
    gap_num: int = 150
    max_concurrent: int = 4


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
    if sem.locked():
        running_count = sum(1 for t in _task_status.values() if t["status"] == "running")
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

    # 检测视频分辨率（H.264 编码需要知道帧尺寸）
    video_w, video_h = 640, 480  # 默认值
    if not is_camera and video_path:
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
    ]
    if req.concurrent_mode:
        cmd.extend(["-c", "--max-concurrent", str(req.max_concurrent or pipeline_cfg.get("max_concurrent", 4))])

    # ── 核心检测参数 ──
    cmd.extend(["--conf", str(req.conf_threshold)])
    cmd.extend(["--iou", str(req.iou_threshold)])
    cmd.extend(["--process-every", str(req.process_every)])
    cmd.extend(["--detect-every", str(req.detect_every)])

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

    if is_camera:
        cmd.append("--camera")
        # 摄像头模式：通过 stream-dir 实现 MJPEG 流
        stream_dir = _get_stream_dir(task_id)
        cmd.extend(["--stream-dir", str(stream_dir)])
    else:
        # 文件模式：raw stdout 输出（H.264 编码用），不写磁盘
        cmd.append("--raw-stdout")
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
        if not is_camera:
            asyncio.create_task(_start_h264_reader(task_id, process, video_w, video_h))

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

    # 写入停止信号文件（pipeline 可以检测到）
    stream_dir = _get_stream_dir(task_id)
    stop_file = stream_dir / "__STOP__"
    try:
        stop_file.write_text("stop")
    except Exception:
        pass

    import signal

    # 第一步：SIGTERM 整个进程组
    try:
        pgid = os.getpgid(process.pid)
        os.killpg(pgid, signal.SIGTERM)
        logger.info("已发送 SIGTERM 到进程组 %d (task=%s)", pgid, task_id)
    except (ProcessLookupError, PermissionError):
        try:
            process.terminate()
        except ProcessLookupError:
            pass

    # 第二步：等待 3 秒，如果还没退出就 SIGKILL
    try:
        await asyncio.wait_for(process.wait(), timeout=3.0)
    except asyncio.TimeoutError:
        logger.warning("SIGTERM 超时，强制 SIGKILL (task=%s)", task_id)
        try:
            pgid = os.getpgid(process.pid)
            os.killpg(pgid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            try:
                process.kill()
            except ProcessLookupError:
                pass
        try:
            await asyncio.wait_for(process.wait(), timeout=2.0)
        except asyncio.TimeoutError:
            pass

    # 更新状态
    async with _state_lock:
        _task_status[task_id]["status"] = "failed"
        _task_status[task_id]["error"] = "用户手动停止"
        _running_processes.pop(task_id, None)
        _stop_signals.discard(task_id)

    # 清理帧目录和停止信号文件
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
    """清理帧共享目录和停止信号文件"""
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
    _cleanup_stream_dir(task_id)


def _get_browser_frames_dir(task_id: str) -> Path:
    """获取浏览器摄像头帧目录"""
    d = Path("./_browser_frames") / task_id
    d.mkdir(parents=True, exist_ok=True)
    return d


# ── H.264 推流管理 ──

async def _start_h264_reader(task_id: str, process: asyncio.subprocess.Process, w: int = 640, h: int = 480):
    """后台任务：从 pipeline stdout 读取 raw BGR 帧，启动 ffmpeg 编码为 H.264 fMP4"""

    # ffmpeg 命令：stdin raw BGR → H.264 fMP4
    ffmpeg_cmd = _find_binary("ffmpeg") or "ffmpeg"
    ffmpeg_args = [
        ffmpeg_cmd, "-hide_banner", "-loglevel", "error",
        "-f", "rawvideo", "-pix_fmt", "bgr24", "-video_size", f"{w}x{h}",
        "-r", "15",  # 输入帧率（匹配 pipeline 实际输出速率）
        "-i", "pipe:0",
        "-c:v", "libx264",
        "-preset", "ultrafast", "-tune", "zerolatency",
        "-profile:v", "baseline", "-level", "3.1",
        "-bf", "0",        # 无 B 帧（降低延迟）
        "-g", "60",        # GOP = 60 帧（每 4 秒一个关键帧）
        "-pix_fmt", "yuv420p",
        "-movflags", "+frag_keyframe+empty_moov+default_base_moof+faststart",
        "-frag_duration", "2000000",  # 2 秒一个 fragment（减少 MSE 重解码频率）
        "-flush_packets", "1",
        "-f", "mp4",
        "pipe:1",
    ]

    # 状态初始化
    async with _state_lock:
        _h264_streams[task_id] = {
            "ffmpeg": None,
            "viewers": set(),
            "init_segment": None,
            "latest_segments": [],
            "max_segments": 30,
            "reader_task": None,
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
            """从 pipeline stdout 读 raw 帧 → ffmpeg stdin"""
            nonlocal running
            try:
                while running:
                    data = await process.stdout.readexactly(frame_size)
                    if not data:
                        break
                    if ffmpeg_proc.stdin and not ffmpeg_proc.stdin.is_closing():
                        ffmpeg_proc.stdin.write(data)
                        await ffmpeg_proc.stdin.drain()
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
                    chunk = await ffmpeg_proc.stdout.read(65536)
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


async def _broadcast_h264(task_id: str, data: bytes):
    """向所有观看此 task 的 WebSocket 客户端广播 fMP4 数据"""
    async with _state_lock:
        stream = _h264_streams.get(task_id)
        if not stream:
            return
        viewers = list(stream["viewers"])

    if not viewers:
        return

    disconnected = []
    for ws in viewers:
        try:
            await ws.send_bytes(data)
        except Exception:
            disconnected.append(ws)

    if disconnected:
        async with _state_lock:
            stream = _h264_streams.get(task_id)
            if stream:
                for ws in disconnected:
                    stream["viewers"].discard(ws)


async def _stop_h264_stream(task_id: str):
    """停止 H.264 推流"""
    async with _state_lock:
        stream = _h264_streams.pop(task_id, None)
    if not stream:
        return
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

    try:
        # 发送初始化段（如果已有）
        init_seg = stream.get("init_segment")
        if init_seg:
            await websocket.send_bytes(b"\x01" + len(init_seg).to_bytes(4, "big") + init_seg)

        # 发送最近的媒体段（避免新客户端黑屏）
        for seg in stream.get("latest_segments", []):
            await websocket.send_bytes(b"\x02" + len(seg).to_bytes(4, "big") + seg)

        # 保持连接，等待后续帧
        while True:
            # 检查任务状态
            async with _state_lock:
                task = _task_status.get(task_id)
                if not task or task["status"] != "running":
                    break
            await asyncio.sleep(1)

    except WebSocketDisconnect:
        pass
    except Exception as e:
        if "AssertionError" not in str(type(e).__name__):
            logger.debug("H.264 WebSocket 异常 [%s]: %s", task_id, e)
    finally:
        async with _state_lock:
            stream = _h264_streams.get(task_id)
            if stream:
                stream["viewers"].discard(websocket)


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

    except WebSocketDisconnect:
        pass
    except Exception as e:
        # 抑制 websockets 库的 drain/ping 断言错误（客户端异常断开时触发）
        if "AssertionError" not in str(type(e).__name__):
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
    """启动浏览器摄像头 Pipeline（等待 WebSocket 推流）"""
    _ensure_dirs()

    # 并发控制
    sem = _get_semaphore()
    if sem.locked():
        running_count = sum(1 for t in _task_status.values() if t["status"] == "running")
        raise HTTPException(
            status_code=429,
            detail=f"已有 {running_count} 个 Pipeline 在运行（上限 {_MAX_PARALLEL_PIPELINES}），请等待完成后再试",
        )

    config = load_config()
    pipeline_cfg = config.get("pipeline", {})

    task_id = str(uuid.uuid4())[:8]
    frames_dir = _get_browser_frames_dir(task_id)
    stream_dir = _get_stream_dir(task_id)

    cmd = [
        sys.executable, "-m", "pipeline",
        f"browser_camera_{task_id}",
        "--frames-dir", str(frames_dir),
        "--virtual-fps", "15",
        "--no-output",  # 不保存输出视频
        "--stream-dir", str(stream_dir),
        "--camera",
        "--demo",
    ]
    if req.concurrent_mode:
        cmd.extend(["-c", "--max-concurrent", str(req.max_concurrent or pipeline_cfg.get("max_concurrent", 4))])

    # ── 核心检测参数 ──
    cmd.extend(["--conf", str(req.conf_threshold)])
    cmd.extend(["--iou", str(req.iou_threshold)])
    cmd.extend(["--process-every", str(req.process_every)])
    cmd.extend(["--detect-every", str(req.detect_every)])

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

    logger.info("启动浏览器摄像头 Pipeline: %s", " ".join(cmd))

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
        }

    try:
        await sem.acquire()
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(Path.cwd()),
            preexec_fn=os.setsid if hasattr(os, 'setsid') else None,
        )
        async with _state_lock:
            _running_processes[task_id] = process
        asyncio.create_task(_wait_pipeline(task_id, process, sem))
    except FileNotFoundError:
        sem.release()
        async with _state_lock:
            _task_status[task_id]["status"] = "failed"
            _task_status[task_id]["error"] = "pipeline 模块不存在"
        raise HTTPException(status_code=500, detail="pipeline 模块不存在")

    return PipelineStartResponse(
        success=True,
        message=f"浏览器摄像头 Pipeline 已启动，请连接 WebSocket 推流",
        task_id=task_id,
    )


@router.websocket("/ws/camera/{task_id}")
async def browser_camera_ws(websocket: WebSocket, task_id: str):
    """WebSocket 端点 — 接收浏览器摄像头 JPEG 帧"""
    async with _state_lock:
        if task_id not in _task_status:
            await websocket.close(code=4004, reason="任务不存在")
            return

    frames_dir = _get_browser_frames_dir(task_id)
    await websocket.accept()
    logger.info("浏览器摄像头 WebSocket 已连接: %s", task_id)

    async with _state_lock:
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
        if "AssertionError" not in str(type(e).__name__):
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
