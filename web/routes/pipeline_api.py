"""Pipeline API 路由 — 视频上传、Demo 播放、摄像头流控制"""

from __future__ import annotations

import asyncio
import logging
import uuid
from pathlib import Path
from typing import Any

from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel

from config import load_config

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])

# ── 全局状态 ──
# 存储正在运行的 pipeline 进程
_running_processes: dict[str, asyncio.subprocess.Process] = {}
# 存储任务状态
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


# ── 请求/响应模型 ──

class PipelineStartRequest(BaseModel):
    video_filename: str
    use_agent: bool = False
    concurrent_mode: bool = True
    display: bool = False


class PipelineStartResponse(BaseModel):
    success: bool
    message: str
    task_id: str | None = None
    output_filename: str | None = None


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str  # "running", "completed", "failed"
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
    name = Path(filename).name
    if not name or name.startswith('.') or '..' in name or '/' in name or '\\' in name:
        raise HTTPException(status_code=400, detail="无效的文件名")
    return name


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
    """上传视频到 demovid 目录"""
    _ensure_dirs()
    cfg = _get_demo_config()
    max_size = cfg.get("max_file_size_mb", 500) * 1024 * 1024
    allowed = _get_allowed_extensions()

    # 检查文件扩展名
    filename = file.filename or "upload.mp4"
    # 安全处理：只保留文件名部分
    filename = Path(filename).name
    if not filename or filename.startswith('.'):
        filename = "upload.mp4"
    ext = Path(filename).suffix.lower()
    if ext not in allowed:
        raise HTTPException(status_code=400, detail=f"不支持的视频格式: {ext}，支持: {', '.join(sorted(allowed))}")

    # 读取并检查大小
    contents = await file.read()
    if len(contents) > max_size:
        raise HTTPException(status_code=400, detail=f"文件过大，最大 {cfg.get('max_file_size_mb', 500)}MB")

    # 保存文件
    demo_dir = _get_demo_dir()
    save_path = demo_dir / filename
    # 避免覆盖，添加后缀
    if save_path.exists():
        stem = save_path.stem
        suffix = save_path.suffix
        counter = 1
        while save_path.exists():
            save_path = demo_dir / f"{stem}_{counter}{suffix}"
            counter += 1

    save_path.write_bytes(contents)
    logger.info("视频已上传: %s (%.2f MB)", save_path.name, len(contents) / (1024 * 1024))

    return {
        "success": True,
        "message": f"视频已上传: {save_path.name}",
        "filename": save_path.name,
        "size_mb": round(len(contents) / (1024 * 1024), 2),
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
    """启动视频处理 Pipeline"""
    _ensure_dirs()
    demo_dir = _get_demo_dir()
    output_dir = _get_output_dir()

    video_filename = _safe_filename(req.video_filename)
    video_path = demo_dir / video_filename
    if not video_path.exists():
        raise HTTPException(status_code=404, detail=f"视频不存在: {video_filename}")

    # 生成任务 ID
    task_id = str(uuid.uuid4())[:8]

    # 输出文件名
    stem = Path(video_filename).stem
    output_filename = f"{stem}_result_{task_id}.mp4"
    output_path = output_dir / output_filename

    # 构建 pipeline 命令
    config = load_config()
    pipeline_cfg = config.get("pipeline", {})

    # 使用 python -m pipeline.cli
    cmd = [
        "python", "-m", "pipeline.cli",
        str(video_path),
        "--output", str(output_path),
    ]

    if req.use_agent:
        cmd.append("--agent")

    if req.concurrent_mode:
        cmd.extend(["-c", "--max-concurrent", str(pipeline_cfg.get("max_concurrent", 4))])

    if req.display:
        cmd.append("--display")

    cmd.append("--demo")

    logger.info("启动 Pipeline: %s", " ".join(cmd))

    # 记录任务状态
    _task_status[task_id] = {
        "task_id": task_id,
        "status": "running",
        "video_filename": video_filename,
        "output_filename": output_filename,
        "output_path": str(output_path),
        "progress": "处理中...",
        "error": None,
    }

    # 后台运行 pipeline
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(Path.cwd()),
        )
        _running_processes[task_id] = process

        # 异步等待进程完成
        asyncio.create_task(_wait_pipeline(task_id, process, output_filename))

    except FileNotFoundError:
        _task_status[task_id]["status"] = "failed"
        _task_status[task_id]["error"] = "pipeline.cli 模块不存在，请确认 pipeline 目录已实现"
        raise HTTPException(status_code=500, detail="pipeline.cli 模块不存在")

    return PipelineStartResponse(
        success=True,
        message=f"Pipeline 已启动，任务 ID: {task_id}",
        task_id=task_id,
        output_filename=output_filename,
    )


async def _wait_pipeline(task_id: str, process: asyncio.subprocess.Process, output_filename: str):
    """异步等待 pipeline 完成"""
    try:
        stdout, stderr = await process.communicate()
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
    return {"success": True, "message": f"已停止任务: {task_id}"}


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
    return FileResponse(
        path=str(video_path),
        media_type="video/mp4",
        filename=filename,
    )


@router.get("/video/{filename}")
async def get_source_video(filename: str):
    """获取源视频用于播放"""
    filename = _safe_filename(filename)
    demo_dir = _get_demo_dir()
    video_path = demo_dir / filename
    if not video_path.exists():
        raise HTTPException(status_code=404, detail=f"视频不存在: {filename}")

    # 根据扩展名设置 MIME 类型
    ext = video_path.suffix.lower()
    mime_map = {
        ".mp4": "video/mp4",
        ".avi": "video/x-msvideo",
        ".mkv": "video/x-matroska",
        ".mov": "video/quicktime",
        ".flv": "video/x-flv",
        ".wmv": "video/x-ms-wmv",
        ".webm": "video/webm",
    }
    mime = mime_map.get(ext, "video/mp4")

    return FileResponse(
        path=str(video_path),
        media_type=mime,
        filename=filename,
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
