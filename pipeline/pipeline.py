"""
ShipPipeline — 主流水线编排

三步链路（硬编码模式，无 LangChain Agent 依赖）：
  Step1: VLM 识别 → hull_number + description
  Step2: db.lookup(hull_number) → 精确匹配
  Step3: db.semantic_search_filtered(description) → 语义检索

级联模式（concurrent_mode=false）：
  YOLO 检测 → 三步链路 → 绑定结果 → 绘制输出

并发模式（concurrent_mode=true）：
  YOLO 检测 → crop 送入队列 → worker 线程异步推理
  → 结果按帧时间戳严格顺序出队 → 绑定到对应帧绘制输出
"""

from __future__ import annotations

import base64
import logging
import os
import queue
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Callable

import cv2
import numpy as np

from agent import AgentResult
from pipeline.detector import ShipDetector, Detection
from pipeline.demo import DemoRenderer
from pipeline.output import ScreenshotSaver
from pipeline.fps import FPSMeter, LatencyMeter
from pipeline.tracker import TrackManager
from pipeline.video_input import InputSource

logger = logging.getLogger(__name__)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


class ShipPipeline:
    """
    船弦号识别视频处理流水线。

    整合 YOLO 检测、三步链路识别、跟踪管理，支持级联/并发双模式。
    """

    def __init__(self, config: dict[str, Any] | None = None):
        if config is None:
            from config import load_config
            config = load_config()

        self._config = config
        pipe_cfg = config.get("pipeline", {})

        self._concurrent_mode: bool = bool(pipe_cfg.get("concurrent_mode", False))
        self._yolo_async: bool = bool(pipe_cfg.get("yolo_async", False))
        self._max_concurrent: int = pipe_cfg.get("max_concurrent") or 4
        self._max_queued_frames: int = pipe_cfg.get("max_queued_frames") or 30
        self._process_every_n: int = max(1, pipe_cfg.get("process_every_n_frames") or 1)
        self._detect_every_n: int = max(1, pipe_cfg.get("detect_every_n_frames") or 1)
        self._demo_enabled: bool = bool(pipe_cfg.get("demo", False))
        self._save_screenshots: bool = bool(pipe_cfg.get("save_screenshots", True))
        self._enable_refresh: bool = bool(pipe_cfg.get("enable_refresh", False))
        self._gap_num: int = pipe_cfg.get("gap_num") or 150
        self._prompt_mode: str = pipe_cfg.get("prompt_mode") or "detailed"
        self._output_size: tuple[int, int] | None = None
        _os = pipe_cfg.get("output_size")
        if _os and len(_os) == 2:
            self._output_size = (int(_os[0]), int(_os[1]))
        self._stop_file: Path | None = Path(pipe_cfg["stop_file"]) if pipe_cfg.get("stop_file") else None

        from database import ShipDatabase
        self._db = ShipDatabase(config=config)

        self._detector = ShipDetector(
            model_path=pipe_cfg.get("yolo_model", "yolov8n.pt"),
            device=pipe_cfg.get("device", ""),
            conf_threshold=pipe_cfg.get("conf_threshold", 0.25),
            iou_threshold=pipe_cfg.get("iou_threshold", 0.45),
            tracker_type=pipe_cfg.get("tracker", "bytetrack"),
            tracker_params=pipe_cfg.get("tracker_params"),
            classes=pipe_cfg.get("detect_classes", [8]),
        )

        self._tracker = TrackManager(max_stale_frames=pipe_cfg.get("max_stale_frames", 300))
        self._fps = FPSMeter(window_seconds=10.0)
        self._latency = LatencyMeter(window_seconds=10.0)
        self._renderer = DemoRenderer(show_fps=True, show_track_id=True)

        output_dir = pipe_cfg.get("output_dir", "./output")
        self._saver = ScreenshotSaver(output_dir=output_dir)

        # 并发模式
        self._task_queue: queue.Queue = queue.Queue(maxsize=self._max_queued_frames)
        self._result_queue: queue.Queue = queue.Queue(maxsize=self._max_queued_frames)
        self._workers: list[threading.Thread] = []
        self._stop_event = threading.Event()

        # YOLO 异步检测（yolo_async 模式）
        self._yolo_frame_queue: queue.Queue = queue.Queue(maxsize=2)
        self._yolo_result_queue: queue.Queue = queue.Queue(maxsize=2)
        self._yolo_thread: threading.Thread | None = None

        # Agent 运行链路日志
        self._agent_trace: list[dict[str, Any]] = []
        self._trace_lock = threading.Lock()
        self._max_trace_entries = 500

        # 渲染帧缓存（非处理帧复用，避免重复 PIL 渲染）
        self._cached_display_frame: np.ndarray | None = None

        logger.info(
            "ShipPipeline 初始化: mode=%s, process_every=%d, refresh=%s(gap=%d)",
            "concurrent" if self._concurrent_mode else "cascade",
            self._process_every_n,
            "on" if self._enable_refresh else "off",
            self._gap_num,
        )

    # ── 链路日志 ──────────────────────────────

    def _log_agent_trace(self, event_type: str, track_id: int, frame_id: int, content: str = "", **extra: Any) -> None:
        entry = {"type": event_type, "track_id": track_id, "frame_id": frame_id, "content": content, "timestamp": time.time(), **extra}
        with self._trace_lock:
            self._agent_trace.append(entry)
            if len(self._agent_trace) > self._max_trace_entries:
                self._agent_trace = self._agent_trace[-(self._max_trace_entries // 2):]

    def _log_track_summary(self, track_id: int) -> None:
        with self._trace_lock:
            entries = [e for e in self._agent_trace if e["track_id"] == track_id]
        if not entries:
            return
        latest_frame = max(e["frame_id"] for e in entries)
        entries = [e for e in entries if e["frame_id"] == latest_frame]
        types = {e["type"]: e["content"] for e in entries}
        step1 = types.get("step1_vlm") or "—"
        step2 = types.get("step2_lookup") or "—"
        step3 = types.get("step3_result") or types.get("step3_fallback") or "—"
        logger.info("[Track %d] frame=%d | Step1(VLM): %s | Step2(Lookup): %s | Step3(Result): %s", track_id, latest_frame, step1, step2, step3)

    # ── 工具方法 ──────────────────────────────

    @staticmethod
    def _encode_image(image: np.ndarray) -> str:
        # quality=85 平衡清晰度与速度（弦号文字需要足够清晰度）
        success, buf = cv2.imencode(".jpg", image, [cv2.IMWRITE_JPEG_QUALITY, 85])
        if not success:
            raise RuntimeError("图像编码失败")
        return base64.b64encode(buf.tobytes()).decode("utf-8")

    # ── 三步链路核心 ──────────────────────────

    def _run_three_step_chain(self, crop: np.ndarray, track_id: int = 0, frame_id: int = 0) -> AgentResult:
        """
        三步链路：VLM识别 → 精确查找 → 语义检索。

        Step1: _vlm_infer(crop) → 弦号 + 描述
        Step2: db.lookup(hull_number) → 有弦号时精确查找
        Step3: db.semantic_search_filtered(description) → 弦号未匹配或无弦号时语义检索
        """
        from tools import _vlm_infer

        # Step1: VLM 识别
        crop_b64 = self._encode_image(crop)
        vlm_result = _vlm_infer(crop_b64, prompt_mode=self._prompt_mode)
        hull_number = vlm_result.get("hull_number", "")
        description = vlm_result.get("description", "")

        self._log_agent_trace(
            "step1_vlm", track_id=track_id, frame_id=frame_id,
            content=f"弦号={hull_number or '(无)'} 描述={description[:40] if description else '(无)'}",
        )

        if not hull_number and not description:
            return AgentResult(answer="VLM 未返回结果")

        return self._local_lookup_retrieve(hull_number, description, track_id=track_id, frame_id=frame_id)

    def _local_lookup_retrieve(self, hull_number: str, description: str, track_id: int = 0, frame_id: int = 0) -> AgentResult:
        """本地查库 + 语义检索（不含 VLM 调用）。"""
        exact_matched = False
        semantic_ids: list[str] = []

        if hull_number:
            # Step2: 精确查找
            desc_in_db = self._db.lookup(hull_number)
            if desc_in_db is not None:
                exact_matched = True
                description = description or desc_in_db
            elif description:
                # Step3: 语义检索
                results = self._db.semantic_search_filtered(description)
                semantic_ids = [r["hull_number"] for r in results if r.get("hull_number")]
        elif description:
            # 无弦号，直接语义检索
            results = self._db.semantic_search_filtered(description)
            semantic_ids = [r["hull_number"] for r in results if r.get("hull_number")]

        match_type = "exact" if exact_matched else ("semantic" if semantic_ids else "none")

        if track_id:
            self._log_agent_trace(
                "step2_lookup", track_id=track_id, frame_id=frame_id,
                content=f"精确查找: {'命中' if exact_matched else '未命中'}",
            )
            self._log_agent_trace(
                "step3_result", track_id=track_id, frame_id=frame_id,
                content=f"弦号={hull_number or '(无)'} 匹配={match_type} 语义候选={semantic_ids}",
            )

        return AgentResult(
            hull_number=hull_number,
            description=description,
            match_type=match_type,
            semantic_match_ids=semantic_ids,
        )

    def _run_recognition(self, crop: np.ndarray, track_id: int = 0, frame_id: int = 0) -> AgentResult:
        """统一识别调度：三步链路（硬编码模式）。"""
        with self._latency.measure("vlm"):
            return self._run_three_step_chain(crop, track_id=track_id, frame_id=frame_id)

    # ── 结果处理 ──────────────────────────────

    def _handle_agent_result(self, track_id: int, frame_id: int, agent_result: AgentResult) -> None:
        self._log_track_summary(track_id)
        self._tracker.bind_result(track_id, agent_result.hull_number, agent_result.description, frame_id=frame_id)
        if agent_result.match_type == "exact":
            self._tracker.bind_db_match(track_id, agent_result.hull_number, agent_result.description)
        elif agent_result.semantic_match_ids:
            self._tracker.bind_semantic_matches(track_id, agent_result.semantic_match_ids)

    def _handle_agent_error(self, track_id: int, frame_id: int, error: str) -> None:
        logger.warning("识别出错 (track=%d, frame=%d): %s", track_id, frame_id, error)
        self._tracker.bind_result(track_id, hull_number="", description="", frame_id=frame_id)
        self._log_track_summary(track_id)

    # ── 级联模式 ──────────────────────────────

    def _cascade_process(self, detections: list[Detection], frame_id: int) -> None:
        for det in detections:
            if det.crop is None or det.crop.size == 0:
                continue
            need_new = self._tracker.needs_recognition(det.track_id)
            need_refresh = self._enable_refresh and self._tracker.needs_refresh(det.track_id, frame_id, self._gap_num)
            if not need_new and not need_refresh:
                continue
            self._tracker.mark_pending(det.track_id)
            try:
                agent_result = self._run_recognition(det.crop, track_id=det.track_id, frame_id=frame_id)
                self._handle_agent_result(det.track_id, frame_id, agent_result)
            except Exception as e:
                self._handle_agent_error(det.track_id, frame_id, str(e))

    # ── 并发模式 ──────────────────────────────

    def _concurrent_process(self, detections: list[Detection], frame_id: int) -> None:
        if self._task_queue.qsize() > self._max_queued_frames // 2:
            return
        for det in detections:
            if det.crop is None or det.crop.size == 0:
                continue
            need_new = self._tracker.needs_recognition(det.track_id)
            need_refresh = self._enable_refresh and self._tracker.needs_refresh(det.track_id, frame_id, self._gap_num)
            if not need_new and not need_refresh:
                continue
            self._tracker.mark_pending(det.track_id)
            try:
                self._task_queue.put_nowait({"frame_id": frame_id, "timestamp": time.time(), "track_id": det.track_id, "crop": det.crop.copy()})
            except queue.Full:
                self._tracker.cancel_pending(det.track_id)

    def _worker_loop(self) -> None:
        try:
            while not self._stop_event.is_set():
                try:
                    task = self._task_queue.get(timeout=0.5)
                except queue.Empty:
                    continue
                track_id, frame_id, crop = task["track_id"], task["frame_id"], task["crop"]
                try:
                    agent_result = self._run_recognition(crop, track_id=track_id, frame_id=frame_id)
                except Exception as e:
                    agent_result = AgentResult(answer=str(e))
                try:
                    self._result_queue.put_nowait({"frame_id": frame_id, "track_id": track_id, "agent_result": agent_result})
                except queue.Full:
                    self._tracker.bind_result(track_id, hull_number="", description="", frame_id=frame_id)
        except Exception:
            logger.exception("Worker 线程意外退出")

    def _drain_results(self) -> int:
        count = 0
        while True:
            try:
                pending = self._result_queue.get_nowait()
                track_id, frame_id, agent_result = pending["track_id"], pending["frame_id"], pending["agent_result"]
                if agent_result.hull_number or agent_result.semantic_match_ids or agent_result.match_type in ("exact", "semantic"):
                    self._handle_agent_result(track_id, frame_id, agent_result)
                else:
                    self._handle_agent_error(track_id, frame_id, agent_result.answer or "无结果")
                count += 1
            except queue.Empty:
                break
        return count

    # ── YOLO 异步检测线程 ─────────────────────

    def _yolo_detect_loop(self) -> None:
        """独立线程：从队列取帧 → YOLO 检测 → 结果入队。主循环不再阻塞。"""
        try:
            while not self._stop_event.is_set():
                try:
                    item = self._yolo_frame_queue.get(timeout=0.5)
                except queue.Empty:
                    continue
                if item is None:  # 哨兵值，退出
                    break
                frame, frame_id = item
                try:
                    with self._latency.measure("yolo"):
                        detections = self._detector.detect(frame, frame_id)
                except Exception as e:
                    logger.error("YOLO 异步检测异常 (frame=%d): %s", frame_id, e)
                    detections = []
                try:
                    self._yolo_result_queue.put_nowait((frame_id, detections))
                except queue.Full:
                    # 队列满，丢掉最旧的
                    try:
                        self._yolo_result_queue.get_nowait()
                    except queue.Empty:
                        pass
                    try:
                        self._yolo_result_queue.put_nowait((frame_id, detections))
                    except queue.Full:
                        pass
        except Exception:
            logger.exception("YOLO 检测线程意外退出")

    def _start_yolo_thread(self) -> None:
        self._yolo_thread = threading.Thread(target=self._yolo_detect_loop, name="yolo-detector", daemon=True)
        self._yolo_thread.start()
        logger.info("YOLO 异步检测线程已启动")

    def _stop_yolo_thread(self) -> None:
        if self._yolo_thread and self._yolo_thread.is_alive():
            try:
                self._yolo_frame_queue.put_nowait(None)  # 哨兵值
            except queue.Full:
                pass
            self._yolo_thread.join(timeout=5.0)
            self._yolo_thread = None
        # 清空残留
        while True:
            try:
                self._yolo_frame_queue.get_nowait()
            except queue.Empty:
                break
        while True:
            try:
                self._yolo_result_queue.get_nowait()
            except queue.Empty:
                break

    def _drain_yolo_results(self) -> tuple[int, list[Detection]]:
        """取最新的 YOLO 检测结果（只保留最新一帧，丢弃中间帧）。"""
        latest_frame_id = -1
        latest_detections: list[Detection] = []
        while True:
            try:
                frame_id, detections = self._yolo_result_queue.get_nowait()
                if frame_id >= latest_frame_id:
                    latest_frame_id = frame_id
                    latest_detections = detections
            except queue.Empty:
                break
        return latest_frame_id, latest_detections

    def _start_workers(self) -> None:
        self._stop_event.clear()
        self._workers.clear()
        for i in range(self._max_concurrent):
            w = threading.Thread(target=self._worker_loop, name=f"worker-{i}", daemon=True)
            w.start()
            self._workers.append(w)
        logger.info("启动 %d 个 Worker 线程", self._max_concurrent)

    def _stop_workers(self) -> None:
        self._stop_event.set()
        for w in self._workers:
            w.join(timeout=10.0)
        self._workers.clear()
        while True:
            try:
                self._task_queue.get_nowait()
            except queue.Empty:
                break
        remaining = self._drain_results()
        if remaining:
            logger.info("处理 %d 个残留结果", remaining)

    # ── MJPEG 帧写入器 ────────────────────────

    class _FrameWriter:
        """后台线程：异步编码 JPEG 并写入磁盘，不阻塞主检测循环。"""

        def __init__(self, stream_dir: Path, quality: int = 50):
            self._path = stream_dir / "latest.jpg"
            self._quality = quality
            self._queue: list = []  # 最多保留 1 帧
            self._lock = threading.Lock()
            self._stop = threading.Event()
            self._frame_count = 0
            self._drop_count = 0
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()

        def write(self, frame: np.ndarray) -> None:
            with self._lock:
                if self._queue:
                    self._drop_count += 1
                self._queue = [frame]

        def stop(self) -> None:
            self._stop.set()
            self._thread.join(timeout=2)
            if self._drop_count:
                logger.info("MJPEG 帧写入: 共 %d 帧, 丢弃 %d 帧", self._frame_count, self._drop_count)

        def _run(self) -> None:
            while not self._stop.is_set():
                frame = None
                with self._lock:
                    if self._queue:
                        frame = self._queue.pop(0)
                if frame is not None:
                    try:
                        _, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, self._quality])
                        tmp = self._path.with_suffix(".tmp")
                        with open(tmp, "wb") as f:
                            f.write(buf.tobytes())
                        tmp.rename(self._path)
                        self._frame_count += 1
                    except Exception:
                        pass
                else:
                    self._stop.wait(0.005)

    # ── Raw stdout 帧写入器 ────────────────────

    class _RawStdoutWriter:
        """后台线程：将原始 BGR 帧写入 stdout（供 ffmpeg H.264 编码）。"""

        def __init__(self):
            self._queue: list = []
            self._lock = threading.Lock()
            self._stop = threading.Event()
            self._frame_count = 0
            self._drop_count = 0
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()

        def write(self, frame: np.ndarray) -> None:
            with self._lock:
                if self._queue:
                    self._drop_count += 1
                self._queue = [frame]

        def stop(self) -> None:
            self._stop.set()
            self._thread.join(timeout=2)
            if self._drop_count:
                logger.info("Raw stdout 写入: 共 %d 帧, 丢弃 %d 帧", self._frame_count, self._drop_count)

        def _run(self) -> None:
            import sys
            stdout = sys.stdout.buffer  # 二进制写入
            while not self._stop.is_set():
                frame = None
                with self._lock:
                    if self._queue:
                        frame = self._queue.pop(0)
                if frame is not None:
                    try:
                        # 直接写 raw BGR 数据（ffmpeg -f rawvideo -pix_fmt bgr24）
                        stdout.write(frame.tobytes())
                        stdout.flush()
                        self._frame_count += 1
                    except (BrokenPipeError, OSError):
                        break
                else:
                    self._stop.wait(0.005)

    # ── H265/H264 转码 ────────────────────────

    _FFMPEG: str | None = None
    _FFPROBE: str | None = None

    @staticmethod
    def _find_binary(name: str) -> str | None:
        """查找二进制文件。"""
        import shutil
        found = shutil.which(name)
        if found:
            return found
        for path in [f"/usr/bin/{name}", f"/usr/local/bin/{name}"]:
            if os.path.isfile(path) and os.access(path, os.X_OK):
                return path
        return None

    @classmethod
    def _ensure_ffmpeg(cls):
        """延迟查找 ffmpeg/ffprobe。"""
        if cls._FFMPEG is None:
            cls._FFMPEG = cls._find_binary("ffmpeg") or ""
        if cls._FFPROBE is None:
            cls._FFPROBE = cls._find_binary("ffprobe") or ""

    @classmethod
    def _probe_video_codec(cls, video_path: str) -> str | None:
        """用 ffprobe 检测视频编码格式。"""
        cls._ensure_ffmpeg()
        if not cls._FFPROBE:
            return None
        try:
            ret = subprocess.run(
                [cls._FFPROBE, "-v", "error", "-select_streams", "v:0",
                 "-show_entries", "stream=codec_name",
                 "-of", "default=noprint_wrappers=1:nokey=1", video_path],
                capture_output=True, text=True, timeout=30,
            )
            if ret.returncode == 0:
                codec = ret.stdout.strip().lower()
                if codec:
                    return codec
        except Exception as e:
            logger.warning("ffprobe 检测失败: %s", e)
        return None

    @classmethod
    def _is_browser_compatible_codec(cls, codec: str | None) -> bool:
        """判断编码是否被主流浏览器原生支持。"""
        if codec is None:
            return False
        compatible = {"h264", "vp8", "vp9", "av1", "h264", "mpeg4part10"}
        return codec in compatible

    @classmethod
    def _transcode_to_h264(cls, source_path: str, target_path: str) -> bool:
        """将视频转码为 H264（浏览器兼容）。"""
        cls._ensure_ffmpeg()
        if not cls._FFMPEG:
            logger.error("ffmpeg 不可用，无法转码 H264")
            return False
        try:
            ret = subprocess.run(
                [cls._FFMPEG, "-y", "-i", source_path,
                 "-c:v", "libx264", "-preset", "fast", "-crf", "23",
                 "-pix_fmt", "yuv420p",
                 "-c:a", "aac", "-b:a", "128k",
                 "-movflags", "+faststart",
                 target_path],
                capture_output=True, timeout=600,
            )
            if ret.returncode == 0 and Path(target_path).exists() and Path(target_path).stat().st_size > 0:
                logger.info("H264 转码成功: %s", target_path)
                return True
            logger.warning("H264 转码失败: %s", ret.stderr.decode()[-300:] if ret.stderr else "")
        except Exception as e:
            logger.warning("H264 转码异常: %s", e)
        return False

    @classmethod
    def _transcode_to_h265(cls, source_path: str, target_path: str) -> bool:
        """将视频转码为 H265。"""
        cls._ensure_ffmpeg()
        if not cls._FFMPEG:
            logger.error("ffmpeg 不可用，无法转码 H265")
            return False
        try:
            ret = subprocess.run(
                [cls._FFMPEG, "-y", "-i", source_path,
                 "-c:v", "libx265", "-preset", "fast", "-crf", "28",
                 "-tag:v", "hvc1",
                 "-pix_fmt", "yuv420p",
                 "-c:a", "aac", "-b:a", "128k",
                 "-movflags", "+faststart",
                 target_path],
                capture_output=True, timeout=600,
            )
            if ret.returncode == 0 and Path(target_path).exists() and Path(target_path).stat().st_size > 0:
                logger.info("H265 转码成功: %s", target_path)
                return True
            logger.warning("H265 转码失败: %s", ret.stderr.decode()[-300:] if ret.stderr else "")
        except Exception as e:
            logger.warning("H265 转码异常: %s", e)
        return False

    @classmethod
    def _transcode_video(cls, source_path: str, target_path: str) -> bool:
        """
        转码视频，优先 H264（浏览器兼容），其次 H265。
        """
        if cls._transcode_to_h264(source_path, target_path):
            return True
        logger.warning("H264 转码失败，尝试 H265")
        return cls._transcode_to_h265(source_path, target_path)

    # ── 主流程 ────────────────────────────────

    def process(
        self,
        source: str | int | object,
        output_path: str | None = None,
        display: bool = False,
        max_frames: int = 0,
        frame_callback: Callable[[np.ndarray, int], None] | None = None,
        stream_dir: str | Path | None = None,
    ) -> dict[str, Any]:
        """
        运行完整的视频处理流水线。

        Args:
            source: 视频输入源（文件路径/相机号/RTSP URL/VirtualCamera 对象）。
            output_path: 输出视频路径（可选）。
            display: 是否实时显示窗口。
            max_frames: 最大处理帧数，0 表示不限制。
            frame_callback: 每帧处理完成后的回调函数。
            stream_dir: MJPEG 帧输出目录（将标注帧写入 latest.jpg 供流读取）。
        """
        input_src = InputSource(source)
        video_writer = None
        last_detections: list[Detection] = []
        frame_id = 0
        total_detections = 0
        start_time = time.time()

        # 帧输出：MJPEG 磁盘写入 + 可选 raw stdout
        frame_writer: ShipPipeline._FrameWriter | None = None
        raw_writer: ShipPipeline._RawStdoutWriter | None = None
        raw_stdout = self._config.get("pipeline", {}).get("raw_stdout", False)

        if raw_stdout:
            raw_writer = ShipPipeline._RawStdoutWriter()
            logger.info("Raw stdout 帧输出已启用（供 H.264 编码）")
        elif stream_dir:
            stream_path = Path(stream_dir)
            stream_path.mkdir(parents=True, exist_ok=True)
            frame_writer = ShipPipeline._FrameWriter(stream_path, quality=70)
            logger.info("MJPEG 帧输出: %s", stream_path / "latest.jpg")

        no_output = self._config.get("pipeline", {}).get("no_output", False)

        try:
            if output_path and not no_output:
                fourcc = cv2.VideoWriter_fourcc(*"mp4v")
                video_writer = cv2.VideoWriter(output_path, fourcc, input_src.source_fps, (input_src.width, input_src.height))
                if not video_writer.isOpened():
                    logger.error("无法创建输出视频: %s", output_path)
                    video_writer = None

            if self._concurrent_mode:
                self._start_workers()
            if self._yolo_async:
                self._start_yolo_thread()

            logger.info("开始处理: source=%s, mode=%s, yolo_async=%s, refresh=%s(gap=%d), detect_every=%d, process_every=%d",
                        source, "concurrent" if self._concurrent_mode else "cascade",
                        "on" if self._yolo_async else "off",
                        "on" if self._enable_refresh else "off", self._gap_num, self._detect_every_n, self._process_every_n)

            # 停止信号文件路径（外部可以通过创建此文件来请求停止）
            stop_file = None
            if stream_dir:
                stop_file = Path(stream_dir) / "__STOP__"
            elif self._stop_file:
                stop_file = self._stop_file

            while True:
                # 检查停止信号文件
                if stop_file and stop_file.exists():
                    logger.info("检测到停止信号文件，优雅退出")
                    break

                ret, frame = input_src.read()
                if not ret:
                    break
                frame_id += 1
                if max_frames > 0 and frame_id > max_frames:
                    break

                self._fps.tick("stream")

                # YOLO 检测（同步 or 异步）
                if self._yolo_async:
                    # 异步模式：提交帧到 YOLO 线程，取最新结果
                    should_detect = (frame_id % self._detect_every_n == 0)
                    if should_detect:
                        try:
                            self._yolo_frame_queue.put_nowait((frame.copy(), frame_id))
                        except queue.Full:
                            pass  # YOLO 线程忙，跳过此帧
                    # 取最新的检测结果
                    yolo_fid, yolo_dets = self._drain_yolo_results()
                    if yolo_fid >= 0:
                        detections = yolo_dets
                        last_detections = detections
                    else:
                        detections = last_detections
                else:
                    # 同步模式（原有逻辑）
                    should_detect = (frame_id % self._detect_every_n == 0)
                    if should_detect:
                        try:
                            with self._latency.measure("yolo"):
                                detections = self._detector.detect(frame, frame_id)
                        except Exception as e:
                            logger.error("YOLO 检测异常 (frame=%d): %s", frame_id, e)
                            detections = []
                        last_detections = detections
                    else:
                        detections = last_detections

                total_detections += len(detections)

                for det in detections:
                    self._tracker.get_or_create(det.track_id, frame_id)

                # 推理
                should_process = (frame_id % self._process_every_n == 0)
                if should_process:
                    if self._concurrent_mode:
                        self._concurrent_process(detections, frame_id)
                    else:
                        self._cascade_process(detections, frame_id)

                if self._concurrent_mode:
                    self._drain_results()

                if frame_id % 30 == 0:
                    self._tracker.cleanup_stale(frame_id)

                # 渲染（非处理帧复用上次渲染结果，省掉昂贵的 PIL 文字绘制）
                if self._demo_enabled or output_path or display or frame_writer:
                    if should_process or self._cached_display_frame is None:
                        with self._latency.measure("demo"):
                            self._cached_display_frame = self._renderer.render(
                                frame, last_detections, self._tracker.active_tracks,
                                self._fps.get_all_fps(), frame_id,
                                self._task_queue.qsize(), self._max_queued_frames,
                            )
                    display_frame = self._cached_display_frame
                else:
                    display_frame = frame

                if self._save_screenshots and should_process:
                    active = self._tracker.active_tracks
                    if any(t.recognized for t in active.values()):
                        self._saver.save(display_frame, frame_id)

                if video_writer:
                    video_writer.write(display_frame)

                # 帧输出：raw stdout（H.264 编码用）或 MJPEG 磁盘写入
                if raw_writer:
                    out_frame = display_frame
                    if self._output_size:
                        ow, oh = self._output_size
                        fh, fw = display_frame.shape[:2]
                        if fw != ow or fh != oh:
                            out_frame = cv2.resize(display_frame, (ow, oh), interpolation=cv2.INTER_LINEAR)
                    raw_writer.write(out_frame)
                elif frame_writer:
                    frame_writer.write(display_frame)

                if frame_callback:
                    frame_callback(display_frame, frame_id)

                if display:
                    cv2.imshow("Ship Pipeline", display_frame)
                    key = cv2.waitKey(1) & 0xFF
                    if key == ord("q"):
                        break

                self._fps.tick("process")

                if self._fps.should_print("stream"):
                    elapsed = time.time() - start_time
                    stream_fps = self._fps.get_fps("stream")
                    process_fps = self._fps.get_fps("process")
                    latency_parts = []
                    for stage in ("yolo", "vlm", "demo"):
                        s = self._latency.get_stats(stage)
                        if s and s["count"] > 0:
                            latency_parts.append(f"{stage}: avg={s['avg']:.1f}ms p95={s['p95']:.1f}ms")
                    latency_str = f" | Latency: {' | '.join(latency_parts)}" if latency_parts else ""
                    logger.info("FPS: stream=%.1f process=%.1f | frames=%d elapsed=%ds tracks=%d%s", stream_fps, process_fps, frame_id, int(elapsed), len(self._tracker), latency_str)

            # ── 处理完成 ──

            if self._concurrent_mode:
                self._drain_results()

            elapsed = time.time() - start_time
            tracks = self._tracker.active_tracks
            total_recognized = sum(1 for t in tracks.values() if t.recognized)

            stats = {
                "total_frames": frame_id,
                "total_detections": total_detections,
                "total_tracks": len(tracks),
                "recognized_tracks": total_recognized,
                "elapsed_seconds": round(elapsed, 1),
                "avg_fps": round(frame_id / elapsed, 1) if elapsed > 0 else 0,
                "mode": "concurrent" if self._concurrent_mode else "cascade",
                "screenshots_saved": self._saver.saved_count,
                "latency": self._latency.get_all_stats(),
            }

            logger.info("=" * 50)
            logger.info("处理完成: 帧=%d 检测=%d 跟踪=%d 识别=%d 耗时=%.1fs FPS=%.1f",
                        stats["total_frames"], stats["total_detections"], stats["total_tracks"], stats["recognized_tracks"], stats["elapsed_seconds"], stats["avg_fps"])
            logger.info("=" * 50)

            # H265/H264 转码（输出视频存在时）
            if output_path and Path(output_path).exists() and Path(output_path).stat().st_size > 0:
                h265_path = Path(output_path).with_suffix(".h265.mp4")
                if self._transcode_video(output_path, str(h265_path)):
                    Path(output_path).unlink()
                    h265_path.rename(output_path)
                    logger.info("已替换为 H265/H264 编码: %s", output_path)
                else:
                    logger.warning("转码失败，保留原始 mp4v 文件: %s", output_path)

            return stats

        except KeyboardInterrupt:
            logger.info("用户中断")
            return {"total_frames": frame_id, "interrupted": True}

        finally:
            if self._yolo_async:
                self._stop_yolo_thread()
            if self._concurrent_mode:
                self._stop_workers()
            if raw_writer:
                raw_writer.stop()
            if frame_writer:
                frame_writer.stop()
            input_src.release()
            if video_writer:
                video_writer.release()
            if display:
                cv2.destroyAllWindows()
            self._detector.cleanup()

    @property
    def agent_trace(self) -> list[dict[str, Any]]:
        with self._trace_lock:
            return list(self._agent_trace)

    def set_demo(self, enabled: bool) -> None:
        self._demo_enabled = enabled

    def set_prompt_mode(self, mode: str) -> None:
        if mode not in ("detailed", "brief"):
            raise ValueError(f"不支持的提示词模式: {mode}")
        self._prompt_mode = mode
        logger.info("提示词模式切换为: %s", mode)

    def switch_to_concurrent(self, enabled: bool) -> None:
        self._concurrent_mode = enabled
        logger.info("切换为 %s 模式", "并发" if enabled else "级联")
