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
        self._max_concurrent: int = pipe_cfg.get("max_concurrent") or 4
        self._max_queued_frames: int = pipe_cfg.get("max_queued_frames") or 30
        self._process_every_n: int = max(1, pipe_cfg.get("process_every_n_frames") or 1)
        self._detect_every_n: int = max(1, pipe_cfg.get("detect_every_n_frames") or 1)
        self._demo_enabled: bool = bool(pipe_cfg.get("demo", False))
        self._save_screenshots: bool = bool(pipe_cfg.get("save_screenshots", True))
        self._enable_refresh: bool = bool(pipe_cfg.get("enable_refresh", False))
        self._gap_num: int = pipe_cfg.get("gap_num") or 150
        self._prompt_mode: str = pipe_cfg.get("prompt_mode") or "detailed"

        from database import ShipDatabase
        self._db = ShipDatabase(config=config)

        self._detector = ShipDetector(
            model_path=pipe_cfg.get("yolo_model", "yolov8n.pt"),
            device=pipe_cfg.get("device", ""),
            conf_threshold=pipe_cfg.get("conf_threshold", 0.25),
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

        # Agent 运行链路日志
        self._agent_trace: list[dict[str, Any]] = []
        self._trace_lock = threading.Lock()
        self._max_trace_entries = 500

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
        success, buf = cv2.imencode(".jpg", image, [cv2.IMWRITE_JPEG_QUALITY, 75])
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

        def __init__(self, stream_dir: Path, quality: int = 70):
            self._path = stream_dir / "latest.jpg"
            self._quality = quality
            self._queue: list = []  # 最多保留 1 帧
            self._lock = threading.Lock()
            self._stop = threading.Event()
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()

        def write(self, frame: np.ndarray) -> None:
            with self._lock:
                self._queue = [frame]

        def stop(self) -> None:
            self._stop.set()
            self._thread.join(timeout=2)

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
                    except Exception:
                        pass
                else:
                    self._stop.wait(0.005)

    # ── H265/H264 转码 ────────────────────────

    @staticmethod
    def _transcode_video(source_path: str, target_path: str) -> bool:
        """
        尝试 H265 转码，失败则回退 H264。
        优先 H265 推流，兼容问题时用 H264。
        """
        # 尝试 H265
        try:
            ret = subprocess.run(
                ["ffmpeg", "-y", "-i", source_path,
                 "-c:v", "libx265", "-preset", "fast", "-crf", "28",
                 "-tag:v", "hvc1",
                 "-c:a", "copy", target_path],
                capture_output=True, timeout=300,
            )
            if ret.returncode == 0 and Path(target_path).exists() and Path(target_path).stat().st_size > 0:
                logger.info("H265 转码成功: %s", target_path)
                return True
            logger.warning("H265 转码失败，尝试 H264: %s", ret.stderr.decode()[-200:] if ret.stderr else "")
        except Exception as e:
            logger.warning("H265 转码异常: %s，尝试 H264", e)

        # 回退 H264
        try:
            ret = subprocess.run(
                ["ffmpeg", "-y", "-i", source_path,
                 "-c:v", "libx264", "-preset", "fast", "-crf", "23",
                 "-c:a", "copy", target_path],
                capture_output=True, timeout=300,
            )
            if ret.returncode == 0 and Path(target_path).exists() and Path(target_path).stat().st_size > 0:
                logger.info("H264 转码成功: %s", target_path)
                return True
            logger.warning("H264 转码也失败: %s", ret.stderr.decode()[-200:] if ret.stderr else "")
        except Exception as e:
            logger.warning("H264 转码异常: %s", e)

        return False

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

        # MJPEG 帧写入器
        frame_writer: ShipPipeline._FrameWriter | None = None
        if stream_dir:
            stream_path = Path(stream_dir)
            stream_path.mkdir(parents=True, exist_ok=True)
            frame_writer = ShipPipeline._FrameWriter(stream_path, quality=70)
            logger.info("MJPEG 帧输出: %s", stream_path / "latest.jpg")

        try:
            if output_path:
                fourcc = cv2.VideoWriter_fourcc(*"mp4v")
                video_writer = cv2.VideoWriter(output_path, fourcc, input_src.source_fps, (input_src.width, input_src.height))
                if not video_writer.isOpened():
                    logger.error("无法创建输出视频: %s", output_path)
                    video_writer = None

            if self._concurrent_mode:
                self._start_workers()

            logger.info("开始处理: source=%s, mode=%s, refresh=%s(gap=%d), detect_every=%d, process_every=%d",
                        source, "concurrent" if self._concurrent_mode else "cascade",
                        "on" if self._enable_refresh else "off", self._gap_num, self._detect_every_n, self._process_every_n)

            while True:
                ret, frame = input_src.read()
                if not ret:
                    break
                frame_id += 1
                if max_frames > 0 and frame_id > max_frames:
                    break

                self._fps.tick("stream")

                # YOLO 检测
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

                # 渲染
                if self._demo_enabled or output_path or display or frame_writer:
                    with self._latency.measure("demo"):
                        display_frame = self._renderer.render(frame, last_detections, self._tracker.active_tracks, self._fps.get_all_fps(), frame_id, self._task_queue.qsize(), self._max_queued_frames)
                else:
                    display_frame = frame

                if self._save_screenshots and should_process:
                    active = self._tracker.active_tracks
                    if any(t.recognized for t in active.values()):
                        self._saver.save(display_frame, frame_id)

                if video_writer:
                    video_writer.write(display_frame)

                # MJPEG 帧输出（后台线程编码，不阻塞主循环）
                if frame_writer:
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
            if self._concurrent_mode:
                self._stop_workers()
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
