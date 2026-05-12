"""
Pipeline CLI 入口 — 视频/摄像头船只识别

用法:
    python -m pipeline <video_path> [选项]
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import sys
import time
from pathlib import Path

logger = logging.getLogger("pipeline")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="船只舷号识别 Pipeline")
    p.add_argument("source", help="视频文件路径 / 摄像头编号(0) / RTSP 地址")
    p.add_argument("--output", "-o", default=None, help="输出视频路径")
    p.add_argument("--demo", action="store_true", help="画检测框")
    p.add_argument("--display", action="store_true", help="弹窗实时显示（仅本地 GUI）")
    p.add_argument("--agent", action="store_true", help="Agent 模式")
    p.add_argument("-c", "--concurrent", action="store_true", help="并发模式")
    p.add_argument("--max-concurrent", type=int, default=4, help="最大并发数")
    p.add_argument("--enable-refresh", action="store_true", help="定时刷新识别")
    p.add_argument("--gap-num", type=int, default=150, help="刷新间隔帧数")
    p.add_argument("--max-frames", type=int, default=0, help="最大处理帧数(0=不限)")
    p.add_argument("--process-every", type=int, default=15, help="每隔 N 帧处理一次")
    p.add_argument("--prompt-mode", default="detailed", choices=["detailed", "brief"])
    p.add_argument("--yolo-model", default="yolov8n.pt")
    p.add_argument("--device", default="", help="cpu / 0 / 1 ...")
    p.add_argument("--conf", type=float, default=0.25, help="检测置信度")
    p.add_argument("--iou", type=float, default=0.45, help="NMS IoU 阈值")
    p.add_argument("--detect-every", type=int, default=1, help="每隔 N 帧检测一次")
    p.add_argument("--camera", action="store_true", help="摄像头模式")
    p.add_argument("--frames-dir", default=None,
                   help="帧目录模式：从指定目录读取 latest.jpg（浏览器摄像头推流）")
    p.add_argument("--virtual-fps", type=float, default=15.0,
                   help="帧目录模式的虚拟帧率 (默认 15)")
    p.add_argument("--stream-dir", default=None,
                   help="将每帧标注结果以 latest.jpg 写入此目录（供 MJPEG 流读取）")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


def _is_camera_source(source: str) -> bool:
    if source.isdigit():
        return True
    return source.startswith(("rtsp://", "rtmp://", "http://", "https://"))


def _atomic_write_jpg(path: Path, data: bytes) -> None:
    """原子写入 JPEG：先写临时文件，再 rename（保证读端不会读到半截）"""
    tmp = path.with_suffix(".tmp")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    tmp.rename(path)


def run_pipeline(args: argparse.Namespace) -> int:
    """执行视频处理流水线，返回退出码"""
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    source = args.source
    is_camera = args.camera or _is_camera_source(source)

    # ── 懒加载依赖 ──
    try:
        import cv2
        import numpy as np
    except ImportError:
        logger.error("缺少 opencv-python 或 numpy，请安装: pip install opencv-python numpy")
        return 1

    try:
        from ultralytics import YOLO
    except ImportError:
        logger.error("缺少 ultralytics，请安装: pip install ultralytics")
        return 1

    from config import load_config

    config = load_config()
    pipeline_cfg = config.get("pipeline", {})

    # ── 加载 YOLO 模型 ──
    yolo_model_path = args.yolo_model or pipeline_cfg.get("yolo_model", "yolov8n.pt")
    device = args.device or pipeline_cfg.get("device", "")
    conf_threshold = args.conf or pipeline_cfg.get("conf_threshold", 0.25)
    iou_threshold = args.iou if args.iou is not None else pipeline_cfg.get("iou_threshold", 0.45)
    detect_every = args.detect_every or pipeline_cfg.get("detect_every_n_frames", 1)
    detect_classes = pipeline_cfg.get("detect_classes", [8])

    logger.info("加载 YOLO 模型: %s (device=%s)", yolo_model_path, device or "auto")
    model = YOLO(yolo_model_path)

    # ── 打开视频源 ──
    if args.frames_dir:
        # 帧目录模式（浏览器摄像头）
        from pipeline.virtual_camera import VirtualCamera
        frames_path = Path(args.frames_dir)
        if not frames_path.exists():
            logger.error("帧目录不存在: %s", frames_path)
            return 1
        cap = VirtualCamera(frames_path, fps=args.virtual_fps)
        is_camera = True
        logger.info("使用帧目录模式: %s (虚拟FPS=%.1f)", frames_path, args.virtual_fps)
    elif is_camera:
        cam_id = int(source) if source.isdigit() else source
        cap = cv2.VideoCapture(cam_id)
    else:
        video_path = Path(source)
        if not video_path.exists():
            logger.error("视频文件不存在: %s", video_path)
            return 1
        cap = cv2.VideoCapture(str(video_path))

    if not cap.isOpened():
        logger.error("无法打开视频源: %s", source)
        return 1

    # ── 视频属性 ──
    fps = cap.get(cv2.CAP_PROP_FPS) or 25.0
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT)) if not is_camera else 0

    if width > 0 and height > 0:
        logger.info("视频源: %s | 分辨率: %dx%d | FPS: %.1f | 总帧数: %s",
                    source, width, height, fps, total_frames if total_frames else "未知(实时)")
    else:
        logger.info("视频源: %s | 分辨率: 等待首帧 | FPS: %.1f | 总帧数: 未知(实时)", source, fps)

    # ── 输出视频（延迟创建，等第一帧确定尺寸）──
    writer = None
    writer_path = None
    writer_fps = fps
    if args.output:
        writer_path = Path(args.output)
        writer_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info("输出视频: %s (延迟创建，等第一帧)", writer_path)

    # ── MJPEG 帧输出目录 ──
    stream_dir: Path | None = None
    stream_latest: Path | None = None
    if args.stream_dir:
        stream_dir = Path(args.stream_dir)
        stream_dir.mkdir(parents=True, exist_ok=True)
        stream_latest = stream_dir / "latest.jpg"
        logger.info("MJPEG 帧输出: %s", stream_latest)

    # ── 初始化 VLM ──
    vlm = None
    try:
        from langchain_openai import ChatOpenAI
        from langchain_core.messages import HumanMessage

        llm_cfg = config.get("llm", {})
        vlm = ChatOpenAI(
            model=llm_cfg.get("model", "Qwen/Qwen3-VL-4B-AWQ"),
            api_key=llm_cfg.get("api_key", ""),
            base_url=llm_cfg.get("base_url", "http://localhost:7890/v1"),
            temperature=0.0,
            max_tokens=512,
        )
        logger.info("VLM 已初始化: %s", llm_cfg.get("model"))
    except ImportError:
        logger.warning("langchain-openai 未安装，跳过 VLM 识别")
    except Exception as e:
        logger.warning("VLM 初始化失败: %s（将仅做检测框标注）", e)

    def recognize_frame(frame_crop) -> str:
        """对裁剪区域调用 VLM 识别舷号"""
        if vlm is None:
            return ""
        try:
            _, buf = cv2.imencode(".jpg", frame_crop, [cv2.IMWRITE_JPEG_QUALITY, 80])
            b64 = base64.b64encode(buf).decode()
            prompt = "识别图中船只的舷号编号，只返回编号文字（如 0014、海巡123），没有则返回空字符串。不要多余文字。"
            msg = HumanMessage(content=[
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}},
            ])
            resp = vlm.invoke([msg])
            return resp.content.strip().strip('"').strip("'")
        except Exception as e:
            logger.debug("VLM 识别异常: %s", e)
            return ""

    # ── 显示窗口（仅 --display 且无 stream-dir）──
    display_enabled = args.display and not stream_dir
    if display_enabled:
        try:
            cv2.namedWindow("Pipeline", cv2.WINDOW_NORMAL)
        except Exception:
            logger.warning("无法创建显示窗口（无 GUI 环境？）")
            display_enabled = False

    # ── 主循环 ──
    frame_idx = 0
    processed = 0
    detections_total = 0
    start_time = time.time()
    max_frames = args.max_frames or 0
    process_every = args.process_every or 15
    tracker_name = pipeline_cfg.get("tracker") or "bytetrack"
    tracker_params = pipeline_cfg.get("tracker_params", {})

    # 如果有自定义 tracker_params，写入临时 yaml 覆盖默认值
    _custom_tracker_yaml: str | None = None
    if tracker_name == "bytetrack" and tracker_params:
        import tempfile, yaml as _yaml
        # 读取内置 bytetrack.yaml 作为基础
        try:
            from pathlib import Path as _P
            import ultralytics
            base_yaml = _P(ultralytics.__file__).parent / "cfg" / "trackers" / "bytetrack.yaml"
            if base_yaml.exists():
                with open(base_yaml) as f:
                    cfg = _yaml.safe_load(f) or {}
            else:
                cfg = {}
        except Exception:
            cfg = {}
        cfg.update(tracker_params)
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", prefix="bytetrack_custom_", delete=False)
        _yaml.dump(cfg, tmp)
        tmp.close()
        _custom_tracker_yaml = tmp.name
        logger.info("使用自定义 tracker 配置: %s", _custom_tracker_yaml)

    track_cache: dict[int, tuple[str, int]] = {}
    enable_refresh = args.enable_refresh
    gap_num = args.gap_num

    # ── 活跃跟踪缓存：每帧都画框，不只是检测帧 ──
    # track_id → (x1, y1, x2, y2, conf, last_seen_frame)
    active_tracks: dict[int, tuple[int, int, int, int, float, int]] = {}
    TRACK_STALE_FRAMES = pipeline_cfg.get("max_stale_frames", 300)

    # 不同 track_id 使用不同颜色
    _track_colors = [
        (0, 255, 0),    # 绿
        (0, 200, 255),  # 橙黄
        (255, 100, 0),  # 蓝
        (0, 0, 255),    # 红
        (255, 0, 255),  # 紫
        (0, 255, 255),  # 黄
        (255, 255, 0),  # 青
        (128, 0, 255),  # 粉
    ]

    def _get_track_color(tid: int) -> tuple[int, int, int]:
        return _track_colors[tid % len(_track_colors)]

    logger.info("开始处理 (max_frames=%s, process_every=%d, detect_every=%d)",
                max_frames or "不限", process_every, detect_every)

    try:
        while True:
            ret, frame = cap.read()
            if not ret:
                if is_camera:
                    if not cap.isOpened():
                        logger.error("摄像头已断开，停止处理")
                        break
                    logger.warning("摄像头读取失败，重试...")
                    time.sleep(0.5)
                    continue
                break

            frame_idx += 1
            if max_frames > 0 and frame_idx > max_frames:
                break

            # ── 延迟创建 VideoWriter（第一帧到达后才能确定尺寸）──
            if writer_path and writer is None:
                h, w = frame.shape[:2]
                fourcc = cv2.VideoWriter_fourcc(*"mp4v")
                writer = cv2.VideoWriter(str(writer_path), fourcc, writer_fps, (w, h))
                logger.info("VideoWriter 已创建: %dx%d", w, h)

            annotated = frame.copy() if (args.demo or stream_dir) else None

            # ── YOLO 检测（仅在检测帧运行）──
            run_detection = (frame_idx % detect_every == 1) or (frame_idx == 1)
            if run_detection:
                track_kwargs: dict = dict(
                    persist=True,
                    conf=conf_threshold,
                    iou=iou_threshold,
                    classes=detect_classes,
                    verbose=False,
                )
                # bytetrack 是 ultralytics 内置默认，无需指定 yaml；其他 tracker 需显式传
                if _custom_tracker_yaml:
                    track_kwargs["tracker"] = _custom_tracker_yaml
                elif tracker_name != "bytetrack":
                    track_kwargs["tracker"] = f"{tracker_name}.yaml"
                results = model.track(frame, **track_kwargs)

                if results and results[0].boxes is not None:
                    boxes = results[0].boxes
                    has_ids = boxes.id is not None

                    # 记录本轮检测到的 track_id，用于清理过期 track
                    seen_ids = set()

                    for i in range(len(boxes)):
                        x1, y1, x2, y2 = map(int, boxes.xyxy[i].tolist())
                        conf = float(boxes.conf[i])
                        cls_id = int(boxes.cls[i])
                        track_id = int(boxes.id[i]) if has_ids and boxes.id is not None else -1
                        detections_total += 1
                        seen_ids.add(track_id)

                        # 更新活跃跟踪缓存
                        active_tracks[track_id] = (x1, y1, x2, y2, conf, frame_idx)

                        # VLM 识别
                        if frame_idx % process_every == 0 or frame_idx == 1:
                            crop = frame[max(0, y1):min(height, y2), max(0, x1):min(width, x2)]
                            if crop.size > 0:
                                needs_recognition = track_id not in track_cache
                                if not needs_recognition and enable_refresh:
                                    _, last_frame = track_cache[track_id]
                                    if frame_idx - last_frame >= gap_num:
                                        needs_recognition = True

                                if needs_recognition:
                                    hull_number = recognize_frame(crop)
                                    if hull_number:
                                        track_cache[track_id] = (hull_number, frame_idx)
                                        logger.info("帧 %d | Track %d → 弦号: %s", frame_idx, track_id, hull_number)

                    # 清理过期 track（连续多帧未检测到的）
                    stale_ids = [
                        tid for tid, (_, _, _, _, _, last) in active_tracks.items()
                        if tid not in seen_ids and frame_idx - last > TRACK_STALE_FRAMES
                    ]
                    for tid in stale_ids:
                        del active_tracks[tid]

            # ── 画框（每帧都画，不只是检测帧）──
            if annotated is not None and active_tracks:
                for track_id, (x1, y1, x2, y2, conf, _) in active_tracks.items():
                    color = _get_track_color(track_id)

                    # 标签：弦号（如有）+ 置信度
                    label_parts = []
                    if track_id in track_cache:
                        hn, _ = track_cache[track_id]
                        label_parts.append(hn)
                    label_parts.append(f"{conf:.2f}")
                    label = " ".join(label_parts)

                    # 画框 + 标签背景
                    cv2.rectangle(annotated, (x1, y1), (x2, y2), color, 2)
                    (tw, th), _ = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, 0.6, 2)
                    cv2.rectangle(annotated, (x1, y1 - th - 10), (x1 + tw + 4, y1), color, -1)
                    cv2.putText(annotated, label, (x1 + 2, y1 - 6),
                                cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 2)

            # ── 写入输出视频 ──
            if writer and annotated is not None:
                writer.write(annotated)
            elif writer:
                writer.write(frame)

            # ── 写入 MJPEG 帧（原子写入，避免读到半截 JPEG）──
            if stream_latest and annotated is not None:
                try:
                    _, buf = cv2.imencode(".jpg", annotated, [cv2.IMWRITE_JPEG_QUALITY, 70])
                    _atomic_write_jpg(stream_latest, buf.tobytes())
                except Exception:
                    pass

            # ── 弹窗显示 ──
            if display_enabled and annotated is not None:
                try:
                    cv2.imshow("Pipeline", annotated)
                    key = cv2.waitKey(1) & 0xFF
                    if key == ord("q"):
                        logger.info("用户退出")
                        break
                except Exception:
                    pass

            # ── 进度输出 ──
            if frame_idx % 100 == 0:
                elapsed = time.time() - start_time
                fps_actual = frame_idx / elapsed if elapsed > 0 else 0
                progress = ""
                if total_frames > 0:
                    pct = frame_idx / total_frames * 100
                    progress = f" | 进度: {pct:.1f}%"
                print(f"处理帧: {frame_idx} | FPS: {fps_actual:.1f} | 检测: {detections_total}{progress}", flush=True)

            processed += 1

    except KeyboardInterrupt:
        logger.info("用户中断")
    finally:
        cap.release()
        if writer:
            writer.release()
        if display_enabled:
            cv2.destroyAllWindows()
        # 清理临时 tracker yaml
        if _custom_tracker_yaml:
            try:
                os.unlink(_custom_tracker_yaml)
            except OSError:
                pass

    # ── 汇总 ──
    elapsed = time.time() - start_time
    logger.info("=" * 50)
    logger.info("处理完成 | 总帧数: %d | 耗时: %.1fs | 平均 FPS: %.1f",
                frame_idx, elapsed, frame_idx / elapsed if elapsed > 0 else 0)
    logger.info("检测总数: %d | 识别弦号数: %d", detections_total, len(track_cache))
    if track_cache:
        logger.info("识别结果:")
        for tid, (hn, _) in sorted(track_cache.items()):
            logger.info("  Track %d → %s", tid, hn)

    # ── 转码为 H.264（浏览器兼容）──
    if writer_path and writer_path.exists() and writer_path.stat().st_size > 0:
        try:
            import subprocess
            h264_path = writer_path.with_suffix(".h264.mp4")
            ret = subprocess.run(
                ["ffmpeg", "-y", "-i", str(writer_path),
                 "-c:v", "libx264", "-preset", "fast", "-crf", "23",
                 "-c:a", "copy", str(h264_path)],
                capture_output=True, timeout=300,
            )
            if ret.returncode == 0 and h264_path.exists():
                # 替换原文件
                writer_path.unlink()
                h264_path.rename(writer_path)
                logger.info("已转码为 H.264: %s", writer_path)
            else:
                logger.warning("H.264 转码失败，保留原始文件: %s", ret.stderr.decode()[-200:])
        except Exception as e:
            logger.warning("H.264 转码异常: %s（保留原始文件）", e)

    summary = {
        "total_frames": frame_idx,
        "elapsed_seconds": round(elapsed, 1),
        "fps": round(frame_idx / elapsed, 1) if elapsed > 0 else 0,
        "detections": detections_total,
        "recognized_ships": {str(tid): hn for tid, (hn, _) in track_cache.items()},
    }
    print(f"\n__PIPELINE_SUMMARY__:{json.dumps(summary, ensure_ascii=False)}", flush=True)

    return 0


def main():
    args = parse_args()
    sys.exit(run_pipeline(args))


if __name__ == "__main__":
    main()
