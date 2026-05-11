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
    p.add_argument("--detect-every", type=int, default=1, help="每隔 N 帧检测一次")
    p.add_argument("--camera", action="store_true", help="摄像头模式")
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
    detect_every = args.detect_every or pipeline_cfg.get("detect_every_n_frames", 1)
    detect_classes = pipeline_cfg.get("detect_classes", [8])

    logger.info("加载 YOLO 模型: %s (device=%s)", yolo_model_path, device or "auto")
    model = YOLO(yolo_model_path)

    # ── 打开视频源 ──
    if is_camera:
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

    logger.info("视频源: %s | 分辨率: %dx%d | FPS: %.1f | 总帧数: %s",
                source, width, height, fps, total_frames if total_frames else "未知(实时)")

    # ── 输出视频 ──
    writer = None
    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        fourcc = cv2.VideoWriter_fourcc(*"mp4v")
        writer = cv2.VideoWriter(str(out_path), fourcc, fps, (width, height))
        logger.info("输出视频: %s", out_path)

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
    tracker_name = pipeline_cfg.get("tracker", "bytetrack")

    track_cache: dict[int, tuple[str, int]] = {}
    enable_refresh = args.enable_refresh
    gap_num = args.gap_num

    logger.info("开始处理 (max_frames=%s, process_every=%d, detect_every=%d)",
                max_frames or "不限", process_every, detect_every)

    try:
        while True:
            ret, frame = cap.read()
            if not ret:
                if is_camera:
                    logger.warning("摄像头读取失败，重试...")
                    time.sleep(0.5)
                    continue
                break

            frame_idx += 1
            if max_frames > 0 and frame_idx > max_frames:
                break

            annotated = frame.copy() if (args.demo or stream_dir) else None

            # ── YOLO 检测 ──
            run_detection = (frame_idx % detect_every == 1) or (frame_idx == 1)
            if run_detection:
                results = model.track(
                    frame,
                    persist=True,
                    conf=conf_threshold,
                    classes=detect_classes,
                    tracker=f"{tracker_name}.yaml" if tracker_name != "bytetrack" else None,
                    verbose=False,
                )

                if results and results[0].boxes is not None:
                    boxes = results[0].boxes
                    has_ids = boxes.id is not None

                    for i in range(len(boxes)):
                        x1, y1, x2, y2 = map(int, boxes.xyxy[i].tolist())
                        conf = float(boxes.conf[i])
                        cls_id = int(boxes.cls[i])
                        track_id = int(boxes.id[i]) if has_ids and boxes.id is not None else -1
                        detections_total += 1

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

                        # 画框
                        if annotated is not None:
                            label_parts = [f"id:{track_id}"]
                            if track_id in track_cache:
                                hn, _ = track_cache[track_id]
                                label_parts.append(hn)
                            label_parts.append(f"{conf:.2f}")
                            label = " ".join(label_parts)

                            cv2.rectangle(annotated, (x1, y1), (x2, y2), (0, 255, 0), 2)
                            cv2.putText(annotated, label, (x1, y1 - 8),
                                        cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 0), 2)

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
