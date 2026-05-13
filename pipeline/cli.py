"""
Pipeline CLI — 视频处理命令行入口

用法:
    python -m pipeline <source> [options]

三步链路：VLM识别 → 精确查找 → 语义检索
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from rich.console import Console
from rich.table import Table

console = Console()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ship-pipeline", description="🚢 船弦号识别视频处理流水线")

    parser.add_argument("source", help="视频输入源：文件路径 / 相机号(0,1,...) / RTSP URL / 帧目录名")
    parser.add_argument("--output", "-o", help="输出视频路径（如 result.mp4）")
    parser.add_argument("--demo", action="store_true", default=None, help="开启 demo 模式（绘制检测框和识别结果）")
    parser.add_argument("--display", action="store_true", help="实时显示窗口（需要有显示器）")
    parser.add_argument("--concurrent", "-c", action="store_true", default=None, help="使用并发模式（YOLO 同步检测 + VLM 线程池并发推理）")
    parser.add_argument("--no-screenshots", action="store_true", default=None, help="关闭自动截图保存")
    parser.add_argument("--max-concurrent", type=int, default=None, help="最大并发推理数（默认 4）")
    parser.add_argument("--max-queued-frames", type=int, default=None, help="最大队列深度（默认 30）")
    parser.add_argument("--process-every", type=int, default=None, help="每 N 帧处理一次（默认沿用 config.yaml）")
    parser.add_argument("--enable-refresh", action="store_true", default=None, help="开启定时刷新")
    parser.add_argument("--no-refresh", action="store_true", default=None, help="关闭定时刷新")
    parser.add_argument("--gap-num", type=int, default=None, help="定时刷新间隔帧数（默认 150）")
    parser.add_argument("--prompt-mode", choices=["detailed", "brief"], default=None, help="提示词模式")
    parser.add_argument("--max-frames", type=int, default=0, help="最大处理帧数（0 = 不限制）")
    parser.add_argument("--yolo-model", default=None, help="YOLO 模型路径")
    parser.add_argument("--device", default=None, help="推理设备（cpu / 0 / 1 ...）")
    parser.add_argument("--conf", type=float, default=None, help="检测置信度阈值")
    parser.add_argument("--iou", type=float, default=None, help="NMS IoU 阈值（兼容参数）")
    parser.add_argument("--detect-every", type=int, default=None, help="每 N 帧做一次 YOLO 检测")
    parser.add_argument("--target-fps", type=float, default=None, help="目标帧率（0=不限制，建议 15-30）")
    parser.add_argument("--camera", action="store_true", help="摄像头模式")
    parser.add_argument("--frames-dir", default=None, help="帧目录模式：从指定目录读取 latest.jpg（浏览器摄像头推流）")
    parser.add_argument("--virtual-fps", type=float, default=15.0, help="帧目录模式的虚拟帧率 (默认 15)")
    parser.add_argument("--stream-dir", default=None, help="将每帧标注结果以 latest.jpg 写入此目录（供 MJPEG 流读取）")
    parser.add_argument("--no-output", action="store_true", help="不保存输出视频文件（仅实时推流）")
    parser.add_argument("--raw-stdout", action="store_true", help="将原始帧写入 stdout（供 H.264 编码，与 --stream-dir 互斥）")
    parser.add_argument("--output-size", default=None, help="输出帧尺寸（WxH，如 640x480），仅用于 raw-stdout 模式缩放")
    parser.add_argument("--stop-file", default=None, help="停止信号文件路径（文件存在时优雅退出）")
    parser.add_argument("--verbose", "-v", action="store_true", help="详细日志输出")

    return parser


def _merge_args_to_config(args, config: dict) -> dict:
    """合并命令行参数到配置字典。"""
    config.setdefault("pipeline", {})
    if args.concurrent is not None:
        config["pipeline"]["concurrent_mode"] = args.concurrent
    if args.max_concurrent is not None:
        config["pipeline"]["max_concurrent"] = args.max_concurrent
    if args.max_queued_frames is not None:
        config["pipeline"]["max_queued_frames"] = args.max_queued_frames
    if args.process_every is not None:
        config["pipeline"]["process_every_n_frames"] = args.process_every
    if args.prompt_mode is not None:
        config["pipeline"]["prompt_mode"] = args.prompt_mode
    if args.demo is not None:
        config["pipeline"]["demo"] = args.demo
    if args.yolo_model is not None:
        config["pipeline"]["yolo_model"] = args.yolo_model
    if args.device is not None:
        config["pipeline"]["device"] = args.device
    if args.conf is not None:
        config["pipeline"]["conf_threshold"] = args.conf
    if args.detect_every is not None:
        config["pipeline"]["detect_every_n_frames"] = args.detect_every
    if args.target_fps is not None:
        config["pipeline"]["target_fps"] = args.target_fps
    if args.iou is not None:
        config["pipeline"]["iou_threshold"] = args.iou
    if args.no_screenshots is not None:
        config["pipeline"]["save_screenshots"] = not args.no_screenshots
    if args.enable_refresh is not None:
        config["pipeline"]["enable_refresh"] = args.enable_refresh
    elif args.no_refresh is not None:
        config["pipeline"]["enable_refresh"] = not args.no_refresh
    if args.gap_num is not None:
        config["pipeline"]["gap_num"] = max(1, args.gap_num)
    if args.no_output:
        config["pipeline"]["no_output"] = True
    if args.raw_stdout:
        config["pipeline"]["raw_stdout"] = True
    if args.output_size:
        try:
            w, h = args.output_size.lower().split("x")
            config["pipeline"]["output_size"] = [int(w), int(h)]
        except ValueError:
            console.print(f"[red]--output-size 格式错误: {args.output_size}，应为 WxH（如 640x480）[/red]")
            sys.exit(1)
    if args.stop_file:
        config["pipeline"]["stop_file"] = args.stop_file
    return config


def _print_config(args, config: dict) -> None:
    """打印启动配置（纯文本，兼容 subprocess stderr 捕获）。"""
    pipe_cfg = config.get("pipeline", {})
    concurrent_mode = pipe_cfg.get("concurrent_mode", False)
    max_concurrent = pipe_cfg.get("max_concurrent", 4)
    enable_refresh = pipe_cfg.get("enable_refresh", False)
    gap_num = pipe_cfg.get("gap_num", 150)
    prompt_mode = pipe_cfg.get("prompt_mode", "detailed")
    demo_enabled = pipe_cfg.get("demo", False)
    yolo_model = pipe_cfg.get("yolo_model", "yolov8n.pt")
    detect_every = pipe_cfg.get("detect_every_n_frames", 2)
    process_every = pipe_cfg.get("process_every_n_frames", 15)

    source_label = args.source
    if args.frames_dir:
        source_label = f"帧目录: {args.frames_dir} (虚拟FPS={args.virtual_fps})"

    lines = [
        "┌─ Pipeline 启动配置 ─────────────────────┐",
        f"│  输入源:    {source_label}",
        f"│  模式:      {'并发' if concurrent_mode else '级联'}",
        f"│  检测间隔:  每 {detect_every} 帧 | 推理间隔: 每 {process_every} 帧",
        f"│  并发数:    {max_concurrent}",
        f"│  刷新:      {'开启 (每%d帧)' % gap_num if enable_refresh else '关闭'}",
        f"│  提示词:    {prompt_mode} | Demo: {'开' if demo_enabled else '关'}",
        f"│  YOLO 模型: {yolo_model}",
        "└─────────────────────────────────────────┘",
    ]
    for line in lines:
        console.print(line)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    # raw-stdout 模式：Rich console 输出到 stderr，避免污染 stdout 的二进制流
    if args.raw_stdout:
        console.file = sys.stderr

    from config import load_config
    config = load_config()
    config = _merge_args_to_config(args, config)
    _print_config(args, config)

    try:
        from pipeline.pipeline import ShipPipeline

        # 帧目录模式（浏览器摄像头）→ 创建 VirtualCamera 作为输入源
        if args.frames_dir:
            from pipeline.virtual_camera import VirtualCamera
            frames_path = Path(args.frames_dir)
            if not frames_path.exists():
                console.print(f"[red]帧目录不存在: {frames_path}[/red]")
                sys.exit(1)
            source = VirtualCamera(frames_path, fps=args.virtual_fps)
            logger = logging.getLogger("pipeline.cli")
            logger.info("使用帧目录模式: %s (虚拟FPS=%.1f)", frames_path, args.virtual_fps)
        else:
            source = args.source

        pipeline = ShipPipeline(config=config)
        stats = pipeline.process(
            source=source,
            output_path=args.output,
            display=args.display and not args.stream_dir,
            max_frames=args.max_frames,
            stream_dir=args.stream_dir,
        )

        # 打印统计表
        table = Table(title="📊 处理统计")
        table.add_column("指标", style="cyan")
        table.add_column("值", style="white")
        for key, value in stats.items():
            table.add_row(key.replace("_", " ").title(), str(value))
        console.print(table)

        # 输出 JSON 摘要供 pipeline_api 解析（raw-stdout 模式下输出到 stderr）
        if args.raw_stdout:
            print(f"\n__PIPELINE_SUMMARY__:{json.dumps(stats, ensure_ascii=False)}", file=sys.stderr, flush=True)
        else:
            print(f"\n__PIPELINE_SUMMARY__:{json.dumps(stats, ensure_ascii=False)}", flush=True)

    except KeyboardInterrupt:
        console.print("\n[yellow]用户中断[/yellow]")
    except Exception as e:
        console.print(f"\n[red]错误: {e}[/red]")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
