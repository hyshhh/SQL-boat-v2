"""
Demo 模块 — 视频演示可视化

支持：检测框 + 跟踪 ID + 识别结果叠加 + FPS HUD
纯 OpenCV 渲染，无 PIL 依赖。
"""

from __future__ import annotations

import logging
from typing import Any

import cv2
import numpy as np

logger = logging.getLogger(__name__)


class DemoRenderer:
    """Demo 渲染器 — 在视频帧上叠加可视化信息。"""

    def __init__(self, show_fps: bool = True, show_track_id: bool = True, font_scale: float = 0.5):
        self._show_fps = show_fps
        self._show_track_id = show_track_id
        self._font_scale = font_scale

    def render(
        self,
        frame: np.ndarray,
        detections: list[Any],
        tracks: dict[int, Any],
        fps_info: dict[str, float] | None = None,
        frame_id: int = 0,
        queue_depth: int = 0,
        max_queue: int = 0,
    ) -> np.ndarray:
        canvas = frame.copy()
        for det in detections:
            self._render_detection(canvas, det, tracks.get(det.track_id))
        if self._show_fps and fps_info:
            self._render_hud(canvas, fps_info, frame_id, queue_depth, max_queue)
        return canvas

    def _render_detection(self, canvas: np.ndarray, det: Any, track_info: Any) -> None:
        x1, y1, x2, y2 = det.bbox

        # 颜色映射
        if track_info and track_info.db_matched:
            color = (0, 200, 0)       # 绿色：精确匹配
        elif track_info and track_info.recognized and track_info.hull_number and track_info.semantic_match_ids:
            color = (0, 215, 255)     # 黄色：有语义候选
        elif track_info and track_info.recognized and track_info.hull_number:
            color = (0, 0, 255)       # 红色：识别到但未匹配
        elif track_info and track_info.recognized and not track_info.hull_number and track_info.semantic_match_ids:
            color = (0, 0, 255)
        elif track_info and track_info.recognized and not track_info.hull_number:
            color = (0, 0, 255)
        elif track_info and track_info.pending:
            color = (255, 255, 0)     # 青色：识别中
        else:
            color = (180, 180, 180)   # 灰色：等待

        cv2.rectangle(canvas, (x1, y1), (x2, y2), color, 2)

        if self._show_track_id:
            label = f"ID:{det.track_id}"
            cv2.putText(canvas, label, (x1, y1 - 8), cv2.FONT_HERSHEY_SIMPLEX, self._font_scale, color, 1)

        if track_info:
            text = self._get_display_text(track_info)
            if text:
                self._render_label(canvas, text, x1, y2, color)

    @staticmethod
    def _get_display_text(track_info: Any) -> str:
        if not getattr(track_info, "recognized", False):
            return "(detecting...)" if getattr(track_info, "pending", False) else ""
        if getattr(track_info, "db_matched", False):
            return f"(DB match: {getattr(track_info, 'db_match_id', '')})"
        hull_number = getattr(track_info, "hull_number", "") or ""
        semantic_ids = getattr(track_info, "semantic_match_ids", []) or []
        desc = getattr(track_info, "description", "")[:15]
        if hull_number and semantic_ids:
            return f"(unknown: {hull_number} maybe: {'/'.join(semantic_ids[:3])})"
        if hull_number:
            return f"(unknown: {hull_number} - {desc})" if desc else f"(unknown: {hull_number})"
        if semantic_ids:
            return f"(unknown: none, maybe: {'/'.join(semantic_ids[:3])})"
        return "(unknown)"

    def _render_label(self, canvas: np.ndarray, text: str, x: int, y: int, color: tuple[int, int, int]) -> None:
        tw = len(text) * 10
        cv2.rectangle(canvas, (x, y + 2), (x + tw + 6, y + 22), color, -1)
        cv2.putText(canvas, text, (x + 3, y + 16), cv2.FONT_HERSHEY_SIMPLEX, 0.45, (255, 255, 255), 1)

    def _render_hud(self, canvas: np.ndarray, fps_info: dict[str, float], frame_id: int, queue_depth: int, max_queue: int) -> None:
        font = cv2.FONT_HERSHEY_SIMPLEX
        scale, color, thickness = 0.45, (0, 255, 0), 1
        y = 18
        lines = [f"Frame: {frame_id}"]
        for ch, fps in fps_info.items():
            lines.append(f"{ch}: {fps:.1f} FPS")
        if max_queue > 0:
            lines.append(f"Queue: {queue_depth}/{max_queue}")
        for line in lines:
            cv2.putText(canvas, line, (10, y), font, scale, color, thickness)
            y += 18
