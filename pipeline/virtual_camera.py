"""
VirtualCamera — 从帧目录读取最新 JPEG 帧

配合浏览器摄像头 WebSocket 推流使用：
- 前端 getUserMedia 捕获帧 → WebSocket 发送到服务器
- 服务器写入帧目录（原子写入）
- 本类从帧目录读取最新帧，模拟 cv2.VideoCapture 接口
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path

import cv2
import numpy as np

logger = logging.getLogger(__name__)


class VirtualCamera:
    """从帧目录读取最新 JPEG 帧，模拟 cv2.VideoCapture 接口"""

    def __init__(self, frames_dir: str | Path, fps: float = 15.0):
        self._dir = Path(frames_dir)
        self._fps = fps
        self._frame_interval = 1.0 / fps
        self._last_frame: np.ndarray | None = None
        self._last_read_time: float = 0.0
        self._frame_count = 0
        self._opened = True
        self._width = 0
        self._height = 0

    def isOpened(self) -> bool:
        return self._opened

    def read(self) -> tuple[bool, np.ndarray | None]:
        """读取最新帧，返回 (ret, frame)"""
        if not self._opened:
            return False, None

        # 节流：控制读取帧率
        now = time.time()
        elapsed = now - self._last_read_time
        if elapsed < self._frame_interval:
            time.sleep(self._frame_interval - elapsed)

        # 检查帧目录是否还存在（WebSocket 断开后可能被清理）
        if not self._dir.exists():
            self._opened = False
            return False, None

        frame_path = self._dir / "latest.jpg"
        if not frame_path.exists():
            # 帧还没到，返回上一帧（如果有）
            if self._last_frame is not None:
                return True, self._last_frame.copy()
            return False, None

        try:
            data = frame_path.read_bytes()
            if not data:
                return (True, self._last_frame.copy()) if self._last_frame is not None else (False, None)

            frame = cv2.imdecode(
                np.frombuffer(data, dtype=np.uint8),
                cv2.IMREAD_COLOR,
            )
            if frame is None:
                return (True, self._last_frame.copy()) if self._last_frame is not None else (False, None)

            self._last_frame = frame
            self._frame_count += 1
            self._last_read_time = time.time()

            if self._width == 0:
                self._height, self._width = frame.shape[:2]

            return True, frame

        except (OSError, ValueError):
            return (True, self._last_frame.copy()) if self._last_frame is not None else (False, None)

    def get(self, prop_id: int) -> float:
        """模拟 cv2.VideoCapture.get()"""
        if prop_id == cv2.CAP_PROP_FPS:
            return self._fps
        if prop_id == cv2.CAP_PROP_FRAME_WIDTH:
            return float(self._width)
        if prop_id == cv2.CAP_PROP_FRAME_HEIGHT:
            return float(self._height)
        if prop_id == cv2.CAP_PROP_FRAME_COUNT:
            return 0.0  # 实时流，总帧数未知
        if prop_id == cv2.CAP_PROP_POS_FRAMES:
            return float(self._frame_count)
        return 0.0

    def set(self, prop_id: int, value: float) -> bool:
        """模拟 cv2.VideoCapture.set()"""
        if prop_id == cv2.CAP_PROP_FPS:
            self._fps = value
            self._frame_interval = 1.0 / max(value, 0.1)
            return True
        return False

    def release(self) -> None:
        self._opened = False
        self._last_frame = None
        logger.info("VirtualCamera 已释放（共读取 %d 帧）", self._frame_count)
