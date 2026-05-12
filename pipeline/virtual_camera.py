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
        self._last_mtime: float = 0.0          # 上次读到的文件修改时间
        self._stale_count: int = 0              # 连续未更新帧计数
        self._max_stale: int = int(fps * 3)     # 3 秒无新帧视为断流
        self._frame_count = 0
        self._opened = True
        self._width = 0
        self._height = 0
        self._first_frame_received = False
        self._startup_timeout: float = 15.0     # 等待第一帧的最大超时（秒）

    def isOpened(self) -> bool:
        return self._opened

    def read(self) -> tuple[bool, np.ndarray | None]:
        """读取最新帧，返回 (ret, frame)"""
        if not self._opened:
            return False, None

        # 不再强制 sleep 节流 — pipeline 的处理速度本身就是帧率瓶颈，
        # 额外 sleep 只会无意义地拉低帧率。帧率由浏览器端采集间隔控制。

        now = time.time()

        # 检查帧目录是否还存在（WebSocket 断开后可能被清理）
        if not self._dir.exists():
            self._opened = False
            return False, None

        frame_path = self._dir / "latest.jpg"

        # 启动阶段：等待第一帧到达（浏览器摄像头需要时间建立连接并发送首帧）
        if not self._first_frame_received:
            deadline = time.time() + self._startup_timeout
            while not frame_path.exists() and time.time() < deadline:
                if not self._dir.exists():
                    self._opened = False
                    return False, None
                time.sleep(0.1)
            if not frame_path.exists():
                logger.error("等待首帧超时 (%.0f 秒)，放弃", self._startup_timeout)
                self._opened = False
                return False, None

        if not frame_path.exists():
            # 帧还没到，返回上一帧（如果有）
            if self._last_frame is not None:
                return True, self._last_frame.copy()
            return False, None

        try:
            # 检查文件是否被更新（WebSocket 还在推流）
            mtime = frame_path.stat().st_mtime
            if mtime == self._last_mtime:
                self._stale_count += 1
                if self._stale_count >= self._max_stale:
                    # 超过 3 秒无新帧，认为推流已断开
                    logger.warning("帧文件 %.1f 秒未更新，推流可能已断开", self._stale_count / self._fps)
                    self._opened = False
                    return False, None
                # 还在容忍范围内，返回上一帧
                if self._last_frame is not None:
                    return True, self._last_frame.copy()
                return False, None
            else:
                self._stale_count = 0
                self._last_mtime = mtime

            data = frame_path.read_bytes()
            if not data:
                return (True, self._last_frame.copy()) if self._last_frame is not None else (False, None)

            frame = cv2.imdecode(
                np.frombuffer(data, dtype=np.uint8),
                cv2.IMREAD_COLOR,
            )
            if frame is None:
                return (True, self._last_frame.copy()) if self._last_frame is not None else (False, None)

            if not self._first_frame_received:
                self._first_frame_received = True
                logger.info("首帧已收到: %dx%d", frame.shape[1], frame.shape[0])

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
