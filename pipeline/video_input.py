"""
InputSource — 视频/相机/视频流统一输入接口

支持：视频文件、USB 相机、RTSP/HTTP 流、VirtualCamera（帧目录模式）
"""

from __future__ import annotations

import logging
from pathlib import Path

import cv2
import numpy as np

logger = logging.getLogger(__name__)


class InputSource:
    """统一视频输入源。自动判断：数字→USB相机，rtsp/http→网络流，路径→文件，VirtualCamera→帧目录。"""

    def __init__(self, source: str | int | object, width: int | None = None, height: int | None = None, buffer_size: int = 1):
        self._source = source
        self._cap = None  # cv2.VideoCapture | VirtualCamera | None
        self._width = width
        self._height = height
        self._buffer_size = buffer_size
        self._frame_count = 0
        self._is_file = False
        self._total_frames = 0
        self._fps = 0.0
        self._open()

    def _open(self) -> None:
        source = self._source

        # VirtualCamera 对象（帧目录模式）
        if hasattr(source, 'read') and hasattr(source, 'isOpened') and not isinstance(source, (int, str)):
            self._cap = source
            self._is_file = False
            self._fps = source.get(cv2.CAP_PROP_FPS) if hasattr(source, 'get') else 15.0
            self._total_frames = 0
            logger.info("使用 VirtualCamera 作为输入源")
            return

        if isinstance(source, int):
            cap_source = source
            self._is_file = False
        elif isinstance(source, str) and source.isdigit():
            cap_source = int(source)
            self._is_file = False
        elif isinstance(source, str) and (
            source.startswith("rtsp://") or source.startswith("http://") or source.startswith("https://")
        ):
            cap_source = source
            self._is_file = False
        else:
            p = Path(str(source))
            if not p.exists():
                raise FileNotFoundError(f"视频文件不存在: {p}")
            cap_source = str(p.resolve())
            self._is_file = True

        logger.info("打开视频源: %s (类型: %s)", source, "文件" if self._is_file else "流/相机")
        self._cap = cv2.VideoCapture(cap_source)
        if not self._cap.isOpened():
            raise RuntimeError(f"无法打开视频源: {source}")

        if self._width:
            self._cap.set(cv2.CAP_PROP_FRAME_WIDTH, self._width)
        if self._height:
            self._cap.set(cv2.CAP_PROP_FRAME_HEIGHT, self._height)
        if not self._is_file:
            try:
                self._cap.set(cv2.CAP_PROP_BUFFERSIZE, self._buffer_size)
            except Exception:
                pass

        self._total_frames = int(self._cap.get(cv2.CAP_PROP_FRAME_COUNT))
        self._fps = self._cap.get(cv2.CAP_PROP_FPS) or 30.0
        if self._fps <= 0:
            self._fps = 30.0

        w = int(self._cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        h = int(self._cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        logger.info("视频源已打开: %dx%d, %.1f FPS, %s帧", w, h, self._fps, self._total_frames if self._is_file else "未知")

    def read(self) -> tuple[bool, np.ndarray | None]:
        if self._cap is None or not self._cap.isOpened():
            return False, None
        ret, frame = self._cap.read()
        if ret:
            self._frame_count += 1
            return True, frame
        return False, None

    def release(self) -> None:
        if self._cap is not None:
            self._cap.release()
            self._cap = None
            logger.info("视频源已释放，共处理 %d 帧", self._frame_count)

    @property
    def is_file(self) -> bool:
        return self._is_file

    @property
    def frame_count(self) -> int:
        return self._frame_count

    @property
    def total_frames(self) -> int:
        return self._total_frames

    @staticmethod
    def probe_resolution(source: str | int) -> tuple[int, int] | None:
        """快速探测视频源分辨率（打开 → 读一帧 → 释放），用于提前获取摄像头尺寸。"""
        cap_source = int(source) if isinstance(source, str) and source.isdigit() else source
        cap = cv2.VideoCapture(cap_source)
        if not cap.isOpened():
            return None
        try:
            w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
            h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
            if w <= 0 or h <= 0:
                ret, frame = cap.read()
                if ret and frame is not None:
                    h, w = frame.shape[:2]
            return (w, h) if w > 0 and h > 0 else None
        except Exception:
            return None
        finally:
            cap.release()

    @property
    def source_fps(self) -> float:
        return self._fps

    @property
    def width(self) -> int:
        return int(self._cap.get(cv2.CAP_PROP_FRAME_WIDTH)) if self._cap else 0

    @property
    def height(self) -> int:
        return int(self._cap.get(cv2.CAP_PROP_FRAME_HEIGHT)) if self._cap else 0
