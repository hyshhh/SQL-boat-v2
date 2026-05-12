"""
Output — 截图保存模块
"""

from __future__ import annotations

import logging
from pathlib import Path

import cv2
import numpy as np

logger = logging.getLogger(__name__)


class ScreenshotSaver:
    """截图保存器。在指定帧触发时保存渲染后的帧。"""

    def __init__(
        self,
        output_dir: str | Path = "./output",
        image_format: str = "jpg",
        jpeg_quality: int = 90,
    ):
        self._output_dir = Path(output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._format = image_format.lower()
        self._jpeg_quality = jpeg_quality
        self._saved_count = 0

        if self._format == "jpg":
            self._ext = ".jpg"
            self._encode_params = [cv2.IMWRITE_JPEG_QUALITY, jpeg_quality]
        elif self._format == "png":
            self._ext = ".png"
            self._encode_params = [cv2.IMWRITE_PNG_COMPRESSION, 3]
        else:
            raise ValueError(f"不支持的图片格式: {image_format}，仅支持 jpg/png")

    def save(self, frame: np.ndarray, frame_id: int) -> str | None:
        filename = f"frame_{frame_id:06d}{self._ext}"
        filepath = self._output_dir / filename
        success = cv2.imwrite(str(filepath), frame, self._encode_params)
        if success:
            self._saved_count += 1
        else:
            logger.error("截图保存失败: %s", filepath)
            return None
        return str(filepath)

    @property
    def saved_count(self) -> int:
        return self._saved_count
