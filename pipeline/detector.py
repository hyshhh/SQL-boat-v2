"""
ShipDetector — 基于 YOLO 的船只检测与跟踪

使用 ultralytics YOLO 原生追踪算法（ByteTrack），输出带 track ID 的检测框。
"""

from __future__ import annotations

import logging
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import cv2
import numpy as np
import yaml

logger = logging.getLogger(__name__)


@dataclass
class Detection:
    """单个检测结果。"""
    track_id: int
    bbox: tuple[int, int, int, int]  # (x1, y1, x2, y2)
    confidence: float
    crop: np.ndarray | None = None


def _build_tracker_yaml(tracker_type: str, tracker_params: dict[str, Any] | None) -> str:
    if not tracker_params:
        return f"{tracker_type}.yaml"
    cfg: dict[str, Any] = {"tracker_type": tracker_type}
    cfg.update(tracker_params)
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", prefix=f"{tracker_type}_", delete=False, encoding="utf-8")
    yaml.dump(cfg, tmp, default_flow_style=False, allow_unicode=True)
    tmp.close()
    return tmp.name


class ShipDetector:
    """YOLO 船只检测器（带原生跟踪）。"""

    def __init__(
        self,
        model_path: str = "yolov8n.pt",
        device: str = "",
        conf_threshold: float = 0.25,
        iou_threshold: float = 0.45,
        tracker_type: str = "bytetrack",
        tracker_params: dict[str, Any] | None = None,
        classes: list[int] | None = None,
    ):
        from ultralytics import YOLO

        self._conf_threshold = conf_threshold
        self._iou_threshold = iou_threshold
        self._classes = classes
        self._device = device
        self._tracker_yaml = _build_tracker_yaml(tracker_type, tracker_params)
        self._tracker_type = tracker_type
        self._tracker_tmp_file: str | None = self._tracker_yaml if self._tracker_yaml != f"{tracker_type}.yaml" else None

        logger.info("加载 YOLO 模型: %s (device=%s)", model_path, device or "auto")
        self._model = YOLO(model_path)
        self._patch_ultralytics_cfg()

        # 预热
        try:
            dummy = np.zeros((640, 640, 3), dtype=np.uint8)
            self._model.track(source=dummy, persist=True, tracker=self._tracker_yaml, verbose=False, device=device or None)
        except Exception as e:
            logger.warning("YOLO 预热失败（不影响后续使用）: %s", e)

        logger.info("YOLO 模型加载完成，追踪器: %s", tracker_type)

    def detect(self, frame: np.ndarray, frame_id: int = 0) -> list[Detection]:
        try:
            results = self._model.track(
                source=frame, persist=True, conf=self._conf_threshold,
                iou=self._iou_threshold,
                tracker=self._tracker_yaml, classes=self._classes,
                verbose=False, device=self._device or None,
            )
        except Exception as e:
            logger.error("YOLO 检测异常 (frame=%d): %s", frame_id, e)
            return []

        detections: list[Detection] = []
        if not results or results[0].boxes is None:
            return detections

        boxes = results[0].boxes
        if boxes.id is None:
            return detections

        for i in range(len(boxes)):
            track_id = int(boxes.id[i].item())
            xyxy = boxes.xyxy[i].cpu().numpy().astype(int)
            x1, y1, x2, y2 = int(xyxy[0]), int(xyxy[1]), int(xyxy[2]), int(xyxy[3])
            if x2 <= x1 or y2 <= y1:
                continue
            conf = float(boxes.conf[i].item())

            # 裁剪（加 padding）
            h, w = frame.shape[:2]
            pad = 20
            cx1, cy1 = max(0, x1 - pad), max(0, y1 - pad)
            cx2, cy2 = min(w, x2 + pad), min(h, y2 + pad)
            crop = frame[cy1:cy2, cx1:cx2].copy()

            crop_h, crop_w = crop.shape[:2]
            if crop_w < 80 or crop_h < 80:
                continue

            # 尺寸归一化：统一到 256~512px
            target_min, target_max = 256, 512
            max_dim = max(crop_w, crop_h)
            if max_dim < target_min:
                scale = target_min / max_dim
                crop = cv2.resize(crop, (int(crop_w * scale), int(crop_h * scale)), interpolation=cv2.INTER_LINEAR)
            elif max_dim > target_max:
                scale = target_max / max_dim
                crop = cv2.resize(crop, (int(crop_w * scale), int(crop_h * scale)), interpolation=cv2.INTER_AREA)

            detections.append(Detection(track_id=track_id, bbox=(x1, y1, x2, y2), confidence=conf, crop=crop))

        return detections

    def cleanup(self) -> None:
        if self._tracker_tmp_file:
            try:
                Path(self._tracker_tmp_file).unlink(missing_ok=True)
            except Exception:
                pass
            self._tracker_tmp_file = None

    @staticmethod
    def _patch_ultralytics_cfg() -> None:
        try:
            from ultralytics.cfg import IterableSimpleNamespace
            _orig_init = IterableSimpleNamespace.__init__

            def _patched_init(self, *args, **kwargs):
                _orig_init(self, *args, **kwargs)
                if not hasattr(self, "fuse_score"):
                    self.fuse_score = False

            if not getattr(IterableSimpleNamespace.__init__, "_fuse_score_patched", False):
                IterableSimpleNamespace.__init__ = _patched_init
                IterableSimpleNamespace.__init__._fuse_score_patched = True
        except Exception:
            pass

    def __del__(self) -> None:
        self.cleanup()
