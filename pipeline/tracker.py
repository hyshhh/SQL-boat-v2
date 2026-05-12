"""
TrackManager — 跟踪状态管理

维护 YOLO track ID 与弦号识别结果的映射关系。
一旦某个 track ID 完成识别，后续帧沿用该结果，无需重复调用 VLM。
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class TrackInfo:
    """单个 track 的状态信息。"""
    track_id: int
    hull_number: str = ""
    description: str = ""
    recognized: bool = False
    pending: bool = False
    first_seen_frame: int = 0
    last_seen_frame: int = 0
    last_recognized_frame: int = 0
    db_match_id: str = ""
    db_match_desc: str = ""
    db_matched: bool = False
    semantic_match_ids: list[str] = field(default_factory=list)


class TrackManager:
    """管理所有活跃的跟踪目标。线程安全。"""

    def __init__(self, max_stale_frames: int = 300):
        self._tracks: dict[int, TrackInfo] = {}
        self._max_stale_frames = max_stale_frames
        self._lock = threading.Lock()

    def get_or_create(self, track_id: int, frame_id: int) -> TrackInfo:
        with self._lock:
            if track_id not in self._tracks:
                self._tracks[track_id] = TrackInfo(track_id=track_id, first_seen_frame=frame_id, last_seen_frame=frame_id)
            else:
                self._tracks[track_id].last_seen_frame = frame_id
            return self._tracks[track_id]

    def needs_recognition(self, track_id: int) -> bool:
        with self._lock:
            if track_id not in self._tracks:
                return True
            info = self._tracks[track_id]
            return not info.recognized and not info.pending

    def needs_refresh(self, track_id: int, frame_id: int, gap_num: int) -> bool:
        with self._lock:
            if track_id not in self._tracks:
                return False
            info = self._tracks[track_id]
            if not info.recognized or info.pending:
                return False
            if info.last_recognized_frame == 0:
                return frame_id - info.first_seen_frame >= gap_num
            return frame_id - info.last_recognized_frame >= gap_num

    def mark_pending(self, track_id: int) -> None:
        with self._lock:
            if track_id in self._tracks:
                self._tracks[track_id].pending = True

    def cancel_pending(self, track_id: int) -> None:
        with self._lock:
            if track_id in self._tracks:
                self._tracks[track_id].pending = False

    def bind_result(self, track_id: int, hull_number: str, description: str, frame_id: int = 0) -> None:
        with self._lock:
            if track_id not in self._tracks:
                return
            info = self._tracks[track_id]
            info.hull_number = hull_number
            info.description = description
            info.recognized = True
            info.pending = False
            info.last_recognized_frame = frame_id

    def bind_db_match(self, track_id: int, db_match_id: str, db_match_desc: str) -> None:
        with self._lock:
            if track_id in self._tracks:
                info = self._tracks[track_id]
                info.db_match_id = db_match_id
                info.db_match_desc = db_match_desc
                info.db_matched = True

    def bind_semantic_matches(self, track_id: int, match_ids: list[str]) -> None:
        with self._lock:
            if track_id in self._tracks:
                self._tracks[track_id].semantic_match_ids = match_ids

    def get_display_text(self, track_id: int) -> str:
        with self._lock:
            if track_id not in self._tracks:
                return "(等待识别...)"
            info = self._tracks[track_id]
            if not info.recognized:
                return "(识别中...)" if info.pending else "(等待识别...)"
            if info.db_matched:
                return f"(库内确定id：{info.db_match_id})"
            label = info.hull_number or "未知"
            desc_short = info.description[:20] if info.description else ""
            return f"(未知id：{label} - {desc_short})" if desc_short else f"(未知id：{label})"

    def cleanup_stale(self, current_frame: int) -> int:
        with self._lock:
            stale_ids = [tid for tid, info in self._tracks.items() if current_frame - info.last_seen_frame > self._max_stale_frames]
            for tid in stale_ids:
                del self._tracks[tid]
        if stale_ids:
            logger.info("清理 %d 个过期 track: %s", len(stale_ids), stale_ids)
        return len(stale_ids)

    def get(self, track_id: int) -> TrackInfo | None:
        with self._lock:
            return self._tracks.get(track_id)

    @property
    def active_tracks(self) -> dict[int, TrackInfo]:
        with self._lock:
            return dict(self._tracks)

    def __len__(self) -> int:
        with self._lock:
            return len(self._tracks)
