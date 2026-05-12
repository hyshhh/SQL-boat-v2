"""
FPSMeter — 10秒滑动窗口 FPS 统计
LatencyMeter — 10秒滑动窗口阶段耗时统计（avg / p50 / p95 / max）
"""

from __future__ import annotations

import logging
import time
from collections import deque
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class FPSMeter:
    """基于滑动窗口的 FPS 计算器，支持多个独立计数通道。"""

    def __init__(self, window_seconds: float = 10.0):
        self._window = max(1.0, window_seconds)
        self._timestamps: dict[str, deque[float]] = {}
        self._last_print: dict[str, float] = {}
        self._print_interval = 5.0

    def tick(self, channel: str = "default") -> None:
        now = time.monotonic()
        if channel not in self._timestamps:
            self._timestamps[channel] = deque()
            self._last_print[channel] = 0.0
        self._timestamps[channel].append(now)
        cutoff = now - self._window
        while self._timestamps[channel] and self._timestamps[channel][0] < cutoff:
            self._timestamps[channel].popleft()

    def get_fps(self, channel: str = "default") -> float:
        if channel not in self._timestamps:
            return 0.0
        timestamps = self._timestamps[channel]
        if len(timestamps) < 2:
            return 0.0
        now = time.monotonic()
        cutoff = now - self._window
        while timestamps and timestamps[0] < cutoff:
            timestamps.popleft()
        if len(timestamps) < 2:
            return 0.0
        elapsed = timestamps[-1] - timestamps[0]
        return (len(timestamps) - 1) / elapsed if elapsed > 0 else 0.0

    def should_print(self, channel: str = "default") -> bool:
        now = time.monotonic()
        if channel not in self._last_print:
            self._last_print[channel] = now
            return False
        if now - self._last_print[channel] >= self._print_interval:
            self._last_print[channel] = now
            return True
        return False

    def get_all_fps(self) -> dict[str, float]:
        return {ch: self.get_fps(ch) for ch in self._timestamps}

    def reset(self, channel: str | None = None) -> None:
        if channel:
            self._timestamps.pop(channel, None)
            self._last_print.pop(channel, None)
        else:
            self._timestamps.clear()
            self._last_print.clear()


class LatencyMeter:
    """滑动窗口阶段耗时统计。支持多阶段通道。"""

    def __init__(self, window_seconds: float = 10.0):
        self._window = max(1.0, window_seconds)
        self._samples: dict[str, deque[tuple[float, float]]] = {}

    def record(self, channel: str, latency_ms: float) -> None:
        now = time.monotonic()
        if channel not in self._samples:
            self._samples[channel] = deque()
        self._samples[channel].append((now, latency_ms))
        self._cleanup(channel, now)

    @contextmanager
    def measure(self, channel: str):
        t0 = time.perf_counter()
        yield
        latency_ms = (time.perf_counter() - t0) * 1000
        self.record(channel, latency_ms)

    def _cleanup(self, channel: str, now: float) -> None:
        cutoff = now - self._window
        samples = self._samples[channel]
        while samples and samples[0][0] < cutoff:
            samples.popleft()

    def get_stats(self, channel: str) -> dict[str, float]:
        if channel not in self._samples:
            return {"avg": 0, "p50": 0, "p95": 0, "max": 0, "count": 0}
        now = time.monotonic()
        self._cleanup(channel, now)
        samples = self._samples[channel]
        if not samples:
            return {"avg": 0, "p50": 0, "p95": 0, "max": 0, "count": 0}
        values = sorted(s[1] for s in samples)
        count = len(values)
        return {
            "avg": round(sum(values) / count, 1),
            "p50": values[count // 2],
            "p95": values[min(int(count * 0.95), count - 1)],
            "max": values[-1],
            "count": count,
        }

    def get_all_stats(self) -> dict[str, dict[str, float]]:
        return {ch: self.get_stats(ch) for ch in self._samples}

    def reset(self, channel: str | None = None) -> None:
        if channel:
            self._samples.pop(channel, None)
        else:
            self._samples.clear()
