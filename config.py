"""配置读取 — 唯一配置源 config.yaml，返回字典"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"

_DEFAULTS: dict[str, Any] = {
    "llm": {
        "model": "Qwen/Qwen3-VL-4B-AWQ",
        "api_key": "abc123",
        "base_url": "http://localhost:7890/v1",
        "temperature": 0.0,
    },
    "embed": {
        "model": "Qwen3-Embedding-0.6B",
        "api_key": "abc123",
        "base_url": "http://localhost:7891/v1",
    },
    "retrieval": {"top_k": 3, "score_threshold": 0.5},
    "vector_store": {"persist_path": "./vector_store", "auto_rebuild": False},
    "database": {"backend": "sqlite", "sqlite_path": "./data/ships.db"},
    "pipeline": {
        "concurrent_mode": False,
        "max_concurrent": 4,
        "max_queued_frames": 30,
        "process_every_n_frames": 30,
        "output_dir": "./output",
        "save_screenshots": True,
        "prompt_mode": "detailed",
        "enable_refresh": False,
        "gap_num": 150,
        "demo": False,
        "yolo_model": "yolov8n.pt",
        "device": "",
        "conf_threshold": 0.25,
        "detect_every_n_frames": 1,
        "tracker": "bytetrack",
        "tracker_params": {
            "track_high_thresh": 0.5,
            "track_low_thresh": 0.05,
            "new_track_thresh": 0.6,
            "track_buffer": 90,
            "match_thresh": 0.5,
        },
        "detect_classes": [8],
        "max_stale_frames": 300,
    },
    "app": {"log_level": "INFO", "ship_db_path": "./data/ships.csv"},
    "web": {"host": "0.0.0.0", "port": 8000},
    "demo_video": {
        "dir": "./demovid",
        "output_dir": "./demo_output",
        "allowed_extensions": [".mp4", ".avi", ".mkv", ".mov", ".flv", ".wmv", ".webm"],
        "max_file_size_mb": 500,
    },
}


def _deep_merge(base: dict, override: dict) -> dict:
    merged = base.copy()
    for k, v in override.items():
        if k in merged and isinstance(merged[k], dict) and isinstance(v, dict):
            merged[k] = _deep_merge(merged[k], v)
        else:
            merged[k] = v
    return merged


def _load_yaml(path: Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f"配置文件 {path} 格式错误，期望顶层为字典")
    return data


def load_config(config_path: str | Path | None = None) -> dict[str, Any]:
    user_dict: dict[str, Any] = {}
    if config_path:
        p = Path(config_path)
        if p.exists():
            user_dict = _load_yaml(p)
        else:
            logger.debug("配置文件不存在: %s，使用默认值", p)
    else:
        candidates = [Path.cwd() / "config.yaml", _DEFAULT_CONFIG_PATH]
        for candidate in candidates:
            if candidate.exists() and candidate.is_file():
                try:
                    user_dict = _load_yaml(candidate)
                except yaml.YAMLError as e:
                    raise SystemExit(f"配置文件解析失败: {e}")
                break
    return _deep_merge(_DEFAULTS, user_dict)
