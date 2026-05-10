"""业务逻辑服务层 — 封装数据库操作和 VLM 识别"""

from __future__ import annotations

import base64
import json
import logging
import re
from pathlib import Path
from typing import Any

from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI

from config import load_config
from database import ShipDatabase

logger = logging.getLogger(__name__)

RECOGNITION_PROMPT = """你是船只弦号识别专家。你的核心任务是读取船体侧面的文字编号。

重要指令：
- 不要评价图片质量（无论清晰还是模糊都不要提）
- 不要说"看不清""质量低"等废话
- 即使图片模糊，也必须尝试读取船体上的任何可见文字、数字、编号
- 重点关注：船体侧面白色/黑色的编号区域、船尾文字、船名

请返回以下 JSON（不要任何其他文字）：
{{
  "hull_number": "读到的弦号编号（如 0014、海巡123、A01 等，完全没有可见文字则返回空字符串）",
  "description": "客观描述船只：船型+船体颜色+上层建筑颜色+特殊标志（不提图片质量）"
}}"""


class ShipService:
    """船只数据管理服务"""

    def __init__(self, config: dict[str, Any] | None = None):
        self._config = config or load_config()
        self._db: ShipDatabase | None = None
        self._vlm_client: ChatOpenAI | None = None

    @property
    def db(self) -> ShipDatabase:
        if self._db is None:
            self._db = ShipDatabase(config=self._config)
        return self._db

    @property
    def vlm(self) -> ChatOpenAI:
        if self._vlm_client is None:
            llm_cfg = self._config.get("llm", {})
            self._vlm_client = ChatOpenAI(
                model=llm_cfg.get("model", "Qwen/Qwen3-VL-4B-AWQ"),
                api_key=llm_cfg.get("api_key", "abc123"),
                base_url=llm_cfg.get("base_url", "http://localhost:7890/v1"),
                temperature=0.0,
                max_tokens=1024,
            )
        return self._vlm_client

    # ── CRUD ──

    def list_ships(self) -> list[dict]:
        data = self.db.source.load_all()
        return [{"hull_number": hn, "description": desc} for hn, desc in sorted(data.items())]

    def get_ship(self, hull_number: str) -> dict | None:
        desc = self.db.lookup(hull_number)
        if desc is None:
            return None
        return {"hull_number": hull_number, "description": desc}

    def create_ship(self, hull_number: str, description: str) -> bool:
        return self.db.add_ship(hull_number, description)

    def update_ship(self, hull_number: str, description: str) -> bool:
        return self.db.update_ship(hull_number, description)

    def delete_ship(self, hull_number: str) -> bool:
        return self.db.delete_ship(hull_number)

    def bulk_create(self, ships: dict[str, str]) -> dict:
        added = self.db.source.bulk_add(ships)
        if added > 0:
            self.db.reload()
        return {"added": added, "skipped": len(ships) - added}

    def search(self, keyword: str) -> list[dict]:
        source = self.db.source
        if hasattr(source, "search_by_description"):
            return source.search_by_description(keyword)
        data = source.load_all()
        return [
            {"hull_number": hn, "description": desc}
            for hn, desc in data.items()
            if keyword.lower() in desc.lower()
        ]

    def stats(self) -> dict:
        source = self.db.source
        backend_type = "sqlite" if hasattr(source, "db_path") else "csv"
        return {"total_ships": source.count(), "backend": backend_type}

    # ── VLM 识别 ──

    def recognize_ship(self, image_bytes: bytes, filename: str) -> dict:
        """从图片识别船只，返回 {hull_number, description}"""
        b64 = base64.b64encode(image_bytes).decode("utf-8")
        ext = Path(filename).suffix.lower()
        mime_map = {
            ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
            ".png": "image/png", ".bmp": "image/bmp",
            ".webp": "image/webp", ".gif": "image/gif",
        }
        mime = mime_map.get(ext, "image/jpeg")

        msg = HumanMessage(content=[
            {"type": "text", "text": RECOGNITION_PROMPT},
            {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{b64}"}},
        ])

        resp = self.vlm.invoke([msg])
        content = resp.content.strip()

        if content.startswith("```"):
            content = content.split("\n", 1)[-1]
            if content.endswith("```"):
                content = content[:-3]
            content = content.strip()

        try:
            result = json.loads(content)
        except json.JSONDecodeError:
            match = re.search(r'\{.*\}', content, re.DOTALL)
            if match:
                try:
                    result = json.loads(match.group())
                except json.JSONDecodeError:
                    result = {"hull_number": "", "description": content}
            else:
                result = {"hull_number": "", "description": content}

        hull_number = str(result.get("hull_number", "")).strip()
        description = str(result.get("description", "")).strip()

        # 检查是否已存在
        existing_desc = None
        if hull_number:
            existing_desc = self.db.lookup(hull_number)

        return {
            "hull_number": hull_number,
            "description": description,
            "already_exists": existing_desc is not None,
            "existing_description": existing_desc,
        }

    def recognize_and_add(self, image_bytes: bytes, filename: str) -> dict:
        """识别后自动入库"""
        result = self.recognize_ship(image_bytes, filename)
        hull_number = result["hull_number"]
        if not hull_number:
            return {"error": "未能识别出弦号，请手动输入", "result": result}
        action = self.db.upsert_ship(hull_number, result["description"])
        result["action"] = action
        return result
