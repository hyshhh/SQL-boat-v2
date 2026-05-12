"""CSV 数据源 — 支持带表头的 hull_number,description 格式"""

from __future__ import annotations

import csv
import logging
from pathlib import Path

from .base import ShipDataSource

logger = logging.getLogger(__name__)


class CsvShipSource(ShipDataSource):
    def __init__(self, csv_path: str):
        self._path = Path(csv_path)
        self._data: dict[str, str] = {}

    def load_all(self) -> dict[str, str]:
        self._data.clear()
        if not self._path.exists():
            return self._data
        with open(self._path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames and "hull_number" in reader.fieldnames:
                # 带表头格式：hull_number,description
                for row in reader:
                    hn = (row.get("hull_number") or "").strip()
                    desc = (row.get("description") or "").strip()
                    if hn:
                        self._data[hn] = desc
            else:
                # 无表头格式：兼容旧数据
                f.seek(0)
                reader = csv.reader(f)
                for row in reader:
                    if len(row) >= 2:
                        hn = row[0].strip()
                        desc = row[1].strip()
                        if hn and hn != "hull_number":
                            self._data[hn] = desc
        logger.info("从 CSV 加载了 %d 条船记录: %s", len(self._data), self._path)
        return self._data

    def lookup(self, hull_number: str) -> str | None:
        return self._data.get(hull_number)

    def add(self, hull_number: str, description: str) -> bool:
        if hull_number in self._data:
            return False
        self._data[hull_number] = description
        self._save()
        return True

    def update(self, hull_number: str, description: str) -> bool:
        if hull_number not in self._data:
            return False
        self._data[hull_number] = description
        self._save()
        return True

    def delete(self, hull_number: str) -> bool:
        if hull_number not in self._data:
            return False
        del self._data[hull_number]
        self._save()
        return True

    def upsert(self, hull_number: str, description: str) -> str:
        if hull_number in self._data:
            self._data[hull_number] = description
            self._save()
            return "updated"
        self._data[hull_number] = description
        self._save()
        return "added"

    def bulk_add(self, ships: dict[str, str]) -> int:
        added = 0
        for hn, desc in ships.items():
            if hn not in self._data:
                self._data[hn] = desc
                added += 1
        if added > 0:
            self._save()
        return added

    def count(self) -> int:
        return len(self._data)

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["hull_number", "description"])
            for hn, desc in sorted(self._data.items()):
                writer.writerow([hn, desc])
