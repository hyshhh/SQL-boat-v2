"""CSV 数据源"""

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
        with open(self._path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 2:
                    self._data[row[0].strip()] = row[1].strip()
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
            for hn, desc in sorted(self._data.items()):
                writer.writerow([hn, desc])
