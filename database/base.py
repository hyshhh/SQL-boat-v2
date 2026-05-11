"""数据源抽象基类"""

from __future__ import annotations
from abc import ABC, abstractmethod


class ShipDataSource(ABC):
    @abstractmethod
    def load_all(self) -> dict[str, str]: ...

    @abstractmethod
    def lookup(self, hull_number: str) -> str | None: ...

    @abstractmethod
    def add(self, hull_number: str, description: str) -> bool: ...

    @abstractmethod
    def update(self, hull_number: str, description: str) -> bool: ...

    @abstractmethod
    def delete(self, hull_number: str) -> bool: ...

    @abstractmethod
    def upsert(self, hull_number: str, description: str) -> str: ...

    @abstractmethod
    def count(self) -> int: ...

    @abstractmethod
    def bulk_add(self, ships: dict[str, str]) -> int: ...

    def search_by_description(self, keyword: str) -> list[dict]:
        """按描述关键词搜索（默认实现：内存过滤子串匹配）"""
        data = self.load_all()
        return [
            {"hull_number": hn, "description": desc}
            for hn, desc in data.items()
            if keyword.lower() in desc.lower()
        ]
