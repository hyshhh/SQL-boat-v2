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
