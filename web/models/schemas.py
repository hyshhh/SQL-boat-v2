"""Pydantic 请求/响应模型"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


# ── 请求模型 ──

class ShipCreate(BaseModel):
    hull_number: str = Field(..., min_length=1, max_length=50, description="舷号")
    description: str = Field(..., min_length=1, max_length=2000, description="船只描述")


class ShipUpdate(BaseModel):
    description: str = Field(..., min_length=1, max_length=2000, description="船只描述")


class ShipBulkCreate(BaseModel):
    ships: dict[str, str] = Field(..., description="批量数据 {hull_number: description}")


# ── 响应模型 ──

class ApiResponse(BaseModel):
    success: bool
    message: str
    data: Any = None


class ShipItem(BaseModel):
    hull_number: str
    description: str


class ShipListResponse(BaseModel):
    total: int
    ships: list[ShipItem]


class StatsResponse(BaseModel):
    total_ships: int
    backend: str


class SearchResponse(BaseModel):
    total: int
    results: list[ShipItem]


class RecognizeData(BaseModel):
    hull_number: str
    description: str
    already_exists: bool = False
    existing_description: str | None = None
