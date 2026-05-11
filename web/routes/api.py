"""REST API 路由 — 船只数据 CRUD + 识别"""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, File, HTTPException, Query, Request, UploadFile

from web.models import (
    ApiResponse,
    SearchResponse,
    ShipBulkCreate,
    ShipCreate,
    ShipItem,
    ShipListResponse,
    ShipUpdate,
    StatsResponse,
)
from web.services import ShipService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/ships", tags=["ships"])


# ── 依赖注入 ──

def get_service(request: Request) -> ShipService:
    """从 app.state 获取在 lifespan 中初始化的 ShipService 单例"""
    return request.app.state.ship_service


# ── 允许的文件类型 ──
ALLOWED_IMAGE_TYPES = {"image/jpeg", "image/png", "image/bmp", "image/webp", "image/gif"}
MAX_FILE_SIZE = 20 * 1024 * 1024  # 20MB


# ── 固定路径路由（必须在 {hull_number} 之前注册，否则会被参数路由吞掉）──

@router.get("", response_model=ShipListResponse)
async def list_ships(svc: Annotated[ShipService, Depends(get_service)]):
    """获取所有船只列表"""
    ships = svc.list_ships()
    return ShipListResponse(total=len(ships), ships=[ShipItem(**s) for s in ships])


@router.get("/search", response_model=SearchResponse)
async def search_ships(
    q: Annotated[str, Query(description="搜索关键词")],
    svc: Annotated[ShipService, Depends(get_service)],
):
    """按描述关键词搜索"""
    if not q.strip():
        raise HTTPException(status_code=400, detail="搜索关键词不能为空")
    results = svc.search(q)
    return SearchResponse(total=len(results), results=[ShipItem(**r) for r in results])


@router.get("/stats", response_model=StatsResponse)
async def stats(svc: Annotated[ShipService, Depends(get_service)]):
    """数据库统计信息"""
    info = svc.stats()
    return StatsResponse(total_ships=info["total_ships"], backend=info["backend"])


@router.post("", response_model=ApiResponse)
async def create_ship(body: ShipCreate, svc: Annotated[ShipService, Depends(get_service)]):
    """新增船只"""
    success = svc.create_ship(body.hull_number, body.description)
    if not success:
        raise HTTPException(status_code=409, detail=f"弦号已存在: {body.hull_number}")
    return ApiResponse(success=True, message=f"成功添加弦号: {body.hull_number}")


@router.post("/bulk", response_model=ApiResponse)
async def bulk_create(body: ShipBulkCreate, svc: Annotated[ShipService, Depends(get_service)]):
    """批量添加船只"""
    result = svc.bulk_create(body.ships)
    return ApiResponse(
        success=True,
        message=f"成功添加 {result['added']} 条（跳过 {result['skipped']} 条已存在的）",
        data=result,
    )


@router.post("/recognize", response_model=ApiResponse)
async def recognize_ship(
    file: UploadFile = File(...),
    svc: ShipService = Depends(get_service),
):
    """上传图片，调用 VLM 自动识别弦号和描述"""
    if file.content_type and file.content_type not in ALLOWED_IMAGE_TYPES:
        raise HTTPException(status_code=400, detail=f"不支持的文件类型: {file.content_type}")
    contents = await file.read()
    if len(contents) > MAX_FILE_SIZE:
        raise HTTPException(status_code=400, detail="文件过大，请上传 20MB 以内的图片")
    try:
        result = svc.recognize_ship(contents, file.filename or "upload.jpg")
    except Exception as e:
        logger.error("VLM 识别失败: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"识别失败: {str(e)}")
    return ApiResponse(success=True, message="识别成功", data=result)


@router.post("/recognize-and-add", response_model=ApiResponse)
async def recognize_and_add(
    file: UploadFile = File(...),
    svc: ShipService = Depends(get_service),
):
    """上传图片，识别后自动添加到数据库"""
    if file.content_type and file.content_type not in ALLOWED_IMAGE_TYPES:
        raise HTTPException(status_code=400, detail=f"不支持的文件类型: {file.content_type}")
    contents = await file.read()
    if len(contents) > MAX_FILE_SIZE:
        raise HTTPException(status_code=400, detail="文件过大，请上传 20MB 以内的图片")
    try:
        result = svc.recognize_and_add(contents, file.filename or "upload.jpg")
    except Exception as e:
        logger.error("VLM 识别失败: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"识别失败: {str(e)}")
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    action = result.get("action", "added")
    hn = result["hull_number"]
    msg = f"弦号 {hn} 已存在，已更新描述" if action == "updated" else f"成功添加弦号: {hn}"
    return ApiResponse(success=True, message=msg, data=result)


# ── 参数路由（放在固定路径之后）──

@router.get("/{hull_number}", response_model=ShipItem)
async def get_ship(hull_number: str, svc: Annotated[ShipService, Depends(get_service)]):
    """查询单条船只"""
    ship = svc.get_ship(hull_number)
    if ship is None:
        raise HTTPException(status_code=404, detail=f"未找到弦号: {hull_number}")
    return ShipItem(**ship)


@router.put("/{hull_number}", response_model=ApiResponse)
async def update_ship(
    hull_number: str,
    body: ShipUpdate,
    svc: Annotated[ShipService, Depends(get_service)],
):
    """更新船只描述"""
    success = svc.update_ship(hull_number, body.description)
    if not success:
        raise HTTPException(status_code=404, detail=f"未找到弦号: {hull_number}")
    return ApiResponse(success=True, message=f"成功更新弦号: {hull_number}")


@router.delete("/{hull_number}", response_model=ApiResponse)
async def delete_ship(hull_number: str, svc: Annotated[ShipService, Depends(get_service)]):
    """删除船只"""
    success = svc.delete_ship(hull_number)
    if not success:
        raise HTTPException(status_code=404, detail=f"未找到弦号: {hull_number}")
    return ApiResponse(success=True, message=f"成功删除弦号: {hull_number}")
