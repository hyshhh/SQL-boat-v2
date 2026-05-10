"""页面路由 — 返回 Jinja2 渲染的 HTML 页面"""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from pathlib import Path

templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

router = APIRouter(tags=["pages"])


@router.get("/")
async def index(request: Request):
    """主页 — 船只管理界面"""
    return templates.TemplateResponse("index.html", {"request": request})
