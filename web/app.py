"""
船只舷号管理系统 — FastAPI 应用入口

启动方式:
    uvicorn web.app:app --host 0.0.0.0 --port 8000 --reload
    或
    python -m web.app
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from config import load_config
from web.routes import api_router, pages_router, pipeline_router
from web.services import ShipService


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期：启动时初始化数据库，挂载到 app.state"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    logging.getLogger(__name__).info("Web 服务启动，初始化数据库…")

    # 检测 ffmpeg/ffprobe 可用性
    import shutil
    ffmpeg = shutil.which("ffmpeg") or ""
    ffprobe = shutil.which("ffprobe") or ""
    if ffmpeg and ffprobe:
        logging.getLogger(__name__).info("ffmpeg/ffprobe 可用: %s / %s", ffmpeg, ffprobe)
    else:
        logging.getLogger(__name__).warning("ffmpeg/ffprobe 不可用！视频转码功能将被禁用。ffmpeg=%s ffprobe=%s", ffmpeg or "(未找到)", ffprobe or "(未找到)")

    config = load_config()
    app.state.ship_service = ShipService(config=config)
    yield
    logging.getLogger(__name__).info("Web 服务关闭")


class _NoKeepaliveMiddleware:
    """ASGI 中间件：禁用 websockets 库内置 keepalive

    websockets 后台 keepalive 任务在 TCP 缓冲区满时会抛出 AssertionError
    并杀死连接。该中间件在 WebSocket scope 中设置 ping_interval=None，
    无论 uvicorn 以何种方式启动（命令行 / python -m）都能生效。
    """
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope.get("type") == "websocket":
            scope["ping_interval"] = None
            scope["ping_timeout"] = None
        await self.app(scope, receive, send)


app = FastAPI(
    title="船只舷号管理系统",
    description="通过 Web 界面管理船只舷号数据，支持 CSV 和 SQLite 后端",
    version="2.0.0",
    lifespan=lifespan,
)
# 禁用 websockets 库内置 keepalive（防止推流时 TCP 缓冲满导致连接崩溃）
app = _NoKeepaliveMiddleware(app)

# ── 静态文件 ──
_static_dir = Path(__file__).parent / "static"
if _static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")

# ── 注册路由 ──
app.include_router(pages_router)
app.include_router(api_router)
app.include_router(pipeline_router)


# ── 启动入口 ──
def main():
    import uvicorn

    config = load_config()
    web_cfg = config.get("web", {})
    host = web_cfg.get("host", "0.0.0.0")
    port = web_cfg.get("port", 8000)
    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info",
        ws_ping_interval=None,  # 禁用 websockets 库自带 keepalive（已有应用层心跳）
    )


if __name__ == "__main__":
    main()
