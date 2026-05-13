"""工具模块 — VLM 推理函数（三步链路 Step1）"""

from __future__ import annotations

import base64
import json
import logging
import re

import cv2
import httpx
import numpy as np

from config import load_config

logger = logging.getLogger(__name__)

# ── 配置缓存（避免每次推理都读磁盘）──
_cached_llm_cfg: dict | None = None


def _get_llm_cfg() -> dict:
    """获取 LLM 配置（带缓存）。"""
    global _cached_llm_cfg
    if _cached_llm_cfg is None:
        config = load_config()
        _cached_llm_cfg = config.get("llm", {})
    return _cached_llm_cfg


def _vlm_infer(image_b64: str, prompt_mode: str = "detailed") -> dict:
    """
    调用 VLM 进行弦号识别，返回 {hull_number, description}。

    三步链路 Step1：对裁剪的船只图像调用视觉大模型，
    识别船体弦号编号和船只外观描述。

    Args:
        image_b64: JPEG 图像的 base64 编码字符串。
        prompt_mode: 提示词模式，"detailed"（详细）或 "brief"（简略）。

    Returns:
        {"hull_number": str, "description": str}
    """
    llm_cfg = _get_llm_cfg()

    api_url = f"{llm_cfg.get('base_url', 'http://localhost:7890/v1').rstrip('/')}/chat/completions"
    headers = {
        "Authorization": f"Bearer {llm_cfg.get('api_key', 'abc123')}",
        "Content-Type": "application/json",
    }

    # 跳过重复编码：crop 已经在 pipeline 中以 JPEG quality=85 编码过，
    # 直接使用原始 base64，避免 解码→重编码→重base64 的 CPU 浪费

    if prompt_mode == "brief":
        prompt = (
            "识别船体上的弦号编号，不要评价图片质量。\n"
            "返回 JSON（不要其他文字）：\n"
            '{"hull_number": "弦号编号（无则空字符串）", '
            '"description": "简要描述：船型+颜色+主要特征（50字内，不提图片质量）"}'
        )
    else:
        prompt = (
            "你是船只弦号识别专家。你的核心任务是读取船体侧面的文字编号。\n\n"
            "重要指令：\n"
            "- 不要评价图片质量（无论清晰还是模糊都不要提）\n"
            "- 不要说\"看不清\"\"质量低\"等废话\n"
            "- 即使图片模糊，也必须尝试读取船体上的任何可见文字、数字、编号\n"
            "- 重点关注：船体侧面白色/黑色的编号区域、船尾文字、船名\n\n"
            "返回 JSON（不要其他文字）：\n"
            '{"hull_number": "读到的弦号编号（如 0014、海巡123、A01 等，完全没有可见文字则返回空字符串）", '
            '"description": "客观描述船只：船型+船体颜色+上层建筑颜色+特殊标志（不提图片质量）"}'
        )

    payload = {
        "model": llm_cfg.get("model", "Qwen/Qwen3-VL-4B-AWQ"),
        "temperature": llm_cfg.get("temperature", 0.0),
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{image_b64}"},
                    },
                ],
            }
        ],
    }

    resp = httpx.post(api_url, headers=headers, json=payload, timeout=8)
    resp.raise_for_status()

    try:
        body = resp.json()
        content = body["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as e:
        logger.error("VLM 返回格式异常: %s, 原始: %s", e, resp.text[:300])
        return {"hull_number": "", "description": ""}

    # 解析 JSON
    content = content.strip()
    if content.startswith("```"):
        content = content.split("\n", 1)[-1]
        if content.endswith("```"):
            content = content[:-3]
        content = content.strip()

    result: dict = {}
    try:
        result = json.loads(content)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", content, re.DOTALL)
        if match:
            try:
                result = json.loads(match.group())
            except json.JSONDecodeError:
                logger.warning("VLM 返回无法解析为 JSON: %s", content[:200])
        else:
            logger.warning("VLM 返回无 JSON 结构: %s", content[:200])

    if not isinstance(result, dict):
        logger.warning("VLM 返回非字典类型: %s", type(result).__name__)
        result = {}

    return {
        "hull_number": str(result.get("hull_number") or "").strip(),
        "description": str(result.get("description") or "").strip(),
    }
