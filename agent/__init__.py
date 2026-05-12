"""
AgentResult — 识别结果数据类（无 LangChain 依赖）

三步链路产出的结构化结果：
  Step1: VLM 识别 → hull_number + description
  Step2: db.lookup(hull_number) → 精确匹配
  Step3: db.semantic_search_filtered(description) → 语义检索
"""

from __future__ import annotations


class AgentResult:
    """三步链路运行结果。"""

    def __init__(
        self,
        hull_number: str = "",
        description: str = "",
        match_type: str = "none",
        semantic_match_ids: list[str] | None = None,
        answer: str = "",
    ):
        self.hull_number = hull_number
        self.description = description
        self.match_type = match_type  # "exact" | "semantic" | "none"
        self.semantic_match_ids = semantic_match_ids or []
        self.answer = answer
