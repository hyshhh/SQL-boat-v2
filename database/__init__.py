"""船弦号数据库 — 可插拔数据源 + SQLite embedding 向量检索"""

from __future__ import annotations

import hashlib
import logging
import math
from pathlib import Path
from typing import Any, Mapping

from langchain_core.embeddings import Embeddings

from config import load_config

logger = logging.getLogger(__name__)

HASH_FILE_NAME = ".db_hash"


class DashScopeEmbeddings(Embeddings):
    """DashScope Embedding 封装"""

    def __init__(self, model: str, api_key: str, base_url: str):
        if not api_key or api_key.startswith("your-"):
            raise ValueError("Embedding API Key 未配置。")
        self.model = model
        self.api_key = api_key
        self._url = f"{base_url.rstrip('/')}/embeddings"
        self._headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        import httpx
        import time

        max_retries = 3
        batch_size = 10
        all_embeddings: list[list[float]] = []

        for batch_start in range(0, len(texts), batch_size):
            batch = texts[batch_start : batch_start + batch_size]
            last_error: Exception | None = None
            for attempt in range(max_retries):
                try:
                    payload = {"model": self.model, "input": batch}
                    resp = httpx.post(self._url, headers=self._headers, json=payload, timeout=60)
                    if resp.status_code == 429:
                        retry_after = int(resp.headers.get("Retry-After", 2**attempt))
                        time.sleep(retry_after)
                        continue
                    if resp.status_code >= 500:
                        time.sleep(2**attempt)
                        continue
                    if not resp.is_success:
                        raise RuntimeError(f"Embedding API 返回 {resp.status_code}")
                    data = resp.json()
                    batch_embeddings = [item["embedding"] for item in data["data"]]
                    all_embeddings.extend(batch_embeddings)
                    break
                except (httpx.TimeoutException, httpx.NetworkError) as e:
                    last_error = e
                    time.sleep(2**attempt)
            else:
                raise RuntimeError(f"Embedding API 调用失败") from last_error
        return all_embeddings

    def embed_query(self, text: str) -> list[float]:
        return self.embed_documents([text])[0]


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def _create_source(config: dict[str, Any], db_path: str | None = None):
    from .csv_source import CsvShipSource
    from .sql_source import SqlShipSource

    db_cfg = config.get("database", {})
    backend = db_cfg.get("backend", "csv")
    if backend == "sqlite":
        sql_path = db_path or db_cfg.get("sqlite_path", "./data/ships.db")
        return SqlShipSource(sql_path)
    else:
        csv_path = db_path or config.get("app", {}).get("ship_db_path", "./data/ships.csv")
        return CsvShipSource(csv_path)


def _get_embedding_store(config: dict[str, Any], source):
    from .sql_source import SqlShipSource

    db_cfg = config.get("database", {})
    backend = db_cfg.get("backend", "csv")
    if backend == "sqlite":
        return source
    else:
        embed_db_path = db_cfg.get("sqlite_path", "./data/ships.db")
        return SqlShipSource(embed_db_path)


class ShipDatabase:
    """船弦号数据库 — 双通道检索：精确查找 + 语义检索"""

    def __init__(self, config: dict[str, Any] | None = None, db_path: str | None = None):
        if config is None:
            config = load_config()
        self._config = config
        embed_cfg = config.get("embed", {})
        retrieval_cfg = config.get("retrieval", {})
        self._source = _create_source(config, db_path)
        self._data = self._source.load_all()
        self._embed_store = _get_embedding_store(config, self._source)
        self._embed_cfg = embed_cfg
        self._embeddings: Embeddings | None = None
        self._top_k = retrieval_cfg.get("top_k", 3)
        self._score_threshold = retrieval_cfg.get("score_threshold", 0.5)
        self._embedding_cache: dict[str, list[float]] | None = None
        self._persist_path = config.get("vector_store", {}).get("persist_path", "./vector_store")

    def _compute_data_hash(self) -> str:
        content = "\n".join(f"{k}|{v}" for k, v in sorted(self._data.items()))
        return hashlib.md5(content.encode("utf-8")).hexdigest()

    def _load_saved_hash(self) -> str | None:
        hash_file = Path(self._persist_path) / HASH_FILE_NAME
        if hash_file.exists():
            return hash_file.read_text(encoding="utf-8").strip()
        return None

    def _save_hash(self, data_hash: str) -> None:
        persist_dir = Path(self._persist_path)
        persist_dir.mkdir(parents=True, exist_ok=True)
        (persist_dir / HASH_FILE_NAME).write_text(data_hash, encoding="utf-8")

    def _data_changed(self) -> bool:
        return self._compute_data_hash() != self._load_saved_hash()

    def _get_embeddings(self) -> Embeddings:
        if self._embeddings is None:
            self._embeddings = DashScopeEmbeddings(
                model=self._embed_cfg.get("model", "Qwen3-Embedding-0.6B"),
                api_key=self._embed_cfg.get("api_key", ""),
                base_url=self._embed_cfg.get("base_url", "http://localhost:7891/v1"),
            )
        return self._embeddings

    def build_embeddings(self, force: bool = False) -> int:
        self._data = self._source.load_all()
        if not self._data:
            return 0
        existing = self._embed_store.load_all_embeddings()
        to_embed = dict(self._data) if force else {hn: desc for hn, desc in self._data.items() if hn not in existing}
        if not to_embed:
            return 0
        texts = [f"弦号 {hn}\n{desc}" for hn, desc in to_embed.items()]
        embeddings = self._get_embeddings().embed_documents(texts)
        records = dict(zip(to_embed.keys(), embeddings))
        count = self._embed_store.store_embeddings_bulk(records)
        self._embedding_cache = None
        return count

    def _load_embedding_cache(self) -> dict[str, list[float]]:
        if self._embedding_cache is None:
            self._embedding_cache = self._embed_store.load_all_embeddings()
        return self._embedding_cache

    def lookup(self, hull_number: str) -> str | None:
        return self._source.lookup(hull_number)

    def semantic_search(self, query: str, top_k: int | None = None) -> list[dict]:
        k = top_k or self._top_k
        query_embedding = self._get_embeddings().embed_query(query)
        all_embeddings = self._load_embedding_cache()
        if not all_embeddings:
            return []
        scored = [(hn, _cosine_similarity(query_embedding, vec)) for hn, vec in all_embeddings.items()]
        scored.sort(key=lambda x: x[1], reverse=True)
        results = []
        for hn, score in scored[:k]:
            desc = self._data.get(hn) or self._source.lookup(hn) or ""
            results.append({"hull_number": hn, "description": desc, "score": round(score, 4)})
        return results

    def semantic_search_filtered(self, query: str) -> list[dict]:
        results = self.semantic_search(query, top_k=self._top_k)
        return [r for r in results if r["score"] >= self._score_threshold]

    def add_ship(self, hull_number: str, description: str) -> bool:
        result = self._source.add(hull_number, description)
        if result:
            self._invalidate_cache()
        return result

    def update_ship(self, hull_number: str, description: str) -> bool:
        result = self._source.update(hull_number, description)
        if result:
            self._invalidate_cache()
        return result

    def delete_ship(self, hull_number: str) -> bool:
        result = self._source.delete(hull_number)
        if result:
            if hasattr(self._embed_store, "delete_embedding"):
                self._embed_store.delete_embedding(hull_number)
            self._invalidate_cache()
        return result

    def upsert_ship(self, hull_number: str, description: str) -> str:
        result = self._source.upsert(hull_number, description)
        self._invalidate_cache()
        return result

    def reload(self) -> None:
        self._data = self._source.load_all()
        self._embedding_cache = None

    def _invalidate_cache(self) -> None:
        self._data = self._source.load_all()
        self._embedding_cache = None

    @property
    def source(self):
        return self._source

    @property
    def embed_store(self):
        return self._embed_store

    @property
    def hull_numbers(self) -> list[str]:
        return list(self._data.keys())

    @property
    def descriptions(self) -> list[str]:
        return list(self._data.values())

    @property
    def items(self) -> Mapping[str, str]:
        return self._data

    def __len__(self) -> int:
        return len(self._data)
