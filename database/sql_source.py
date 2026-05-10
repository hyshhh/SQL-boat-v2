"""SQLite 数据源"""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path

from .base import ShipDataSource

logger = logging.getLogger(__name__)


class SqlShipSource(ShipDataSource):
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._path = Path(db_path)
        self._ensure_table()

    def _connect(self) -> sqlite3.Connection:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self._path))
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_table(self) -> None:
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ships (
                    hull_number TEXT PRIMARY KEY,
                    description TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ship_embeddings (
                    hull_number TEXT PRIMARY KEY,
                    embedding TEXT NOT NULL
                )
            """)
            conn.commit()

    def load_all(self) -> dict[str, str]:
        with self._connect() as conn:
            rows = conn.execute("SELECT hull_number, description FROM ships").fetchall()
            return {row["hull_number"]: row["description"] for row in rows}

    def lookup(self, hull_number: str) -> str | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT description FROM ships WHERE hull_number = ?", (hull_number,)
            ).fetchone()
            return row["description"] if row else None

    def add(self, hull_number: str, description: str) -> bool:
        if self.lookup(hull_number) is not None:
            return False
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO ships (hull_number, description) VALUES (?, ?)",
                (hull_number, description),
            )
            conn.commit()
        return True

    def update(self, hull_number: str, description: str) -> bool:
        if self.lookup(hull_number) is None:
            return False
        with self._connect() as conn:
            conn.execute(
                "UPDATE ships SET description = ? WHERE hull_number = ?",
                (description, hull_number),
            )
            conn.commit()
        return True

    def delete(self, hull_number: str) -> bool:
        if self.lookup(hull_number) is None:
            return False
        with self._connect() as conn:
            conn.execute("DELETE FROM ships WHERE hull_number = ?", (hull_number,))
            conn.execute("DELETE FROM ship_embeddings WHERE hull_number = ?", (hull_number,))
            conn.commit()
        return True

    def upsert(self, hull_number: str, description: str) -> str:
        existing = self.lookup(hull_number)
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO ships (hull_number, description) VALUES (?, ?)",
                (hull_number, description),
            )
            conn.commit()
        return "updated" if existing is not None else "added"

    def bulk_add(self, ships: dict[str, str]) -> int:
        added = 0
        with self._connect() as conn:
            for hn, desc in ships.items():
                try:
                    conn.execute(
                        "INSERT INTO ships (hull_number, description) VALUES (?, ?)",
                        (hn, desc),
                    )
                    added += 1
                except sqlite3.IntegrityError:
                    pass
            conn.commit()
        return added

    def search_by_description(self, keyword: str) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT hull_number, description FROM ships WHERE description LIKE ?",
                (f"%{keyword}%",),
            ).fetchall()
            return [{"hull_number": row["hull_number"], "description": row["description"]} for row in rows]

    def count(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) as cnt FROM ships").fetchone()
            return row["cnt"]

    # ── Embedding 操作 ──

    def load_all_embeddings(self) -> dict[str, list[float]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT hull_number, embedding FROM ship_embeddings").fetchall()
            return {row["hull_number"]: json.loads(row["embedding"]) for row in rows}

    def store_embeddings_bulk(self, records: dict[str, list[float]]) -> int:
        with self._connect() as conn:
            for hn, emb in records.items():
                conn.execute(
                    "INSERT OR REPLACE INTO ship_embeddings (hull_number, embedding) VALUES (?, ?)",
                    (hn, json.dumps(emb)),
                )
            conn.commit()
        return len(records)

    def delete_embedding(self, hull_number: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM ship_embeddings WHERE hull_number = ?", (hull_number,))
            conn.commit()
