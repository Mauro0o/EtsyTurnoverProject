"""
checkpoint.py - Run tracking and resumable scraping support.

Stores run state in the same SQLite database as the listing data.

Tables managed here:
  scrape_runs         – one row per invocation; tracks status and shop info.
  scrape_checkpoints  – one row per (run_id, stage, page_number); tracks
                        page-level progress so scraping can resume safely
                        after an interruption.

Resume flow:
  1. On --resume, find the most recent 'started' run for the given shop/domain.
  2. Restore that run_id and skip pages already marked 'completed'.
  3. Continue from the first non-completed page.
"""

from __future__ import annotations

import logging
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Stage names used as checkpoint keys.
STAGE_SOLD = "sold"
STAGE_STOREFRONT = "storefront"

# Run status values.
STATUS_STARTED = "started"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"


class CheckpointManager:
    """Manages run records and per-page checkpoints in SQLite."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        self.run_id: Optional[str] = None

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Open the SQLite connection and create tables if absent."""
        self._conn = sqlite3.connect(str(self.db_path))
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._create_tables()

    def close(self) -> None:
        """Commit pending changes and close the connection."""
        if self._conn:
            try:
                self._conn.commit()
            except Exception:
                pass
            self._conn.close()
            self._conn = None

    def _create_tables(self) -> None:
        assert self._conn is not None
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS scrape_runs (
                run_id      TEXT PRIMARY KEY,
                started_at  TEXT NOT NULL,
                finished_at TEXT,
                status      TEXT NOT NULL DEFAULT 'started',
                shop_name   TEXT,
                shop_id     TEXT,
                domain      TEXT
            );

            CREATE TABLE IF NOT EXISTS scrape_checkpoints (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id       TEXT NOT NULL,
                stage        TEXT NOT NULL,
                page_number  INTEGER NOT NULL,
                status       TEXT NOT NULL DEFAULT 'started',
                started_at   TEXT NOT NULL,
                finished_at  TEXT,
                error_msg    TEXT,
                UNIQUE(run_id, stage, page_number)
            );
            """
        )
        self._conn.commit()

    # ------------------------------------------------------------------
    # Run management
    # ------------------------------------------------------------------

    def start_run(self, shop_name: str, shop_id: str, domain: str) -> str:
        """
        Insert a new scrape_run record and return its run_id UUID.

        Also sets self.run_id so subsequent checkpoint calls work without
        requiring the caller to pass the run_id explicitly.
        """
        run_id = str(uuid.uuid4())
        assert self._conn is not None
        self._conn.execute(
            """
            INSERT INTO scrape_runs (run_id, started_at, status, shop_name, shop_id, domain)
            VALUES (?, ?, 'started', ?, ?, ?)
            """,
            (run_id, _utcnow(), shop_name, shop_id, domain),
        )
        self._conn.commit()
        self.run_id = run_id
        logger.info("Started new run: %s (shop=%s, domain=%s)", run_id, shop_name, domain)
        return run_id

    def finish_run(self, status: str = STATUS_COMPLETED) -> None:
        """Mark the current run as finished with the given status."""
        if not self._conn or not self.run_id:
            return
        self._conn.execute(
            "UPDATE scrape_runs SET finished_at=?, status=? WHERE run_id=?",
            (_utcnow(), status, self.run_id),
        )
        self._conn.commit()
        logger.info("Run %s finished with status: %s", self.run_id, status)

    def find_incomplete_run(self, shop_name: str, domain: str) -> Optional[str]:
        """
        Look for the most recent incomplete run for this shop/domain.

        Returns the run_id string, or None if no resumable run exists.
        """
        assert self._conn is not None
        row = self._conn.execute(
            """
            SELECT run_id FROM scrape_runs
            WHERE shop_name=? AND domain=? AND status='started'
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (shop_name, domain),
        ).fetchone()
        if row:
            logger.info("Found resumable run: %s", row["run_id"])
            return str(row["run_id"])
        return None

    # ------------------------------------------------------------------
    # Page-level checkpoints
    # ------------------------------------------------------------------

    def mark_page_started(self, stage: str, page_number: int) -> None:
        """Record that we are beginning to scrape (stage, page_number)."""
        if not self._conn or not self.run_id:
            return
        self._conn.execute(
            """
            INSERT INTO scrape_checkpoints (run_id, stage, page_number, status, started_at)
            VALUES (?, ?, ?, 'started', ?)
            ON CONFLICT(run_id, stage, page_number) DO UPDATE
              SET status='started', started_at=excluded.started_at, error_msg=NULL
            """,
            (self.run_id, stage, page_number, _utcnow()),
        )
        self._conn.commit()

    def mark_page_completed(self, stage: str, page_number: int) -> None:
        """Record that (stage, page_number) was scraped successfully."""
        if not self._conn or not self.run_id:
            return
        self._conn.execute(
            """
            UPDATE scrape_checkpoints
            SET status='completed', finished_at=?
            WHERE run_id=? AND stage=? AND page_number=?
            """,
            (_utcnow(), self.run_id, stage, page_number),
        )
        self._conn.commit()

    def mark_page_failed(self, stage: str, page_number: int, error: str) -> None:
        """Record that (stage, page_number) failed with an error message."""
        if not self._conn or not self.run_id:
            return
        self._conn.execute(
            """
            UPDATE scrape_checkpoints
            SET status='failed', finished_at=?, error_msg=?
            WHERE run_id=? AND stage=? AND page_number=?
            """,
            (_utcnow(), error[:500], self.run_id, stage, page_number),
        )
        self._conn.commit()

    def get_last_completed_page(self, stage: str) -> int:
        """
        Return the highest completed page number for a stage (0 if none).

        Used to calculate where to resume pagination from.
        """
        if not self._conn or not self.run_id:
            return 0
        row = self._conn.execute(
            """
            SELECT MAX(page_number) AS last_page FROM scrape_checkpoints
            WHERE run_id=? AND stage=? AND status='completed'
            """,
            (self.run_id, stage),
        ).fetchone()
        if row and row["last_page"] is not None:
            return int(row["last_page"])
        return 0

    def is_page_completed(self, stage: str, page_number: int) -> bool:
        """Return True if this (stage, page_number) was already completed."""
        if not self._conn or not self.run_id:
            return False
        row = self._conn.execute(
            """
            SELECT 1 FROM scrape_checkpoints
            WHERE run_id=? AND stage=? AND page_number=? AND status='completed'
            """,
            (self.run_id, stage, page_number),
        ).fetchone()
        return row is not None


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()
