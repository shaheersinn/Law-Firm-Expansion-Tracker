"""
SQLite persistence layer.

Tables:
  signals       — all raw signals ever seen (dedup key = firm_id + title + department)
  weekly_scores — rolled-up weekly expansion scores per (firm, department)
  alerts_sent   — prevents re-alerting on same firm/dept in same ISO week
  website_hashes— content hashes for practice area pages (change detection)
"""

import sqlite3
import hashlib
import logging
from datetime import datetime, timedelta

logger = logging.getLogger("database.db")


class Database:
    def __init__(self, db_path: str = "law_firm_tracker.db"):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

    # ------------------------------------------------------------------ #
    #  Schema
    # ------------------------------------------------------------------ #

    def _create_tables(self):
        cur = self.conn.cursor()
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS signals (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_id        TEXT    NOT NULL,
                firm_name      TEXT    NOT NULL,
                signal_type    TEXT    NOT NULL,
                title          TEXT    NOT NULL,
                body           TEXT,
                url            TEXT,
                department     TEXT,
                department_score REAL  DEFAULT 0,
                matched_keywords TEXT,
                seen_at        TEXT    DEFAULT (datetime('now')),
                dedup_key      TEXT    UNIQUE
            );

            CREATE TABLE IF NOT EXISTS weekly_scores (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_id        TEXT    NOT NULL,
                firm_name      TEXT    NOT NULL,
                department     TEXT    NOT NULL,
                score          REAL    NOT NULL,
                signal_count   INTEGER DEFAULT 0,
                breakdown      TEXT,
                week_start     TEXT    NOT NULL,
                recorded_at    TEXT    DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS alerts_sent (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_id        TEXT    NOT NULL,
                department     TEXT    NOT NULL,
                score          REAL,
                week_start     TEXT    NOT NULL,
                sent_at        TEXT    DEFAULT (datetime('now')),
                UNIQUE(firm_id, department, week_start)
            );

            CREATE TABLE IF NOT EXISTS website_hashes (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_id        TEXT    NOT NULL,
                url            TEXT    NOT NULL,
                content_hash   TEXT    NOT NULL,
                updated_at     TEXT    DEFAULT (datetime('now')),
                UNIQUE(firm_id, url)
            );
        """)
        self.conn.commit()

    # ------------------------------------------------------------------ #
    #  Signals
    # ------------------------------------------------------------------ #

    def is_new_signal(self, signal: dict) -> bool:
        """Return True if this signal has never been seen before."""
        key = self._dedup_key(signal)
        cur = self.conn.execute(
            "SELECT 1 FROM signals WHERE dedup_key = ?", (key,)
        )
        return cur.fetchone() is None

    def save_signal(self, signal: dict):
        key = self._dedup_key(signal)
        kws = ",".join(signal.get("matched_keywords") or [])
        try:
            self.conn.execute(
                """INSERT OR IGNORE INTO signals
                   (firm_id, firm_name, signal_type, title, body, url,
                    department, department_score, matched_keywords, dedup_key)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    signal["firm_id"],
                    signal["firm_name"],
                    signal["signal_type"],
                    signal["title"],
                    signal.get("body", ""),
                    signal.get("url", ""),
                    signal.get("department", ""),
                    signal.get("department_score", 0),
                    kws,
                    key,
                ),
            )
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"DB save_signal error: {e}")

    def get_signals_this_week(self) -> list[dict]:
        """Return all signals recorded in the current ISO week."""
        week_start = self._week_start()
        cur = self.conn.execute(
            "SELECT * FROM signals WHERE seen_at >= ?", (week_start,)
        )
        return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------ #
    #  Weekly scores
    # ------------------------------------------------------------------ #

    def save_weekly_score(
        self,
        firm_id: str,
        firm_name: str,
        department: str,
        score: float,
        signal_count: int,
        breakdown: dict,
    ):
        import json
        week_start = self._week_start()
        try:
            self.conn.execute(
                """INSERT OR REPLACE INTO weekly_scores
                   (firm_id, firm_name, department, score, signal_count, breakdown, week_start)
                   VALUES (?,?,?,?,?,?,?)""",
                (firm_id, firm_name, department, score, signal_count, json.dumps(breakdown), week_start),
            )
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"DB save_weekly_score error: {e}")

    def get_weekly_baseline(self, firm_id: str, department: str, weeks: int = 4) -> float:
        """
        Average weekly expansion score over the past N weeks (excluding current week).
        Returns 0.0 if no history.
        """
        current_week = self._week_start()
        cutoff = (datetime.utcnow() - timedelta(weeks=weeks)).strftime("%Y-%m-%d")
        cur = self.conn.execute(
            """SELECT AVG(score) as avg_score
               FROM weekly_scores
               WHERE firm_id = ? AND department = ?
                 AND week_start >= ? AND week_start < ?""",
            (firm_id, department, cutoff, current_week),
        )
        row = cur.fetchone()
        return float(row["avg_score"]) if row and row["avg_score"] else 0.0

    # ------------------------------------------------------------------ #
    #  Alerts dedup
    # ------------------------------------------------------------------ #

    def was_alert_sent(self, firm_id: str, department: str) -> bool:
        week_start = self._week_start()
        cur = self.conn.execute(
            "SELECT 1 FROM alerts_sent WHERE firm_id=? AND department=? AND week_start=?",
            (firm_id, department, week_start),
        )
        return cur.fetchone() is not None

    def mark_alert_sent(self, firm_id: str, department: str, score: float):
        week_start = self._week_start()
        try:
            self.conn.execute(
                """INSERT OR REPLACE INTO alerts_sent (firm_id, department, score, week_start)
                   VALUES (?,?,?,?)""",
                (firm_id, department, score, week_start),
            )
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"DB mark_alert_sent error: {e}")

    # ------------------------------------------------------------------ #
    #  Website hashes
    # ------------------------------------------------------------------ #

    def save_website_hash(self, firm_id: str, url: str, content: str):
        h = hashlib.sha256(content.encode()).hexdigest()
        try:
            self.conn.execute(
                """INSERT OR REPLACE INTO website_hashes (firm_id, url, content_hash)
                   VALUES (?,?,?)""",
                (firm_id, url, h),
            )
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"DB save_website_hash error: {e}")

    def get_last_website_hash(self, firm_id: str, url: str) -> str:
        cur = self.conn.execute(
            "SELECT content_hash FROM website_hashes WHERE firm_id=? AND url=?",
            (firm_id, url),
        )
        row = cur.fetchone()
        return row["content_hash"] if row else ""

    # ------------------------------------------------------------------ #
    #  Helpers
    # ------------------------------------------------------------------ #

    def _dedup_key(self, signal: dict) -> str:
        raw = f"{signal['firm_id']}|{signal['signal_type']}|{signal['title']}|{signal.get('department','')}"
        return hashlib.md5(raw.encode()).hexdigest()

    def _week_start(self) -> str:
        """ISO Monday of current week as YYYY-MM-DD."""
        today = datetime.utcnow().date()
        monday = today - timedelta(days=today.weekday())
        return monday.isoformat()

    def close(self):
        self.conn.close()
