"""
SQLite database layer.
Tables: signals, website_hashes, weekly_scores, sent_alerts
"""

import json
import logging
import sqlite3
from datetime import datetime, timezone, timedelta
import os

logger = logging.getLogger("database")

LOOKBACK_DAYS = int(os.getenv("SIGNAL_LOOKBACK_DAYS", "21"))

SCHEMA = """
CREATE TABLE IF NOT EXISTS signals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    firm_id         TEXT    NOT NULL,
    firm_name       TEXT    NOT NULL,
    signal_type     TEXT    NOT NULL,
    title           TEXT    NOT NULL,
    body            TEXT,
    url             TEXT,
    department      TEXT,
    department_score REAL,
    matched_keywords TEXT,
    source          TEXT,
    published_at    TEXT,
    scraped_at      TEXT    NOT NULL,
    UNIQUE(firm_id, signal_type, url)
);

CREATE TABLE IF NOT EXISTS website_hashes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    firm_id     TEXT NOT NULL,
    url         TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    checked_at  TEXT NOT NULL,
    UNIQUE(firm_id, url)
);

CREATE TABLE IF NOT EXISTS weekly_scores (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    firm_id     TEXT NOT NULL,
    firm_name   TEXT NOT NULL,
    department  TEXT NOT NULL,
    score       REAL NOT NULL,
    signal_count INTEGER NOT NULL,
    breakdown   TEXT,
    week_start  TEXT NOT NULL,
    UNIQUE(firm_id, department, week_start)
);

CREATE TABLE IF NOT EXISTS sent_alerts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    firm_id     TEXT NOT NULL,
    department  TEXT NOT NULL,
    score       REAL NOT NULL,
    sent_at     TEXT NOT NULL,
    week_start  TEXT NOT NULL,
    UNIQUE(firm_id, department, week_start)
);
"""


def _week_start() -> str:
    today = datetime.now(timezone.utc).date()
    return (today - timedelta(days=today.weekday())).isoformat()


class Database:
    def __init__(self, path: str = "law_firm_tracker.db"):
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        logger.info(f"DB ready: {path}")

    # ------------------------------------------------------------------ #
    #  Signals
    # ------------------------------------------------------------------ #

    def is_new_signal(self, signal: dict) -> bool:
        row = self.conn.execute(
            "SELECT id FROM signals WHERE firm_id=? AND signal_type=? AND url=?",
            (signal["firm_id"], signal["signal_type"], signal["url"]),
        ).fetchone()
        return row is None

    def save_signal(self, signal: dict):
        kw = json.dumps(signal.get("matched_keywords", []))
        try:
            self.conn.execute(
                """INSERT OR IGNORE INTO signals
                   (firm_id, firm_name, signal_type, title, body, url,
                    department, department_score, matched_keywords,
                    source, published_at, scraped_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    signal["firm_id"], signal["firm_name"],
                    signal["signal_type"], signal["title"],
                    signal.get("body", ""), signal.get("url", ""),
                    signal.get("department", ""), signal.get("department_score", 0),
                    kw, signal.get("source", ""), signal.get("published_at", ""),
                    signal.get("scraped_at", datetime.now(timezone.utc).isoformat()),
                ),
            )
            self.conn.commit()
        except Exception as e:
            logger.error(f"save_signal: {e}")

    def get_signals_this_week(self) -> list[dict]:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).isoformat()
        rows = self.conn.execute(
            "SELECT * FROM signals WHERE scraped_at >= ? ORDER BY scraped_at DESC",
            (cutoff,),
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------ #
    #  Website hashes
    # ------------------------------------------------------------------ #

    def save_website_hash(self, firm_id: str, url: str, content: str):
        import hashlib
        h = hashlib.sha256(content.encode()).hexdigest()
        self.conn.execute(
            """INSERT INTO website_hashes (firm_id, url, content_hash, checked_at)
               VALUES (?,?,?,?)
               ON CONFLICT(firm_id, url) DO UPDATE SET
               content_hash=excluded.content_hash,
               checked_at=excluded.checked_at""",
            (firm_id, url, h, datetime.now(timezone.utc).isoformat()),
        )
        self.conn.commit()

    def get_website_hash(self, firm_id: str, url: str) -> str | None:
        row = self.conn.execute(
            "SELECT content_hash FROM website_hashes WHERE firm_id=? AND url=?",
            (firm_id, url),
        ).fetchone()
        return row["content_hash"] if row else None

    # ------------------------------------------------------------------ #
    #  Weekly scores
    # ------------------------------------------------------------------ #

    def save_weekly_score(self, firm_id, firm_name, department, score,
                          signal_count, breakdown):
        ws = _week_start()
        self.conn.execute(
            """INSERT INTO weekly_scores
               (firm_id, firm_name, department, score, signal_count, breakdown, week_start)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(firm_id, department, week_start) DO UPDATE SET
               score=excluded.score, signal_count=excluded.signal_count,
               breakdown=excluded.breakdown""",
            (firm_id, firm_name, department, score, signal_count,
             json.dumps(breakdown), ws),
        )
        self.conn.commit()

    def get_baseline(self, firm_id: str, department: str, weeks: int = 4) -> list[float]:
        rows = self.conn.execute(
            """SELECT score FROM weekly_scores
               WHERE firm_id=? AND department=?
               ORDER BY week_start DESC LIMIT ?""",
            (firm_id, department, weeks),
        ).fetchall()
        return [r["score"] for r in rows]

    # ------------------------------------------------------------------ #
    #  Sent alerts
    # ------------------------------------------------------------------ #

    def was_alert_sent(self, firm_id: str, department: str) -> bool:
        ws = _week_start()
        row = self.conn.execute(
            "SELECT id FROM sent_alerts WHERE firm_id=? AND department=? AND week_start=?",
            (firm_id, department, ws),
        ).fetchone()
        return row is not None

    def mark_alert_sent(self, firm_id: str, department: str, score: float):
        ws = _week_start()
        self.conn.execute(
            """INSERT OR IGNORE INTO sent_alerts
               (firm_id, department, score, sent_at, week_start)
               VALUES (?,?,?,?,?)""",
            (firm_id, department, score,
             datetime.now(timezone.utc).isoformat(), ws),
        )
        self.conn.commit()

    def close(self):
        self.conn.close()
