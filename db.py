"""
SQLite database layer.

Tables:
  signals       — every scraped signal, deduplicated by hash
  weekly_scores — aggregated scores per (firm, department, week)
  website_hashes— content snapshots for change detection
  alerts_sent   — prevents duplicate Telegram alerts within same week
"""

import sqlite3
import json
import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger("database")

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS signals (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_hash      TEXT    NOT NULL,
    firm_id          TEXT    NOT NULL,
    firm_name        TEXT    NOT NULL,
    signal_type      TEXT    NOT NULL,
    title            TEXT    NOT NULL,
    body             TEXT,
    url              TEXT,
    department       TEXT,
    department_score REAL    DEFAULT 0,
    matched_keywords TEXT,
    published_date   TEXT,
    collected_at     TEXT    NOT NULL DEFAULT (datetime('now')),
    week_label       TEXT    NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_signals_hash ON signals (signal_hash);
CREATE INDEX        IF NOT EXISTS ix_signals_firm  ON signals (firm_id, department);
CREATE INDEX        IF NOT EXISTS ix_signals_week  ON signals (week_label);

CREATE TABLE IF NOT EXISTS weekly_scores (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    firm_id      TEXT  NOT NULL,
    firm_name    TEXT  NOT NULL,
    department   TEXT  NOT NULL,
    week_label   TEXT  NOT NULL,
    score        REAL  DEFAULT 0,
    signal_count INTEGER DEFAULT 0,
    breakdown    TEXT,
    updated_at   TEXT  NOT NULL DEFAULT (datetime('now')),
    UNIQUE (firm_id, department, week_label)
);

CREATE TABLE IF NOT EXISTS website_hashes (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    firm_id    TEXT NOT NULL,
    url        TEXT NOT NULL,
    hash       TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (firm_id, url)
);

CREATE TABLE IF NOT EXISTS alerts_sent (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    firm_id    TEXT NOT NULL,
    department TEXT NOT NULL,
    week_label TEXT NOT NULL,
    score      REAL,
    sent_at    TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (firm_id, department, week_label)
);
"""


def _week_label(dt: datetime = None) -> str:
    if dt is None:
        dt = datetime.now(timezone.utc)
    # ISO week: YYYY-Www
    return dt.strftime("%Y-W%W")


class Database:
    def __init__(self, path: str = "law_firm_tracker.db"):
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()
        logger.info(f"Database ready: {path}")

    def _init_schema(self):
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    # ── Signal operations ──────────────────────────────────────────────

    def is_new_signal(self, signal: dict) -> bool:
        row = self._conn.execute(
            "SELECT id FROM signals WHERE signal_hash = ?",
            (signal["signal_hash"],)
        ).fetchone()
        return row is None

    def save_signal(self, signal: dict):
        try:
            self._conn.execute("""
                INSERT OR IGNORE INTO signals
                  (signal_hash, firm_id, firm_name, signal_type, title, body, url,
                   department, department_score, matched_keywords, published_date, week_label)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                signal["signal_hash"],
                signal["firm_id"],
                signal["firm_name"],
                signal["signal_type"],
                signal["title"],
                signal.get("body", ""),
                signal.get("url", ""),
                signal.get("department", ""),
                signal.get("department_score", 0),
                json.dumps(signal.get("matched_keywords", [])),
                signal.get("published_date", ""),
                _week_label(),
            ))
            self._conn.commit()
        except sqlite3.Error as e:
            logger.error(f"save_signal error: {e}")

    def get_signals_this_week(self) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM signals WHERE week_label = ? AND signal_type != 'website_snapshot'",
            (_week_label(),)
        ).fetchall()
        result = []
        for row in rows:
            d = dict(row)
            d["matched_keywords"] = json.loads(d.get("matched_keywords") or "[]")
            result.append(d)
        return result

    def get_signals_last_n_weeks(self, n: int = 4) -> list[dict]:
        cutoff = (datetime.now(timezone.utc) - timedelta(weeks=n)).isoformat()
        rows = self._conn.execute(
            "SELECT * FROM signals WHERE collected_at >= ? AND signal_type != 'website_snapshot'",
            (cutoff,)
        ).fetchall()
        result = []
        for row in rows:
            d = dict(row)
            d["matched_keywords"] = json.loads(d.get("matched_keywords") or "[]")
            result.append(d)
        return result

    # ── Weekly scores ──────────────────────────────────────────────────

    def save_weekly_score(self, firm_id, firm_name, department, score, signal_count, breakdown):
        try:
            self._conn.execute("""
                INSERT OR REPLACE INTO weekly_scores
                  (firm_id, firm_name, department, week_label, score, signal_count, breakdown)
                VALUES (?,?,?,?,?,?,?)
            """, (
                firm_id, firm_name, department, _week_label(),
                score, signal_count, json.dumps(breakdown),
            ))
            self._conn.commit()
        except sqlite3.Error as e:
            logger.error(f"save_weekly_score error: {e}")

    def get_weekly_baseline(self, firm_id: str, department: str, weeks: int = 4) -> float:
        rows = self._conn.execute("""
            SELECT score FROM weekly_scores
            WHERE firm_id=? AND department=? AND week_label != ?
            ORDER BY week_label DESC LIMIT ?
        """, (firm_id, department, _week_label(), weeks)).fetchall()
        if not rows:
            return 0.0
        return sum(r["score"] for r in rows) / len(rows)

    def get_all_weekly_scores(self) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM weekly_scores ORDER BY week_label DESC, score DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    # ── Website hashes ─────────────────────────────────────────────────

    def save_website_hash(self, firm_id: str, url: str, hash_val: str):
        try:
            self._conn.execute("""
                INSERT OR REPLACE INTO website_hashes (firm_id, url, hash)
                VALUES (?,?,?)
            """, (firm_id, url, hash_val))
            self._conn.commit()
        except sqlite3.Error as e:
            logger.error(f"save_website_hash error: {e}")

    def get_last_website_hash(self, firm_id: str, url: str) -> str | None:
        row = self._conn.execute(
            "SELECT hash FROM website_hashes WHERE firm_id=? AND url=?",
            (firm_id, url)
        ).fetchone()
        return row["hash"] if row else None

    # ── Alerts ─────────────────────────────────────────────────────────

    def was_alert_sent(self, firm_id: str, department: str) -> bool:
        row = self._conn.execute("""
            SELECT id FROM alerts_sent
            WHERE firm_id=? AND department=? AND week_label=?
        """, (firm_id, department, _week_label())).fetchone()
        return row is not None

    def mark_alert_sent(self, firm_id: str, department: str, score: float):
        try:
            self._conn.execute("""
                INSERT OR IGNORE INTO alerts_sent (firm_id, department, week_label, score)
                VALUES (?,?,?,?)
            """, (firm_id, department, _week_label(), score))
            self._conn.commit()
        except sqlite3.Error as e:
            logger.error(f"mark_alert_sent error: {e}")

    def close(self):
        self._conn.close()
