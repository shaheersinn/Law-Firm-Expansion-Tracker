"""
SQLite database layer — v5
==========================
Schema-versioned with automatic migration on startup.
Any column added to SCHEMA is automatically ALTERed into existing DBs,
so GitHub Actions cached databases are never stale.

Tables:
  signals        — every scraped signal
  website_hashes — SHA-256 snapshots for change detection
  weekly_scores  — aggregated expansion scores per (firm, dept, week)
  sent_alerts    — dedup gate so each alert fires once per week
  run_stats      — per-run telemetry for trend analysis
"""

import hashlib
import json
import logging
import sqlite3
import os
from datetime import datetime, timezone, timedelta

logger = logging.getLogger("database")

# Bump this when you add/change any column — also update tracker.yml cache key
SCHEMA_VERSION = 5
LOOKBACK_DAYS  = int(os.getenv("SIGNAL_LOOKBACK_DAYS", "21"))

SCHEMA = """
CREATE TABLE IF NOT EXISTS signals (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    firm_id          TEXT    NOT NULL,
    firm_name        TEXT    NOT NULL,
    signal_type      TEXT    NOT NULL,
    title            TEXT    NOT NULL,
    body             TEXT,
    url              TEXT,
    department       TEXT,
    department_score REAL    DEFAULT 0,
    confidence       REAL    DEFAULT 0,
    matched_keywords TEXT,
    source           TEXT,
    published_at     TEXT,
    scraped_at       TEXT,
    seen_at          TEXT,
    dedup_key        TEXT,
    UNIQUE(firm_id, signal_type, url)
);

CREATE TABLE IF NOT EXISTS website_hashes (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    firm_id      TEXT NOT NULL,
    url          TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    checked_at   TEXT NOT NULL,
    UNIQUE(firm_id, url)
);

CREATE TABLE IF NOT EXISTS weekly_scores (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    firm_id      TEXT NOT NULL,
    firm_name    TEXT NOT NULL,
    department   TEXT NOT NULL,
    score        REAL NOT NULL,
    signal_count INTEGER NOT NULL,
    breakdown    TEXT,
    week_start   TEXT NOT NULL,
    UNIQUE(firm_id, department, week_start)
);

CREATE TABLE IF NOT EXISTS sent_alerts (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    firm_id    TEXT NOT NULL,
    department TEXT NOT NULL,
    score      REAL NOT NULL,
    sent_at    TEXT NOT NULL,
    week_start TEXT NOT NULL,
    UNIQUE(firm_id, department, week_start)
);

CREATE TABLE IF NOT EXISTS run_stats (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at          TEXT NOT NULL,
    total_signals   INTEGER NOT NULL DEFAULT 0,
    new_signals     INTEGER NOT NULL DEFAULT 0,
    alerts_fired    INTEGER NOT NULL DEFAULT 0,
    website_changes INTEGER NOT NULL DEFAULT 0,
    error_count     INTEGER NOT NULL DEFAULT 0,
    duration_secs   REAL    NOT NULL DEFAULT 0,
    schema_version  INTEGER NOT NULL DEFAULT 0
);
"""

# Missing columns that must be added to legacy (cached) DBs
# Format: { table_name: { col_name: "TYPE DEFAULT ..." } }
MIGRATION_COLUMNS: dict[str, dict[str, str]] = {
    "signals": {
        "department_score": "REAL DEFAULT 0",
        "confidence":       "REAL DEFAULT 0",
        "matched_keywords": "TEXT DEFAULT ''",
        "source":           "TEXT DEFAULT ''",
        "published_at":     "TEXT DEFAULT ''",
        "scraped_at":       "TEXT DEFAULT NULL",
        "seen_at":          "TEXT DEFAULT NULL",
        "dedup_key":        "TEXT DEFAULT NULL",
    },
    "website_hashes": {
        "checked_at": "TEXT NOT NULL DEFAULT ''",
    },
}


def _week_start() -> str:
    today = datetime.now(timezone.utc).date()
    return (today - timedelta(days=today.weekday())).isoformat()


def _title_similarity(a: str, b: str) -> float:
    """Jaccard similarity on word sets — fast, no extra libs."""
    wa = set((a or "").split())
    wb = set((b or "").split())
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / len(wa | wb)


def _compute_confidence(signal: dict) -> float:
    """
    Composite confidence 0–1:
      40 % — signal type tier weight (3.5 = top)
      40 % — keyword match density (capped at 5 keywords = 1.0)
      20 % — has a direct URL
    """
    from analysis.signals import SIGNAL_WEIGHTS
    type_score = min(SIGNAL_WEIGHTS.get(signal.get("signal_type", ""), 1.0) / 3.5, 1.0)
    kw_score   = min(len(signal.get("matched_keywords") or []) / 5.0, 1.0)
    url_score  = 1.0 if signal.get("url") else 0.0
    return round(0.4 * type_score + 0.4 * kw_score + 0.2 * url_score, 3)


class Database:
    def __init__(self, path: str = "law_firm_tracker.db"):
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        self._migrate()  # add any missing columns to legacy cached DBs
        logger.info(f"DB ready (schema v{SCHEMA_VERSION}): {path}")

    # ------------------------------------------------------------------ #
    #  Schema migration
    # ------------------------------------------------------------------ #

    def _migrate(self):
        """
        Add any missing columns to legacy cached DBs.
        Safe to run every startup — no-op when schema is already current.
        """
        for table, columns in MIGRATION_COLUMNS.items():
            existing = self._existing_columns(table)
            for col_name, col_def in columns.items():
                if col_name not in existing:
                    try:
                        self.conn.execute(
                            f"ALTER TABLE {table} ADD COLUMN {col_name} {col_def}"
                        )
                        self.conn.commit()
                        logger.info(f"Migration: added column {table}.{col_name}")
                    except sqlite3.OperationalError as exc:
                        logger.warning(f"Migration skipped {table}.{col_name}: {exc}")

    def _existing_columns(self, table: str) -> set[str]:
        rows = self.conn.execute(f"PRAGMA table_info({table})").fetchall()
        return {r["name"] for r in rows}

    # ------------------------------------------------------------------ #
    #  Signals
    # ------------------------------------------------------------------ #

    def is_new_signal(self, signal: dict) -> bool:
        """Primary dedup: exact (firm_id, signal_type, url) match."""
        row = self.conn.execute(
            "SELECT id FROM signals WHERE firm_id=? AND signal_type=? AND url=?",
            (signal["firm_id"], signal["signal_type"], signal.get("url", "")),
        ).fetchone()
        return row is None

    def is_duplicate_title(self, signal: dict, lookback_hours: int = 72) -> bool:
        """
        Soft dedup: returns True if an ~identical title was already stored for
        this firm+signal_type within the last N hours.
        Catches the same article appearing under a different URL (e.g. Google
        cache vs direct link, mobile vs desktop).
        """
        cutoff = (
            datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        ).isoformat()
        title_norm = signal.get("title", "")[:80].lower()
        rows = self.conn.execute(
            """SELECT title FROM signals
               WHERE firm_id=? AND signal_type=?
               AND COALESCE(scraped_at, seen_at, '') >= ?""",
            (signal["firm_id"], signal["signal_type"], cutoff),
        ).fetchall()
        for r in rows:
            stored = (r["title"] or "")[:80].lower()
            if _title_similarity(title_norm, stored) >= 0.82:
                return True
        return False

    def save_signal(self, signal: dict):
        kw         = json.dumps(signal.get("matched_keywords", []))
        confidence = _compute_confidence(signal)
        try:
            self.conn.execute(
                """INSERT OR IGNORE INTO signals
                   (firm_id, firm_name, signal_type, title, body, url,
                    department, department_score, confidence, matched_keywords,
                    source, published_at, scraped_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    signal["firm_id"],
                    signal["firm_name"],
                    signal["signal_type"],
                    signal["title"],
                    signal.get("body", ""),
                    signal.get("url", ""),
                    signal.get("department", ""),
                    signal.get("department_score", 0.0),
                    confidence,
                    kw,
                    signal.get("source", ""),
                    signal.get("published_at", ""),
                    signal.get("scraped_at",
                               datetime.now(timezone.utc).isoformat()),
                ),
            )
            self.conn.commit()
        except Exception as exc:
            logger.error(f"save_signal: {exc}")

    def get_signals_this_week(self) -> list[dict]:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
        ).isoformat()
        rows = self.conn.execute(
            """SELECT * FROM signals
               WHERE COALESCE(scraped_at, seen_at, '') >= ?
               ORDER BY COALESCE(scraped_at, seen_at, '') DESC""",
            (cutoff,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_signals_by_dept_recent(self, department: str, days: int = 7) -> list[dict]:
        """All signals for a department across all firms in the last N days."""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        rows = self.conn.execute(
            """SELECT * FROM signals
               WHERE department=?
               AND COALESCE(scraped_at, seen_at, '') >= ?
               ORDER BY COALESCE(scraped_at, seen_at, '') DESC""",
            (department, cutoff),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_signal_velocity(self, firm_id: str, department: str) -> tuple[int, int]:
        """
        Returns (this_week_count, last_week_count) for a (firm, dept) pair.
        Used to render velocity arrows in the Telegram digest.
        """
        now = datetime.now(timezone.utc)
        this_start = (now - timedelta(days=7)).isoformat()
        prev_start = (now - timedelta(days=14)).isoformat()

        this = self.conn.execute(
            """SELECT COUNT(*) c FROM signals
               WHERE firm_id=? AND department=?
               AND COALESCE(scraped_at, seen_at, '') >= ?""",
            (firm_id, department, this_start),
        ).fetchone()["c"]

        prev = self.conn.execute(
            """SELECT COUNT(*) c FROM signals
               WHERE firm_id=? AND department=?
               AND COALESCE(scraped_at, seen_at, '') >= ?
               AND COALESCE(scraped_at, seen_at, '') < ?""",
            (firm_id, department, prev_start, this_start),
        ).fetchone()["c"]

        return (this, prev)

    # ------------------------------------------------------------------ #
    #  Website hashes
    # ------------------------------------------------------------------ #

    def save_website_hash(self, firm_id: str, url: str, content: str):
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

    def save_weekly_score(self, firm_id, firm_name, department,
                          score, signal_count, breakdown):
        ws = _week_start()
        self.conn.execute(
            """INSERT INTO weekly_scores
               (firm_id, firm_name, department, score, signal_count, breakdown, week_start)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(firm_id, department, week_start) DO UPDATE SET
               score=excluded.score,
               signal_count=excluded.signal_count,
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
        return self.conn.execute(
            "SELECT id FROM sent_alerts WHERE firm_id=? AND department=? AND week_start=?",
            (firm_id, department, ws),
        ).fetchone() is not None

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

    # ------------------------------------------------------------------ #
    #  Run stats
    # ------------------------------------------------------------------ #

    def save_run_stats(self, total_signals: int, new_signals: int,
                       alerts_fired: int, website_changes: int,
                       error_count: int, duration_secs: float):
        self.conn.execute(
            """INSERT INTO run_stats
               (run_at, total_signals, new_signals, alerts_fired,
                website_changes, error_count, duration_secs, schema_version)
               VALUES (?,?,?,?,?,?,?,?)""",
            (datetime.now(timezone.utc).isoformat(),
             total_signals, new_signals, alerts_fired,
             website_changes, error_count,
             round(duration_secs, 1), SCHEMA_VERSION),
        )
        self.conn.commit()

    def get_run_trends(self, last_n: int = 7) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM run_stats ORDER BY run_at DESC LIMIT ?",
            (last_n,),
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------ #

    def close(self):
        self.conn.close()
