"""
Database layer — SQLite-backed persistence for signals, scores, alerts.
"""

import sqlite3
import json
import hashlib
import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, db_path: str = "law_firm_tracker.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self):
        cur = self.conn.cursor()
        cur.executescript("""
        CREATE TABLE IF NOT EXISTS signals (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            firm_id         TEXT    NOT NULL,
            firm_name       TEXT    NOT NULL,
            signal_type     TEXT    NOT NULL,
            title           TEXT    NOT NULL,
            body            TEXT,
            url             TEXT,
            department      TEXT,
            dept_score      REAL    DEFAULT 0,
            matched_keywords TEXT,
            content_hash    TEXT    UNIQUE,
            collected_at    TEXT    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS weekly_scores (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            firm_id         TEXT    NOT NULL,
            firm_name       TEXT    NOT NULL,
            department      TEXT    NOT NULL,
            score           REAL    NOT NULL,
            signal_count    INTEGER NOT NULL,
            breakdown       TEXT,
            week_start      TEXT    NOT NULL,
            created_at      TEXT    NOT NULL,
            UNIQUE(firm_id, department, week_start)
        );

        CREATE TABLE IF NOT EXISTS alerts_sent (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            firm_id         TEXT    NOT NULL,
            department      TEXT    NOT NULL,
            score           REAL    NOT NULL,
            sent_at         TEXT    NOT NULL,
            week_start      TEXT    NOT NULL,
            UNIQUE(firm_id, department, week_start)
        );

        CREATE TABLE IF NOT EXISTS website_hashes (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            firm_id         TEXT    NOT NULL,
            url             TEXT    NOT NULL,
            content_hash    TEXT    NOT NULL,
            last_seen       TEXT    NOT NULL,
            UNIQUE(firm_id, url)
        );

        CREATE TABLE IF NOT EXISTS signal_feedback (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id       INTEGER,
            firm_id         TEXT    NOT NULL,
            department      TEXT    NOT NULL,
            was_correct     INTEGER NOT NULL,
            notes           TEXT,
            created_at      TEXT    NOT NULL
        );
        """)
        self.conn.commit()

    # ── Signals ──────────────────────────────────────────────────────────

    def _hash(self, firm_id: str, title: str, url: str) -> str:
        raw = f"{firm_id}|{title[:120]}|{url}"
        return hashlib.sha256(raw.encode()).hexdigest()[:32]

    def is_new_signal(self, signal: dict) -> bool:
        h = self._hash(signal["firm_id"], signal["title"], signal.get("url", ""))
        cur = self.conn.execute("SELECT 1 FROM signals WHERE content_hash=?", (h,))
        return cur.fetchone() is None

    def save_signal(self, signal: dict):
        h = self._hash(signal["firm_id"], signal["title"], signal.get("url", ""))
        try:
            self.conn.execute("""
                INSERT OR IGNORE INTO signals
                    (firm_id, firm_name, signal_type, title, body, url,
                     department, dept_score, matched_keywords, content_hash, collected_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (
                signal["firm_id"], signal["firm_name"],
                signal["signal_type"], signal["title"],
                signal.get("body", ""), signal.get("url", ""),
                signal.get("department", ""), signal.get("department_score", 0),
                json.dumps(signal.get("matched_keywords", [])),
                h,
                datetime.now(timezone.utc).isoformat(),
            ))
            self.conn.commit()
        except sqlite3.IntegrityError:
            pass

    def get_signals_this_week(self) -> list[dict]:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        cur = self.conn.execute(
            "SELECT * FROM signals WHERE collected_at >= ? ORDER BY collected_at DESC",
            (cutoff,)
        )
        return [dict(r) for r in cur.fetchall()]

    def get_signals_last_n_weeks(self, n: int = 4) -> list[dict]:
        cutoff = (datetime.now(timezone.utc) - timedelta(weeks=n)).isoformat()
        cur = self.conn.execute(
            "SELECT * FROM signals WHERE collected_at >= ? ORDER BY collected_at DESC",
            (cutoff,)
        )
        return [dict(r) for r in cur.fetchall()]

    # ── Website hashes ────────────────────────────────────────────────

    def get_website_hash(self, firm_id: str, url: str) -> str | None:
        cur = self.conn.execute(
            "SELECT content_hash FROM website_hashes WHERE firm_id=? AND url=?",
            (firm_id, url)
        )
        row = cur.fetchone()
        return row["content_hash"] if row else None

    def save_website_hash(self, firm_id: str, url: str, content: str):
        h = hashlib.md5(content.encode()).hexdigest()
        self.conn.execute("""
            INSERT INTO website_hashes (firm_id, url, content_hash, last_seen)
            VALUES (?,?,?,?)
            ON CONFLICT(firm_id, url) DO UPDATE SET
                content_hash=excluded.content_hash,
                last_seen=excluded.last_seen
        """, (firm_id, url, h, datetime.now(timezone.utc).isoformat()))
        self.conn.commit()

    # ── Weekly scores ─────────────────────────────────────────────────

    def save_weekly_score(self, firm_id, firm_name, department, score, signal_count, breakdown):
        week_start = _week_start()
        self.conn.execute("""
            INSERT INTO weekly_scores
                (firm_id, firm_name, department, score, signal_count, breakdown, week_start, created_at)
            VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(firm_id, department, week_start) DO UPDATE SET
                score=excluded.score, signal_count=excluded.signal_count,
                breakdown=excluded.breakdown, created_at=excluded.created_at
        """, (
            firm_id, firm_name, department, score, signal_count,
            json.dumps(breakdown), week_start,
            datetime.now(timezone.utc).isoformat(),
        ))
        self.conn.commit()

    def get_historical_scores(self, firm_id: str, department: str, weeks: int = 4) -> list[float]:
        cutoff = (datetime.now(timezone.utc) - timedelta(weeks=weeks)).isoformat()
        cur = self.conn.execute("""
            SELECT score FROM weekly_scores
            WHERE firm_id=? AND department=? AND week_start >= ?
            ORDER BY week_start ASC
        """, (firm_id, department, cutoff))
        return [r["score"] for r in cur.fetchall()]

    # ── Alerts ────────────────────────────────────────────────────────

    def was_alert_sent(self, firm_id: str, department: str) -> bool:
        week_start = _week_start()
        cur = self.conn.execute(
            "SELECT 1 FROM alerts_sent WHERE firm_id=? AND department=? AND week_start=?",
            (firm_id, department, week_start)
        )
        return cur.fetchone() is not None

    def mark_alert_sent(self, firm_id: str, department: str, score: float):
        week_start = _week_start()
        self.conn.execute("""
            INSERT OR IGNORE INTO alerts_sent (firm_id, department, score, sent_at, week_start)
            VALUES (?,?,?,?,?)
        """, (firm_id, department, score, datetime.now(timezone.utc).isoformat(), week_start))
        self.conn.commit()

    # ── Feedback (for evolution) ──────────────────────────────────────

    def save_feedback(self, firm_id: str, department: str, was_correct: bool, notes: str = ""):
        self.conn.execute("""
            INSERT INTO signal_feedback (firm_id, department, was_correct, notes, created_at)
            VALUES (?,?,?,?,?)
        """, (firm_id, department, int(was_correct), notes, datetime.now(timezone.utc).isoformat()))
        self.conn.commit()

    def get_accuracy_by_department(self) -> dict:
        cur = self.conn.execute("""
            SELECT department,
                   SUM(was_correct) as correct,
                   COUNT(*) as total
            FROM signal_feedback GROUP BY department
        """)
        return {r["department"]: r["correct"] / r["total"] for r in cur.fetchall() if r["total"] > 0}

    def close(self):
        self.conn.close()


def _week_start() -> str:
    today = datetime.now(timezone.utc).date()
    monday = today - timedelta(days=today.weekday())
    return monday.isoformat()
