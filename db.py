"""
database/db.py  (v2 — adds practice_area + dedup hash)
────────────────────────────────────────────────────────
"""

import sqlite3
import json
import hashlib
from datetime import datetime, date

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config_calgary import DB_PATH


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_conn()
    cur  = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS signals (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        firm_id        TEXT    NOT NULL,
        signal_type    TEXT    NOT NULL,
        practice_area  TEXT,
        weight         REAL    NOT NULL DEFAULT 1.0,
        title          TEXT,
        description    TEXT,
        source_url     TEXT,
        raw_data       TEXT,
        dedup_hash     TEXT    UNIQUE,
        detected_at    TEXT    NOT NULL DEFAULT (datetime('now')),
        alerted        INTEGER NOT NULL DEFAULT 0
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS canlii_appearances (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        firm_id        TEXT    NOT NULL,
        case_id        TEXT    NOT NULL UNIQUE,
        case_title     TEXT,
        citation       TEXT,
        decision_date  TEXT,
        court          TEXT,
        counsel_raw    TEXT,
        file_type      TEXT,
        practice_area  TEXT,
        fetched_at     TEXT    NOT NULL DEFAULT (datetime('now'))
    )""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ca_firm_date ON canlii_appearances(firm_id, decision_date)")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS sedar_filings (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        filing_id     TEXT    NOT NULL UNIQUE,
        issuer        TEXT,
        doc_type      TEXT,
        filed_date    TEXT,
        counsel_firms TEXT,
        deal_value    REAL,
        source_url    TEXT,
        fetched_at    TEXT    NOT NULL DEFAULT (datetime('now'))
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS linkedin_roster (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        firm_id       TEXT    NOT NULL,
        linkedin_url  TEXT    NOT NULL UNIQUE,
        full_name     TEXT,
        title         TEXT,
        practice_area TEXT,
        start_date    TEXT,
        seniority     TEXT,
        last_checked  TEXT,
        is_active     INTEGER NOT NULL DEFAULT 1,
        left_date     TEXT,
        new_employer  TEXT
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS lsa_students (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        firm_id        TEXT    NOT NULL,
        full_name      TEXT    NOT NULL,
        lsa_id         TEXT,
        articling_year INTEGER,
        status         TEXT,
        as_of_date     TEXT,
        new_firm_id    TEXT,
        notes          TEXT
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS spillage_edges (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        biglaw_id      TEXT    NOT NULL,
        boutique_id    TEXT    NOT NULL,
        co_appearances INTEGER NOT NULL DEFAULT 0,
        last_seen      TEXT,
        source         TEXT,
        UNIQUE(biglaw_id, boutique_id, source)
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS outreach_log (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        firm_id      TEXT    NOT NULL,
        trigger_type TEXT    NOT NULL,
        subject      TEXT,
        body         TEXT,
        sent_at      TEXT,
        status       TEXT    DEFAULT 'draft'
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS firm_appearance_stats (
        firm_id      TEXT    NOT NULL,
        week_start   TEXT    NOT NULL,
        appearances  INTEGER NOT NULL DEFAULT 0,
        zscore       REAL,
        PRIMARY KEY (firm_id, week_start)
    )""")

    conn.commit()
    conn.close()
    print("[DB] Initialised:", DB_PATH)


def make_dedup_hash(firm_id: str, signal_type: str, title: str) -> str:
    normalised = " ".join(title.lower().split())
    key = f"{firm_id}|{signal_type}|{normalised}"
    return hashlib.sha256(key.encode()).hexdigest()[:24]


def insert_signal(firm_id: str, signal_type: str, weight: float, title: str,
                  description: str = "", source_url: str = "", raw_data: dict = None,
                  practice_area: str = None) -> bool:
    """Returns True if NEW, False if duplicate (suppressed)."""
    dh   = make_dedup_hash(firm_id, signal_type, title)
    conn = get_conn()
    try:
        conn.execute("""
            INSERT INTO signals
                (firm_id, signal_type, practice_area, weight, title,
                 description, source_url, raw_data, dedup_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (firm_id, signal_type, practice_area, weight, title,
              description, source_url, json.dumps(raw_data or {}), dh))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def upsert_canlii_appearance(row: dict):
    conn = get_conn()
    conn.execute("""
        INSERT OR IGNORE INTO canlii_appearances
            (firm_id, case_id, case_title, citation, decision_date,
             court, counsel_raw, file_type, practice_area)
        VALUES (:firm_id, :case_id, :case_title, :citation, :decision_date,
                :court, :counsel_raw, :file_type, :practice_area)
    """, row)
    conn.commit()
    conn.close()


def insert_sedar_filing(row: dict):
    conn = get_conn()
    row["counsel_firms"] = json.dumps(row.get("counsel_firms", []))
    conn.execute("""
        INSERT OR IGNORE INTO sedar_filings
            (filing_id, issuer, doc_type, filed_date, counsel_firms, deal_value, source_url)
        VALUES (:filing_id, :issuer, :doc_type, :filed_date,
                :counsel_firms, :deal_value, :source_url)
    """, row)
    conn.commit()
    conn.close()


def upsert_linkedin_associate(row: dict):
    conn = get_conn()
    conn.execute("""
        INSERT INTO linkedin_roster
            (firm_id, linkedin_url, full_name, title, practice_area,
             start_date, seniority, last_checked)
        VALUES (:firm_id, :linkedin_url, :full_name, :title, :practice_area,
                :start_date, :seniority, :last_checked)
        ON CONFLICT(linkedin_url) DO UPDATE SET
            title=excluded.title, practice_area=excluded.practice_area,
            seniority=excluded.seniority, last_checked=excluded.last_checked,
            is_active=excluded.is_active, left_date=excluded.left_date,
            new_employer=excluded.new_employer
    """, row)
    conn.commit()
    conn.close()


def get_recent_appearances(firm_id: str, days: int = 35) -> list:
    conn = get_conn()
    rows = conn.execute("""
        SELECT * FROM canlii_appearances
        WHERE firm_id = ?
          AND date(decision_date) >= date('now', ? || ' days')
        ORDER BY decision_date DESC
    """, (firm_id, f"-{days}")).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_unalerted_signals() -> list:
    conn = get_conn()
    rows = conn.execute("""
        SELECT * FROM signals
        WHERE alerted = 0
        ORDER BY weight DESC, detected_at DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def mark_alerted(signal_id: int):
    conn = get_conn()
    conn.execute("UPDATE signals SET alerted=1 WHERE id=?", (signal_id,))
    conn.commit()
    conn.close()


def upsert_spillage_edge(biglaw_id: str, boutique_id: str, source: str):
    conn = get_conn()
    conn.execute("""
        INSERT INTO spillage_edges (biglaw_id, boutique_id, source, co_appearances, last_seen)
        VALUES (?, ?, ?, 1, date('now'))
        ON CONFLICT(biglaw_id, boutique_id, source) DO UPDATE SET
            co_appearances = co_appearances + 1, last_seen = date('now')
    """, (biglaw_id, boutique_id, source))
    conn.commit()
    conn.close()


def get_spillage_graph() -> list:
    conn = get_conn()
    rows = conn.execute("""
        SELECT biglaw_id, boutique_id, source, co_appearances, last_seen
        FROM spillage_edges ORDER BY co_appearances DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_all_signals_for_dashboard(days: int = 90) -> list:
    conn = get_conn()
    rows = conn.execute("""
        SELECT * FROM signals
        WHERE date(detected_at) >= date('now', ? || ' days')
        ORDER BY detected_at DESC
    """, (f"-{days}",)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


if __name__ == "__main__":
    init_db()
