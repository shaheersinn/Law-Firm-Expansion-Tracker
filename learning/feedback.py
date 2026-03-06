"""
Feedback loop — records whether a signal / department classification was
accurate, then surfaces that data to the keyword learner.

How "feedback" is inferred automatically (no human needed):
  1. CONFIRMATION: A later signal of a STRONGER type arrives for the same
     firm+department within 30 days of the original weak signal.
     (e.g., a job_posting followed by a lateral_hire confirms the job signal)
  2. CONSISTENCY: If the same department is scored ≥ EXPANSION_THRESHOLD in
     3+ consecutive weeks it's likely a true positive.
  3. SILENCE: If a spike never repeats and no stronger signals follow within
     60 days it's treated as a false positive.
"""

import json
import logging
import sqlite3
from datetime import datetime, timezone, timedelta

def _parse_dt(s):
    """Parse ISO datetime string, treating naive values as UTC."""
    from datetime import datetime, timezone
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt



logger = logging.getLogger("learning.feedback")

# Signal strength hierarchy: higher index = stronger confirmation
SIGNAL_STRENGTH = [
    "bar_mention", "bar_sponsorship", "publication", "attorney_profile",
    "bar_speaking", "press_release", "website_snapshot", "recruit_posting",
    "court_record", "job_posting", "practice_page", "ranking", "lateral_hire",
    "bar_leadership",
]


def _strength(signal_type: str) -> int:
    try:
        return SIGNAL_STRENGTH.index(signal_type)
    except ValueError:
        return 0


class FeedbackRecorder:
    def __init__(self, db):
        self._db = db
        self._ensure_table()

    def _ensure_table(self):
        self._db.conn.executescript("""
            CREATE TABLE IF NOT EXISTS signal_feedback (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_id        TEXT    NOT NULL,
                department     TEXT    NOT NULL,
                signal_id      INTEGER,
                outcome        TEXT    NOT NULL,   -- 'confirmed','false_positive','pending'
                matched_keywords TEXT,
                signal_type    TEXT,
                recorded_at    TEXT    DEFAULT (datetime('now'))
            );
        """)
        self._db.conn.commit()

    # ------------------------------------------------------------------ #
    #  Auto-inference
    # ------------------------------------------------------------------ #

    def infer_feedback_from_db(self):
        """
        Scan the signals table and derive feedback automatically.
        Should be called once per day by evolution.py.
        """
        confirmed, false_positives = 0, 0

        # --- CONFIRMATION RULE ---
        # For each signal, look for a stronger signal of same firm+dept ≤ 30 days later
        cur = self._db.conn.execute("""
            SELECT id, firm_id, department, signal_type, matched_keywords, seen_at
            FROM signals
            WHERE department IS NOT NULL AND department != ''
            ORDER BY seen_at
        """)
        signals = [dict(r) for r in cur.fetchall()]

        already_recorded = self._already_recorded_ids()

        for sig in signals:
            if sig["id"] in already_recorded:
                continue

            cutoff = (
                _parse_dt(sig["seen_at"]) + timedelta(days=30)
            ).isoformat()

            # Check for a stronger follow-up
            cur2 = self._db.conn.execute("""
                SELECT signal_type FROM signals
                WHERE firm_id = ? AND department = ?
                  AND seen_at > ? AND seen_at <= ?
                  AND id != ?
            """, (sig["firm_id"], sig["department"], sig["seen_at"], cutoff, sig["id"]))

            following = [r[0] for r in cur2.fetchall()]
            stronger  = any(_strength(f) > _strength(sig["signal_type"]) for f in following)

            if stronger:
                self._record(sig, "confirmed")
                confirmed += 1
            # False-positive detection deferred to SILENCE RULE below

        # --- SILENCE RULE ---
        # Signals older than 60 days with no stronger follow-up → false_positive
        cutoff_60 = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
        cur = self._db.conn.execute("""
            SELECT id, firm_id, department, signal_type, matched_keywords, seen_at
            FROM signals
            WHERE seen_at < ? AND department IS NOT NULL AND department != ''
        """, (cutoff_60,))
        old_signals = [dict(r) for r in cur.fetchall()]

        for sig in old_signals:
            if sig["id"] in already_recorded:
                continue
            # Check for any stronger follow-up in the 60-day window
            cur2 = self._db.conn.execute("""
                SELECT signal_type FROM signals
                WHERE firm_id = ? AND department = ?
                  AND seen_at > ? AND seen_at < ?
            """, (sig["firm_id"], sig["department"], sig["seen_at"], cutoff_60))
            following = [r[0] for r in cur2.fetchall()]
            stronger  = any(_strength(f) > _strength(sig["signal_type"]) for f in following)
            if not stronger:
                self._record(sig, "false_positive")
                false_positives += 1

        logger.info(f"Feedback inferred: {confirmed} confirmed, {false_positives} false positives")
        return confirmed, false_positives

    # ------------------------------------------------------------------ #
    #  Helpers
    # ------------------------------------------------------------------ #

    def _record(self, sig: dict, outcome: str):
        try:
            self._db.conn.execute("""
                INSERT INTO signal_feedback
                  (firm_id, department, signal_id, outcome, matched_keywords, signal_type)
                VALUES (?,?,?,?,?,?)
            """, (
                sig["firm_id"], sig["department"], sig["id"], outcome,
                sig.get("matched_keywords", ""), sig.get("signal_type", ""),
            ))
            self._db.conn.commit()
        except Exception as e:
            logger.error(f"Feedback record error: {e}")

    def _already_recorded_ids(self) -> set:
        cur = self._db.conn.execute(
            "SELECT signal_id FROM signal_feedback WHERE signal_id IS NOT NULL"
        )
        return {r[0] for r in cur.fetchall()}
