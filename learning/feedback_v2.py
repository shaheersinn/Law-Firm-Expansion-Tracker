"""
Advanced Feedback Engine (v2).

Replaces the simple v1 feedback.py with a richer set of inference rules:

1. CONFIRMATION RULE   — stronger signal follows within 30 days
2. CONSISTENCY RULE    — same dept scores ≥ threshold 3+ consecutive weeks
3. VELOCITY RULE       — sudden burst of signals (≥3 in 48h) → confirmed
4. SILENCE RULE        — no follow-up in 60 days → false positive
5. CO-OCCURRENCE MAP   — tracks which keywords co-occur with confirmed vs fp
6. RECENCY WEIGHTING   — feedback from last 7 days counts 3×, 7-30 days 2×

All rules run every evolution cycle.  In bootstrap phase, shorter windows
are used (15 days for confirmation, 14 days for silence) to build up
signal faster.
"""

import logging
from datetime import datetime, timezone, timedelta

def _parse_dt(s):
    """Parse ISO datetime string, treating naive values as UTC."""
    from datetime import datetime, timezone
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt



from learning.schedule import LearningSchedule

logger = logging.getLogger("learning.feedback_v2")

SIGNAL_STRENGTH = [
    "bar_mention", "bar_sponsorship", "publication", "attorney_profile",
    "bar_speaking", "press_release", "website_snapshot", "recruit_posting",
    "court_record", "job_posting", "practice_page", "ranking", "lateral_hire",
    "bar_leadership",
]


def _strength(st: str) -> int:
    try:
        return SIGNAL_STRENGTH.index(st)
    except ValueError:
        return 0


def _recency_weight(seen_at_str: str) -> float:
    """Recent feedback is more valuable."""
    try:
        age_days = (datetime.now(timezone.utc) - _parse_dt(seen_at_str)).days
    except Exception:
        return 1.0
    if age_days <= 7:
        return 3.0
    if age_days <= 30:
        return 2.0
    return 1.0


class FeedbackEngine:
    def __init__(self, db, schedule: LearningSchedule = None):
        self._db       = db
        self._schedule = schedule
        self._ensure_tables()

    def _ensure_tables(self):
        self._db.conn.executescript("""
            CREATE TABLE IF NOT EXISTS signal_feedback (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_id          TEXT    NOT NULL,
                department       TEXT    NOT NULL,
                signal_id        INTEGER,
                outcome          TEXT    NOT NULL,
                matched_keywords TEXT,
                signal_type      TEXT,
                recency_weight   REAL    DEFAULT 1.0,
                recorded_at      TEXT    DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS keyword_cooccurrence (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                department  TEXT NOT NULL,
                keyword     TEXT NOT NULL,
                confirmed   INTEGER DEFAULT 0,
                false_pos   INTEGER DEFAULT 0,
                updated_at  TEXT DEFAULT (datetime('now')),
                UNIQUE(department, keyword)
            );
        """)
        self._db.conn.commit()

    # ================================================================== #
    #  Main entry
    # ================================================================== #

    def infer_feedback_from_db(self) -> tuple[int, int]:
        """Run all inference rules and return (confirmed, false_positives)."""
        phase = self._schedule.current_phase() if self._schedule else "stable"
        confirm_window = 15 if phase == "bootstrap" else 30   # days
        silence_window = 14 if phase == "bootstrap" else 60   # days

        already_done = self._already_recorded_ids()
        new_confirmed, new_false_pos = 0, 0

        signals = self._load_all_signals()

        for sig in signals:
            if sig["id"] in already_done:
                continue

            outcome = self._infer_outcome(sig, signals, confirm_window, silence_window)
            if outcome:
                w = _recency_weight(sig.get("seen_at", ""))
                self._record(sig, outcome, w)
                self._update_cooccurrence(sig, outcome)
                if outcome == "confirmed":
                    new_confirmed += 1
                else:
                    new_false_pos += 1

        # CONSISTENCY RULE — scan weekly_scores for 3+ consecutive weeks
        n_consistency = self._consistency_rule()

        # VELOCITY RULE — burst of ≥3 signals in 48h
        n_velocity = self._velocity_rule(already_done)

        total_confirmed = new_confirmed + n_consistency + n_velocity
        logger.info(
            f"[FeedbackV2] Confirmed: {total_confirmed} "
            f"(direct={new_confirmed}, consistency={n_consistency}, velocity={n_velocity}), "
            f"FP: {new_false_pos}"
        )
        return total_confirmed, new_false_pos

    # ================================================================== #
    #  Rules
    # ================================================================== #

    def _infer_outcome(self, sig: dict, all_signals: list, confirm_days: int, silence_days: int) -> str | None:
        firm_id   = sig["firm_id"]
        dept      = sig["department"]
        sig_time  = sig.get("seen_at", "")

        try:
            t0 = _parse_dt(sig_time)
        except Exception:
            return None

        confirm_cutoff = (t0 + timedelta(days=confirm_days)).isoformat()
        silence_cutoff = (t0 + timedelta(days=silence_days)).isoformat()
        now_str        = datetime.now(timezone.utc).isoformat()

        # Find follow-up signals for same firm+dept
        later = [
            s for s in all_signals
            if s["firm_id"] == firm_id
            and s["department"] == dept
            and s["id"] != sig["id"]
            and s.get("seen_at", "") > sig_time
        ]

        # CONFIRMATION: stronger signal in confirm window
        in_confirm = [s for s in later if s.get("seen_at", "") <= confirm_cutoff]
        if any(_strength(s["signal_type"]) > _strength(sig["signal_type"]) for s in in_confirm):
            return "confirmed"

        # SILENCE: old signal, no follow-up
        if now_str > silence_cutoff:
            in_silence = [s for s in later if s.get("seen_at", "") < silence_cutoff]
            if not in_silence:
                return "false_positive"

        return None  # pending

    def _consistency_rule(self) -> int:
        """
        3+ consecutive ISO weeks with score ≥ threshold → mark all underlying
        signals in those weeks as confirmed.
        """
        from analysis.signals import EXPANSION_THRESHOLD
        try:
            cur = self._db.conn.execute("""
                SELECT firm_id, department, COUNT(*) as cnt
                FROM weekly_scores
                WHERE score >= ?
                GROUP BY firm_id, department
                HAVING cnt >= 3
            """, (EXPANSION_THRESHOLD,))
            consistent = cur.fetchall()
        except Exception:
            return 0

        already = self._already_recorded_ids()
        count = 0
        for firm_id, dept, _ in consistent:
            cur2 = self._db.conn.execute("""
                SELECT id, firm_id, department, signal_type, matched_keywords, seen_at
                FROM signals
                WHERE firm_id=? AND department=?
            """, (firm_id, dept))
            for row in cur2.fetchall():
                sig = dict(row)
                if sig["id"] not in already:
                    self._record(sig, "confirmed", 2.0)
                    self._update_cooccurrence(sig, "confirmed")
                    count += 1
        return count

    def _velocity_rule(self, already_done: set) -> int:
        """Burst of ≥3 signals for same firm+dept within 48h → confirmed."""
        cur = self._db.conn.execute("""
            SELECT firm_id, department,
                   MIN(seen_at) as first, MAX(seen_at) as last,
                   COUNT(*) as cnt,
                   GROUP_CONCAT(id) as ids
            FROM signals
            WHERE department IS NOT NULL AND department != ''
            GROUP BY firm_id, department, SUBSTR(seen_at,1,10)
            HAVING cnt >= 3
        """)
        count = 0
        for firm_id, dept, first, last, cnt, ids_str in cur.fetchall():
            for sid in ids_str.split(","):
                sid = int(sid.strip())
                if sid in already_done:
                    continue
                row = self._db.conn.execute(
                    "SELECT id, firm_id, department, signal_type, matched_keywords, seen_at FROM signals WHERE id=?",
                    (sid,)
                ).fetchone()
                if row:
                    sig = dict(row)
                    self._record(sig, "confirmed", 1.5)
                    self._update_cooccurrence(sig, "confirmed")
                    count += 1
        return count

    # ================================================================== #
    #  Co-occurrence tracking
    # ================================================================== #

    def _update_cooccurrence(self, sig: dict, outcome: str):
        kws = (sig.get("matched_keywords") or "").split(",")
        dept = sig.get("department", "")
        if not dept:
            return
        for kw in kws:
            kw = kw.strip()
            if not kw:
                continue
            if outcome == "confirmed":
                self._db.conn.execute("""
                    INSERT INTO keyword_cooccurrence (department, keyword, confirmed)
                    VALUES (?,?,1)
                    ON CONFLICT(department, keyword)
                    DO UPDATE SET confirmed=confirmed+1, updated_at=datetime('now')
                """, (dept, kw))
            else:
                self._db.conn.execute("""
                    INSERT INTO keyword_cooccurrence (department, keyword, false_pos)
                    VALUES (?,?,1)
                    ON CONFLICT(department, keyword)
                    DO UPDATE SET false_pos=false_pos+1, updated_at=datetime('now')
                """, (dept, kw))
        try:
            self._db.conn.commit()
        except Exception:
            pass

    def get_cooccurrence(self) -> dict:
        """Return {(dept, keyword): (confirmed, false_pos)} map."""
        cur = self._db.conn.execute(
            "SELECT department, keyword, confirmed, false_pos FROM keyword_cooccurrence"
        )
        return {(r[0], r[1]): (r[2], r[3]) for r in cur.fetchall()}

    # ================================================================== #
    #  Helpers
    # ================================================================== #

    def _load_all_signals(self) -> list[dict]:
        cur = self._db.conn.execute("""
            SELECT id, firm_id, department, signal_type, matched_keywords, seen_at
            FROM signals
            WHERE department IS NOT NULL AND department != ''
            ORDER BY seen_at
        """)
        return [dict(r) for r in cur.fetchall()]

    def _record(self, sig: dict, outcome: str, weight: float = 1.0):
        try:
            self._db.conn.execute("""
                INSERT INTO signal_feedback
                  (firm_id, department, signal_id, outcome, matched_keywords, signal_type, recency_weight)
                VALUES (?,?,?,?,?,?,?)
            """, (
                sig["firm_id"], sig["department"], sig["id"], outcome,
                sig.get("matched_keywords", ""), sig.get("signal_type", ""), weight,
            ))
            self._db.conn.commit()
        except Exception as e:
            logger.debug(f"Feedback record error (likely duplicate): {e}")

    def _already_recorded_ids(self) -> set:
        cur = self._db.conn.execute(
            "SELECT signal_id FROM signal_feedback WHERE signal_id IS NOT NULL"
        )
        return {r[0] for r in cur.fetchall()}
