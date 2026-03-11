"""
Learning Schedule Manager.

Controls the adaptive learning cadence:
  • First 48 hours (bootstrap):  learn every HOUR  → high plasticity
  • After 48 hours:              learn every DAY   → stable refinement

Why two phases?
  Bootstrap phase: the model has seen very few signals.  Every new
  data point is precious, so we learn from it immediately with a high
  learning rate (EMA_ALPHA=0.40) so weights move fast.

  Stable phase: enough history has accumulated.  Rapid weight changes
  would cause oscillation, so we slow down (EMA_ALPHA=0.15) and only
  consolidate once per day.

The schedule is stored in the DB so it survives restarts.
GitHub Actions cron runs the evolution job HOURLY; this module decides
whether to actually update weights or skip (fast no-op if too soon).
"""

import logging
from datetime import datetime, timezone

def _parse_dt(s):
    """Parse ISO datetime string, treating naive values as UTC."""
    from datetime import datetime, timezone
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt



logger = logging.getLogger("learning.schedule")

BOOTSTRAP_HOURS   = 48       # How long the fast-learning window lasts
BOOTSTRAP_ALPHA   = 0.40     # EMA learning rate during bootstrap
STABLE_ALPHA      = 0.15     # EMA learning rate after bootstrap
MIN_INTERVAL_BOOTSTRAP = 55  # Minimum minutes between runs in bootstrap (allow cron drift)
MIN_INTERVAL_STABLE    = 23 * 60  # ~23 hours in stable phase


class LearningSchedule:
    def __init__(self, db):
        self._db = db
        self._ensure_table()

    def _ensure_table(self):
        self._db.conn.executescript("""
            CREATE TABLE IF NOT EXISTS learning_schedule (
                id              INTEGER PRIMARY KEY,
                first_run_at    TEXT,
                last_run_at     TEXT,
                run_count       INTEGER DEFAULT 0,
                phase           TEXT    DEFAULT 'bootstrap',
                total_confirmed INTEGER DEFAULT 0,
                total_false_pos INTEGER DEFAULT 0
            );
        """)
        self._db.conn.commit()
        # Insert singleton row if missing
        self._db.conn.execute("""
            INSERT OR IGNORE INTO learning_schedule (id, first_run_at)
            VALUES (1, datetime('now'))
        """)
        self._db.conn.commit()

    # ------------------------------------------------------------------ #

    def should_run(self) -> bool:
        """
        Returns True if enough time has passed since the last evolution run.
        Called at the TOP of run_evolution() — skips the heavy work if not due.
        """
        row = self._get_row()
        if not row["last_run_at"]:
            return True   # first ever run

        last = _parse_dt(row["last_run_at"])
        elapsed_min = (datetime.now(timezone.utc) - last).total_seconds() / 60

        phase = self._current_phase(row)
        threshold = MIN_INTERVAL_BOOTSTRAP if phase == "bootstrap" else MIN_INTERVAL_STABLE

        if elapsed_min < threshold:
            logger.info(
                f"[Schedule] Skipping — only {elapsed_min:.1f} min since last run "
                f"(threshold: {threshold} min, phase: {phase})"
            )
            return False
        return True

    def current_alpha(self) -> float:
        """Return the EMA learning rate for the current phase."""
        row = self._get_row()
        phase = self._current_phase(row)
        alpha = BOOTSTRAP_ALPHA if phase == "bootstrap" else STABLE_ALPHA
        logger.info(f"[Schedule] Phase={phase}, alpha={alpha}")
        return alpha

    def current_phase(self) -> str:
        return self._current_phase(self._get_row())

    def record_run(self, confirmed: int, false_pos: int):
        """Call after a successful evolution run."""
        row = self._get_row()
        phase = self._current_phase(row)
        self._db.conn.execute("""
            UPDATE learning_schedule
            SET last_run_at    = datetime('now'),
                run_count      = run_count + 1,
                phase          = ?,
                total_confirmed = total_confirmed + ?,
                total_false_pos = total_false_pos + ?
            WHERE id = 1
        """, (phase, confirmed, false_pos))
        self._db.conn.commit()
        logger.info(
            f"[Schedule] Run #{row['run_count']+1} recorded. "
            f"Phase={phase}. Total confirmed={row['total_confirmed']+confirmed}"
        )

    def get_stats(self) -> dict:
        row = self._get_row()
        first = _parse_dt(row["first_run_at"]) if row["first_run_at"] else datetime.now(timezone.utc)
        elapsed_h = (datetime.now(timezone.utc) - first).total_seconds() / 3600
        return {
            "phase":           self._current_phase(row),
            "elapsed_hours":   round(elapsed_h, 1),
            "run_count":       row["run_count"],
            "total_confirmed": row["total_confirmed"],
            "total_false_pos": row["total_false_pos"],
            "alpha":           self.current_alpha(),
        }

    # ------------------------------------------------------------------ #

    def _get_row(self) -> dict:
        cur = self._db.conn.execute("SELECT * FROM learning_schedule WHERE id=1")
        row = cur.fetchone()
        if row is None:
            return {"first_run_at": None, "last_run_at": None, "run_count": 0,
                    "phase": "bootstrap", "total_confirmed": 0, "total_false_pos": 0}
        return dict(row)

    def _current_phase(self, row: dict) -> str:
        first_str = row.get("first_run_at")
        if not first_str:
            return "bootstrap"
        first = _parse_dt(first_str)
        elapsed_h = (datetime.now(timezone.utc) - first).total_seconds() / 3600
        return "bootstrap" if elapsed_h < BOOTSTRAP_HOURS else "stable"
