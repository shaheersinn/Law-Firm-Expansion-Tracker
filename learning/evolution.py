"""
Evolution / self-learning module.
Analyzes which signal types and departments have been most predictive
and adjusts effective weights for the next run.

Run with: python main.py --evolve
"""

import json
import logging
import os
import sqlite3
from collections import defaultdict

logger = logging.getLogger("learning.evolution")

DB_PATH = os.getenv("DB_PATH", "law_firm_tracker.db")
WEIGHTS_PATH = "learned_weights.json"


def run_evolution(
    log_path: str | None = None,
    force: bool = False,
    db_path: str | None = None,
):
    """
    Reads the DB, computes hit-rate per signal type per department,
    and saves adjusted weights to learned_weights.json.

    Args:
        log_path: Path to the log file (optional, used by LearningSchedule).
        force:    If True, skip the schedule check and always run.
        db_path:  Path to the SQLite database. Falls back to DB_PATH env var.

    Returns:
        dict with keys "learning_schedule", "keywords_updated",
        "signal_type_weights" if evolution ran, or None if skipped.
    """
    effective_db = db_path or os.getenv("DB_PATH", DB_PATH)

    if not os.path.exists(effective_db):
        logger.warning(f"No DB found at {effective_db!r} — skipping evolution")
        return None

    conn = sqlite3.connect(effective_db)
    conn.row_factory = sqlite3.Row

    # ── Schedule check ───────────────────────────────────────────────────────
    from database.db import Database
    from learning.schedule import LearningSchedule

    db_obj = Database(effective_db)
    schedule = LearningSchedule(db_obj)

    if not force and not schedule.should_run():
        db_obj.close()
        conn.close()
        return None  # Not yet due — caller treats None as "skipped"

    alpha = schedule.current_alpha()
    phase = schedule.current_phase()

    # ── Count signals by type and department ─────────────────────────────────
    rows = conn.execute(
        "SELECT signal_type, department, COUNT(*) as cnt "
        "FROM signals GROUP BY signal_type, department"
    ).fetchall()

    type_dept_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in rows:
        type_dept_counts[row["signal_type"]][row["department"]] += row["cnt"]

    # Firms with historically high weekly scores
    high_score_firms = {
        row["firm_id"]
        for row in conn.execute(
            "SELECT firm_id FROM weekly_scores WHERE score >= 10"
        ).fetchall()
    }

    # ── Compute EMA-adjusted weights ─────────────────────────────────────────
    from analysis.signals import SIGNAL_WEIGHTS

    learned: dict[str, float] = {}
    for sig_type, base_w in SIGNAL_WEIGHTS.items():
        high_count = sum(v for v in type_dept_counts.get(sig_type, {}).values())
        # EMA nudge: scale by alpha so bootstrap phase moves faster
        nudge = min(0.3 * alpha, high_count * 0.005 * alpha)
        learned[sig_type] = round(base_w + nudge, 3)

    with open(WEIGHTS_PATH, "w") as f:
        json.dump(learned, f, indent=2)

    logger.info(f"Evolution complete — weights saved to {WEIGHTS_PATH}")
    logger.info(json.dumps(learned, indent=2))

    # ── Update keyword learner ────────────────────────────────────────────────
    keywords_updated = 0
    try:
        from learning.keyword_learner import update_keywords
        keywords_updated = update_keywords(conn, alpha=alpha)
    except Exception as exc:
        logger.debug(f"keyword_learner optional step: {exc}")

    # Record this run in the schedule
    schedule.record_run(confirmed=len(high_score_firms), false_pos=0)
    db_obj.close()
    conn.close()

    return {
        "learning_schedule": {
            "phase":      phase,
            "alpha":      alpha,
            "forced":     force,
        },
        "keywords_updated":   keywords_updated,
        "signal_type_weights": learned,
    }
