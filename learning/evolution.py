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


def run_evolution(log_path: str | None = None, force: bool = False) -> dict | None:
    """
    Reads the DB, computes hit-rate per signal type per department,
    and saves adjusted weights to learned_weights.json.

    Args:
        log_path: Optional path to the run log (unused beyond future diagnostics).
        force:    When True, skip the schedule gate and always run.

    Returns:
        A dict with keys learning_schedule, keywords_updated, signal_type_weights
        when evolution ran, or None when the schedule says it is too soon.
    """
    from database.db import Database
    from learning.schedule import LearningSchedule

    db_path = os.getenv("DB_PATH", DB_PATH)
    if not os.path.exists(db_path):
        logger.warning("No DB found — skipping evolution")
        return None

    db = Database(db_path)
    schedule = LearningSchedule(db)

    if not force and not schedule.should_run():
        db.close()
        return None

    conn = db.conn

    # Count signals by type and department
    rows = conn.execute(
        "SELECT signal_type, department, COUNT(*) as cnt "
        "FROM signals GROUP BY signal_type, department"
    ).fetchall()

    type_dept_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in rows:
        type_dept_counts[row["signal_type"]][row["department"]] += row["cnt"]

    # EMA learning rate for the current schedule phase
    alpha = schedule.current_alpha()

    from analysis.signals import SIGNAL_WEIGHTS

    # Load existing learned weights for EMA blending
    existing: dict[str, float] = {}
    if os.path.exists(WEIGHTS_PATH):
        try:
            with open(WEIGHTS_PATH) as f:
                existing = json.load(f)
        except Exception:
            pass

    learned: dict[str, float] = {}
    for sig_type, base_w in SIGNAL_WEIGHTS.items():
        high_count = sum(v for v in type_dept_counts.get(sig_type, {}).values())
        nudge = min(0.2, high_count * 0.005)
        new_w = base_w + nudge
        # EMA blend: smoothly move toward the observed weight
        old_w = existing.get(sig_type, new_w)
        blended = round(old_w * (1 - alpha) + new_w * alpha, 3)
        learned[sig_type] = blended

    with open(WEIGHTS_PATH, "w") as f:
        json.dump(learned, f, indent=2)

    schedule.record_run(confirmed=0, false_pos=0)
    schedule_stats = schedule.get_stats()

    db.close()

    logger.info(f"Evolution complete — weights saved to {WEIGHTS_PATH}")
    logger.info(json.dumps(learned, indent=2))

    return {
        "learning_schedule":  schedule_stats,
        "keywords_updated":   len(learned),
        "signal_type_weights": learned,
    }
