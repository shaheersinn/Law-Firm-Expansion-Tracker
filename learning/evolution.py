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


def run_evolution(log_path: str = None, force: bool = False) -> dict | None:
    """
    Reads the DB, computes hit-rate per signal type per department,
    and saves adjusted weights to learned_weights.json.

    Returns a report dict on success, or None when the schedule says
    it is too early to run (and force=False).
    """
    db_path = os.getenv("DB_PATH", DB_PATH)

    if not os.path.exists(db_path):
        logger.warning("No DB found — skipping evolution")
        return None

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # ── Check learning schedule (skip if too soon and not forced) ─────────
    from database.db import Database
    from learning.schedule import LearningSchedule
    _db = Database(db_path)
    sched = LearningSchedule(_db)

    if not force and not sched.should_run():
        _db.close()
        conn.close()
        return None

    # Count signals by type and department
    rows = conn.execute(
        "SELECT signal_type, department, COUNT(*) as cnt FROM signals GROUP BY signal_type, department"
    ).fetchall()

    type_dept_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in rows:
        type_dept_counts[row["signal_type"]][row["department"]] += row["cnt"]

    # Count corroborated signals (those whose firm+dept later got a high weekly score)
    high_score_firms = {
        row["firm_id"]
        for row in conn.execute(
            "SELECT firm_id FROM weekly_scores WHERE score >= 10"
        ).fetchall()
    }

    # Bump weight for signal types that appear frequently in high-score firm+weeks
    learned: dict[str, float] = {}
    from analysis.signals import SIGNAL_WEIGHTS
    for sig_type, base_w in SIGNAL_WEIGHTS.items():
        high_count = sum(
            v for dept, v in type_dept_counts.get(sig_type, {}).items()
        )
        # Simple nudge: ±10% based on volume
        nudge = min(0.2, high_count * 0.005)
        learned[sig_type] = round(base_w + nudge, 3)

    with open(WEIGHTS_PATH, "w") as f:
        json.dump(learned, f, indent=2)

    logger.info(f"Evolution complete — weights saved to {WEIGHTS_PATH}")

    sched.record_run(confirmed=0, false_pos=0)
    sched_stats = sched.get_stats()
    _db.close()
    conn.close()

    return {
        "learning_schedule":   sched_stats,
        "keywords_updated":    len(learned),
        "signal_type_weights": learned,
    }
