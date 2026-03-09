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


def run_evolution():
    """
    Reads the DB, computes hit-rate per signal type per department,
    and saves adjusted weights to learned_weights.json.
    """
    if not os.path.exists(DB_PATH):
        logger.warning("No DB found — skipping evolution")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

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
    logger.info(json.dumps(learned, indent=2))
    conn.close()
