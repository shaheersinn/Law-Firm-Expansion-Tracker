"""
Evolution / self-learning module.
Analyzes which signal types and departments have been most predictive
and adjusts effective weights for the next run.

Run with: python main.py --evolve
"""

import json
import logging
import math
import os
import sqlite3
from collections import defaultdict

logger = logging.getLogger("learning.evolution")

DB_PATH = os.getenv("DB_PATH", "law_firm_tracker.db")
WEIGHTS_PATH = "learned_weights.json"
NEUTRAL_HIT_RATE_PRIOR = 0.5


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

    # ── Infer feedback + train keyword weights every evolution run ────────────
    from learning.feedback_v2 import FeedbackEngine
    from learning.keyword_learner_v2 import KeywordLearnerV2

    feedback_engine = FeedbackEngine(db_obj, schedule=schedule)
    confirmed, false_pos = feedback_engine.infer_feedback_from_db()
    cooccurrence = feedback_engine.get_cooccurrence()

    learner = KeywordLearnerV2(db_obj, schedule=schedule)
    keywords_updated = learner.update_weights(cooccurrence=cooccurrence)
    keyword_candidates = learner.discover_new_keywords()
    keywords_penalised = learner.penalise_cross_dept_noise()

    feedback_by_type = defaultdict(lambda: {"confirmed": 0, "false_positive": 0})
    for row in conn.execute(
        """
        SELECT COALESCE(s.signal_type, sf.signal_type) AS signal_type,
               SUM(CASE WHEN sf.outcome='confirmed' THEN 1 ELSE 0 END) AS confirmed,
               SUM(CASE WHEN sf.outcome='false_positive' THEN 1 ELSE 0 END) AS false_positive
        FROM signal_feedback sf
        LEFT JOIN signals s ON s.id = sf.signal_id
        GROUP BY COALESCE(s.signal_type, sf.signal_type)
        """
    ).fetchall():
        sig_type = row["signal_type"]
        if not sig_type:
            continue
        feedback_by_type[sig_type] = {
            "confirmed": row["confirmed"] or 0,
            "false_positive": row["false_positive"] or 0,
        }

    # ── Compute EMA-adjusted weights ─────────────────────────────────────────
    from analysis.signals import SIGNAL_WEIGHTS

    learned: dict[str, float] = {}
    for sig_type, base_w in SIGNAL_WEIGHTS.items():
        high_count = sum(v for v in type_dept_counts.get(sig_type, {}).values())
        stats = feedback_by_type.get(sig_type, {"confirmed": 0, "false_positive": 0})
        feedback_total = stats["confirmed"] + stats["false_positive"]
        # No feedback yet = neutral prior, so the base weight stays centered.
        hit_rate = (
            stats["confirmed"] / feedback_total
        ) if feedback_total else NEUTRAL_HIT_RATE_PRIOR
        reliability_delta = (hit_rate - NEUTRAL_HIT_RATE_PRIOR) * 0.8 * alpha
        volume_nudge = min(0.25 * alpha, math.log1p(high_count) * 0.03 * alpha)
        learned_weight = base_w * (1 + reliability_delta) + volume_nudge
        learned[sig_type] = round(max(0.0, learned_weight), 3)

    with open(WEIGHTS_PATH, "w") as f:
        json.dump(learned, f, indent=2)

    logger.info(f"Evolution complete — weights saved to {WEIGHTS_PATH}")
    logger.info(json.dumps(learned, indent=2))

    # Record this run in the schedule
    schedule.record_run(
        confirmed=confirmed + len(high_score_firms),
        false_pos=false_pos,
    )
    db_obj.close()
    conn.close()

    return {
        "learning_schedule": {
            "phase":      phase,
            "alpha":      alpha,
            "forced":     force,
        },
        "feedback_summary": {
            "confirmed": confirmed,
            "false_positive": false_pos,
        },
        "keywords_updated":   keywords_updated,
        "keyword_candidates": keyword_candidates,
        "keywords_penalised": keywords_penalised,
        "signal_type_weights": learned,
    }
