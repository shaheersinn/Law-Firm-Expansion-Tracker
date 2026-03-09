"""
Evolution / self-learning module — v2
======================================
Analyzes which signal types and departments have been most predictive
and adjusts effective weights for the next run.

Run with: python main.py --evolve

New in v2:
  - Accepts log_path and force parameters (compatible with 50-cycle harness)
  - Returns a structured report dict with learning_schedule, keywords_updated,
    and signal_type_weights keys
  - Integrates with LearningSchedule for adaptive cadence
  - Parses tracker.log to count recent errors per scraper
"""

import json
import logging
import os
import re
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone

logger = logging.getLogger("learning.evolution")

DB_PATH      = os.getenv("DB_PATH", "law_firm_tracker.db")
WEIGHTS_PATH = "learned_weights.json"


# Explicit mapping from signal type to scraper class name for log parsing
_SIGNAL_TYPE_TO_SCRAPER: dict[str, str] = {
    "lateral_hire":       "LateralTrackScraper",
    "job_posting":        "JobsScraper",
    "press_release":      "PressScraper",
    "publication":        "PublicationsScraper",
    "practice_page":      "WebsiteScraper",
    "website_snapshot":   "WebsiteScraper",
    "ranking":            "ChambersScraper",
    "bar_leadership":     "BarAssociationScraper",
    "alumni_hire":        "AlumniTrackScraper",
    "thought_leadership": "ThoughtLeaderScraper",
    "diversity_signal":   "DiversityScraper",
    "ip_filing":          "CIPOScraper",
    "bar_speaking":       "EventScraper",
    "bar_sponsorship":    "EventScraper",
    "recruit_posting":    "RecruiterScraper",
    "court_record":       "CanLIIScraper",
    "deal_record":        "DealTrackScraper",
    "office_lease":       "OfficeTracker",
}


def _parse_log_errors(log_path: str) -> dict[str, int]:
    """
    Parse tracker.log and return a {scraper_name: error_count} dict
    so the evolution step can down-weight scrapers that keep failing.
    """
    error_counts: dict[str, int] = defaultdict(int)
    if not log_path or not os.path.exists(log_path):
        return error_counts
    try:
        with open(log_path, "r", errors="replace") as fh:
            for line in fh:
                if "[ERROR]" not in line:
                    continue
                m = re.search(r"(\w+Scraper) failed", line)
                if m:
                    error_counts[m.group(1)] += 1
    except Exception:
        pass
    return dict(error_counts)


def run_evolution(
    log_path: str = "tracker.log",
    force: bool = False,
) -> dict | None:
    """
    Reads the DB, computes hit-rate per signal type per department,
    and saves adjusted weights to learned_weights.json.

    Returns a report dict or None if the schedule says it's too soon
    (unless force=True).

    Report keys:
      learning_schedule   — phase / alpha / run stats
      keywords_updated    — count of keyword weight rows updated
      signal_type_weights — {signal_type: learned_weight}
    """
    # Resolve DB path
    db_path = os.getenv("DB_PATH", DB_PATH)

    if not os.path.exists(db_path):
        logger.warning("No DB found — skipping evolution")
        return None

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # ── Learning schedule ────────────────────────────────────────────────────
    try:
        from database.db import Database
        from learning.schedule import LearningSchedule
        _db = Database(db_path)
        schedule = LearningSchedule(_db)
        schedule_stats = schedule.get_stats()

        if not force and not schedule.should_run():
            _db.close()
            conn.close()
            return None   # too soon — caller should respect this
    except Exception as e:
        logger.warning(f"Schedule check failed ({e}) — running anyway")
        schedule_stats = {"phase": "unknown", "alpha": 0.2, "run_count": 0,
                          "elapsed_hours": 0, "total_confirmed": 0, "total_false_pos": 0}
        schedule = None
        _db = None

    # ── Signal-type weight learning ──────────────────────────────────────────
    rows = conn.execute(
        "SELECT signal_type, department, COUNT(*) as cnt "
        "FROM signals GROUP BY signal_type, department"
    ).fetchall()

    type_dept_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in rows:
        type_dept_counts[row["signal_type"]][row["department"]] += row["cnt"]

    # Count log errors to apply a reliability penalty
    error_counts = _parse_log_errors(log_path)

    from analysis.signals import SIGNAL_WEIGHTS
    alpha = schedule_stats.get("alpha", 0.2)

    learned: dict[str, float] = {}
    for sig_type, base_w in SIGNAL_WEIGHTS.items():
        volume = sum(type_dept_counts.get(sig_type, {}).values())
        # Positive nudge from volume (capped at +0.5)
        volume_nudge = min(0.5, volume * 0.003)
        # Reliability penalty from log errors (capped at -0.4)
        scraper_name = _SIGNAL_TYPE_TO_SCRAPER.get(sig_type, "")
        error_penalty = min(0.4, error_counts.get(scraper_name, 0) * 0.05)
        # EMA-style blend toward (base_w + nudge - penalty)
        target = base_w + volume_nudge - error_penalty
        prev = learned.get(sig_type, base_w)
        learned[sig_type] = round(alpha * target + (1 - alpha) * prev, 3)

    with open(WEIGHTS_PATH, "w") as f:
        json.dump(learned, f, indent=2)

    # ── Keyword learning ─────────────────────────────────────────────────────
    keywords_updated = 0
    try:
        from learning.keyword_learner import KeywordLearner
        if _db is None:
            from database.db import Database
            _db = Database(db_path)
        kl = KeywordLearner(_db)
        keywords_updated = kl.update_weights()
    except Exception as e:
        logger.warning(f"Keyword learning failed: {e}")

    # ── Record this run in schedule ──────────────────────────────────────────
    if schedule is not None:
        try:
            schedule.record_run(confirmed=0, false_pos=0)
        except Exception:
            pass

    logger.info(f"Evolution complete — weights saved to {WEIGHTS_PATH}")
    logger.info(json.dumps(learned, indent=2))

    conn.close()
    if _db is not None:
        try:
            _db.close()
        except Exception:
            pass

    return {
        "learning_schedule":   schedule_stats,
        "keywords_updated":    keywords_updated,
        "signal_type_weights": learned,
    }
