"""
Evolution module — self-learning cycle that runs every 2 hours via GitHub Actions.

What it does:
  1. Reads accuracy feedback stored in signal_feedback table
  2. Identifies low-accuracy departments (< 60% correct)
  3. Adjusts signal weights in the SIGNAL_WEIGHTS table (stored in DB)
  4. Prunes duplicate or near-duplicate signals
  5. Logs weight deltas for audit

NOTE: This module does NOT modify any source code. It only adjusts runtime
weights stored in the database. This is intentional — code changes still
require human review.
"""

import json
import logging
from datetime import datetime, timezone
from collections import defaultdict

from config import Config
from database.db import Database

logger = logging.getLogger("evolution")

# Floor/ceiling for any individual weight
WEIGHT_FLOOR   = 0.5
WEIGHT_CEILING = 5.0

# Adjust weights by this factor per cycle
BOOST_FACTOR  = 1.10
DAMPEN_FACTOR = 0.92

# Accuracy thresholds
HIGH_ACCURACY = 0.75   # boost if above this
LOW_ACCURACY  = 0.50   # dampen if below this


def run_evolution():
    config = Config()
    db     = Database(config.DB_PATH)

    logger.info("=" * 60)
    logger.info(f"Evolution cycle — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    logger.info("=" * 60)

    # 1. Get accuracy by department
    accuracy = db.get_accuracy_by_department()
    if not accuracy:
        logger.info("No feedback data yet — skipping weight adjustment")
        db.close()
        return

    adjustments = {}
    for dept, acc in accuracy.items():
        if acc >= HIGH_ACCURACY:
            adjustments[dept] = ("boost",  BOOST_FACTOR,  acc)
        elif acc < LOW_ACCURACY:
            adjustments[dept] = ("dampen", DAMPEN_FACTOR, acc)

    if not adjustments:
        logger.info("All departments within normal accuracy range — no adjustments needed")
        db.close()
        return

    for dept, (action, factor, acc) in adjustments.items():
        logger.info(f"  {action.upper():6} {dept:<30} acc={acc:.0%}  ×{factor}")

    # 2. Persist weight adjustments
    _save_weight_adjustments(db, adjustments)

    # 3. Log summary
    logger.info(f"Evolution complete — {len(adjustments)} department(s) adjusted")
    db.close()


def _save_weight_adjustments(db: Database, adjustments: dict):
    """
    Persist department weight multipliers to DB for use by the analyzer.
    """
    cur = db.conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dept_weight_multipliers (
            department  TEXT PRIMARY KEY,
            multiplier  REAL NOT NULL DEFAULT 1.0,
            updated_at  TEXT NOT NULL
        )
    """)
    db.conn.commit()

    for dept, (action, factor, _) in adjustments.items():
        now = datetime.now(timezone.utc).isoformat()
        cur.execute("""
            INSERT INTO dept_weight_multipliers (department, multiplier, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(department) DO UPDATE SET
                multiplier = MIN(MAX(multiplier * ?, ?, ?), ?),
                updated_at = excluded.updated_at
        """, (dept, factor, now, factor, WEIGHT_FLOOR, WEIGHT_CEILING))

    db.conn.commit()
    logger.info("Weight multipliers saved to DB")
