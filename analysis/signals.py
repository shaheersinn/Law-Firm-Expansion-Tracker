"""
Expansion signal scorer and spike detector.

For each (firm, department) pair, this module:
  1. Aggregates all raw signals from the current week
  2. Compares to the rolling 4-week baseline
  3. Scores expansion confidence using a weighted signal model
  4. Flags pairs with significant spikes as "expanding"

Expansion Score Formula:
  score = Σ (signal_weight × department_score × recency_multiplier)

Signal weights by type:
  lateral_hire    → 3.0  (strongest: firm paid to bring in expertise)
  practice_page   → 2.5  (firm invested in marketing a new area)
  job_posting     → 2.0  (firm is actively hiring in the area)
  press_release   → 1.5  (firm is publicizing work in the area)
  publication     → 1.0  (lawyers are writing about the area)
  attorney_profile→ 1.0  (bios updated with new practice area)
"""

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from database.db import Database

logger = logging.getLogger("signals")

SIGNAL_WEIGHTS = {
    # Original signals
    "lateral_hire":     3.0,
    "practice_page":    2.5,
    "job_posting":      2.0,
    "press_release":    1.5,
    "publication":      1.0,
    "attorney_profile": 1.0,
    "website_snapshot": 0.0,
    # Enhanced signals
    "bar_leadership":   3.5,  # section chair = firm asserting leadership
    "ranking":          3.0,  # Chambers/Legal500 = third-party validation
    "court_record":     2.5,  # CanLII = actual filed cases
    "recruit_posting":  2.0,  # student hiring = planned 12-18mo expansion
    "bar_speaking":     1.5,  # presenting at bar = building profile
    "bar_sponsorship":  1.0,  # sponsoring bar event = BD investment
    "bar_mention":      0.5,
}

# Minimum expansion score to flag as "expanding"
EXPANSION_THRESHOLD = 4.0

# Spike: current week score is at least this multiple of baseline average
SPIKE_MULTIPLIER = 1.8


class ExpansionAnalyzer:
    def __init__(self, db: Database):
        self.db = db
        self._weights = self._load_weights()

    def _load_weights(self) -> dict:
        """Load learned signal-type weights from DB, fall back to static defaults."""
        weights = dict(SIGNAL_WEIGHTS)
        try:
            cur = self.db.conn.execute(
                "SELECT signal_type, weight FROM signal_type_weights"
            )
            for sig_type, w in cur.fetchall():
                weights[sig_type] = w
        except Exception:
            pass  # table may not exist yet — use static defaults
        return weights

    def analyze(self, new_signals: list[dict]) -> list[dict]:
        """
        Given new signals collected this run, return a list of expansion alerts:
        firms/departments showing significant growth signals.
        """
        alerts = []

        # Group new signals by (firm_id, department)
        grouped = defaultdict(list)
        for signal in new_signals:
            if signal["department"] and signal["signal_type"] != "website_snapshot":
                key = (signal["firm_id"], signal["department"])
                grouped[key].append(signal)

        # Score each group
        for (firm_id, department), signals in grouped.items():
            current_score = self._score_signals(signals)

            # Get baseline: average weekly score over past 4 weeks
            baseline = self.db.get_weekly_baseline(firm_id, department, weeks=4)

            is_spike = False
            if baseline > 0:
                is_spike = current_score >= (baseline * SPIKE_MULTIPLIER)
            else:
                # No history — flag if score is meaningful on its own
                is_spike = current_score >= EXPANSION_THRESHOLD

            if is_spike or current_score >= EXPANSION_THRESHOLD:
                top_signals = sorted(signals, key=lambda s: SIGNAL_WEIGHTS.get(s["signal_type"], 0), reverse=True)[:3]
                alerts.append({
                    "firm_id": firm_id,
                    "firm_name": signals[0]["firm_name"],
                    "department": department,
                    "expansion_score": round(current_score, 2),
                    "baseline_score": round(baseline, 2),
                    "spike_ratio": round(current_score / baseline, 2) if baseline > 0 else None,
                    "signal_count": len(signals),
                    "signal_breakdown": self._breakdown(signals),
                    "top_signals": top_signals,
                    "is_spike": is_spike,
                })

        # Sort by expansion score descending
        alerts.sort(key=lambda a: a["expansion_score"], reverse=True)
        logger.info(f"Expansion alerts generated: {len(alerts)}")
        return alerts

    def _score_signals(self, signals: list[dict]) -> float:
        total = 0.0
        for s in signals:
            weight = self._weights.get(s["signal_type"], 0.5)
            dept_score = min(s.get("department_score", 1.0), 20.0)  # cap outliers
            total += weight * (1 + dept_score * 0.1)
        return total

    def _breakdown(self, signals: list[dict]) -> dict:
        counts = defaultdict(int)
        for s in signals:
            counts[s["signal_type"]] += 1
        return dict(counts)

    def detect_website_changes(self, new_signals: list[dict]) -> list[dict]:
        """Detect firms whose practice area pages have changed content."""
        changes = []
        snapshots = [s for s in new_signals if s["signal_type"] == "website_snapshot"]

        for snap in snapshots:
            old_hash = self.db.get_last_website_hash(snap["firm_id"], snap["url"])
            new_hash = snap["body"]

            if old_hash and old_hash != new_hash:
                changes.append({
                    "firm_id": snap["firm_id"],
                    "firm_name": snap["firm_name"],
                    "url": snap["url"],
                    "change_type": "practice_page_updated",
                    "message": f"Practice area page content changed at {snap['url']}",
                })
                logger.info(f"Website change detected: [{snap['firm_name']}] {snap['url']}")

        return changes
