"""
Expansion signal analyzer.
- Groups signals by (firm, department)
- Applies signal-type weights
- Z-score spike detection vs 4-week rolling baseline
- Website change detection
"""

import hashlib
import logging
import statistics
from collections import defaultdict

logger = logging.getLogger("analysis.signals")

# Signal type base weights
SIGNAL_WEIGHTS = {
    "lateral_hire":      3.0,
    "bar_leadership":    3.5,
    "ranking":           3.0,
    "office_lease":      3.0,
    "alumni_hire":       2.5,
    "court_record":      2.5,
    "practice_page":     2.5,
    "job_posting":       2.0,
    "recruit_posting":   2.0,
    "deal_record":       2.0,
    "ip_filing":         2.0,
    "diversity_signal":  1.8,
    "thought_leadership": 1.5,
    "bar_speaking":      1.5,
    "bar_sponsorship":   1.5,
    "press_release":     1.5,
    "publication":       1.0,
    "website_snapshot":  0.0,   # used for change detection only
}

SPIKE_Z_THRESHOLD   = 1.5
SPIKE_MIN_SCORE     = 3.5


class ExpansionAnalyzer:
    def __init__(self, db):
        self.db = db

    def analyze(self, signals: list[dict]) -> list[dict]:
        """
        Returns list of expansion-alert dicts, one per (firm, department),
        sorted by expansion_score descending.
        """
        # Group by (firm_id, department)
        groups: dict[tuple, list] = defaultdict(list)
        for s in signals:
            if s.get("signal_type") == "website_snapshot":
                continue
            key = (s["firm_id"], s.get("department", "Corporate/M&A"))
            groups[key].append(s)

        alerts = []
        for (firm_id, dept), sigs in groups.items():
            score = self._score(sigs)
            baseline = self.db.get_baseline(firm_id, dept)
            z = self._zscore(score, baseline)
            is_spike = (z >= SPIKE_Z_THRESHOLD) or (not baseline and score >= SPIKE_MIN_SCORE)

            if not is_spike:
                continue

            breakdown = defaultdict(int)
            for s in sigs:
                breakdown[s["signal_type"]] += 1

            # Pick 3 most relevant signals as preview bullets
            top = sorted(sigs, key=lambda x: SIGNAL_WEIGHTS.get(x["signal_type"], 1.0), reverse=True)[:3]

            alerts.append({
                "firm_id":         firm_id,
                "firm_name":       sigs[0]["firm_name"],
                "department":      dept,
                "expansion_score": round(score, 2),
                "z_score":         round(z, 2),
                "signal_count":    len(sigs),
                "signal_breakdown": dict(breakdown),
                "top_signals":     top,
                "is_new_baseline": not bool(baseline),
            })

        alerts.sort(key=lambda x: x["expansion_score"], reverse=True)
        return alerts

    def detect_website_changes(self, new_signals: list[dict]) -> list[dict]:
        """
        Compares website snapshot hashes from new_signals against stored hashes.
        Returns list of changed-page dicts.
        """
        changes = []
        for s in new_signals:
            if s.get("signal_type") != "website_snapshot":
                continue
            prev = self.db.get_website_hash(s["firm_id"], s["url"])
            curr = hashlib.sha256(s.get("body", "").encode()).hexdigest()
            if prev and prev != curr:
                changes.append({
                    "firm_id":   s["firm_id"],
                    "firm_name": s["firm_name"],
                    "url":       s["url"],
                    "title":     s["title"],
                })
        return changes

    # ------------------------------------------------------------------ #
    #  Helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _score(signals: list[dict]) -> float:
        total = 0.0
        for s in signals:
            w = SIGNAL_WEIGHTS.get(s.get("signal_type", "publication"), 1.0)
            dept_score = s.get("department_score", 1.0)
            total += w * min(dept_score, 5.0)
        return total

    @staticmethod
    def _zscore(value: float, baseline: list[float]) -> float:
        if len(baseline) < 2:
            return 0.0
        mean = statistics.mean(baseline)
        stdev = statistics.stdev(baseline)
        if stdev == 0:
            return 0.0
        return (value - mean) / stdev
