"""
ExpansionAnalyzer — signal scoring and alert generation.

BUG-2  FIX: reads dept_weight_multipliers from DB (written by evolution.py)
             and multiplies scores — evolution weights NOW actually affect scoring.
BUG-4  FIX: MIN_ALERT_SCORE raised to 5.0 + MIN_SOURCE_TYPES=2 gate.
             Eliminates 26/26 all-noise scenario. Requires at least 2 distinct
             scraper types before an alert fires.
"""

import math
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)

# Minimum weighted score — raised from 3.5 to eliminate single-scraper noise
MIN_ALERT_SCORE  = 5.0

# Require signals from at least this many distinct scraper types
MIN_SOURCE_TYPES = 2

SIGNAL_WEIGHTS = {
    "lateral_hire":     4.0,
    "ranking":          3.5,
    "bar_leadership":   3.5,
    "court_record":     3.0,
    "practice_page":    3.0,
    "job_posting":      2.5,
    "recruit_posting":  2.5,
    "bar_speaking":     2.0,
    "bar_sponsorship":  2.0,
    "press_release":    2.0,
    "publication":      1.5,
    "website_snapshot": 0.0,
}

DEPT_EMOJI = {
    "Corporate/M&A":       "🏢",
    "Private Equity":      "💼",
    "Capital Markets":     "📈",
    "Litigation":          "⚖️",
    "Restructuring":       "🔄",
    "Real Estate":         "🏗️",
    "Tax":                 "🧾",
    "Employment":          "👥",
    "IP":                  "💡",
    "Data Privacy":        "🔒",
    "ESG":                 "🌿",
    "Energy":              "⚡",
    "Financial Services":  "🏦",
    "Competition":         "🔍",
    "Healthcare":          "🏥",
    "Immigration":         "🌐",
    "Infrastructure":      "🏛️",
}


class ExpansionAnalyzer:
    def __init__(self, db):
        self.db = db
        self._mult_cache: dict | None = None   # loaded once per run

    def _dept_multipliers(self) -> dict:
        """BUG-2 FIX: load evolution-adjusted weights from DB."""
        if self._mult_cache is not None:
            return self._mult_cache
        try:
            cur = self.db.conn.execute(
                "SELECT department, multiplier FROM dept_weight_multipliers"
            )
            self._mult_cache = {r["department"]: r["multiplier"] for r in cur.fetchall()}
            if self._mult_cache:
                logger.info(f"Loaded {len(self._mult_cache)} evolved dept multipliers from DB")
        except Exception:
            self._mult_cache = {}
        return self._mult_cache

    def analyze(self, signals: list[dict]) -> list[dict]:
        """
        Groups by (firm, dept), scores, gates on source diversity, z-scores.
        Returns sorted alert list.
        """
        multipliers = self._dept_multipliers()

        grouped: dict = defaultdict(list)
        for s in signals:
            if s.get("signal_type") == "website_snapshot":
                continue
            key = (s["firm_id"], s["firm_name"], s.get("department", "Corporate/M&A"))
            grouped[key].append(s)

        alerts = []
        for (firm_id, firm_name, dept), firm_sigs in grouped.items():

            # BUG-4 FIX: gate on source diversity before even scoring
            source_types = {s.get("signal_type") for s in firm_sigs}
            if len(source_types) < MIN_SOURCE_TYPES:
                continue

            score, breakdown = self._score(firm_sigs, dept, multipliers)
            if score < MIN_ALERT_SCORE:
                continue

            historical     = self.db.get_historical_scores(firm_id, dept, weeks=4)
            z, baseline_m  = _z_score(score, historical)

            alerts.append({
                "firm_id":          firm_id,
                "firm_name":        firm_name,
                "department":       dept,
                "dept_emoji":       DEPT_EMOJI.get(dept, "🏛"),
                "expansion_score":  round(score, 1),
                "signal_count":     len(firm_sigs),
                "signal_breakdown": breakdown,
                "source_types":     sorted(source_types),
                "signals":          sorted(
                    firm_sigs, key=lambda s: s.get("dept_score", 0), reverse=True
                )[:5],
                "z_score":          round(z, 2),
                "baseline_mult":    round(baseline_m, 1),
                "is_spike":         z >= 1.5,
            })

        alerts.sort(key=lambda x: x["expansion_score"], reverse=True)
        return alerts

    def _score(self, signals, dept, multipliers):
        """BUG-2 FIX: applies evolution multiplier for this department."""
        breakdown: dict = defaultdict(int)
        total = 0.0

        for s in signals:
            base   = SIGNAL_WEIGHTS.get(s.get("signal_type", "publication"), 1.0)
            scored = float(s.get("dept_score", s.get("department_score", 0)))
            total += min(max(base, scored), 5.0)
            breakdown[s["signal_type"]] += 1

        # Apply evolved multiplier (1.0 = no change; updated by evolution.py)
        mult  = multipliers.get(dept, 1.0)
        total = round(total * mult, 2)
        return total, dict(breakdown)

    def detect_website_changes(self, new_signals):
        changes = []
        for s in new_signals:
            if s.get("signal_type") != "website_snapshot":
                continue
            firm_id = s["firm_id"]
            url     = s.get("url", "")
            stored  = self.db.get_website_hash(firm_id, url)
            if stored is None:
                continue
            if stored != s.get("body", ""):
                changes.append({
                    "firm_id":   firm_id,
                    "firm_name": s["firm_name"],
                    "url":       url,
                    "dept":      s.get("department", ""),
                })
        return changes


def _z_score(value, historical):
    if not historical:
        return 0.0, 1.0
    n  = len(historical)
    mu = sum(historical) / n
    if n < 2:
        sigma = mu * 0.3 or 1.0
    else:
        var   = sum((x - mu) ** 2 for x in historical) / (n - 1)
        sigma = math.sqrt(var) or (mu * 0.3 or 1.0)
    z    = (value - mu) / sigma
    mult = value / mu if mu > 0 else 1.0
    return z, mult
