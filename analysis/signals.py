"""
ExpansionAnalyzer — aggregates signals, computes expansion scores,
runs z-score spike detection, and detects website changes.
"""

import math
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)

# Minimum score to surface as an alert
MIN_ALERT_SCORE = 3.5

# Signal type base weights (tunable — also overridden by dept_score)
SIGNAL_WEIGHTS = {
    "lateral_hire":    3.0,
    "ranking":         3.0,
    "bar_leadership":  3.5,
    "court_record":    2.5,
    "practice_page":   2.5,
    "job_posting":     2.0,
    "recruit_posting": 2.0,
    "bar_speaking":    1.5,
    "bar_sponsorship": 2.0,
    "press_release":   1.5,
    "publication":     1.0,
    "website_snapshot": 0.0,  # scored only on change
}

# Department display emojis
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
    "Competition":         "⚡",
    "Healthcare":          "🏥",
    "Immigration":         "🌐",
    "Infrastructure":      "🏛️",
}


class ExpansionAnalyzer:
    def __init__(self, db):
        self.db = db

    def analyze(self, signals: list[dict]) -> list[dict]:
        """
        Groups signals by (firm, department), computes expansion scores,
        applies z-score spike detection, returns sorted alert list.
        """
        grouped = defaultdict(list)
        for s in signals:
            if s.get("signal_type") == "website_snapshot":
                continue
            key = (s["firm_id"], s["firm_name"], s.get("department", "Corporate/M&A"))
            grouped[key].append(s)

        alerts = []
        for (firm_id, firm_name, department), firm_signals in grouped.items():
            score, breakdown = self._compute_score(firm_signals)
            if score < MIN_ALERT_SCORE:
                continue

            # Z-score vs 4-week baseline
            historical = self.db.get_historical_scores(firm_id, department, weeks=4)
            z_score, baseline_mult = _z_score(score, historical)

            alerts.append({
                "firm_id":          firm_id,
                "firm_name":        firm_name,
                "department":       department,
                "dept_emoji":       DEPT_EMOJI.get(department, "🏛"),
                "expansion_score":  round(score, 1),
                "signal_count":     len(firm_signals),
                "signal_breakdown": breakdown,
                "signals":          firm_signals,
                "z_score":          round(z_score, 2),
                "baseline_mult":    round(baseline_mult, 1),
                "is_spike":         z_score >= 1.5 or (not historical and score >= MIN_ALERT_SCORE),
            })

        alerts.sort(key=lambda x: x["expansion_score"], reverse=True)
        return alerts

    def _compute_score(self, signals: list[dict]) -> tuple[float, dict]:
        breakdown = defaultdict(int)
        total = 0.0

        for s in signals:
            base_weight = SIGNAL_WEIGHTS.get(s.get("signal_type", "publication"), 1.0)
            dept_score  = float(s.get("dept_score", s.get("department_score", 0)))
            # Use the higher of base_weight or dept_score (capped at 5.0)
            score = min(max(base_weight, dept_score), 5.0)
            total += score
            breakdown[s["signal_type"]] = breakdown[s["signal_type"]] + 1

        return total, dict(breakdown)

    def detect_website_changes(self, new_signals: list[dict]) -> list[dict]:
        """
        Compares website_snapshot signals against stored hashes.
        Returns list of changed pages as high-weight signals.
        """
        changes = []
        for s in new_signals:
            if s.get("signal_type") != "website_snapshot":
                continue

            firm_id = s["firm_id"]
            url     = s.get("url", "")
            new_hash = s.get("body", "")

            stored = self.db.get_website_hash(firm_id, url)
            if stored is None:
                # First time seeing — store and skip
                continue
            if stored != new_hash:
                changes.append({
                    "firm_id":   firm_id,
                    "firm_name": s["firm_name"],
                    "url":       url,
                    "dept":      s.get("department", ""),
                })

        return changes


# ── Statistics helpers ────────────────────────────────────────────────────

def _z_score(value: float, historical: list[float]) -> tuple[float, float]:
    if not historical:
        return 0.0, 1.0

    n   = len(historical)
    mu  = sum(historical) / n
    if n < 2:
        sigma = mu * 0.3 or 1.0
    else:
        variance = sum((x - mu) ** 2 for x in historical) / (n - 1)
        sigma = math.sqrt(variance) or (mu * 0.3 or 1.0)

    z = (value - mu) / sigma
    mult = value / mu if mu > 0 else 1.0
    return z, mult
