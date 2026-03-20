"""
Expansion signal analyzer — v2
================================
Improvements over v1:
  - Time-decay weighting: recent signals score higher than 3-week-old ones
  - Confidence-adjusted scoring: uses stored confidence col for precision
  - Sector momentum detection: flags departments with 3+ firms spiking
  - Velocity-aware z-score: uses sample stdev only when n >= 3
  - Smarter baseline fallback for brand-new (firm, dept) pairs
"""

import hashlib
import logging
import math
import statistics
from collections import defaultdict
from datetime import datetime, timezone

logger = logging.getLogger("analysis.signals")

# ── Signal type base weights ───────────────────────────────────────────────────
SIGNAL_WEIGHTS: dict[str, float] = {
    "lateral_hire":       3.0,
    "bar_leadership":     3.5,
    "ranking":            3.0,
    "office_lease":       3.0,
    "alumni_hire":        2.5,
    "court_record":       2.5,
    "practice_page":      2.5,
    "job_posting":        2.0,
    "recruit_posting":    2.0,
    "deal_record":        2.0,
    "ip_filing":          2.0,
    "diversity_signal":   1.8,
    "thought_leadership": 1.5,
    "bar_speaking":       1.5,
    "bar_sponsorship":    1.5,
    "press_release":      1.5,
    "publication":        1.0,
    "website_snapshot":   0.0,   # used for change detection only
}

SPIKE_Z_THRESHOLD    = 1.5    # z-score above mean to flag as spike
SPIKE_MIN_SCORE      = 3.5    # minimum raw score to flag with no baseline
EXPANSION_THRESHOLD  = SPIKE_MIN_SCORE
SECTOR_MIN_FIRMS     = 3      # number of distinct firms needed for sector momentum
DECAY_HALF_LIFE_DAYS = 10.0   # signals lose 50% weight every 10 days


class ExpansionAnalyzer:
    def __init__(self, db):
        self.db = db

    # ------------------------------------------------------------------ #
    #  Primary analysis
    # ------------------------------------------------------------------ #

    def analyze(self, signals: list[dict]) -> list[dict]:
        """
        Returns expansion-alert dicts, one per (firm, department),
        sorted by expansion_score descending.
        Includes sector_momentum field listing departments trending
        industry-wide across 3+ firms.
        """
        now = datetime.now(timezone.utc)

        # Group by (firm_id, department)
        groups: dict[tuple, list] = defaultdict(list)
        for s in signals:
            if s.get("signal_type") == "website_snapshot":
                continue
            key = (s["firm_id"], s.get("department") or "Corporate/M&A")
            groups[key].append(s)

        alerts = []
        for (firm_id, dept), sigs in groups.items():
            score    = self._score(sigs, now)
            baseline = self.db.get_baseline(firm_id, dept)
            z        = self._zscore(score, baseline)
            is_spike = (z >= SPIKE_Z_THRESHOLD) or (not baseline and score >= SPIKE_MIN_SCORE)

            if not is_spike:
                continue

            breakdown = defaultdict(int)
            for s in sigs:
                breakdown[s["signal_type"]] += 1

            top = sorted(
                sigs,
                key=lambda x: (
                    SIGNAL_WEIGHTS.get(x.get("signal_type", ""), 1.0)
                    * x.get("confidence", 0.5)
                ),
                reverse=True,
            )[:3]

            # Velocity arrows
            this_w, last_w = self.db.get_signal_velocity(firm_id, dept)
            velocity_arrow = _velocity_arrow(this_w, last_w)

            alerts.append({
                "firm_id":          firm_id,
                "firm_name":        sigs[0]["firm_name"],
                "department":       dept,
                "expansion_score":  round(score, 2),
                "z_score":          round(z, 2),
                "signal_count":     len(sigs),
                "signal_breakdown": dict(breakdown),
                "top_signals":      top,
                "is_new_baseline":  not bool(baseline),
                "velocity_arrow":   velocity_arrow,
                "this_week_count":  this_w,
                "last_week_count":  last_w,
            })

        alerts.sort(key=lambda x: x["expansion_score"], reverse=True)

        # Annotate each alert with sector momentum flag
        sector_momentum = self._sector_momentum(alerts)
        for a in alerts:
            a["sector_momentum"] = a["department"] in sector_momentum

        return alerts

    # ------------------------------------------------------------------ #
    #  Sector momentum
    # ------------------------------------------------------------------ #

    def _sector_momentum(self, alerts: list[dict]) -> set[str]:
        """
        Returns set of department names where SECTOR_MIN_FIRMS distinct
        firms are all spiking this run — indicates industry-wide movement.
        """
        dept_firms: dict[str, set] = defaultdict(set)
        for a in alerts:
            dept_firms[a["department"]].add(a["firm_id"])
        return {
            dept
            for dept, firms in dept_firms.items()
            if len(firms) >= SECTOR_MIN_FIRMS
        }

    # ------------------------------------------------------------------ #
    #  Website changes
    # ------------------------------------------------------------------ #

    def detect_website_changes(self, new_signals: list[dict]) -> list[dict]:
        """Compares website snapshot hashes against stored hashes."""
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
    #  Scoring helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _score(signals: list[dict], now: datetime | None = None) -> float:
        """
        Weighted score with time-decay.
        Each signal's contribution = base_weight × dept_score_cap × confidence × decay_factor
        where decay_factor = e^(-λ × age_days), λ = ln(2) / DECAY_HALF_LIFE_DAYS
        """
        if now is None:
            now = datetime.now(timezone.utc)

        lam   = math.log(2) / DECAY_HALF_LIFE_DAYS
        total = 0.0

        for s in signals:
            base_w     = SIGNAL_WEIGHTS.get(s.get("signal_type", "publication"), 1.0)
            dept_score = min(s.get("department_score") or 1.0, 5.0)
            confidence = s.get("confidence") or 0.5

            # Compute age in days from scraped_at
            try:
                scraped = datetime.fromisoformat(s["scraped_at"].rstrip("Z"))
                if scraped.tzinfo is None:
                    scraped = scraped.replace(tzinfo=timezone.utc)
                age_days = max((now - scraped).total_seconds() / 86400, 0)
            except Exception:
                age_days = 0

            decay = math.exp(-lam * age_days)
            total += base_w * dept_score * confidence * decay

        return total

    @staticmethod
    def _zscore(value: float, baseline: list[float]) -> float:
        if len(baseline) < 2:
            return 0.0
        mean = statistics.mean(baseline)
        # Use sample stdev only when n >= 3, else population stdev (less volatile)
        stdev = (statistics.stdev(baseline) if len(baseline) >= 3
                 else statistics.pstdev(baseline))
        if stdev < 0.01:
            return 0.0
        return (value - mean) / stdev


# ── Module-level helpers ───────────────────────────────────────────────────────

def _velocity_arrow(this_week: int, last_week: int) -> str:
    """Returns an emoji arrow based on week-over-week signal velocity."""
    if last_week == 0:
        return "🆕" if this_week > 0 else "→"
    ratio = this_week / last_week
    if ratio >= 1.5:
        return "↑↑"
    if ratio >= 1.1:
        return "↑"
    if ratio <= 0.67:
        return "↓↓"
    if ratio <= 0.9:
        return "↓"
    return "→"
