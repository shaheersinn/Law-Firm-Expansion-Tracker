"""
Expansion Signal Scorer & Spike Detector
==========================================
For each (firm, department) pair:
  1. Aggregate weighted signals from current week
  2. Compare to 4-week rolling baseline using z-score
  3. Flag pairs with significant activity

Expansion Score Formula:
  score = Σ (signal_weight × min(dept_score, 20) × 0.1 + 1)

Spike detection:
  - z-score >= 1.5  → significant spike (flagged)
  - OR score >= 4.0 with no prior history → new signal
  - spike_ratio = current / baseline (for display)
"""

import logging
import statistics
from collections import defaultdict
from database.db import Database

logger = logging.getLogger("signals")

SIGNAL_WEIGHTS = {
    # ── Tier 1: Firm made a real financial or reputational commitment ──
    "bar_leadership":   3.5,
    "ranking":          3.0,
    "lateral_hire":     3.0,
    # ── Tier 2: Observable, verifiable activity ─────────────────────────
    "court_record":     2.5,
    "practice_page":    2.5,
    "job_posting":      2.0,
    "recruit_posting":  2.0,
    # ── Tier 3: Early indicators ─────────────────────────────────────────
    "press_release":    1.5,
    "bar_speaking":     1.5,
    "publication":      1.0,
    "bar_sponsorship":  1.0,
    "attorney_profile": 1.0,
    "bar_mention":      0.5,
    "website_snapshot": 0.0,
}

# Minimum score to report
EXPANSION_THRESHOLD = 3.5

# Signal confidence tier labels for display
CONFIDENCE_TIER = {
    "bar_leadership":   "Tier 1",
    "ranking":          "Tier 1",
    "lateral_hire":     "Tier 1",
    "court_record":     "Tier 2",
    "practice_page":    "Tier 2",
    "job_posting":      "Tier 2",
    "recruit_posting":  "Tier 2",
    "press_release":    "Tier 3",
    "bar_speaking":     "Tier 3",
    "publication":      "Tier 3",
    "bar_sponsorship":  "Tier 3",
    "attorney_profile": "Tier 3",
}


class ExpansionAnalyzer:
    def __init__(self, db: Database):
        self.db = db

    def analyze(self, new_signals: list[dict]) -> list[dict]:
        alerts = []

        grouped = defaultdict(list)
        for sig in new_signals:
            if sig.get("department") and sig["signal_type"] != "website_snapshot":
                grouped[(sig["firm_id"], sig["department"])].append(sig)

        for (firm_id, department), signals in grouped.items():
            current_score = self._score_signals(signals)

            # Get historical weekly scores for z-score calculation
            history = self._get_history(firm_id, department)
            baseline_mean = statistics.mean(history) if history else 0.0
            baseline_std  = statistics.stdev(history) if len(history) >= 2 else 0.0

            # Z-score spike detection
            z_score = 0.0
            if baseline_std > 0:
                z_score = (current_score - baseline_mean) / baseline_std

            is_spike = (
                (z_score >= 1.5) or
                (baseline_mean == 0 and current_score >= EXPANSION_THRESHOLD)
            )

            if not (is_spike or current_score >= EXPANSION_THRESHOLD):
                continue

            top_signals = sorted(
                signals,
                key=lambda s: SIGNAL_WEIGHTS.get(s["signal_type"], 0),
                reverse=True
            )[:3]

            # Highest-confidence signal type present
            top_tier = self._highest_tier(signals)

            alerts.append({
                "firm_id":          firm_id,
                "firm_name":        signals[0]["firm_name"],
                "department":       department,
                "expansion_score":  round(current_score, 2),
                "baseline_score":   round(baseline_mean, 2),
                "baseline_std":     round(baseline_std, 2),
                "z_score":          round(z_score, 2),
                "spike_ratio":      round(current_score / baseline_mean, 2) if baseline_mean > 0 else None,
                "signal_count":     len(signals),
                "signal_breakdown": self._breakdown(signals),
                "top_signals":      top_signals,
                "top_tier":         top_tier,
                "is_spike":         is_spike,
            })

        alerts.sort(key=lambda a: a["expansion_score"], reverse=True)
        logger.info(f"Expansion alerts generated: {len(alerts)}")
        return alerts

    def _score_signals(self, signals: list[dict]) -> float:
        total = 0.0
        for s in signals:
            weight    = SIGNAL_WEIGHTS.get(s["signal_type"], 0.5)
            dept_score = min(s.get("department_score", 1.0), 20.0)
            total += weight * (1 + dept_score * 0.1)
        return total

    def _get_history(self, firm_id: str, department: str) -> list[float]:
        all_scores = self.db.get_all_weekly_scores()
        return [
            r["score"] for r in all_scores
            if r["firm_id"] == firm_id and r["department"] == department
        ][-4:]  # last 4 weeks

    def _breakdown(self, signals: list[dict]) -> dict:
        counts = defaultdict(int)
        for s in signals:
            counts[s["signal_type"]] += 1
        return dict(counts)

    def _highest_tier(self, signals: list[dict]) -> str:
        tiers = [CONFIDENCE_TIER.get(s["signal_type"], "Tier 3") for s in signals]
        if "Tier 1" in tiers:
            return "Tier 1"
        if "Tier 2" in tiers:
            return "Tier 2"
        return "Tier 3"

    def detect_website_changes(self, new_signals: list[dict]) -> list[dict]:
        changes = []
        for snap in (s for s in new_signals if s["signal_type"] == "website_snapshot"):
            old_hash = self.db.get_last_website_hash(snap["firm_id"], snap["url"])
            if old_hash and old_hash != snap["body"]:
                changes.append({
                    "firm_id":   snap["firm_id"],
                    "firm_name": snap["firm_name"],
                    "url":       snap["url"],
                    "message":   f"Practice area page content changed — {snap['url']}",
                })
        return changes
