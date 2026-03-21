"""
scoring/aggregator.py
──────────────────────
Multi-signal aggregation and scoring engine.

Combines signals from all 5 strategies into a unified firm-level score.
Applies time-decay so fresh signals outweigh stale ones.
Produces a ranked leaderboard of hiring opportunities.

Scoring formula:
  firm_score = Σ (signal_weight × recency_decay × corroboration_boost)

Recency decay: e^(-λt), λ = 0.1/day (half-life ≈ 7 days)
Corroboration boost: if ≥2 independent strategy types agree → ×1.3
"""

import json
import math
import logging
from datetime import datetime, date, timedelta
from collections import defaultdict

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config_calgary import CALGARY_FIRMS, FIRM_BY_ID, SIGNAL_WEIGHTS
from database.db import get_conn

log = logging.getLogger(__name__)

DECAY_LAMBDA          = 0.10   # exponential decay rate (per day)
CORROBORATION_BOOST   = 1.30   # applied when 2+ strategy types agree
RECENCY_WINDOW_DAYS   = 30     # only score signals from the last 30 days


# ─── Strategy type groupings ──────────────────────────────────────────────────

STRATEGY_GROUPS = {
    "litigation": ["canlii_appearance_spike", "canlii_new_large_file"],
    "corporate":  ["sedar_major_deal", "sedar_counsel_named"],
    "turnover":   ["linkedin_turnover_detected", "linkedin_new_vacancy"],
    "hireback":   ["lsa_student_not_retained", "lsa_retention_gap"],
    "spillage":   ["biglaw_spillage_predicted"],
    "baseline":   ["job_posting", "lateral_hire", "ranking", "press_release"],
}

# Build reverse map: signal_type → strategy_group
SIGNAL_TO_STRATEGY = {}
for group, types in STRATEGY_GROUPS.items():
    for t in types:
        SIGNAL_TO_STRATEGY[t] = group


def recency_decay(detected_at_str: str) -> float:
    """e^(-λ × days_ago), so today = 1.0, 7 days ago ≈ 0.50, 30 days ago ≈ 0.05."""
    try:
        detected = datetime.fromisoformat(detected_at_str)
        days_ago = (datetime.utcnow() - detected).days
        return math.exp(-DECAY_LAMBDA * max(0, days_ago))
    except Exception:
        return 0.5


def compute_firm_scores() -> list[dict]:
    """
    Returns a ranked list of firms with their aggregated opportunity score,
    signal breakdown, and recommended outreach urgency.
    """
    conn     = get_conn()
    cutoff   = (date.today() - timedelta(days=RECENCY_WINDOW_DAYS)).isoformat()

    rows = conn.execute("""
        SELECT firm_id, signal_type, weight, detected_at, title, source_url
        FROM signals
        WHERE date(detected_at) >= ?
        ORDER BY detected_at DESC
    """, (cutoff,)).fetchall()
    conn.close()

    # Group signals by firm
    firm_signals: dict[str, list] = defaultdict(list)
    for r in rows:
        firm_signals[r["firm_id"]].append(dict(r))

    results = []
    for firm_id, signals in firm_signals.items():
        firm    = FIRM_BY_ID.get(firm_id, {"name": firm_id, "tier": "?"})
        score   = 0.0
        groups_seen = set()
        breakdown   = defaultdict(list)

        for sig in signals:
            decay     = recency_decay(sig["detected_at"])
            raw_score = sig["weight"] * decay
            score    += raw_score
            group     = SIGNAL_TO_STRATEGY.get(sig["signal_type"], "other")
            groups_seen.add(group)
            breakdown[group].append({
                "type":   sig["signal_type"],
                "weight": sig["weight"],
                "decay":  round(decay, 3),
                "title":  sig["title"][:80],
                "url":    sig["source_url"],
            })

        # Corroboration boost
        if len(groups_seen) >= 2:
            score *= CORROBORATION_BOOST

        # Tier multiplier (boutiques and mid-size have lower baseline — higher relative signal)
        tier_mult = {"boutique": 1.2, "mid": 1.1, "big": 1.0}.get(firm.get("tier", "big"), 1.0)
        score    *= tier_mult

        # Determine urgency
        has_same_day = any(
            sig["signal_type"] in ["biglaw_spillage_predicted", "sedar_major_deal",
                                   "linkedin_turnover_detected"]
            for sig in signals
        )
        urgency = "🚨 Same-Day" if has_same_day else ("⚡ This Week" if score > 8 else "📅 This Month")

        results.append({
            "firm_id":      firm_id,
            "firm_name":    firm.get("name", firm_id),
            "tier":         firm.get("tier", "?"),
            "focus":        ", ".join(firm.get("focus", [])),
            "score":        round(score, 2),
            "signal_count": len(signals),
            "strategies":   sorted(groups_seen),
            "corroborated": len(groups_seen) >= 2,
            "urgency":      urgency,
            "breakdown":    dict(breakdown),
            "top_signal":   max(signals, key=lambda s: s["weight"])["title"],
        })

    results.sort(key=lambda x: x["score"], reverse=True)
    return results


def print_leaderboard(results: list[dict], top_n: int = 15):
    print("\n" + "═" * 72)
    print("🏛  CALGARY LAW FIRM HIRING OPPORTUNITY LEADERBOARD")
    print(f"    Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("═" * 72)

    for i, r in enumerate(results[:top_n], 1):
        corr_tag = " ✅ CORROBORATED" if r["corroborated"] else ""
        strats   = " · ".join(r["strategies"])
        print(f"\n{i:>2}. {r['firm_name']}  [{r['tier'].upper()}]")
        print(f"    Score: {r['score']:.1f}  |  {r['urgency']}  |  Signals: {r['signal_count']}{corr_tag}")
        print(f"    Strategies: {strats}")
        print(f"    Focus: {r['focus']}")
        print(f"    Top signal: {r['top_signal'][:70]}")

    print("\n" + "═" * 72)


def export_leaderboard_json(results: list[dict], path: str = "reports/leaderboard.json"):
    import json, pathlib
    pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    log.info("[Aggregator] Leaderboard exported to %s", path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scores = compute_firm_scores()
    print_leaderboard(scores)
    export_leaderboard_json(scores)
