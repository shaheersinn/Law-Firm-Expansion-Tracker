"""
dashboard/generator.py  (v3 — 90-day window, verification-aware)
═══════════════════════════════════════════════════════════════════
Changes from v2:
  • Uses get_verified_signals_for_dashboard(days=90) instead of
    get_all_signals_for_dashboard(days=30) — 3x more history
  • No longer truncates signals to 50 — all verified signals included
  • Passes confidence_score and low_confidence to dashboard JSON
  • Leaderboard excludes low_confidence signals from scoring by default
  • Urgency map expanded to cover all v5 signal types
"""
import json, pathlib, logging, sys, os, math
from datetime import datetime
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config_calgary import FIRM_BY_ID, REPORT_OUTPUT_DIR, DASHBOARD_OUTPUT

log = logging.getLogger(__name__)

DECAY_LAMBDA = 0.07   # slower decay over 90-day window

URGENCY_MAP = {
    "sedar_major_deal":            "today",
    "biglaw_spillage_predicted":   "today",
    "linkedin_turnover_detected":  "today",
    "linkedin_new_vacancy":        "today",
    "ica_review_announced":        "today",
    "counterfactual_conflict_free":"today",
    "canlii_appearance_spike":     "week",
    "canlii_new_large_file":       "week",
    "sedar_counsel_named":         "week",
    "lsa_retention_gap":           "3days",
    "lsa_student_not_retained":    "3days",
    "aer_hearing_upcoming":        "week",
    "competition_bureau":          "week",
    "new_court_filing":            "week",
    "fiscal_pressure_incoming":    "week",
    "macro_ma_wave_incoming":      "month",
    "macro_demand_surge":          "month",
    "sec_edgar_filing":            "month",
    "job_posting":                 "month",
    "lateral_hire":                "month",
    "ranking":                     "month",
    "web_signal":                  "month",
}

STRATEGY_GROUPS = {
    "litigation":  ["canlii_appearance_spike","canlii_new_large_file","new_court_filing"],
    "corporate":   ["sedar_major_deal","sedar_counsel_named","ica_review_announced"],
    "turnover":    ["linkedin_turnover_detected","linkedin_new_vacancy"],
    "hireback":    ["lsa_student_not_retained","lsa_retention_gap"],
    "spillage":    ["biglaw_spillage_predicted","counterfactual_conflict_free"],
    "macro":       ["macro_ma_wave_incoming","macro_demand_surge","macro_demand_collapse"],
    "regulatory":  ["aer_hearing_upcoming","competition_bureau","fiscal_pressure_incoming","sec_edgar_filing"],
    "jobs":        ["job_posting","lateral_hire","web_signal"],
    "baseline":    ["ranking"],
}
SIG_TO_STRAT = {t: g for g, ts in STRATEGY_GROUPS.items() for t in ts}


def _decay(ts: str) -> float:
    try:
        days = (datetime.utcnow() - datetime.fromisoformat(ts)).days
        return math.exp(-DECAY_LAMBDA * max(0, days))
    except Exception:
        return 0.5


def build_leaderboard(signals: list) -> list:
    """Score firms. High-confidence signals get full weight; low-confidence get 50%."""
    firm_sigs: dict = defaultdict(list)
    for s in signals:
        if s.get("firm_id") != "market":
            firm_sigs[s["firm_id"]].append(s)

    rows = []
    for fid, sigs in firm_sigs.items():
        firm  = FIRM_BY_ID.get(fid, {"name": fid, "tier": "?", "focus": []})
        score = 0.0
        strats: set = set()

        for s in sigs:
            conf       = s.get("confidence_score") or 0.6
            is_low     = s.get("low_confidence", 0)
            conf_mult  = 0.5 if is_low else 1.0
            score     += s["weight"] * _decay(s["detected_at"]) * conf * conf_mult
            strats.add(SIG_TO_STRAT.get(s["signal_type"], "other"))

        if len(strats) >= 2:
            score *= 1.30

        score *= {"boutique": 1.2, "mid": 1.1, "big": 1.0}.get(
            firm.get("tier", "big"), 1.0)

        # Best signal = highest weight AND verified
        verified_sigs = [s for s in sigs if not s.get("low_confidence")]
        top_pool = verified_sigs if verified_sigs else sigs
        top = max(top_pool, key=lambda s: s["weight"])

        rows.append({
            "firm_id":        fid,
            "firm_name":      firm.get("name", fid),
            "tier":           firm.get("tier", "?"),
            "focus":          ", ".join(firm.get("focus", [])),
            "score":          round(score, 2),
            "signal_count":   len(sigs),
            "verified_count": len([s for s in sigs if not s.get("low_confidence")]),
            "strategies":     sorted(strats),
            "corroborated":   len(strats) >= 2,
            "urgency":        URGENCY_MAP.get(top["signal_type"], "month"),
            "top_signal":     top.get("title", "")[:80],
            "top_confidence": top.get("confidence_score"),
        })

    return sorted(rows, key=lambda x: x["score"], reverse=True)


def generate_dashboard():
    from database.signal_verifier import get_verified_signals_for_dashboard

    raw   = get_verified_signals_for_dashboard(days=90)
    lb    = build_leaderboard(raw)

    # Spillage graph
    from database.db import get_spillage_graph
    edges = get_spillage_graph()
    spill = [
        {
            "boutique": FIRM_BY_ID.get(e["boutique_id"], {}).get("name", e["boutique_id"]),
            "biglaw":   FIRM_BY_ID.get(e["biglaw_id"], {}).get("name", e["biglaw_id"]),
            "count":    e["co_appearances"],
        }
        for e in edges[:15]
    ]

    # All signals (not truncated), enriched for UI
    enriched = []
    for s in raw:
        firm = FIRM_BY_ID.get(s.get("firm_id", ""), {})
        enriched.append({
            **s,
            "firm_name":        firm.get("name", s.get("firm_id", "")),
            "tier":             firm.get("tier", "?"),
            "focus":            " · ".join(firm.get("focus", [])),
            "urgency":          URGENCY_MAP.get(s.get("signal_type", ""), "month"),
            "confidence_score": s.get("confidence_score"),
            "low_confidence":   bool(s.get("low_confidence")),
            "verified":         not bool(s.get("low_confidence")),
        })

    # Stats for dashboard header
    verified_count = len([s for s in enriched if s["verified"]])
    low_conf_count = len(enriched) - verified_count

    payload = {
        "generated_at":   datetime.utcnow().isoformat() + "Z",
        "lookback_days":  90,
        "total_signals":  len(enriched),
        "verified_count": verified_count,
        "low_conf_count": low_conf_count,
        "signals":        enriched,
        "leaderboard":    lb,
        "spillage":       spill,
    }

    js   = json.dumps(payload, default=str, ensure_ascii=False)
    tmpl = pathlib.Path(DASHBOARD_OUTPUT).read_text(encoding="utf-8")
    out  = tmpl.replace(
        "const RAW_DATA = typeof __TRACKER_DATA__ !== 'undefined' ? __TRACKER_DATA__ : null;",
        f"const __TRACKER_DATA__ = {js};\nconst RAW_DATA = __TRACKER_DATA__;",
    )
    pathlib.Path(DASHBOARD_OUTPUT).write_text(out, encoding="utf-8")
    pathlib.Path(REPORT_OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    with open(f"{REPORT_OUTPUT_DIR}/leaderboard.json", "w") as f:
        json.dump(lb, f, indent=2, default=str)
    with open(f"{REPORT_OUTPUT_DIR}/signals.json", "w") as f:
        json.dump(enriched, f, indent=2, default=str)

    log.info("[Dashboard] %d signals (%d verified, %d low-conf) → %s",
             len(enriched), verified_count, low_conf_count, DASHBOARD_OUTPUT)
    return payload


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    generate_dashboard()
