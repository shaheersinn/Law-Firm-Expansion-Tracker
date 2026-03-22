"""
dashboard/generator.py  (v4 — fixed data injection)
═══════════════════════════════════════════════════════
ROOT CAUSE FIX (confirmed in March 22 log):
  The generator searched for:
    "const RAW_DATA = typeof __TRACKER_DATA__ !== ..."
  but docs/index.html actually contains:
    "const __TD__ = typeof __TRACKER_DATA__ !== ..."

  Because the variable name was wrong, str.replace() silently found nothing,
  wrote the unchanged template back, git diff saw no change, and every run
  produced "Everything up-to-date" — signals never reached the dashboard.

FIX: use re.sub() targeting the actual __TD__ line, with a fallback that
     also handles the case where __TRACKER_DATA__ was already injected from
     a previous successful run (second-run safety).

Additional improvements over v2:
  • 90-day signal window (was 30)
  • No 50-signal cap — all signals included
  • Urgency map covers all v5 signal types
  • Leaderboard excludes firm_id="market" (macro market-wide signals)
"""

import json
import math
import pathlib
import logging
import re
import sys
import os
from datetime import datetime
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config_calgary import FIRM_BY_ID, REPORT_OUTPUT_DIR, DASHBOARD_OUTPUT
from database.db import get_all_signals_for_dashboard, get_spillage_graph

log = logging.getLogger(__name__)

DECAY_LAMBDA = 0.07   # slower decay over 90-day window

URGENCY_MAP = {
    # Act today
    "sedar_major_deal":            "today",
    "biglaw_spillage_predicted":   "today",
    "linkedin_turnover_detected":  "today",
    "linkedin_new_vacancy":        "today",
    "ica_review_announced":        "today",
    "counterfactual_conflict_free":"today",
    # This week
    "canlii_appearance_spike":     "week",
    "canlii_new_large_file":       "week",
    "sedar_counsel_named":         "week",
    "aer_hearing_upcoming":        "week",
    "competition_bureau":          "week",
    "new_court_filing":            "week",
    "fiscal_pressure_incoming":    "week",
    # 3 days
    "lsa_retention_gap":           "3days",
    "lsa_student_not_retained":    "3days",
    # This month
    "macro_ma_wave_incoming":      "month",
    "macro_demand_surge":          "month",
    "sec_edgar_filing":            "month",
    "job_posting":                 "month",
    "lateral_hire":                "month",
    "ranking":                     "month",
    "web_signal":                  "month",
}

STRATEGY_GROUPS = {
    "litigation":  ["canlii_appearance_spike", "canlii_new_large_file", "new_court_filing"],
    "corporate":   ["sedar_major_deal", "sedar_counsel_named", "ica_review_announced"],
    "turnover":    ["linkedin_turnover_detected", "linkedin_new_vacancy"],
    "hireback":    ["lsa_student_not_retained", "lsa_retention_gap"],
    "spillage":    ["biglaw_spillage_predicted", "counterfactual_conflict_free"],
    "macro":       ["macro_ma_wave_incoming", "macro_demand_surge", "macro_demand_collapse"],
    "regulatory":  ["aer_hearing_upcoming", "competition_bureau",
                    "fiscal_pressure_incoming", "sec_edgar_filing"],
    "jobs":        ["job_posting", "lateral_hire", "web_signal"],
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
    firm_sigs: dict = defaultdict(list)
    for s in signals:
        fid = s.get("firm_id", "")
        if fid and fid != "market":   # exclude market-wide macro signals
            firm_sigs[fid].append(s)

    rows = []
    for fid, sigs in firm_sigs.items():
        firm   = FIRM_BY_ID.get(fid, {"name": fid, "tier": "?", "focus": []})
        score  = sum(s["weight"] * _decay(s["detected_at"]) for s in sigs)
        strats = {SIG_TO_STRAT.get(s["signal_type"], "other") for s in sigs}
        if len(strats) >= 2:
            score *= 1.30
        score *= {"boutique": 1.2, "mid": 1.1, "big": 1.0}.get(
            firm.get("tier", "big"), 1.0)
        top = max(sigs, key=lambda s: s["weight"])
        rows.append({
            "firm_id":      fid,
            "firm_name":    firm.get("name", fid),
            "tier":         firm.get("tier", "?"),
            "focus":        ", ".join(firm.get("focus", [])),
            "score":        round(score, 2),
            "signal_count": len(sigs),
            "strategies":   sorted(strats),
            "corroborated": len(strats) >= 2,
            "urgency":      URGENCY_MAP.get(top["signal_type"], "month"),
            "top_signal":   top.get("title", "")[:80],
        })

    return sorted(rows, key=lambda x: x["score"], reverse=True)


def _inject_data(html: str, js_payload: str) -> str:
    """
    Inject JSON payload into the HTML template.

    Handles three cases:
      1. Fresh template — placeholder line:
            const __TD__ = typeof __TRACKER_DATA__ !== 'undefined' ? __TRACKER_DATA__ : null;
      2. Already injected (previous run) — line starting with:
            const __TRACKER_DATA__ = {...};
      3. Old generator format (RAW_DATA variant) — kept for safety.

    In all cases the target line is replaced with TWO lines:
        const __TRACKER_DATA__ = <json>;
        const __TD__ = __TRACKER_DATA__;
    so the existing `const D = __TD__ || DEMO;` in the HTML still works.
    """
    replacement = (
        f"const __TRACKER_DATA__ = {js_payload};\n"
        f"const __TD__ = __TRACKER_DATA__;"
    )

    # Pattern 1: fresh template placeholder (actual HTML variable is __TD__)
    fresh = re.sub(
        r"const __TD__\s*=\s*typeof __TRACKER_DATA__[^\n]*",
        replacement,
        html,
        count=1,
    )
    if fresh != html:
        return fresh

    # Pattern 2: already injected from a previous run
    injected = re.sub(
        r"const __TRACKER_DATA__\s*=\s*\{.*?\};\s*\nconst __TD__[^\n]*",
        replacement,
        html,
        count=1,
        flags=re.DOTALL,
    )
    if injected != html:
        return injected

    # Pattern 3: old RAW_DATA variant (safety fallback)
    old_style = re.sub(
        r"const RAW_DATA\s*=\s*typeof __TRACKER_DATA__[^\n]*",
        replacement,
        html,
        count=1,
    )
    if old_style != html:
        return old_style

    # Nothing matched — log clearly so the problem is obvious
    log.error(
        "[Dashboard] INJECTION FAILED — could not find placeholder in %s. "
        "Check that docs/index.html contains 'const __TD__' or 'const RAW_DATA'.",
        DASHBOARD_OUTPUT
    )
    return html


def generate_dashboard():
    raw   = get_all_signals_for_dashboard(days=90)
    edges = get_spillage_graph()
    lb    = build_leaderboard(raw)

    spill = [
        {
            "boutique": FIRM_BY_ID.get(e["boutique_id"], {}).get("name", e["boutique_id"]),
            "biglaw":   FIRM_BY_ID.get(e["biglaw_id"],   {}).get("name", e["biglaw_id"]),
            "count":    e["co_appearances"],
        }
        for e in edges[:15]
    ]

    # Enrich every signal — no 50-signal cap
    enriched = []
    for s in raw:
        firm = FIRM_BY_ID.get(s.get("firm_id", ""), {})
        enriched.append({
            **s,
            "firm_name": firm.get("name",  s.get("firm_id", "")),
            "tier":      firm.get("tier",  "?"),
            "focus":     " · ".join(firm.get("focus", [])),
            "urgency":   URGENCY_MAP.get(s.get("signal_type", ""), "month"),
        })

    payload = {
        "generated_at":  datetime.utcnow().isoformat() + "Z",
        "lookback_days": 90,
        "total_signals": len(enriched),
        "signals":       enriched,
        "leaderboard":   lb,
        "spillage":      spill,
    }

    js   = json.dumps(payload, default=str, ensure_ascii=False)
    html = pathlib.Path(DASHBOARD_OUTPUT).read_text(encoding="utf-8")
    out  = _inject_data(html, js)

    # Always write — even if inject failed (preserves existing data rather
    # than breaking the site; the ERROR log above makes it visible)
    pathlib.Path(DASHBOARD_OUTPUT).write_text(out, encoding="utf-8")
    pathlib.Path(REPORT_OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    with open(f"{REPORT_OUTPUT_DIR}/leaderboard.json", "w") as f:
        json.dump(lb, f, indent=2, default=str)
    with open(f"{REPORT_OUTPUT_DIR}/signals.json", "w") as f:
        json.dump(enriched, f, indent=2, default=str)

    injected_ok = out != html
    log.info("[Dashboard] %d signals → %s  (injection=%s)",
             len(enriched), DASHBOARD_OUTPUT,
             "OK" if injected_ok else "FAILED — check placeholder")
    return payload


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    generate_dashboard()
