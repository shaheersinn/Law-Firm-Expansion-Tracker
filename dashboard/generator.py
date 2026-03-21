"""
dashboard/generator.py  (v2)
Reads live DB and injects JSON into docs/index.html.
"""
import json, pathlib, logging, sys, os, math
from datetime import datetime
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config_calgary import FIRM_BY_ID, REPORT_OUTPUT_DIR, DASHBOARD_OUTPUT
from database.db import get_all_signals_for_dashboard, get_spillage_graph

log = logging.getLogger(__name__)

DECAY_LAMBDA = 0.10
URGENCY_MAP = {
    "sedar_major_deal": "today", "biglaw_spillage_predicted": "today",
    "linkedin_turnover_detected": "today", "linkedin_new_vacancy": "today",
    "canlii_appearance_spike": "week", "canlii_new_large_file": "week",
    "sedar_counsel_named": "week", "lsa_retention_gap": "3days",
    "lsa_student_not_retained": "3days", "job_posting": "month",
    "lateral_hire": "month", "ranking": "month",
}
STRATEGY_GROUPS = {
    "litigation": ["canlii_appearance_spike","canlii_new_large_file"],
    "corporate":  ["sedar_major_deal","sedar_counsel_named"],
    "turnover":   ["linkedin_turnover_detected","linkedin_new_vacancy"],
    "hireback":   ["lsa_student_not_retained","lsa_retention_gap"],
    "spillage":   ["biglaw_spillage_predicted"],
    "baseline":   ["job_posting","lateral_hire","ranking"],
}
SIG_TO_STRAT = {t: g for g, ts in STRATEGY_GROUPS.items() for t in ts}

def _decay(ts):
    try:
        days = (datetime.utcnow() - datetime.fromisoformat(ts)).days
        return math.exp(-DECAY_LAMBDA * max(0, days))
    except: return 0.5

def build_leaderboard(signals):
    firm_sigs = defaultdict(list)
    for s in signals: firm_sigs[s["firm_id"]].append(s)
    rows = []
    for fid, sigs in firm_sigs.items():
        firm  = FIRM_BY_ID.get(fid, {"name": fid, "tier":"?","focus":[]})
        score = sum(s["weight"] * _decay(s["detected_at"]) for s in sigs)
        strats = set(SIG_TO_STRAT.get(s["signal_type"],"other") for s in sigs)
        if len(strats) >= 2: score *= 1.30
        score *= {"boutique":1.2,"mid":1.1,"big":1.0}.get(firm.get("tier","big"),1.0)
        top = max(sigs, key=lambda s: s["weight"])
        rows.append({
            "firm_id": fid, "firm_name": firm.get("name",fid),
            "tier": firm.get("tier","?"), "score": round(score,2),
            "signal_count": len(sigs), "strategies": sorted(strats),
            "corroborated": len(strats)>=2,
            "urgency": URGENCY_MAP.get(top["signal_type"],"month"),
            "top_signal": top.get("title","")[:80],
        })
    return sorted(rows, key=lambda x: x["score"], reverse=True)

def generate_dashboard():
    raw  = get_all_signals_for_dashboard(days=30)
    edges = get_spillage_graph()
    lb   = build_leaderboard(raw)
    spill = [{"boutique": FIRM_BY_ID.get(e["boutique_id"],{}).get("name",e["boutique_id"]),
               "biglaw":   FIRM_BY_ID.get(e["biglaw_id"],{}).get("name",e["biglaw_id"]),
               "count":    e["co_appearances"]} for e in edges[:12]]

    enriched = []
    for s in raw[:50]:
        firm = FIRM_BY_ID.get(s["firm_id"],{})
        enriched.append({**s,
            "firm_name": firm.get("name",s["firm_id"]),
            "tier":      firm.get("tier","?"),
            "focus":     " · ".join(firm.get("focus",[])),
            "urgency":   URGENCY_MAP.get(s["signal_type"],"month"),
        })

    payload = {"generated_at": datetime.utcnow().isoformat()+"Z",
               "signals": enriched, "leaderboard": lb, "spillage": spill}
    js = json.dumps(payload, default=str, ensure_ascii=False)

    tmpl = pathlib.Path(DASHBOARD_OUTPUT).read_text(encoding="utf-8")
    out  = tmpl.replace(
        "const RAW_DATA = typeof __TRACKER_DATA__ !== 'undefined' ? __TRACKER_DATA__ : null;",
        f"const __TRACKER_DATA__ = {js};\nconst RAW_DATA = __TRACKER_DATA__;"
    )
    pathlib.Path(DASHBOARD_OUTPUT).write_text(out, encoding="utf-8")
    pathlib.Path(REPORT_OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    with open(f"{REPORT_OUTPUT_DIR}/leaderboard.json","w") as f: json.dump(lb, f, indent=2, default=str)
    with open(f"{REPORT_OUTPUT_DIR}/signals.json","w") as f: json.dump(enriched, f, indent=2, default=str)
    log.info("[Dashboard] %d signals → %s", len(enriched), DASHBOARD_OUTPUT)
    return payload

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    generate_dashboard()
