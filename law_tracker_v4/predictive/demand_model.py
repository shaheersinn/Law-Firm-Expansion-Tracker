"""
predictive/demand_model.py
───────────────────────────
Predictive Demand Model — P(firm hires in 30/60/90 days)

Goes beyond reactive signal scoring to output PROBABILITY ESTIMATES of
hiring need in three time horizons, using a logistic-regression-inspired
feature weighting approach.

Features used (17 total):
  Firm-level:
    F1. CanLII appearance z-score (last 30d)
    F2. SEDAR+ deal count (last 30d)
    F3. LinkedIn turnover rate (last 90d)
    F4. Website headcount delta (last 30d)
    F5. Glassdoor overwork score
    F6. Glassdoor turnover score
    F7. LSA retention gap (last cohort)
    F8. Job posting active (1/0)
    F9. Days since last junior hire (inverse)
    F10. Spillage centrality score

  Market-level:
    F11. WTI 3-month trend (bullish = 1, bearish = -1)
    F12. AER hearing density (upcoming 60d)
    F13. Newswire deal count (last 14d, Calgary)
    F14. SEDI insider cluster active (1/0)

  Firm metadata:
    F15. Firm tier (boutique/mid/big → float)
    F16. Articling class size (larger class = more potential hires)
    F17. Practice focus match to hot areas

Output per firm:
  {
    "p30":  0.82,   # P(hires in 30 days)
    "p60":  0.91,   # P(hires in 60 days)
    "p90":  0.95,   # P(hires in 90 days)
    "confidence": "high",
    "drivers": ["canlii_spike", "sedar_deal", "glassdoor_overwork"],
    "horizon_recommendation": "Contact within 7 days"
  }
"""

import math, logging, json
from datetime import datetime, date, timedelta
from collections import defaultdict
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from database.db import get_conn, get_recent_appearances, get_spillage_graph
from config_calgary import CALGARY_FIRMS, FIRM_BY_ID

log = logging.getLogger(__name__)

# ── Feature weights (learned from hand-labelled historical data)
# Positive = increases P(hire), negative = decreases
FEATURE_WEIGHTS = {
    "canlii_zscore":           0.45,   # strong real-time signal
    "sedar_deal_count":        0.60,   # strongest deal-flow signal
    "linkedin_turnover_rate":  0.80,   # direct vacancy signal
    "headcount_drop":          0.90,   # direct vacancy signal
    "glassdoor_overwork":      0.35,   # leading indicator
    "glassdoor_turnover":      0.50,   # leading indicator
    "lsa_retention_gap":       0.70,   # direct budget signal
    "job_posting_active":      0.40,   # already posted = late signal
    "days_since_last_hire_inv":0.30,   # long gap = due for hire
    "spillage_centrality":     0.25,   # market position
    "wti_trend":               0.20,   # macro
    "aer_hearing_density":     0.30,   # regulatory workload
    "newswire_deal_density":   0.40,   # near-term deal flow
    "sedi_cluster_active":     0.50,   # 30-60d predictive
    "tier_score":              0.15,   # boutique > mid > big for accessibility
    "articling_class_size":    0.10,   # larger class = more vacancy risk
    "practice_match":          0.20,   # alignment with hot practices
}

# Intercept (base rate of hiring in any given month)
INTERCEPT_30  = -1.8
INTERCEPT_60  = -1.2
INTERCEPT_90  = -0.7


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))


# ── Feature extractors ────────────────────────────────────────────────────────

def _get_canlii_zscore(firm_id: str) -> float:
    conn   = get_conn()
    row    = conn.execute("""
        SELECT zscore FROM firm_appearance_stats
        WHERE firm_id=?
        ORDER BY week_start DESC LIMIT 1
    """, (firm_id,)).fetchone()
    conn.close()
    return float(row["zscore"]) if row and row["zscore"] else 0.0


def _get_sedar_deal_count(firm_id: str, days: int = 30) -> int:
    conn = get_conn()
    rows = conn.execute("""
        SELECT count(*) as cnt FROM sedar_filings
        WHERE counsel_firms LIKE ?
          AND date(filed_date) >= date('now', ? || ' days')
    """, (f'%"{firm_id}"%', f"-{days}")).fetchone()
    conn.close()
    return int(rows["cnt"]) if rows else 0


def _get_linkedin_turnover_rate(firm_id: str, days: int = 90) -> float:
    """Returns fraction of roster that departed in last `days`."""
    conn   = get_conn()
    total  = conn.execute(
        "SELECT count(*) as c FROM linkedin_roster WHERE firm_id=?", (firm_id,)
    ).fetchone()["c"]
    gone   = conn.execute("""
        SELECT count(*) as c FROM linkedin_roster
        WHERE firm_id=? AND is_active=0
          AND date(left_date) >= date('now', ? || ' days')
    """, (firm_id, f"-{days}")).fetchone()["c"]
    conn.close()
    return (gone / total) if total > 0 else 0.0


def _get_headcount_delta(firm_id: str) -> int:
    """Net headcount change from website scraper."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT lawyer_count FROM team_page_snapshots
        WHERE firm_id=? ORDER BY snapped_at DESC LIMIT 2
    """, (firm_id,)).fetchall()
    conn.close()
    if len(rows) >= 2:
        return rows[0]["lawyer_count"] - rows[1]["lawyer_count"]
    return 0


def _get_glassdoor_scores(firm_id: str) -> tuple[float, float]:
    conn = get_conn()
    try:
        row  = conn.execute("""
            SELECT overwork_score, turnover_score FROM glassdoor_snapshots
            WHERE firm_id=? ORDER BY snapped_at DESC LIMIT 1
        """, (firm_id,)).fetchone()
        conn.close()
        if row:
            return float(row["overwork_score"]), float(row["turnover_score"])
    except Exception:
        conn.close()
    return 0.0, 0.0


def _get_lsa_retention_gap(firm_id: str) -> int:
    conn  = get_conn()
    row   = conn.execute("""
        SELECT raw_data FROM signals
        WHERE firm_id=? AND signal_type='lsa_retention_gap'
        ORDER BY detected_at DESC LIMIT 1
    """, (firm_id,)).fetchone()
    conn.close()
    if row:
        try:
            d = json.loads(row["raw_data"])
            return d.get("gap", 0)
        except Exception:
            pass
    return 0


def _get_job_posting_active(firm_id: str) -> float:
    conn = get_conn()
    row  = conn.execute("""
        SELECT count(*) as c FROM signals
        WHERE firm_id=? AND signal_type='job_posting'
          AND date(detected_at) >= date('now', '-14 days')
    """, (firm_id,)).fetchone()
    conn.close()
    return 1.0 if row and row["c"] > 0 else 0.0


def _get_spillage_centrality(firm_id: str, edges: list) -> float:
    """Fraction of distinct BigLaw firms this boutique co-appears with."""
    biglaw_links = set()
    from config_calgary import BIGLAW_FIRMS
    for e in edges:
        if e["boutique_id"] == firm_id and e["biglaw_id"] in BIGLAW_FIRMS:
            biglaw_links.add(e["biglaw_id"])
        if e["biglaw_id"] == firm_id:
            biglaw_links.add(e["biglaw_id"])
    return min(1.0, len(biglaw_links) / max(len(BIGLAW_FIRMS), 1))


def _get_macro_wti_trend() -> float:
    """Returns +1 (bullish), 0 (neutral), -1 (bearish) from macro snapshot."""
    conn = get_conn()
    try:
        row  = conn.execute("""
            SELECT bullish_count FROM macro_snapshots
            ORDER BY snapped_at DESC LIMIT 1
        """).fetchone()
        conn.close()
        if row:
            bc = row["bullish_count"]
            if bc >= 4:  return 1.0
            if bc <= 1:  return -1.0
    except Exception:
        conn.close()
    return 0.0


def _get_aer_hearing_density(firm_id: str) -> float:
    """Count of AER/AUC proceedings signals for this firm in next 90 days."""
    conn  = get_conn()
    row   = conn.execute("""
        SELECT count(*) as c FROM signals
        WHERE firm_id=? AND signal_type='aer_proceeding_upcoming'
          AND date(detected_at) >= date('now', '-30 days')
    """, (firm_id,)).fetchone()
    conn.close()
    return min(1.0, (row["c"] if row else 0) / 5.0)


def _get_newswire_density() -> float:
    """Global newswire deal count in last 14 days / 10."""
    conn = get_conn()
    row  = conn.execute("""
        SELECT count(*) as c FROM signals
        WHERE signal_type IN ('breaking_deal_announcement','newswire_deal_detected','breaking_ccaa_filing')
          AND date(detected_at) >= date('now','-14 days')
    """).fetchone()
    conn.close()
    return min(1.0, (row["c"] if row else 0) / 10.0)


def _get_sedi_cluster(firm_id: str) -> float:
    conn = get_conn()
    row  = conn.execute("""
        SELECT count(*) as c FROM signals
        WHERE firm_id=? AND signal_type='sedi_insider_cluster'
          AND date(detected_at) >= date('now','-14 days')
    """, (firm_id,)).fetchone()
    conn.close()
    return 1.0 if row and row["c"] > 0 else 0.0


def _get_tier_score(firm: dict) -> float:
    return {"boutique": 1.0, "mid": 0.7, "big": 0.3}.get(firm.get("tier","big"), 0.3)


def _get_practice_match(firm: dict) -> float:
    """Is the firm's focus aligned with currently hot practice areas?"""
    hot = {"energy", "corporate", "securities", "restructuring", "litigation"}
    focus = set(firm.get("focus", []))
    overlap = len(hot & focus) / max(len(focus), 1)
    return min(1.0, overlap)


# ── Main model ────────────────────────────────────────────────────────────────

class DemandPredictor:
    """
    Runs the predictive model for all 30 target firms.
    Returns probability estimates for 30/60/90-day hiring likelihood.
    """

    def __init__(self):
        self.edges = get_spillage_graph()

    def predict_all(self) -> list[dict]:
        results = []
        for firm in CALGARY_FIRMS:
            pred = self.predict_firm(firm)
            results.append(pred)
        results.sort(key=lambda x: x["p30"], reverse=True)
        return results

    def predict_firm(self, firm: dict) -> dict:
        fid = firm["id"]

        # ── Extract features ─────────────────────────────────────────────────
        f = {
            "canlii_zscore":           max(0, _get_canlii_zscore(fid)),
            "sedar_deal_count":        min(1.0, _get_sedar_deal_count(fid) / 3.0),
            "linkedin_turnover_rate":  _get_linkedin_turnover_rate(fid),
            "headcount_drop":          max(0, -_get_headcount_delta(fid)) / 5.0,
            "glassdoor_overwork":      min(1.0, _get_glassdoor_scores(fid)[0] / 5.0),
            "glassdoor_turnover":      min(1.0, _get_glassdoor_scores(fid)[1] / 3.0),
            "lsa_retention_gap":       min(1.0, _get_lsa_retention_gap(fid) / 4.0),
            "job_posting_active":      _get_job_posting_active(fid),
            "days_since_last_hire_inv":0.5,   # placeholder without alumni DB
            "spillage_centrality":     _get_spillage_centrality(fid, self.edges),
            "wti_trend":               max(0, _get_macro_wti_trend()),
            "aer_hearing_density":     _get_aer_hearing_density(fid),
            "newswire_deal_density":   _get_newswire_density(),
            "sedi_cluster_active":     _get_sedi_cluster(fid),
            "tier_score":              _get_tier_score(firm),
            "articling_class_size":    0.5,   # placeholder
            "practice_match":          _get_practice_match(firm),
        }

        # ── Weighted sum ─────────────────────────────────────────────────────
        linear  = sum(FEATURE_WEIGHTS[k] * v for k, v in f.items())

        p30 = _clamp(_sigmoid(linear + INTERCEPT_30))
        p60 = _clamp(_sigmoid(linear + INTERCEPT_60))
        p90 = _clamp(_sigmoid(linear + INTERCEPT_90))

        # ── Top drivers ──────────────────────────────────────────────────────
        feature_contribs = {k: FEATURE_WEIGHTS[k] * v for k, v in f.items()}
        top_drivers = sorted(feature_contribs.items(), key=lambda x: x[1], reverse=True)[:4]
        top_driver_labels = [k for k, _ in top_drivers if _ > 0.05]

        confidence = (
            "high"   if len(top_driver_labels) >= 3 else
            "medium" if len(top_driver_labels) >= 2 else
            "low"
        )

        if p30 >= 0.7:
            horizon = "Contact within 7 days"
        elif p60 >= 0.6:
            horizon = "Contact within 30 days"
        elif p90 >= 0.5:
            horizon = "Monitor — contact within 60 days"
        else:
            horizon = "Low priority — revisit next month"

        result = {
            "firm_id":   fid,
            "firm_name": firm["name"],
            "tier":      firm.get("tier","?"),
            "p30":       round(p30, 3),
            "p60":       round(p60, 3),
            "p90":       round(p90, 3),
            "confidence": confidence,
            "drivers":   top_driver_labels,
            "horizon":   horizon,
            "features":  {k: round(v, 3) for k, v in f.items()},
        }

        log.debug("[Predict] %s: p30=%.2f p60=%.2f p90=%.2f [%s]",
                  firm["name"], p30, p60, p90, ", ".join(top_driver_labels[:3]))
        return result

    def print_predictions(self, preds: list[dict], top_n: int = 15):
        print("\n" + "═" * 72)
        print("🔮  PREDICTIVE HIRING PROBABILITY MODEL")
        print(f"    {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
        print("═" * 72)
        print(f"  {'Firm':<38}  P(30d)  P(60d)  P(90d)  Confidence  Action")
        print("  " + "─" * 70)
        for p in preds[:top_n]:
            bar = "█" * int(p["p30"] * 10) + "░" * (10 - int(p["p30"] * 10))
            print(f"  {p['firm_name']:<38}  "
                  f"{p['p30']:.0%}    {p['p60']:.0%}    {p['p90']:.0%}    "
                  f"{p['confidence']:<8}  {p['horizon']}")
        print("═" * 72 + "\n")

    def save_predictions(self, preds: list[dict]):
        import pathlib, json
        pathlib.Path("reports").mkdir(exist_ok=True)
        with open("reports/predictions.json", "w") as f:
            json.dump(preds, f, indent=2, default=str)
        log.info("[Predict] Saved %d predictions → reports/predictions.json", len(preds))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    model = DemandPredictor()
    preds = model.predict_all()
    model.print_predictions(preds)
    model.save_predictions(preds)
