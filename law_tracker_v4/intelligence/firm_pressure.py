"""
intelligence/firm_pressure.py
──────────────────────────────
The "Firm Pressure Model"

Estimates the associate-hours burden at each Calgary firm based on all
observable signals, then computes a Pressure Index (hours_estimated / known_capacity).

When Pressure Index > 1.0, the firm is mathematically overloaded.
When Pressure Index > 1.3, they are in crisis mode — fire Tier-0 alert.

─── Hours Estimation Model ───────────────────────────────────────────────────

Each signal type maps to an estimated associate-hours multiplier:

  SEDAR+ major deal ($1B+)         → 800–1,500 associate hours
  SEDAR+ deal ($100M–$1B)          → 200–600 associate hours
  ABQB commercial file (large)     → 300–800 associate hours
  ABQB appearance spike            → 50 hours per extra appearance
  AER hearing                      → 150 hours per hearing
  SEC 40-F cross-listing           → 600–1,200 associate hours
  Competition Bureau merger        → 200–500 associate hours
  ASC enforcement defence          → 400–900 associate hours
  Litigation (active CCAA)         → 500–1,000 associate hours

─── Capacity Estimation ──────────────────────────────────────────────────────

  Known associates from LinkedIn roster (is_active=1) × 1,800 hrs/yr ÷ 12 months
  Adjusted for seniority (1st yr = 0.7, 2nd yr = 0.85, senior = 1.0 factor)
  Plus a buffer: boutiques run lean (0.8), mid-size (0.9), BigLaw (1.0)

─── Output ───────────────────────────────────────────────────────────────────

  pressure_index = estimated_hours_this_month / capacity_hours_this_month
  
  < 0.8   → Comfortable
  0.8–1.0 → Busy
  1.0–1.3 → Overstretched → HIRE signal
  > 1.3   → Crisis → URGENT HIRE signal
"""

import logging
import math
from datetime import datetime, date, timedelta
from collections import defaultdict

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config_calgary import CALGARY_FIRMS, FIRM_BY_ID
from database.db import get_conn, insert_signal

log = logging.getLogger(__name__)

# ─── Hours-per-signal-type estimates (midpoint of range) ─────────────────────

HOURS_MAP = {
    "sedar_major_deal":           1_100,   # $500M+ deal
    "sec_crossborder_filing":       900,
    "sedar_counsel_named":          350,
    "asc_enforcement_defence":      650,
    "competition_merger_filing":    350,
    "canlii_new_large_file":        400,
    "canlii_appearance_spike":       80,   # per extra appearance above baseline
    "aer_hearing_load":             150,   # per hearing
    "biglaw_spillage_predicted":    500,
    "regulatory_wave":              120,
    "partner_clock":                200,   # new partner needs team immediately
    "linkedin_turnover_detected":     0,   # departure REDUCES capacity
    "lsa_retention_gap":              0,   # gap = lost capacity
    # Generic
    "job_posting":                   50,
    "lateral_hire":                  50,
    "ranking":                        0,
}

# Capacity factors
HOURS_PER_ASSOCIATE_PER_MONTH  = 150     # conservative (1,800 / 12)
SENIORITY_FACTORS = {
    "1st year":         0.70,
    "2nd year":         0.85,
    "junior associate": 0.80,
    "associate":        1.00,
}
TIER_BUFFER = {"boutique": 0.80, "mid": 0.90, "big": 1.00}


def get_active_associate_count(firm_id: str) -> dict:
    """Returns {seniority: count} for active associates at a firm."""
    conn  = get_conn()
    rows  = conn.execute("""
        SELECT seniority, count(*) as cnt
        FROM linkedin_roster
        WHERE firm_id = ? AND is_active = 1
        GROUP BY seniority
    """, (firm_id,)).fetchall()
    conn.close()
    return {r["seniority"]: r["cnt"] for r in rows}


def estimate_capacity(firm: dict) -> float:
    """Total estimated associate-hours available this month."""
    fid    = firm["id"]
    counts = get_active_associate_count(fid)
    tier   = firm.get("tier", "big")
    buffer = TIER_BUFFER.get(tier, 1.0)

    capacity = 0.0
    for seniority, count in counts.items():
        factor    = SENIORITY_FACTORS.get(seniority, 0.90)
        capacity += count * HOURS_PER_ASSOCIATE_PER_MONTH * factor

    # If no LinkedIn data, use a rough firm-size estimate
    if capacity == 0:
        size_est = {"boutique": 2, "mid": 8, "big": 20}.get(tier, 8)
        capacity = size_est * HOURS_PER_ASSOCIATE_PER_MONTH * buffer

    return capacity * buffer


def estimate_workload(firm_id: str, lookback_days: int = 30) -> float:
    """Sum of estimated associate-hours from all recent signals at this firm."""
    conn  = get_conn()
    sigs  = conn.execute("""
        SELECT signal_type, weight, raw_data
        FROM signals
        WHERE firm_id = ?
          AND date(detected_at) >= date('now', ? || ' days')
    """, (firm_id, f"-{lookback_days}")).fetchall()
    conn.close()

    total_hours = 0.0
    for sig in sigs:
        st      = sig["signal_type"]
        base_h  = HOURS_MAP.get(st, 50)
        # Scale by weight (heavier signal = more hours)
        weight  = sig["weight"] or 1.0
        hours   = base_h * (weight / 3.0)   # normalise around weight=3.0

        # Capacity-reducers
        if st in ("linkedin_turnover_detected", "lsa_retention_gap",
                  "lsa_student_not_retained"):
            total_hours -= 150   # lost associate = lost capacity
        else:
            total_hours += hours

    return max(0, total_hours)


def compute_pressure_index(firm: dict) -> dict:
    """
    Returns {pressure_index, estimated_hours, capacity_hours, status, associates}.
    """
    fid       = firm["id"]
    workload  = estimate_workload(fid)
    capacity  = estimate_capacity(firm)
    index     = workload / capacity if capacity > 0 else 0.0

    if   index > 1.30: status = "CRISIS"
    elif index > 1.00: status = "OVERSTRETCHED"
    elif index > 0.80: status = "BUSY"
    else:              status = "comfortable"

    return {
        "firm_id":        fid,
        "firm_name":      firm.get("name", fid),
        "tier":           firm.get("tier", "?"),
        "pressure_index": round(index, 3),
        "estimated_hours":round(workload),
        "capacity_hours": round(capacity),
        "status":         status,
        "associates":     sum(get_active_associate_count(fid).values()),
    }


class FirmPressureAnalyzer:
    """
    Computes pressure indices for all tracked firms and fires hiring signals
    for firms operating above capacity.
    """

    def run(self) -> list[dict]:
        results      = []
        new_signals  = []
        log.info("[Pressure] Computing firm pressure indices")

        for firm in CALGARY_FIRMS:
            p = compute_pressure_index(firm)
            results.append(p)

            if p["status"] in ("CRISIS", "OVERSTRETCHED"):
                weight = 5.5 if p["status"] == "CRISIS" else 4.8
                desc   = (
                    f"Pressure Index: {p['pressure_index']:.2f} — {p['status']}. "
                    f"Estimated workload: {p['estimated_hours']:,} associate-hours this month. "
                    f"Available capacity: {p['capacity_hours']:,} hours "
                    f"({p['associates']} active associates). "
                    f"The firm is mathematically overloaded. "
                    f"{'They are in CRISIS — expect a job posting within 2-3 weeks.' if p['status'] == 'CRISIS' else 'They are overstretched — junior hire decision is imminent.'}"
                )
                is_new = insert_signal(
                    firm_id=firm["id"],
                    signal_type="pressure_index_alert",
                    weight=weight,
                    title=f"Pressure {p['status']}: {firm.get('name','')} at {p['pressure_index']:.2f}× capacity",
                    description=desc,
                    source_url="",
                    practice_area=firm.get("focus", ["general"])[0],
                    raw_data=p,
                )
                if is_new:
                    new_signals.append({**p, "signal_type": "pressure_index_alert",
                                        "weight": weight, "description": desc})

        results.sort(key=lambda x: x["pressure_index"], reverse=True)
        log.info("[Pressure] Top firm: %s (%.2f×)",
                 results[0]["firm_name"] if results else "—",
                 results[0]["pressure_index"] if results else 0)
        return results, new_signals
