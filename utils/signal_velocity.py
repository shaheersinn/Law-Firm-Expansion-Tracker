"""
signal_velocity.py  —  Momentum scoring and trend detection.

Observation from logs:
  • The tracker accumulates 161 total signals and 45 alerts, but only finds
    1 new signal per run on this particular day. Raw counts tell us very little
    about whether a firm is *accelerating* or *decelerating*.
  • A firm that goes from 0 signals/week → 8 signals/week is more interesting
    than one sitting at a steady 3/week — even if the latter has a higher total.

This module computes:
  1. 7-day rolling signal velocity per firm (signals per day)
  2. Momentum delta: velocity(this week) − velocity(last week)
  3. Multi-scraper burst: ≥ 3 distinct scrapers firing on the same firm
     within a 72-hour window → "coordinated expansion signal"
  4. Cross-firm market trend: ≥ 4 firms showing velocity spikes in the
     same practice area → sector-level intelligence alert
"""

from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional

log = logging.getLogger(__name__)

DB_PATH = "law_firm_tracker.db"


# ── helpers ──────────────────────────────────────────────────────────────────

def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _days_ago(n: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=n)).isoformat()


# ── velocity ─────────────────────────────────────────────────────────────────

def get_firm_velocity(firm_name: str, window_days: int = 7) -> float:
    """Signals per day for a given firm over the last `window_days` days."""
    conn = _connect()
    try:
        row = conn.execute(
            """
            SELECT COUNT(*) as cnt
            FROM signals
            WHERE firm = ?
              AND discovered_at >= ?
            """,
            (firm_name, _days_ago(window_days)),
        ).fetchone()
        return (row["cnt"] / window_days) if row else 0.0
    finally:
        conn.close()


def get_velocity_delta(firm_name: str) -> float:
    """
    Returns velocity(last 7 days) − velocity(prior 7 days).
    Positive = accelerating. Negative = decelerating.
    """
    v_recent = get_firm_velocity(firm_name, 7)
    conn = _connect()
    try:
        row = conn.execute(
            """
            SELECT COUNT(*) as cnt
            FROM signals
            WHERE firm = ?
              AND discovered_at >= ?
              AND discovered_at < ?
            """,
            (firm_name, _days_ago(14), _days_ago(7)),
        ).fetchone()
        v_prior = (row["cnt"] / 7) if row else 0.0
    finally:
        conn.close()
    return v_recent - v_prior


def get_all_firm_velocities() -> list[dict]:
    """
    Returns a sorted list of {firm, velocity, delta, momentum_label}
    for all firms tracked, sorted by velocity descending.
    """
    conn = _connect()
    try:
        firms = [r[0] for r in conn.execute("SELECT DISTINCT firm FROM signals").fetchall()]
    finally:
        conn.close()

    results = []
    for firm in firms:
        v = get_firm_velocity(firm)
        d = get_velocity_delta(firm)
        if d > 0.5:
            label = "🚀 Accelerating"
        elif d < -0.5:
            label = "📉 Decelerating"
        else:
            label = "➡️ Steady"
        results.append({"firm": firm, "velocity": round(v, 2), "delta": round(d, 2), "momentum": label})

    return sorted(results, key=lambda x: -x["velocity"])


# ── burst detection ───────────────────────────────────────────────────────────

def detect_multi_scraper_burst(firm_name: str, window_hours: int = 72, min_scrapers: int = 3) -> Optional[str]:
    """
    Returns an alert string if ≥ min_scrapers distinct scrapers fired on this
    firm within window_hours. This is a "coordinated expansion signal."
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=window_hours)).isoformat()
    conn = _connect()
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT scraper_name
            FROM signals
            WHERE firm = ?
              AND discovered_at >= ?
            """,
            (firm_name, cutoff),
        ).fetchall()
    finally:
        conn.close()

    active_scrapers = [r[0] for r in rows]
    if len(active_scrapers) >= min_scrapers:
        return (
            f"🔥 Burst detected: {firm_name} — {len(active_scrapers)} distinct "
            f"scrapers fired within {window_hours}h "
            f"({', '.join(sorted(active_scrapers))})"
        )
    return None


def detect_all_bursts(window_hours: int = 72, min_scrapers: int = 3) -> list[str]:
    """Run burst detection across all firms. Returns list of alert strings."""
    conn = _connect()
    try:
        firms = [r[0] for r in conn.execute("SELECT DISTINCT firm FROM signals").fetchall()]
    finally:
        conn.close()

    alerts = []
    for firm in firms:
        msg = detect_multi_scraper_burst(firm, window_hours, min_scrapers)
        if msg:
            alerts.append(msg)
    return alerts


# ── cross-firm market trends ──────────────────────────────────────────────────

PRACTICE_AREA_KEYWORDS: dict[str, list[str]] = {
    "M&A / Corporate":        ["merger", "acquisition", "corporate", "M&A", "deal"],
    "Capital Markets":        ["securities", "capital market", "IPO", "equity", "bond"],
    "Restructuring / CCAA":   ["CCAA", "restructur", "insolvenc", "receivership", "BIA"],
    "Energy / ESG":            ["energy", "ESG", "climate", "renewab", "carbon", "LNG"],
    "Technology / IP":         ["technology", "IP", "patent", "intellectual property", "cyber"],
    "Real Estate":             ["real estate", "REIT", "property", "development", "zoning"],
    "Tax / BEPS":              ["tax", "BEPS", "Pillar Two", "transfer pricing", "CRA"],
    "Indigenous / Duty":       ["duty to consult", "First Nation", "Indigenous", "UNDRIP"],
    "Crypto / DeFi":           ["crypto", "DeFi", "blockchain", "digital asset", "token"],
}


def detect_sector_trends(window_days: int = 14, min_firms: int = 4) -> list[str]:
    """
    Returns alert strings for practice areas where ≥ min_firms are simultaneously
    showing activity in that sector within window_days.
    """
    cutoff = _days_ago(window_days)
    conn = _connect()
    try:
        rows = conn.execute(
            """
            SELECT firm, title, summary
            FROM signals
            WHERE discovered_at >= ?
            """,
            (cutoff,),
        ).fetchall()
    finally:
        conn.close()

    area_firms: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        text = f"{row['title']} {row.get('summary', '')}".lower()
        for area, keywords in PRACTICE_AREA_KEYWORDS.items():
            if any(kw.lower() in text for kw in keywords):
                area_firms[area].add(row["firm"])

    alerts = []
    for area, firms in area_firms.items():
        if len(firms) >= min_firms:
            alerts.append(
                f"📊 Sector trend [{area}]: {len(firms)} firms active "
                f"in last {window_days}d — "
                f"{', '.join(sorted(firms)[:5])}{'…' if len(firms) > 5 else ''}"
            )

    return sorted(alerts, key=lambda x: -int(x.split(":")[1].split("firms")[0].strip()))


# ── velocity digest ───────────────────────────────────────────────────────────

def build_velocity_digest(top_n: int = 8) -> str:
    """
    Returns a compact Telegram-ready string summarising the top movers.
    """
    firms = get_all_firm_velocities()
    if not firms:
        return ""

    lines = ["*Signal Velocity — Top Movers*", ""]
    for f in firms[:top_n]:
        if f["velocity"] == 0:
            continue
        lines.append(
            f"{f['momentum']}  `{f['firm'][:30]}`\n"
            f"   {f['velocity']} sig/day  Δ {f['delta']:+.2f}"
        )

    burst_alerts = detect_all_bursts()
    if burst_alerts:
        lines.append("")
        lines.extend(burst_alerts)

    sector_alerts = detect_sector_trends()
    if sector_alerts:
        lines.append("")
        lines.extend(sector_alerts)

    return "\n".join(lines)
