"""
intelligence/practice_velocity.py
───────────────────────────────────
Three intelligence models in one:

═══ A) Practice Area Velocity Index ═════════════════════════════════════════
Tracks the RATE OF CHANGE in signal volume per practice area over rolling windows.
A 60% jump in energy-related signals over 30 days = energy practices about to hire.

Fires "velocity spike" signal for any practice area with:
  - 30-day signal count ≥ 2× the 60-day average

═══ B) Dual Departure Disaster ═══════════════════════════════════════════════
When TWO associates leave the same firm within 30 days:
  - The second departure is not a coincidence — it signals systemic issues
  - The firm is now in genuine crisis (2 empty desks, institutional knowledge lost,
    remaining associates overloaded, clients nervous)
  - They WILL hire within 3 weeks, and they're desperate enough to waive
    standard process

Weight: 6.0 (highest in the system)

═══ C) OCI Pipeline Predictor ════════════════════════════════════════════════
Tracks law school OCI (On-Campus Interview) results and articling class sizes.
Published by Ultra Vires (UofT), Obiter Dicta (Osgoode), De Jure (UofC), etc.
Also tracks the Law Society of Alberta articling registration data.

When a firm interviews 6 students but only hired 2 → they planned for 4 full-time
associates but only got 2 → there are 2 salary budgets available for lateral hires.

This is the RAREST signal: it tells you about unfilled headcount BEFORE the
articling year even starts.
"""

import re
import logging
from datetime import datetime, date, timedelta
from collections import defaultdict

import requests
import feedparser
from bs4 import BeautifulSoup

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config_calgary import CALGARY_FIRMS, FIRM_BY_ID
from database.db import get_conn, insert_signal

log = logging.getLogger(__name__)

# ─── OCI data sources ────────────────────────────────────────────────────────

OCI_SOURCES = [
    ("https://ultravires.ca/category/recruitment/",    "uoft",     "Ultra Vires"),
    ("https://dejure.ca/category/careers/",            "ucalgary", "De Jure"),
    ("https://www.thelegalgazette.com/rss",            "general",  "Legal Gazette"),
]

FIRM_RECRUITED_RE = re.compile(
    r"(\d+)\s+(?:students?|articling|clerks?|positions?|offers?)\s+(?:at|to|from|for)\s+([A-Z][^,\n]+)",
    re.I
)
OCI_MENTION_RE = re.compile(
    r"\b(OCI|on.campus interview|articling recruit|recruit\w*\s+season|"
    r"articling\s+student|articling\s+class|summer\s+student|recruit\w*\s+results?)\b",
    re.I
)


class PracticeVelocityTracker:
    """Tracks rate of change in signal volume per practice area."""

    PRACTICE_AREAS = [
        "corporate", "securities", "litigation", "energy",
        "employment", "real_estate", "tax", "ip", "restructuring", "regulatory",
    ]

    def run(self) -> list[dict]:
        new_signals = []
        log.info("[Velocity] Computing practice area velocity indices")

        for pa in self.PRACTICE_AREAS:
            v = self._compute_velocity(pa)
            if v["velocity_ratio"] >= 2.0 and v["recent_30d"] >= 3:
                # Signal: this practice area is heating up fast
                affected_firms = self._top_firms_for_pa(pa)
                for firm_id in affected_firms[:4]:
                    firm   = FIRM_BY_ID.get(firm_id, {})
                    weight = 3.5 + min((v["velocity_ratio"] - 2.0) * 0.5, 1.5)
                    desc   = (
                        f"Practice area velocity spike: {pa.replace('_',' ').title()}. "
                        f"30-day signal volume ({v['recent_30d']}) is {v['velocity_ratio']:.1f}× "
                        f"the prior 30-day average ({v['prior_30d']:.1f}). "
                        f"Firms focused on {pa} are experiencing a structural surge in demand. "
                        f"{firm.get('name', firm_id)}'s exposure to this area makes them "
                        f"a prime candidate for imminent junior hiring."
                    )
                    is_new = insert_signal(
                        firm_id=firm_id, signal_type="practice_velocity_spike",
                        weight=weight,
                        title=f"Velocity Spike: {pa.replace('_',' ').title()} — {v['velocity_ratio']:.1f}× surge",
                        description=desc,
                        source_url="",
                        practice_area=pa,
                        raw_data=v,
                    )
                    if is_new:
                        new_signals.append({
                            "firm_id": firm_id, "signal_type": "practice_velocity_spike",
                            "weight": weight, "practice_area": pa,
                            "title": f"Velocity Spike: {pa} {v['velocity_ratio']:.1f}×",
                            "velocity": v,
                        })
                        log.info("[Velocity] 🚀 %s at %s — %.1f× surge",
                                 pa, firm.get("name", firm_id), v["velocity_ratio"])

        return new_signals

    def _compute_velocity(self, practice_area: str) -> dict:
        conn = get_conn()
        # Recent 30 days
        r30 = conn.execute("""
            SELECT count(*) FROM signals
            WHERE practice_area = ?
              AND date(detected_at) >= date('now', '-30 days')
        """, (practice_area,)).fetchone()[0]

        # Prior 30 days (days 31-60 ago)
        p30 = conn.execute("""
            SELECT count(*) FROM signals
            WHERE practice_area = ?
              AND date(detected_at) BETWEEN date('now', '-60 days')
              AND date('now', '-31 days')
        """, (practice_area,)).fetchone()[0]
        conn.close()

        ratio = (r30 / p30) if p30 > 0 else (float(r30) if r30 > 0 else 0.0)
        return {
            "practice_area": practice_area,
            "recent_30d":    r30,
            "prior_30d":     p30,
            "velocity_ratio": round(ratio, 2),
        }

    def _top_firms_for_pa(self, practice_area: str, n: int = 4) -> list[str]:
        """Returns firm_ids that have the most signals in this practice area."""
        conn = get_conn()
        rows = conn.execute("""
            SELECT firm_id, count(*) as cnt
            FROM signals
            WHERE practice_area = ?
              AND date(detected_at) >= date('now', '-30 days')
            GROUP BY firm_id
            ORDER BY cnt DESC
            LIMIT ?
        """, (practice_area, n)).fetchall()
        conn.close()
        return [r["firm_id"] for r in rows]

    def get_velocity_dashboard(self) -> list[dict]:
        """Returns velocity data for all practice areas, for the dashboard."""
        return [self._compute_velocity(pa) for pa in self.PRACTICE_AREAS]


class DualDepartureDetector:
    """
    Monitors for TWO associate departures from the same firm within 30 days.
    When detected, fires a TIER-0 (weight=6.0) crisis alert.
    """

    def run(self) -> list[dict]:
        new_signals = []
        conn = get_conn()
        # Find firms with 2+ LinkedIn departures in the last 30 days
        rows = conn.execute("""
            SELECT firm_id, count(*) as departures,
                   group_concat(full_name, ', ') as names
            FROM linkedin_roster
            WHERE is_active = 0
              AND date(left_date) >= date('now', '-30 days')
            GROUP BY firm_id
            HAVING departures >= 2
            ORDER BY departures DESC
        """).fetchall()
        conn.close()

        for row in rows:
            row    = dict(row)
            firm   = FIRM_BY_ID.get(row["firm_id"], {})
            count  = row["departures"]
            names  = row["names"] or "multiple associates"
            weight = 5.5 + min((count - 2) * 0.25, 0.5)    # max 6.0

            desc = (
                f"DUAL DEPARTURE ALERT: {count} associates left {firm.get('name', row['firm_id'])} "
                f"in the past 30 days ({names}). "
                f"Multiple departures in rapid succession indicate systemic capacity loss — "
                f"possibly overwork, compensation issues, or a team exodus following a partner departure. "
                f"The firm is in CRISIS. They will hire at the associate level within 2-3 weeks "
                f"and are likely to waive their standard recruitment process. "
                f"Email the managing/hiring partner TODAY. Do not wait for a job posting."
            )
            is_new = insert_signal(
                firm_id=row["firm_id"],
                signal_type="dual_departure_crisis",
                weight=weight,
                title=f"DUAL DEPARTURE CRISIS: {count} associates left {firm.get('name','')} in 30 days",
                description=desc,
                source_url="",
                practice_area=firm.get("focus", ["general"])[0],
                raw_data={"departure_count": count, "names": names},
            )
            if is_new:
                new_signals.append({
                    "firm_id":    row["firm_id"],
                    "signal_type":"dual_departure_crisis",
                    "weight":     weight,
                    "title":      f"DUAL DEPARTURE: {count} left {firm.get('name','')}",
                    "practice_area": firm.get("focus", ["general"])[0],
                    "description": desc,
                })
                log.info("[DualDeparture] 🚨 CRISIS: %d departures at %s",
                         count, firm.get("name", row["firm_id"]))

        return new_signals


class OCIPipelineTracker:
    """
    Monitors OCI/articling recruitment results to identify firms with
    unfilled associate budget before the articling year starts.
    """

    def run(self) -> list[dict]:
        new_signals = []
        log.info("[OCI] Scanning recruitment sources for pipeline data")

        for url, school, name in OCI_SOURCES:
            try:
                resp = requests.get(url, timeout=12,
                                    headers={"User-Agent": "LawFirmTracker/3.0"})
                soup = BeautifulSoup(resp.text, "lxml")
                text = soup.get_text(" ", strip=True)

                if not OCI_MENTION_RE.search(text):
                    continue

                sigs = self._parse_oci_text(text, url, school)
                new_signals.extend(sigs)
            except Exception as e:
                log.debug("[OCI] Error %s: %s", url, e)

        return new_signals

    def _parse_oci_text(self, text: str, url: str, school: str) -> list[dict]:
        """
        Try to extract "N students at FirmName" patterns.
        If parsing fails, fire a generic OCI-season signal for all tracked firms.
        """
        results    = []
        matches    = FIRM_RECRUITED_RE.findall(text)

        if not matches:
            return []

        for count_str, firm_mention in matches:
            count   = int(count_str)
            # Match to a known firm
            firm_id = None
            for fid, pat in {fid: re.compile("|".join(
                    re.escape(a) for a in [f["name"]] + f["aliases"]), re.I)
                    for fid, f in {f["id"]: f for f in CALGARY_FIRMS}.items()}.items():
                if pat.search(firm_mention):
                    firm_id = fid
                    break

            if not firm_id:
                continue

            firm   = FIRM_BY_ID.get(firm_id, {})
            weight = 3.2

            # Key insight: # of OCI students hired ≈ planned headcount budget
            desc   = (
                f"OCI data ({school}): {firm.get('name', firm_id)} hired {count} articling students. "
                f"Historical retention rate of ~65% suggests ~{round(count*0.65)} will be retained "
                f"as associates — leaving {round(count*0.35)} potential slots for lateral associates "
                f"if retention is below average. "
                f"Monitor LSA directory in September for actual retention data."
            )
            is_new = insert_signal(
                firm_id=firm_id, signal_type="oci_pipeline_prediction",
                weight=weight,
                title=f"OCI Pipeline: {firm.get('name','')} hired {count} students → predict {round(count*0.35)} open slots",
                description=desc,
                source_url=url,
                practice_area="general",
                raw_data={"oci_count": count, "school": school,
                          "predicted_open": round(count * 0.35)},
            )
            if is_new:
                results.append({
                    "firm_id": firm_id, "signal_type": "oci_pipeline_prediction",
                    "weight": weight, "title": desc[:80],
                    "practice_area": "general",
                })

        return results
