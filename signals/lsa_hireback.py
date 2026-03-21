"""
signals/lsa_hireback.py
────────────────────────
Strategy 4 — "Hireback Vacuum" Exploit

Tracks articling students through the Law Society of Alberta (LSA) public
lawyer directory. After the articling term ends (August), checks which students
were NOT retained as associates at their articling firm. Firms that budgeted
for 4 juniors but only retained 2 have unused salary budget → prime targets.

LSA public lookup: https://www.lawsociety.ab.ca/lawyer-lookup/

NOTE: The LSA directory is public record. We only scrape publicly visible
      information (name, membership status, employer). We do not attempt to
      access any private or member-only areas.
"""

import re
import time
import logging
from datetime import datetime, date
from collections import defaultdict

import requests
from bs4 import BeautifulSoup

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config_calgary import (
    LSA_SEARCH_URL, LSA_SCRAPE_DELAY_S,
    CALGARY_FIRMS, FIRM_ALIASES, SIGNAL_WEIGHTS,
    ARTICLING_YEAR, ARTICLING_START_MONTH, ARTICLING_END_MONTH,
)
from database.db import insert_signal, get_conn

log = logging.getLogger(__name__)


# ─── LSA scraper ─────────────────────────────────────────────────────────────

class LSADirectoryClient:
    """
    Wraps the LSA public lawyer lookup form.
    Searches by firm name + member type to find articling students.
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = (
            "LawFirmTracker/2.0 (research tool; contact: admin@example.com)"
        )

    def _get(self, url: str, params: dict = None) -> BeautifulSoup:
        time.sleep(LSA_SCRAPE_DELAY_S)
        resp = self.session.get(url, params=params or {}, timeout=20)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "lxml")

    def search_by_firm(self, firm_name: str, member_type: str = "Student") -> list[dict]:
        """
        Search the LSA directory for members with a given firm name and type.
        member_type: "Student" | "Lawyer" | "Articled Clerk"
        """
        params = {
            "firm":        firm_name,
            "member_type": member_type,
            "city":        "Calgary",
        }
        try:
            soup = self._get(LSA_SEARCH_URL, params)
        except Exception as e:
            log.error("[LSA] Search failed for %s: %s", firm_name, e)
            return []

        results = []
        # Parse the results table — adapt selectors to actual LSA HTML structure
        for row in soup.select("table.results-table tbody tr"):
            cells = row.select("td")
            if len(cells) < 3:
                continue
            results.append({
                "full_name":   cells[0].get_text(strip=True),
                "member_type": cells[1].get_text(strip=True),
                "firm":        cells[2].get_text(strip=True),
                "city":        cells[3].get_text(strip=True) if len(cells) > 3 else "",
                "lsa_id":      self._extract_id(cells[0]),
            })
        return results

    def get_member_detail(self, lsa_id: str) -> dict:
        """Fetch individual member detail page."""
        url = f"{LSA_SEARCH_URL}?member={lsa_id}"
        try:
            soup = self._get(url)
            firm = soup.select_one(".member-firm")
            return {
                "lsa_id": lsa_id,
                "firm":   firm.get_text(strip=True) if firm else "",
            }
        except Exception as e:
            log.debug("[LSA] Member detail failed %s: %s", lsa_id, e)
            return {}

    @staticmethod
    def _extract_id(cell) -> str:
        link = cell.find("a")
        if link and link.get("href"):
            m = re.search(r"member=(\w+)", link["href"])
            if m:
                return m.group(1)
        return ""


# ─── Retention gap detector ──────────────────────────────────────────────────

class HirebackVacuumTracker:
    """
    Post-articling: compare the class of students from last year to the
    associate list at each firm. Gap = unspent headcount budget.
    """

    def __init__(self):
        self.client = LSADirectoryClient()
        self.new_signals: list[dict] = []

    def run(self):
        today = date.today()
        # Only run after articling term ends (August → wait until Sept-Oct)
        if today.month < ARTICLING_END_MONTH:
            log.info("[LSA] Articling term not yet complete. Skipping hireback check.")
            return []

        log.info("[LSA] Running hireback vacuum check for %d cohort", ARTICLING_YEAR)
        for firm in CALGARY_FIRMS:
            self._check_firm(firm)

        log.info("[LSA] Done. %d signals.", len(self.new_signals))
        return self.new_signals

    def _check_firm(self, firm: dict):
        firm_name = firm["name"]

        # Step 1: Find articling students from the cohort year
        students = self._load_from_db(firm["id"])
        if not students:
            # Fall back: query LSA for current students
            students = self.client.search_by_firm(firm_name, member_type="Student")
            self._persist_students(firm["id"], students)

        # Step 2: Find current associates at the firm (members who became lawyers)
        retained = self.client.search_by_firm(firm_name, member_type="Lawyer")
        retained_names = {r["full_name"].lower() for r in retained}

        student_names    = [s["full_name"] for s in students]
        not_retained     = [n for n in student_names if n.lower() not in retained_names]
        retention_gap    = len(not_retained)
        cohort_size      = len(student_names)

        if cohort_size == 0:
            return

        retention_rate   = (cohort_size - retention_gap) / cohort_size

        log.info("[LSA] %s: cohort=%d retained=%d gap=%d (%.0f%%)",
                 firm["name"], cohort_size, cohort_size - retention_gap,
                 retention_gap, retention_rate * 100)

        if retention_gap == 0:
            return

        # ── Score: larger gap = more urgent, boutiques with any gap = very urgent ──
        if retention_gap >= 2 or (firm["tier"] == "boutique" and retention_gap >= 1):
            weight    = SIGNAL_WEIGHTS["lsa_retention_gap"]
            sig_type  = "lsa_retention_gap"
        else:
            weight    = SIGNAL_WEIGHTS["lsa_student_not_retained"]
            sig_type  = "lsa_student_not_retained"

        msg = (f"Post-articling analysis: {firm['name']} had {cohort_size} articling students "
               f"in {ARTICLING_YEAR} but only retained {cohort_size - retention_gap}. "
               f"Gap of {retention_gap} associate positions are budgeted but unfilled. "
               f"Not-retained: {', '.join(not_retained[:5])}.")

        self.new_signals.append({
            "firm_id":     firm["id"],
            "signal_type": sig_type,
            "weight":      weight,
            "title":       f"Hireback gap at {firm['name']}: {retention_gap} unfilled associate slots",
            "description": msg,
            "source_url":  LSA_SEARCH_URL,
            "raw_data":    {
                "cohort_size":    cohort_size,
                "retained_count": cohort_size - retention_gap,
                "gap":            retention_gap,
                "not_retained":   not_retained,
            },
        })
        insert_signal(
            firm_id=firm["id"],
            signal_type=sig_type,
            weight=weight,
            title=f"Hireback gap at {firm['name']}: {retention_gap} unfilled slots",
            description=msg,
            source_url=LSA_SEARCH_URL,
            raw_data={
                "cohort_size": cohort_size,
                "gap": retention_gap,
                "not_retained": not_retained,
            },
        )

        # Store departing student records
        for name in not_retained:
            conn = get_conn()
            conn.execute("""
                INSERT OR IGNORE INTO lsa_students
                    (firm_id, full_name, articling_year, status, as_of_date)
                VALUES (?, ?, ?, 'departed', ?)
            """, (firm["id"], name, ARTICLING_YEAR, date.today().isoformat()))
            conn.commit()
            conn.close()

    def _load_from_db(self, firm_id: str) -> list[dict]:
        conn = get_conn()
        rows = conn.execute("""
            SELECT full_name, lsa_id FROM lsa_students
            WHERE firm_id = ? AND articling_year = ?
        """, (firm_id, ARTICLING_YEAR)).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def _persist_students(self, firm_id: str, students: list[dict]):
        conn = get_conn()
        for s in students:
            conn.execute("""
                INSERT OR IGNORE INTO lsa_students
                    (firm_id, full_name, lsa_id, articling_year, status, as_of_date)
                VALUES (?, ?, ?, ?, 'articling', ?)
            """, (firm_id, s["full_name"], s.get("lsa_id", ""),
                  ARTICLING_YEAR, date.today().isoformat()))
        conn.commit()
        conn.close()


# ─── Bonus: LSA new-call monitor ─────────────────────────────────────────────

class LSANewCallMonitor:
    """
    Watches for new lawyer admissions in Calgary (Law Society "call to the bar").
    A new call from a known firm = confirms retention; from an unknown firm = 
    that person was probably at a firm that let them go → signal.
    
    Can also identify "free agents" — newly called lawyers without an employer —
    who may be looking for their first associate position.
    """

    def __init__(self):
        self.client = LSADirectoryClient()

    def find_new_calls_without_employers(self, days_back: int = 90) -> list[dict]:
        """
        Finds recently called lawyers who do not list a firm employer.
        These are prime candidates for outreach as they may be job-hunting.
        """
        # Implemented as a template; adapt to actual LSA directory search
        results = self.client.search_by_firm("", member_type="Lawyer")
        no_employer = [r for r in results if not r.get("firm")]
        log.info("[LSA] Found %d newly called lawyers without listed employer", len(no_employer))
        return no_employer


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    tracker = HirebackVacuumTracker()
    signals = tracker.run()
    for s in signals:
        print(f"  [{s['signal_type']}] {s['firm_id']}: {s['title']}")
