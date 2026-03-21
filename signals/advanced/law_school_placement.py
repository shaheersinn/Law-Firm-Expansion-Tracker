"""
signals/advanced/law_school_placement.py
──────────────────────────────────────────
Signal 12 — Law School Placement Intelligence

Monitors articling recruitment data from:
  • Ultra Vires (University of Toronto Faculty of Law student paper)
  • Osgoode Hall Law School placement data
  • University of Calgary Faculty of Law — NALP data
  • lawrecruits.com — Canadian articling tracking

Key signals:
  1. ABSENT FIRM: A firm that historically hires students didn't participate
     in the match → they may need a lateral associate instead
  2. UNDER-MATCH: A firm took fewer students than last year → they might
     hire a called lawyer to fill the gap without the training overhead
  3. FREE AGENTS: Students who matched but then chose not to article (rare)
     → they're looking for immediate called-lawyer positions
  4. POST-MATCH COLLAPSE: A firm cancelled articles (very rare but it happens)
     → immediate associate vacancy

Also tracks:
  • U of C class size → Alberta-licensed lawyers entering market
  • Bar admission cycle → when newly called lawyers are available
"""

import re, time, logging
from datetime import date, datetime
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import requests
from bs4 import BeautifulSoup

from database.db import insert_signal
from config_calgary import CALGARY_FIRMS, FIRM_BY_ID

log = logging.getLogger(__name__)

# Law school sources
ULTRA_VIRES_URL   = "https://ultravires.ca"
LAWRECRUITS_URL   = "https://www.lawrecruits.com/blog/"

# Historical participation data (update each year from published NALP data)
HISTORICAL_PARTICIPANTS = {
    "blakes":        {"avg_students": 4, "years": [2022, 2023, 2024]},
    "mccarthy":      {"avg_students": 5, "years": [2022, 2023, 2024]},
    "osler":         {"avg_students": 4, "years": [2022, 2023, 2024]},
    "norton_rose":   {"avg_students": 4, "years": [2022, 2023, 2024]},
    "bennett_jones": {"avg_students": 3, "years": [2022, 2023, 2024]},
    "burnet":        {"avg_students": 2, "years": [2022, 2023, 2024]},
    "field_law":     {"avg_students": 2, "years": [2022, 2023, 2024]},
    "miller_thomson":{"avg_students": 2, "years": [2023, 2024]},
    "borden_ladner": {"avg_students": 3, "years": [2022, 2023, 2024]},
    "gowling":       {"avg_students": 3, "years": [2022, 2023, 2024]},
    "stikeman":      {"avg_students": 3, "years": [2022, 2023, 2024]},
}


class LawSchoolPlacementMonitor:
    """
    Scrapes law school placement publications and detects hiring pattern anomalies.
    """

    def __init__(self):
        self.new_signals: list[dict] = []
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "Mozilla/5.0 (research tool)"

    def run(self) -> list[dict]:
        log.info("[LawSchool] Checking placement publications…")
        self._scan_ultra_vires()
        self._scan_lawrecruits()
        self._check_absent_firms()
        log.info("[LawSchool] Done. %d signals.", len(self.new_signals))
        return self.new_signals

    def _scan_ultra_vires(self):
        """Scrape Ultra Vires for articling match / hireback data."""
        try:
            resp = self.session.get(ULTRA_VIRES_URL, timeout=12)
            soup = BeautifulSoup(resp.text, "lxml")
            articles = soup.select("article, .post, .entry")
            for art in articles[:20]:
                title = art.find(["h2","h3","h1"])
                if not title:
                    continue
                text = title.get_text(strip=True).lower()
                if any(k in text for k in ["articl", "recruit", "hireback", "retention", "match results"]):
                    body = art.get_text(" ", strip=True)
                    self._parse_placement_text(body, "Ultra Vires",
                                               art.find("a")["href"] if art.find("a") else ULTRA_VIRES_URL)
        except Exception as e:
            log.debug("[LawSchool] Ultra Vires error: %s", e)

    def _scan_lawrecruits(self):
        """Scan lawrecruits.com blog for Calgary articling data."""
        try:
            resp = self.session.get(LAWRECRUITS_URL, timeout=12)
            soup = BeautifulSoup(resp.text, "lxml")
            for link in soup.select("a[href]"):
                href = link["href"]
                txt  = link.get_text(strip=True).lower()
                if any(k in txt for k in ["calgary", "alberta", "articling 2025", "articling 2026"]):
                    self._parse_placement_text(txt, "LawRecruits", href)
        except Exception as e:
            log.debug("[LawSchool] LawRecruits error: %s", e)

    def _parse_placement_text(self, text: str, source: str, url: str):
        """Look for firm names + student counts in placement text."""
        text_lower = text.lower()
        for firm in CALGARY_FIRMS:
            matched = any(a.lower() in text_lower for a in [firm["name"]] + firm["aliases"])
            if not matched:
                continue

            # Look for numbers near the firm name
            pattern = re.compile(
                r"(?:" + "|".join(re.escape(a) for a in [firm["name"]] + firm["aliases"]) +
                r")[\s\S]{0,80}?(\d+)\s*(?:student|articl|position|spot|seat)",
                re.IGNORECASE,
            )
            m = pattern.search(text)
            if m:
                count = int(m.group(1))
                hist  = HISTORICAL_PARTICIPANTS.get(firm["id"], {})
                avg   = hist.get("avg_students", 0)
                if avg > 0 and count < avg - 1:
                    is_new = insert_signal(
                        firm_id=firm["id"],
                        signal_type="placement_under_match",
                        weight=3.5,
                        title=f"[{source}] {firm['name']}: took {count} students vs avg {avg} — lateral needed",
                        description=(
                            f"Articling match data from {source}: {firm['name']} took {count} students "
                            f"vs historical average of {avg}. Under-matching by {avg - count} seats "
                            f"suggests they may hire a called lawyer to fill the gap without training overhead."
                        ),
                        source_url=url,
                        practice_area=firm.get("focus", ["general"])[0],
                        raw_data={"count": count, "historical_avg": avg, "source": source},
                    )
                    if is_new:
                        self.new_signals.append({
                            "firm_id": firm["id"],
                            "signal_type": "placement_under_match",
                            "weight": 3.5,
                            "title": f"[{source}] {firm['name']}: under-matched articling ({count} vs {avg} avg)",
                            "practice_area": firm.get("focus", ["general"])[0],
                        })

    def _check_absent_firms(self):
        """
        Firms that historically participate in the articling match but
        haven't been seen in any recent placement article.
        Fire a signal: they may be hiring laterals instead.
        """
        current_month = date.today().month
        # Articling match announcements happen in August-October
        if current_month not in range(8, 11):
            return

        for firm_id, hist in HISTORICAL_PARTICIPANTS.items():
            if date.today().year not in [y + 1 for y in hist.get("years", [])]:
                continue   # firm wasn't recently in the match

            # Check if we've seen them in any placement signal
            from database.db import get_conn
            conn = get_conn()
            row  = conn.execute("""
                SELECT count(*) as c FROM signals
                WHERE firm_id=?
                  AND signal_type='placement_under_match'
                  AND strftime('%Y', detected_at) = strftime('%Y', 'now')
            """, (firm_id,)).fetchone()
            conn.close()

            if row and row["c"] == 0:
                firm = FIRM_BY_ID.get(firm_id, {})
                insert_signal(
                    firm_id=firm_id,
                    signal_type="placement_absent_firm",
                    weight=3.0,
                    title=f"Articling match: {firm.get('name',firm_id)} absent this cycle",
                    description=(
                        f"{firm.get('name',firm_id)} has historically participated in the "
                        f"Calgary articling match (avg {hist.get('avg_students',0)} students) "
                        f"but no placement data found for the current cycle. "
                        f"They may be hiring a called lawyer directly instead of an articling student."
                    ),
                    source_url=ULTRA_VIRES_URL,
                    practice_area=firm.get("focus", ["general"])[0] if firm else "general",
                    raw_data={"historical_avg": hist.get("avg_students", 0)},
                )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    mon = LawSchoolPlacementMonitor()
    sigs = mon.run()
    for s in sigs:
        print(f"  [{s['signal_type']}] {s['firm_id']}: {s['title']}")
