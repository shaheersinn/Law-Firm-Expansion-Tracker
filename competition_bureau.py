"""
signals/deep/competition_bureau.py
────────────────────────────────────
Signal 18 — Competition Bureau Pre-Merger Notification Monitor
             + Investment Canada Act Review Monitor

THE MOST UNDERUSED PUBLIC SIGNAL IN CANADA.

Under the Competition Act s.114, transactions over ~$93M (2024 threshold)
must be pre-notified to the Competition Bureau BEFORE closing. These
notifications are made public on the Bureau's website with a 30-day
review window.

WHY THIS IS GOLD:
The notification is filed BEFORE the deal closes and often BEFORE it's
publicly announced (parties can request confidentiality, but the EXISTENCE
of a notification is always public). This gives you the earliest possible
signal that:
  1. A major Calgary M&A deal is in progress
  2. Which law firms are doing the work (firms are named on notifications)
  3. What the transaction involves (industry, parties)

Timeline: Pre-notification → public at filing → 30-day waiting period → close
You see it at Day 0. The deal is typically announced on Day 7-14.
SEDAR+ filing appears on Day 14-30. CanLII sees a related case in 12+ months.

Sources:
  https://www.canada.ca/en/competition-bureau/services/merger-review.html
  https://www.canada.ca/en/competition-bureau/news/merger-notifications.html
  RSS: https://www.canada.ca/en/competition-bureau/news/merger-notifications.rss

Investment Canada Act (ICA) Reviews:
Large foreign investments in Canada ($1.287B+ threshold, 2024) require
ICA review. These are announced publicly by Innovation, Science and Economic
Development Canada (ISED). Alberta energy company as target = Calgary legal work.
  https://www.ic.gc.ca/eic/site/ica-lic.nsf/eng/lk81126.html

Competition Bureau enforcement actions:
  - Section 79 abuse of dominance (big companies fighting government)
  - Consent agreements (massive compliance work)
  - Criminal cartel investigations (simultaneous civil and criminal counsel)
"""

import re, time, logging, hashlib
from datetime import date, datetime, timedelta
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import requests, feedparser
from bs4 import BeautifulSoup

from database.db import insert_signal, get_conn
from config_calgary import CALGARY_FIRMS, FIRM_BY_ID
from signals.advanced.aer_hearings import COMPANY_TO_COUNSEL

log = logging.getLogger(__name__)

BUREAU_RSS    = "https://www.canada.ca/en/competition-bureau/news.rss"
BUREAU_NOTIF  = "https://www.canada.ca/en/competition-bureau/services/merger-review/merger-notifications.html"
ICA_ANNOUNCEMENTS = "https://www.ic.gc.ca/eic/site/ica-lic.nsf/eng/lk81126.html"

CALGARY_KEYWORDS = re.compile(
    r"\b(Calgary|Alberta|Edmonton|Fort McMurray|oil sands|pipeline|"
    r"energy|TSX|oilfield|bitumen|LNG|Lloydminster)\b", re.IGNORECASE
)

DOLLAR_RE = re.compile(r"\$\s*([\d,\.]+)\s*(billion|million|B|M)\b", re.IGNORECASE)

# Competition/M&A boutiques and BigLaw that do Bureau work
COMPETITION_SPECIALISTS = [
    "mccarthy",      # top Competition group in Canada
    "blakes",
    "osler",
    "stikeman",      # strong Competition practice
    "borden_ladner",
    "dentons",
    "fmc_law",       # Fasken — strong Competition
    "bennett_jones",
    "gowling",
]


def _parse_value(text: str) -> float | None:
    for m in DOLLAR_RE.finditer(text):
        num  = float(m.group(1).replace(",", ""))
        unit = m.group(2).lower()
        if unit in ("billion", "b"): num *= 1000
        return num
    return None


class CompetitionBureauMonitor:

    def __init__(self):
        self.new_signals: list[dict] = []
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "LawTracker/5.0 (research)"

    def run(self) -> list[dict]:
        log.info("[CompBureau] Scanning Competition Bureau + ICA notifications…")
        self._scan_bureau_rss()
        self._scan_bureau_notifications()
        self._scan_ica_reviews()
        log.info("[CompBureau] Done. %d signals.", len(self.new_signals))
        return self.new_signals

    def _scan_bureau_rss(self):
        try:
            feed = feedparser.parse(BUREAU_RSS)
        except Exception as e:
            log.debug("[CompBureau] RSS error: %s", e); return

        for entry in feed.entries:
            title   = getattr(entry, "title",   "")
            link    = getattr(entry, "link",    "")
            summary = getattr(entry, "summary", "")
            combined = f"{title} {summary}"

            is_calgary = bool(CALGARY_KEYWORDS.search(combined))
            is_merger  = bool(re.search(r"\b(merger|acquisition|pre-notification|transaction|consent agreement|abuse)\b", combined, re.I))

            if not (is_calgary or is_merger):
                continue

            value   = _parse_value(combined)
            is_big  = value and value >= 93
            weight  = 5.0 if is_big and is_calgary else (4.0 if is_calgary else 3.0)
            pa      = "corporate"
            st      = "competition_bureau_notification"

            desc = (
                f"[Competition Bureau] {title}. "
                f"{'Calgary/Alberta connection detected. ' if is_calgary else ''}"
                f"{'Value: $' + f'{value:.0f}M. ' if value else ''}"
                f"Pre-merger notifications are filed BEFORE public announcement. "
                f"30-day waiting period = window to contact counsel now."
            )

            for firm_id in COMPETITION_SPECIALISTS[:5]:
                is_new = insert_signal(
                    firm_id=firm_id, signal_type=st,
                    weight=weight, title=f"[CompBureau] {title[:70]}",
                    description=desc, source_url=link, practice_area=pa,
                    raw_data={"value_m": value, "is_calgary": is_calgary},
                )
                if is_new:
                    self.new_signals.append({
                        "firm_id": firm_id, "signal_type": st,
                        "weight": weight, "title": f"[CompBureau] {title[:70]}",
                        "practice_area": pa, "description": desc,
                    })

    def _scan_bureau_notifications(self):
        """Scrape the Bureau's merger notifications page directly."""
        try:
            resp = self.session.get(BUREAU_NOTIF, timeout=12)
            soup = BeautifulSoup(resp.text, "lxml")
            # Look for tables listing notifications
            tables = soup.select("table")
            for tbl in tables[:3]:
                for row in tbl.select("tr")[1:20]:
                    cells = row.select("td")
                    if len(cells) < 3: continue
                    text    = " ".join(c.get_text(strip=True) for c in cells)
                    parties = cells[0].get_text(strip=True) if cells else ""
                    date_str= cells[-1].get_text(strip=True) if cells else ""

                    if not CALGARY_KEYWORDS.search(text): continue
                    value = _parse_value(text)

                    # Identify which Calgary companies are involved
                    company = ""
                    for co in COMPANY_TO_COUNSEL:
                        if co.lower() in text.lower():
                            company = co; break

                    counsel = COMPANY_TO_COUNSEL.get(company, []) + COMPETITION_SPECIALISTS[:3]
                    for firm_id in list(set(counsel))[:4]:
                        insert_signal(
                            firm_id=firm_id,
                            signal_type="competition_bureau_notification",
                            weight=5.0,
                            title=f"[CompBureau Pre-Notif] {parties[:70]}",
                            description=(
                                f"Competition Bureau merger pre-notification filed: {text[:200]}. "
                                f"This filing precedes public announcement. "
                                f"Calgary company involved. Competition counsel retained immediately."
                            ),
                            source_url=BUREAU_NOTIF,
                            practice_area="corporate",
                            raw_data={"parties": parties, "value_m": value, "company": company},
                        )
        except Exception as e:
            log.debug("[CompBureau] Notification page error: %s", e)

    def _scan_ica_reviews(self):
        """Investment Canada Act review announcements."""
        try:
            resp = self.session.get(ICA_ANNOUNCEMENTS, timeout=12)
            soup = BeautifulSoup(resp.text, "lxml")

            recent_cutoff = (date.today() - timedelta(days=90)).isoformat()
            for link in soup.select("a[href*='review'], a[href*='investment']"):
                text = link.get_text(strip=True)
                href = link.get("href", "")
                if not CALGARY_KEYWORDS.search(text): continue

                value   = _parse_value(text)
                weight  = 5.5 if value and value >= 1000 else 4.5
                desc = (
                    f"[ICA Review] Foreign investment review of Calgary/Alberta entity: {text}. "
                    f"{'$' + f'{value:.0f}M+ ' if value else ''}"
                    f"ICA reviews require heavy legal advisory work — regulatory counsel, "
                    f"net benefit analysis, national security review filings."
                )
                for firm_id in ["mccarthy","blakes","osler","borden_ladner","gowling"][:4]:
                    is_new = insert_signal(
                        firm_id=firm_id,
                        signal_type="ica_review_announced",
                        weight=weight,
                        title=f"[ICA] Foreign investment review: {text[:70]}",
                        description=desc,
                        source_url=href or ICA_ANNOUNCEMENTS,
                        practice_area="corporate",
                        raw_data={"text": text, "value_m": value},
                    )
                    if is_new:
                        self.new_signals.append({
                            "firm_id": firm_id, "signal_type": "ica_review_announced",
                            "weight": weight, "title": f"[ICA] {text[:60]}",
                            "practice_area": "corporate",
                        })
        except Exception as e:
            log.debug("[CompBureau] ICA error: %s", e)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    mon = CompetitionBureauMonitor()
    for s in mon.run():
        print(f"  [{s['signal_type']}] {s['firm_id']}: {s['title']}")
