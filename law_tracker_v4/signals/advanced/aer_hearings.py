"""
signals/advanced/aer_hearings.py
──────────────────────────────────
Signal 7 — Alberta Energy Regulator (AER) + Alberta Utilities Commission (AUC)
            Hearing Calendar Monitor

The insight nobody uses: AER and AUC post upcoming regulatory hearings publicly.
Each hearing has:
  - A named applicant/respondent company
  - A proceeding type (facility amendment, acquisition approval, pipeline, rate case)
  - A hearing DATE — usually 60-90 days out

Cross-referencing the company names against our BigLaw conflict map tells you
EXACTLY which law firm will be retained for that proceeding — before they've
even started billing. Fire the signal the day the hearing is POSTED.

Sources:
  • AER Proceedings: https://www.aer.ca/regulating-development/applications-and-hearings
  • AUC Proceedings: https://www.auc.ab.ca/regulatory_documents/ProceedingDocuments/
  • AER RSS:         https://www.aer.ca/rss/applications
  • AUC RSS:         https://www.auc.ab.ca/rss

Additional:
  • Canada Energy Regulator (CER/NEB) — national pipeline proceedings
  • Competition Bureau merger notifications — Calgary deals

Maps company → known outside counsel via CanLII/SEDAR historical data.
"""

import re, time, logging, hashlib
from datetime import datetime, date, timedelta
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import requests, feedparser
from bs4 import BeautifulSoup

from database.db import insert_signal, get_conn
from config_calgary import CALGARY_FIRMS, FIRM_BY_ID, BIGLAW_FIRMS

log = logging.getLogger(__name__)

AER_RSS  = "https://www.aer.ca/rss/applications.xml"
AUC_RSS  = "https://www.auc.ab.ca/regulatory_documents/ProceedingDocuments/RSS.xml"
CER_RSS  = "https://www.cer-rec.gc.ca/en/applications-hearings/hear/rss.xml"

# Proceeding types that require HEAVY legal work (junior doc-review intensive)
HEAVY_PROCEEDING_TYPES = re.compile(
    r"\b(acquisition|amalgamation|merger|transfer of ownership|change of control|"
    r"facility amendment|export licence|project application|CCAA|receivership|"
    r"group 1|facility abandonment|reclamation|compensation|Section 39|Section 41)\b",
    re.IGNORECASE,
)

# Major Alberta energy companies and their typical outside counsel
# Built from historical CanLII/SEDAR data — update as patterns emerge
COMPANY_TO_COUNSEL = {
    "cenovus":        ["mccarthy", "blakes", "norton_rose"],
    "suncor":         ["osler", "mccarthy", "blakes"],
    "arc resources":  ["norton_rose", "burnet"],
    "tourmaline":     ["norton_rose", "bennett_jones"],
    "tc energy":      ["blakes", "mccarthy"],
    "enbridge":       ["mccarthy", "gowling", "blakes"],
    "pembina":        ["bennett_jones", "burnet"],
    "whitecap":       ["field_law", "burnet"],
    "baytex":         ["osler", "borden_ladner"],
    "meg energy":     ["bennett_jones", "norton_rose"],
    "transalta":      ["blakes", "borden_ladner"],
    "capital power":  ["mccarthy", "parlee_mclaws"],
    "keyera":         ["bennett_jones", "burnet"],
    "crescent point": ["norton_rose", "field_law"],
    "canadian natural": ["blakes", "mccarthy", "stikeman"],
}

# Boutiques most likely to represent the other side in regulatory proceedings
REGULATORY_BOUTIQUES = [
    "burnet", "field_law", "parlee_mclaws", "miller_thomson",
    "walsh_law", "witten", "reynolds_mirth",
]


class AERHearingMonitor:
    """
    Polls AER + AUC RSS feeds and proceeding calendars.
    Fires signals up to 90 days before a hearing, giving maximum lead time.
    """

    def __init__(self):
        self.new_signals: list[dict] = []
        self.session = requests.Session()
        self.session.headers["User-Agent"] = (
            "LawFirmTracker/3.0 (research; admin@example.com)"
        )

    def run(self) -> list[dict]:
        log.info("[AER/AUC] Polling regulatory hearing calendars…")
        self._poll_rss(AER_RSS,  "AER")
        self._poll_rss(AUC_RSS,  "AUC")
        self._poll_rss(CER_RSS,  "CER")
        self._scrape_aer_proceedings()
        log.info("[AER/AUC] Done. %d signals.", len(self.new_signals))
        return self.new_signals

    # ── RSS polling ────────────────────────────────────────────────────────────

    def _poll_rss(self, url: str, source: str):
        try:
            feed = feedparser.parse(url)
        except Exception as e:
            log.debug("[AER/AUC] RSS parse error %s: %s", url, e)
            return

        for entry in feed.entries:
            title   = getattr(entry, "title",   "")
            link    = getattr(entry, "link",    "")
            summary = getattr(entry, "summary", "")
            self._process_proceeding(f"{title} {summary}", link, source)

    # ── AER proceeding scraper ─────────────────────────────────────────────────

    def _scrape_aer_proceedings(self):
        """
        Scrape the AER applications table for upcoming hearings.
        AER publishes a public applications list at aer.ca.
        """
        url = "https://www.aer.ca/regulating-development/applications-and-hearings/hearings"
        try:
            resp = self.session.get(url, timeout=15)
            soup = BeautifulSoup(resp.text, "lxml")
            rows = soup.select("table tr")
            for row in rows[1:]:   # skip header
                cells = row.select("td")
                if len(cells) < 3:
                    continue
                applicant = cells[0].get_text(strip=True)
                proc_type = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                date_str  = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                link_tag  = cells[0].find("a")
                link      = link_tag["href"] if link_tag and link_tag.get("href") else url
                self._process_proceeding(
                    f"{applicant} {proc_type}",
                    link, "AER_WEB",
                    applicant=applicant,
                    hearing_date=self._parse_date(date_str),
                )
        except Exception as e:
            log.debug("[AER] Scrape failed: %s", e)

    # ── Processing ────────────────────────────────────────────────────────────

    def _process_proceeding(self, text: str, link: str, source: str,
                            applicant: str = "", hearing_date: date = None):
        if not HEAVY_PROCEEDING_TYPES.search(text):
            return

        company   = self._match_company(text)
        firms_for = COMPANY_TO_COUNSEL.get(company.lower(), []) if company else []
        # Also alert boutiques likely to be on the other side
        firms_for.extend(REGULATORY_BOUTIQUES)
        firms_for  = list(set(firms_for))

        if not firms_for and not company:
            return

        days_until = (hearing_date - date.today()).days if hearing_date else 60
        if days_until < 0:
            return   # past hearing

        weight = self._weight(days_until)
        pa     = "energy" if "energy" in text.lower() or company else "regulatory"

        dedup_key = hashlib.md5(f"{text[:60]}{source}".encode()).hexdigest()[:16]

        for firm_id in firms_for:
            firm = FIRM_BY_ID.get(firm_id)
            if not firm:
                continue
            hearing_str = f" (hearing: {hearing_date.isoformat()})" if hearing_date else ""
            desc = (
                f"[{source}] Regulatory proceeding filed: {text[:200]}. "
                f"Applicant: {applicant or 'unknown'}. "
                f"Likely counsel: {firm['name']}. "
                f"Lead time: {days_until} days{hearing_str}. "
                f"Junior regulatory and energy law support will be needed."
            )
            is_new = insert_signal(
                firm_id=firm_id,
                signal_type="aer_proceeding_upcoming",
                weight=weight,
                title=f"[{source}] Regulatory proceeding: {(applicant or text)[:60]}",
                description=desc,
                source_url=link,
                practice_area=pa,
                raw_data={"source": source, "company": company,
                          "days_until": days_until, "hearing_date": str(hearing_date)},
            )
            if is_new:
                self.new_signals.append({
                    "firm_id": firm_id,
                    "signal_type": "aer_proceeding_upcoming",
                    "weight": weight,
                    "title": f"[{source}] Regulatory proceeding: {(applicant or text)[:60]}",
                    "practice_area": pa,
                })

    @staticmethod
    def _match_company(text: str) -> str:
        text_lower = text.lower()
        for company in COMPANY_TO_COUNSEL:
            if company in text_lower:
                return company
        return ""

    @staticmethod
    def _weight(days_until: int) -> float:
        """More urgent = higher weight. <14 days = emergency."""
        if days_until <= 14:  return 4.5
        if days_until <= 30:  return 4.0
        if days_until <= 60:  return 3.5
        return 3.0

    @staticmethod
    def _parse_date(s: str) -> date:
        for fmt in ("%Y-%m-%d", "%B %d, %Y", "%b %d, %Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(s.strip(), fmt).date()
            except ValueError:
                pass
        return date.today() + timedelta(days=60)   # fallback


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    mon = AERHearingMonitor()
    sigs = mon.run()
    for s in sigs:
        print(f"  [{s['signal_type']}] {s['firm_id']}: {s['title']}")
