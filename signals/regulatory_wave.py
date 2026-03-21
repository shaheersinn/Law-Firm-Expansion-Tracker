"""
signals/regulatory_wave.py
───────────────────────────
The "Regulatory Wave Rider" Signal

New regulations and regulatory proceedings are a law firm's rain.
Two primary sources:

A) Alberta Energy Regulator (AER) Hearings Docket
   - Public hearings list: https://www.aer.ca/regulating-development/
     application-status/decisions-orders/upcoming-hearings
   - Every AER hearing has counsel appearing for applicants/interveners
   - A Calgary energy firm suddenly appearing on 5+ AER hearings = capacity crunch
   - Pattern: AER application surge → energy firms need juniors within 30 days

B) Alberta Gazette / Canada Gazette
   - New regulations = new compliance work = firms need junior associates
   - Track mentions of Calgary firms as "legal advisors" in gazette notices
   - Key triggers: new energy transition regulations, pipeline rules,
     royalty framework changes, securities amendments

C) ASC (Alberta Securities Commission) Enforcement Bulletins
   - When a Calgary firm is defending an ASC enforcement action,
     the partner in charge suddenly needs maximum associate hours
   - https://www.albertasecurities.com/enforcement/enforcement-proceedings

D) Competition Bureau M&A Clearance Filings
   - Every merger notification lists outside counsel
   - https://www.canada.ca/en/competition-bureau/services/merger-review.html
   - Cross-reference to Calgary firms = immediate due diligence work

Signal weight: 3.8–5.0 depending on volume/size
"""

import re
import time
import logging
from datetime import datetime, date, timedelta
from collections import defaultdict

import requests
import feedparser
from bs4 import BeautifulSoup

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config_calgary import (
    CALGARY_FIRMS, FIRM_ALIASES, FIRM_BY_ID, SIGNAL_WEIGHTS,
    CANLII_RATE_LIMIT_S,
)
from database.db import insert_signal

log = logging.getLogger(__name__)

# ─── Source URLs ──────────────────────────────────────────────────────────────

AER_HEARINGS_URL  = "https://www.aer.ca/regulating-development/application-status/decisions-orders/upcoming-hearings"
AER_DECISIONS_URL = "https://www.aer.ca/regulating-development/application-status/decisions-orders"
ALBERTA_GAZETTE_RSS = "https://www.alberta.ca/alberta-gazette.aspx"   # scrape if no RSS
CANADA_GAZETTE_RSS  = "https://canadagazette.gc.ca/rss/p2-eng.xml"
ASC_ENFORCEMENT_URL = "https://www.albertasecurities.com/enforcement/enforcement-proceedings"
COMPETITION_BUREAU_URL = "https://www.canada.ca/en/competition-bureau/services/merger-review/merger-filings-pending-or-under-review.html"

# ─── Keywords ─────────────────────────────────────────────────────────────────

AER_ENERGY_KW = re.compile(
    r"\b(pipeline|oil sands|bitumen|natural gas|LNG|oilfield|well|AER|EPEA|"
    r"Energy Regulator|royalt|reclamation|abandonment|midstream)\b", re.I
)
REGULATORY_COMPLIANCE_KW = re.compile(
    r"\b(regulation|compliance|amendment|Act|Order in Council|OIC|"
    r"rule-making|framework|policy|directive|bulletin|notice)\b", re.I
)
DOLLAR_RE = re.compile(r"\$\s*([\d,]+(?:\.\d+)?)\s*(billion|million|B|M)\b", re.I)


# ─── Firm pattern matcher ─────────────────────────────────────────────────────

_FIRM_RE = {}
for _f in CALGARY_FIRMS:
    _tokens = [re.escape(a) for a in [_f["name"]] + _f["aliases"]]
    _FIRM_RE[_f["id"]] = re.compile("|".join(_tokens), re.IGNORECASE)


def find_firms(text: str) -> list[str]:
    return [fid for fid, pat in _FIRM_RE.items() if pat.search(text)]


def parse_deal_value(text: str) -> float | None:
    vals = []
    for m in DOLLAR_RE.finditer(text):
        n = float(m.group(1).replace(",", ""))
        u = m.group(2).lower()
        if u in ("billion", "b"): n *= 1000
        vals.append(n)
    return max(vals) if vals else None


class RegulatoryWaveTracker:
    """Monitors AER, Alberta Gazette, ASC, and Competition Bureau for legal work surges."""

    def __init__(self):
        self.new_signals: list[dict] = []
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "LawFirmTracker/3.0"

    def run(self) -> list[dict]:
        log.info("[RegWave] Scanning regulatory sources")
        self._scan_aer_hearings()
        self._scan_canada_gazette()
        self._scan_asc_enforcement()
        self._scan_competition_bureau()
        log.info("[RegWave] Done. %d signals.", len(self.new_signals))
        return self.new_signals

    # ── A) AER Hearings ───────────────────────────────────────────────────────

    def _scan_aer_hearings(self):
        try:
            resp = self.session.get(AER_HEARINGS_URL, timeout=15)
            soup = BeautifulSoup(resp.text, "lxml")
            # AER lists hearing items in table rows
            rows = soup.select("table tr, .hearing-item, article")
            hearing_text = " ".join(r.get_text(" ", strip=True) for r in rows[:60])
        except Exception as e:
            log.debug("[RegWave] AER fetch error: %s", e)
            return

        # Count appearances per firm
        firm_counts: dict[str, int] = defaultdict(int)
        for fid, pat in _FIRM_RE.items():
            count = len(pat.findall(hearing_text))
            if count > 0:
                firm_counts[fid] = count

        for firm_id, count in firm_counts.items():
            if count < 2:
                continue
            firm  = FIRM_BY_ID.get(firm_id, {})
            weight = 3.5 + min(count * 0.2, 1.5)
            desc   = (
                f"{firm.get('name', firm_id)} appears in {count} upcoming AER hearings. "
                f"Energy litigation and regulatory counsel load is elevated. "
                f"Junior associates needed for hearing prep, record review, "
                f"and regulatory filing support within the next 30 days."
            )
            is_new = insert_signal(
                firm_id=firm_id, signal_type="aer_hearing_load",
                weight=weight,
                title=f"AER Hearing Load: {firm.get('name', firm_id)} in {count} upcoming hearings",
                description=desc,
                source_url=AER_HEARINGS_URL,
                practice_area="energy",
                raw_data={"hearing_count": count},
            )
            if is_new:
                self.new_signals.append({
                    "firm_id": firm_id, "signal_type": "aer_hearing_load",
                    "weight": weight, "title": f"AER Hearing Load: {count} hearings",
                    "practice_area": "energy",
                })

    # ── B) Canada Gazette ─────────────────────────────────────────────────────

    def _scan_canada_gazette(self):
        try:
            feed = feedparser.parse(CANADA_GAZETTE_RSS)
        except Exception as e:
            log.debug("[RegWave] Gazette RSS error: %s", e)
            return

        for entry in feed.entries[:30]:
            title   = getattr(entry, "title", "")
            summary = getattr(entry, "summary", "")
            link    = getattr(entry, "link", "")
            text    = f"{title} {summary}"

            if not (AER_ENERGY_KW.search(text) or REGULATORY_COMPLIANCE_KW.search(text)):
                continue

            # Identify affected practice area
            pa = "energy" if AER_ENERGY_KW.search(text) else "regulatory"

            # High-impact regulatory change → alert ALL energy/regulatory Calgary firms
            affected = [f for f in CALGARY_FIRMS if pa in f.get("focus", [])]
            for firm in affected[:5]:   # top 5 most focused firms
                weight = 3.2
                desc   = (
                    f"New Canada Gazette notice: '{title[:100]}'. "
                    f"This regulatory change is likely to generate immediate compliance "
                    f"advisory work, client memos, and potential regulatory filings "
                    f"across {firm['name']}'s {pa} practice. Junior associate demand "
                    f"for research and drafting will rise sharply."
                )
                is_new = insert_signal(
                    firm_id=firm["id"], signal_type="regulatory_wave",
                    weight=weight,
                    title=f"Regulatory Wave: {title[:70]}",
                    description=desc,
                    source_url=link,
                    practice_area=pa,
                    raw_data={"gazette_title": title, "practice_area": pa},
                )
                if is_new:
                    self.new_signals.append({
                        "firm_id": firm["id"], "signal_type": "regulatory_wave",
                        "weight": weight, "practice_area": pa,
                        "title": f"Regulatory Wave: {title[:60]}",
                    })

    # ── C) ASC Enforcement ────────────────────────────────────────────────────

    def _scan_asc_enforcement(self):
        try:
            resp = self.session.get(ASC_ENFORCEMENT_URL, timeout=15)
            soup = BeautifulSoup(resp.text, "lxml")
            text = soup.get_text(" ", strip=True)
        except Exception as e:
            log.debug("[RegWave] ASC fetch error: %s", e)
            return

        firms_in_asc = find_firms(text)
        for firm_id in firms_in_asc:
            firm   = FIRM_BY_ID.get(firm_id, {})
            weight = 4.0
            desc   = (
                f"{firm.get('name', firm_id)} appears in active ASC enforcement proceedings. "
                f"Securities enforcement defence is enormously document-intensive — "
                f"disclosure reviews, affidavits, regulatory submissions. "
                f"The lead securities partner will need maximum junior hours immediately."
            )
            is_new = insert_signal(
                firm_id=firm_id, signal_type="asc_enforcement_defence",
                weight=weight,
                title=f"ASC Enforcement: {firm.get('name', firm_id)} defending proceeding",
                description=desc,
                source_url=ASC_ENFORCEMENT_URL,
                practice_area="securities",
                raw_data={"source": "ASC enforcement list"},
            )
            if is_new:
                self.new_signals.append({
                    "firm_id": firm_id, "signal_type": "asc_enforcement_defence",
                    "weight": weight, "practice_area": "securities",
                    "title": desc[:80],
                })

    # ── D) Competition Bureau Clearance ───────────────────────────────────────

    def _scan_competition_bureau(self):
        try:
            resp = self.session.get(COMPETITION_BUREAU_URL, timeout=15)
            soup = BeautifulSoup(resp.text, "lxml")
            text = soup.get_text(" ", strip=True)
        except Exception as e:
            log.debug("[RegWave] Competition Bureau fetch error: %s", e)
            return

        firms_in_cb = find_firms(text)
        deal_value  = parse_deal_value(text)

        for firm_id in firms_in_cb:
            firm   = FIRM_BY_ID.get(firm_id, {})
            weight = 4.5 if (deal_value and deal_value >= 500) else 3.8
            desc   = (
                f"{firm.get('name', firm_id)} is acting as counsel on a merger filing "
                f"pending Competition Bureau clearance"
                + (f" (estimated deal value: ~${deal_value:.0f}M)" if deal_value else "")
                + ". Competition/antitrust mergers require extensive economic analysis, "
                "document productions, and regulatory submissions — "
                "all heavily associate-hours intensive."
            )
            is_new = insert_signal(
                firm_id=firm_id, signal_type="competition_merger_filing",
                weight=weight,
                title=f"Competition Bureau Merger: {firm.get('name', firm_id)} as counsel",
                description=desc,
                source_url=COMPETITION_BUREAU_URL,
                practice_area="corporate",
                raw_data={"deal_value_m": deal_value},
            )
            if is_new:
                self.new_signals.append({
                    "firm_id": firm_id, "signal_type": "competition_merger_filing",
                    "weight": weight, "practice_area": "corporate",
                    "title": f"Competition merger: {firm.get('name',firm_id)}",
                })


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    signals = RegulatoryWaveTracker().run()
    for s in signals:
        print(f"  [{s['signal_type']}] {s['firm_id']}: {s.get('title','')}")
