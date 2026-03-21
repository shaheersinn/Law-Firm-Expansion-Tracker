"""
signals/advanced/newswire_monitor.py
──────────────────────────────────────
Signal 8 — Newswire Monitor (CNW Group + Globe Newswire + PR Newswire Canada)

The critical timing insight: press releases on CNW Group and Globe Newswire
are published HOURS before SEDAR+ filings land. When a Calgary company
announces a transaction at 7 AM, the law firms are already in war-room mode.
You can email their hiring partner by 8 AM — before anyone else knows.

Sources:
  • CNW Group RSS:         https://www.newswire.ca/rss/
  • Globe Newswire CA RSS: https://www.globenewswire.com/RssFeed/country/Canada
  • Business Wire CA:      https://www.businesswire.com/rss/home/20230101005254/en/
  • Stockwatch.com:        https://www.stockwatch.com  (Calgary-listed company news)

Detection chain:
  1. Parse RSS entries mentioning deal keywords
  2. Extract dollar amount
  3. Named entity recognition: find company names → lookup known counsel
  4. Fire signal with "BREAKING DEAL" weight (highest tier)
  5. Generate instant Telegram alert with draft outreach subject line

Also monitors for: officer/director departures (in-house counsel open),
financings, CCAA filings (debt restructuring = massive junior hours).
"""

import re, time, logging, hashlib
from datetime import datetime, date
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import requests, feedparser

from database.db import insert_signal
from config_calgary import CALGARY_FIRMS, FIRM_BY_ID, BIGLAW_FIRMS
from signals.advanced.aer_hearings import COMPANY_TO_COUNSEL

log = logging.getLogger(__name__)

NEWS_FEEDS = [
    ("CNW",           "https://www.newswire.ca/en/rss/news"),
    ("GlobeNewswire", "https://www.globenewswire.com/RssFeed/country/Canada"),
    ("BizWire",       "https://www.businesswire.com/rss/home/20230101005254/en/"),
]

DEAL_KEYWORDS = re.compile(
    r"\b(merger|acquisition|amalgamation|arrangement|take-?over|going.private|"
    r"privatization|divest|spin.?off|IPO|initial public offering|"
    r"private placement|bought deal|CCAA|receivership|insolvency|"
    r"strategic review|going.concern|creditor protection|joint venture|"
    r"partnership agreement|definitive agreement|letter of intent|LOI)\b",
    re.IGNORECASE,
)

CALGARY_KEYWORDS = re.compile(
    r"\b(Calgary|Alberta|Edmonton|Fort McMurray|Lloydminster|"
    r"oil sands|oilfield|SAGD|heavy oil|bitumen|LNG|pipeline|"
    r"TSX|TSXV|TSX-V|Toronto Stock Exchange)\b",
    re.IGNORECASE,
)

DOLLAR_RE = re.compile(
    r"\$\s*([\d,\.]+)\s*(billion|million|B|M)\b", re.IGNORECASE
)

CCAA_RE = re.compile(
    r"\b(CCAA|Companies.Creditors Arrangement|receivership|insolvency|"
    r"creditor protection|going.concern)\b",
    re.IGNORECASE,
)

# Known Calgary-listed companies — expands via learned data
CALGARY_COMPANIES = set([
    c.lower() for c in [
        "Cenovus", "Suncor", "ARC Resources", "Tourmaline", "TC Energy",
        "Enbridge", "Pembina Pipeline", "Whitecap", "Baytex", "MEG Energy",
        "TransAlta", "Capital Power", "Keyera", "Crescent Point",
        "Precision Drilling", "Trican Well", "Calfrac", "Savanna Energy",
        "Peyto Exploration", "Freehold Royalties", "InPlay Oil", "Gear Energy",
        "Athabasca Oil", "Perpetual Energy", "Perpetual Energy",
        "Birchcliff Energy", "Tamarack Valley", "Spartan Delta",
    ]
])


def _parse_deal_value(text: str) -> float | None:
    vals = []
    for m in DOLLAR_RE.finditer(text):
        num  = float(m.group(1).replace(",", ""))
        unit = m.group(2).lower()
        if unit in ("billion", "b"): num *= 1000
        vals.append(num)
    return max(vals) if vals else None


def _is_calgary_deal(text: str) -> bool:
    if not CALGARY_KEYWORDS.search(text):
        return False
    if not DEAL_KEYWORDS.search(text):
        return False
    return True


def _extract_company(text: str) -> str | None:
    text_lower = text.lower()
    for co in CALGARY_COMPANIES:
        if co in text_lower:
            return co
    return None


def _guess_counsel(company: str | None, deal_value: float | None) -> list[str]:
    """
    Returns list of firm_ids likely to be retained.
    For large deals ($500M+), BigLaw is almost certain.
    For mid deals, include boutiques.
    """
    specific = COMPANY_TO_COUNSEL.get(company or "", [])
    if deal_value and deal_value >= 500:
        # Large deal: specific counsel + all BigLaw that handle M&A
        return list(set(specific + ["blakes", "mccarthy", "osler", "bennett_jones", "norton_rose"]))
    if deal_value and deal_value >= 100:
        return list(set(specific + ["burnet", "field_law", "miller_thomson"]))
    return specific


class NewswireMonitor:
    """
    Monitors Canadian newswires for Calgary deal announcements in real-time.
    Fires BREAKING_DEAL signals — the highest-weight signals in the system.
    """

    def __init__(self):
        self.new_signals: list[dict] = []
        self._seen: set[str]         = set()

    def run(self) -> list[dict]:
        log.info("[Newswire] Polling %d feeds…", len(NEWS_FEEDS))
        for name, url in NEWS_FEEDS:
            self._poll(name, url)
            time.sleep(0.5)
        log.info("[Newswire] Done. %d new signals.", len(self.new_signals))
        return self.new_signals

    def _poll(self, source: str, url: str):
        try:
            feed = feedparser.parse(url)
        except Exception as e:
            log.debug("[Newswire] %s parse error: %s", source, e)
            return

        for entry in feed.entries:
            title   = getattr(entry, "title",   "")
            link    = getattr(entry, "link",    "")
            summary = getattr(entry, "summary", "")
            full    = f"{title} {summary}"

            uid = hashlib.md5(link.encode()).hexdigest()[:16]
            if uid in self._seen:
                continue
            self._seen.add(uid)

            if not _is_calgary_deal(full):
                continue

            self._process(title, summary, link, source)

    def _process(self, title: str, summary: str, link: str, source: str):
        full        = f"{title} {summary}"
        deal_value  = _parse_deal_value(full)
        company     = _extract_company(full)
        is_ccaa     = bool(CCAA_RE.search(full))
        is_big      = deal_value and deal_value >= 500

        firms = _guess_counsel(company, deal_value)
        if not firms:
            return

        # ── Weight ───────────────────────────────────────────────────────────
        if is_ccaa:
            weight     = 5.5   # CCAA = most junior-intensive work in existence
            sig_type   = "breaking_ccaa_filing"
            pa         = "restructuring"
        elif is_big:
            weight     = 5.0
            sig_type   = "breaking_deal_announcement"
            pa         = "corporate"
        else:
            weight     = 4.0
            sig_type   = "newswire_deal_detected"
            pa         = "corporate"

        deal_str = f"${deal_value:.0f}M" if deal_value else "undisclosed"
        co_str   = company.title() if company else "Calgary company"

        for firm_id in firms:
            firm = FIRM_BY_ID.get(firm_id, {})
            desc = (
                f"[{source}] BREAKING: {title[:120]}. "
                f"Deal value: {deal_str}. Company: {co_str}. "
                f"{'CCAA/RESTRUCTURING — extremely junior-intensive. ' if is_ccaa else ''}"
                f"This was posted on {source} — likely hours before SEDAR+ landing. "
                f"Email {firm.get('name','this firm')}'s hiring partner NOW."
            )
            is_new = insert_signal(
                firm_id=firm_id,
                signal_type=sig_type,
                weight=weight,
                title=f"[{source}] {title[:70]}",
                description=desc,
                source_url=link,
                practice_area=pa,
                raw_data={
                    "source": source, "company": company,
                    "deal_value_m": deal_value, "is_ccaa": is_ccaa,
                    "is_big_deal": bool(is_big),
                },
            )
            if is_new:
                self.new_signals.append({
                    "firm_id": firm_id,
                    "signal_type": sig_type,
                    "weight": weight,
                    "title": f"[{source}] BREAKING: {title[:60]}",
                    "practice_area": pa,
                    "description": desc,
                    "source_url": link,
                    "raw_data": {"deal_value_m": deal_value, "is_ccaa": is_ccaa},
                })
                log.info("[Newswire] 🔴 BREAKING signal → %s | %s | %s",
                         firm_id, sig_type, title[:50])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    mon = NewswireMonitor()
    sigs = mon.run()
    for s in sigs:
        print(f"  [{s['signal_type']}] {s['firm_id']}: {s['title']}")
