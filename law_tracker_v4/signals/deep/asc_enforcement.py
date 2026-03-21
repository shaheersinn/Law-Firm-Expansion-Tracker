"""
signals/deep/asc_enforcement.py
────────────────────────────────
Signal 13 — Alberta Securities Commission (ASC) Enforcement Monitor
             + TSXV New Listings Pipeline

TWO SOURCES, ONE INSIGHT: both create immediate, high-volume securities
legal work at Calgary boutiques that nobody else is watching.

═══════════════════════════════════════════════════════════════════════
SOURCE A: ASC Enforcement Actions
═══════════════════════════════════════════════════════════════════════
When the ASC issues a cease-trade order, initiates a hearing, or
announces a settlement, the RESPONDENT needs outside counsel immediately.
These are published on the ASC website the same day.

The pattern:
  ASC issues CTO against XYZ Corp at 9 AM
  → XYZ Corp's counsel (usually a boutique) is in emergency war-room mode
  → That boutique needs every available junior for document review
  → You email them at 10 AM

Also: when ASC STAFF takes a position, the respondent firm often retains
a DIFFERENT firm from their normal outside counsel (conflict or specialisation),
creating a new mandate at a firm that wasn't expecting it.

Source: https://www.securities-administrators.ca/enforcement/
        https://www.asc.ca/en/market-participants/enforcement/

═══════════════════════════════════════════════════════════════════════
SOURCE B: TSXV New Listings Pipeline
═══════════════════════════════════════════════════════════════════════
Every TSXV IPO and reverse takeover (RTO) requires:
  - A qualifying transaction circular
  - Due diligence on all assets
  - TSXV policy compliance work
  - Securities law opinion letters

This work lands at Calgary boutiques (Hamilton Cahoon, DS Simon,
Cassels Brock for the mining/energy TSXV segment).

The TSXV publishes bulletins daily listing new listings, halts,
and transaction approvals. An approved listing = 2-6 months of
full-time junior associate work about to begin.

Source: https://www.tsx.com/trading/market-data-and-statistics/bulletins
        TSXV Daily Bulletins RSS
"""

import re, time, logging, hashlib
from datetime import date, datetime, timedelta
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import requests, feedparser
from bs4 import BeautifulSoup

from database.db import insert_signal
from config_calgary import FIRM_BY_ID, CALGARY_FIRMS

log = logging.getLogger(__name__)

ASC_ENFORCEMENT_URL = "https://www.asc.ca/en/market-participants/enforcement/orders-and-proceedings"
ASC_NEWS_RSS        = "https://www.asc.ca/rss/news"
TSXV_BULLETINS_RSS  = "https://www.tsx.com/rss/tsxv-bulletins"
TSXV_BULLETINS_URL  = "https://www.tsx.com/trading/market-data-and-statistics/bulletins"

# Calgary firms that specialise in ASC defence work
ASC_DEFENCE_SPECIALISTS = [
    "bennett_jones", "blakes", "norton_rose", "mccarthy",
    "osler", "burnet", "hamilton_law", "ds_simon",
]

# Calgary firms known for TSXV work
TSXV_SPECIALISTS = [
    "hamilton_law", "ds_simon", "cassels", "burnet",
    "walsh_law", "mccarthy", "norton_rose",
]

# High-urgency ASC proceedings
URGENT_ASC_KEYWORDS = re.compile(
    r"\b(cease.trade|CTO|temporary order|suspension|freeze order|"
    r"management cease.trade|trading halt|cease trading|"
    r"section 144|section 154|section 162|mandatory insider|"
    r"misrepresentation|fraud|registration|unregistered)\b",
    re.IGNORECASE,
)

# TSXV signals
TSXV_LISTING_KEYWORDS = re.compile(
    r"\b(new listing|qualifying transaction|reverse takeover|RTO|"
    r"graduated to TSX|new issuer|listing approved|conditional approval|"
    r"management information circular|information circular approved)\b",
    re.IGNORECASE,
)

DOLLAR_RE = re.compile(r"\$\s*([\d,\.]+)\s*(billion|million|B|M)\b", re.IGNORECASE)


def _parse_value(text: str) -> float | None:
    for m in DOLLAR_RE.finditer(text):
        num  = float(m.group(1).replace(",",""))
        unit = m.group(2).lower()
        if unit in ("billion","b"): num *= 1000
        return num
    return None


class ASCEnforcementMonitor:

    def __init__(self):
        self.new_signals: list[dict] = []
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "Mozilla/5.0 (research; admin@example.com)"

    def run(self) -> list[dict]:
        log.info("[ASC] Scanning enforcement actions + TSXV bulletins…")
        self._scan_asc_rss()
        self._scan_asc_orders()
        self._scan_tsxv()
        log.info("[ASC/TSXV] Done. %d signals.", len(self.new_signals))
        return self.new_signals

    # ── ASC enforcement ────────────────────────────────────────────────────────

    def _scan_asc_rss(self):
        try:
            feed = feedparser.parse(ASC_NEWS_RSS)
        except Exception as e:
            log.debug("[ASC] RSS error: %s", e); return

        for entry in feed.entries:
            title   = getattr(entry,"title","")
            link    = getattr(entry,"link","")
            summary = getattr(entry,"summary","")
            combined = f"{title} {summary}"

            if not URGENT_ASC_KEYWORDS.search(combined):
                continue

            # Determine urgency: CTO / freeze = same-day emergency
            is_emergency = bool(re.search(
                r"(cease.trade|freeze order|temporary order|suspension)", combined, re.I
            ))
            weight   = 5.5 if is_emergency else 4.0
            sig_type = "asc_enforcement_emergency" if is_emergency else "asc_enforcement_proceeding"
            pa       = "securities"

            respondent = self._extract_respondent(title)
            desc = (
                f"[ASC] {title}. "
                f"{'EMERGENCY: ' if is_emergency else ''}"
                f"Respondent: {respondent or 'unknown'}. "
                f"ASC defence work creates immediate junior demand at securities boutiques. "
                f"Firms likely retained for defence: see signal targets."
            )

            for firm_id in ASC_DEFENCE_SPECIALISTS:
                firm = FIRM_BY_ID.get(firm_id, {})
                if "securities" not in firm.get("focus",[]) and "corporate" not in firm.get("focus",[]):
                    continue
                is_new = insert_signal(
                    firm_id=firm_id, signal_type=sig_type,
                    weight=weight, title=f"[ASC] {title[:70]}",
                    description=desc, source_url=link, practice_area=pa,
                    raw_data={"respondent": respondent, "is_emergency": is_emergency},
                )
                if is_new:
                    self.new_signals.append({
                        "firm_id": firm_id, "signal_type": sig_type,
                        "weight": weight, "title": f"[ASC] {title[:70]}",
                        "practice_area": pa, "description": desc,
                    })

    def _scan_asc_orders(self):
        """Scrape the ASC orders & proceedings page directly."""
        try:
            resp = self.session.get(ASC_ENFORCEMENT_URL, timeout=12)
            soup = BeautifulSoup(resp.text, "lxml")
            rows = soup.select("table tr, .order-row, .proceeding-row")
            for row in rows[1:15]:   # recent 15 rows
                text = row.get_text(" ", strip=True)
                link_tag = row.find("a")
                link = link_tag["href"] if link_tag else ASC_ENFORCEMENT_URL
                if URGENT_ASC_KEYWORDS.search(text):
                    self._fire_asc_signal(text[:120], link, "asc_order")
        except Exception as e:
            log.debug("[ASC] Orders page error: %s", e)

    def _fire_asc_signal(self, text: str, url: str, sig_type: str):
        for firm_id in ASC_DEFENCE_SPECIALISTS[:4]:
            insert_signal(
                firm_id=firm_id, signal_type=sig_type,
                weight=4.0, title=f"[ASC Order] {text[:70]}",
                description=f"ASC order: {text}. Securities defence boutiques likely retained.",
                source_url=url, practice_area="securities",
            )

    # ── TSXV listings ──────────────────────────────────────────────────────────

    def _scan_tsxv(self):
        try:
            feed = feedparser.parse(TSXV_BULLETINS_RSS)
            entries = feed.entries
        except Exception:
            entries = []

        # Fallback: scrape bulletins page
        if not entries:
            try:
                resp = self.session.get(TSXV_BULLETINS_URL, timeout=12)
                soup = BeautifulSoup(resp.text, "lxml")
                items = soup.select(".bulletin-item, .bulletin-row, article, .news-item")
                for item in items[:20]:
                    text = item.get_text(" ", strip=True)
                    link = item.find("a")
                    self._process_tsxv_item(text, link["href"] if link else TSXV_BULLETINS_URL)
                return
            except Exception as e:
                log.debug("[TSXV] Scrape fallback error: %s", e); return

        for entry in entries[:30]:
            title   = getattr(entry,"title","")
            link    = getattr(entry,"link","")
            summary = getattr(entry,"summary","")
            self._process_tsxv_item(f"{title} {summary}", link)

    def _process_tsxv_item(self, text: str, url: str):
        if not TSXV_LISTING_KEYWORDS.search(text):
            return

        value    = _parse_value(text)
        is_rto   = bool(re.search(r"(reverse takeover|RTO)", text, re.I))
        weight   = 4.5 if is_rto else 3.5
        sig_type = "tsxv_rto_announced" if is_rto else "tsxv_new_listing"
        pa       = "securities"

        desc = (
            f"TSXV: {text[:200]}. "
            f"{'RTO/qualifying transaction — ' if is_rto else 'New listing — '}"
            f"requires full QT circular, due diligence, securities opinions. "
            f"2-6 months of full-time junior work starting immediately."
            + (f" Transaction value: ${value:.0f}M." if value else "")
        )

        for firm_id in TSXV_SPECIALISTS:
            is_new = insert_signal(
                firm_id=firm_id, signal_type=sig_type,
                weight=weight, title=f"[TSXV] {text[:70]}",
                description=desc, source_url=url, practice_area=pa,
                raw_data={"is_rto": is_rto, "value_m": value},
            )
            if is_new:
                self.new_signals.append({
                    "firm_id": firm_id, "signal_type": sig_type,
                    "weight": weight, "title": f"[TSXV] {text[:70]}",
                    "practice_area": pa,
                })

    @staticmethod
    def _extract_respondent(title: str) -> str:
        # "In the Matter of XYZ Corp" or "Re: XYZ Corp"
        m = re.search(r"(?:matter of|re:|regarding|against)\s+(.+?)(?:\sand\s|\s[-–]\s|$)",
                      title, re.IGNORECASE)
        return m.group(1).strip() if m else ""


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    mon = ASCEnforcementMonitor()
    for s in mon.run():
        print(f"  [{s['signal_type']}] {s['firm_id']}: {s['title']}")
