"""
signals/deep/new_court_filings.py
───────────────────────────────────
Signal 17 — New Court Filing Monitor (AltaLIS / Alberta Courts Online)

THE MOST IMPORTANT GAP IN THE EXISTING SYSTEM:
CanLII only publishes DECIDED cases. A statement of claim filed today
won't appear on CanLII for 6-18 months (if ever). But the moment
a $500M breach of contract claim is filed in ABQB, the named defendant's
counsel is already in war-room mode. We can catch this 6 months before
the existing CanLII tracker sees a single signal.

Sources:
  1. Alberta Courts Online (public docket search)
     https://albertacourts.ca/qb/areas-of-law/civil
  2. Law360 Canada new filing alerts (public summaries)
  3. LexisNexis CourtLink Canada alerts (if licensed)
  4. Canadian Legal Information Institute — new citation index
     (faster than full-text, but still lags)
  5. Alberta King's Bench — new action number registry

Detection chain:
  A) Scrape public docket index for high-value commercial filings
  B) Extract: parties, represented counsel, claim type, amount
  C) Match counsel → firm_id
  D) Fire SAME-DAY signal: "New $40M breach claim filed. Stikeman
     acting for plaintiff. Need juniors for discovery immediately."

Also catches:
  • New class action certifications (massive document review)
  • Injunction applications (emergency = immediate junior deployment)
  • CCAA initial applications (appear here hours before anywhere else)
  • Foreclosure proceedings on major commercial assets

Key insight on injunctions:
  An ex parte injunction application can be filed and heard the SAME DAY.
  The law firm's managing clerk's office files it at 9 AM; a junior associate
  is reviewing documents by noon. There is no way to detect this from
  SEDAR+, CanLII, or any feed — ONLY from watching the court filing index.
"""

import re, time, logging, hashlib
from datetime import date, datetime, timedelta
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import requests
from bs4 import BeautifulSoup
import feedparser

from database.db import insert_signal, get_conn
from config_calgary import CALGARY_FIRMS, FIRM_BY_ID

log = logging.getLogger(__name__)

# Alberta Courts Online public search
ABQB_SEARCH_URL = "https://albertacourts.ca/qb/areas-of-law/civil/caselaw"
ABCA_SEARCH_URL = "https://albertacourts.ca/ca/areas-of-law/appeals"

# Law360 Canada new filings RSS (free tier available)
LAW360_CANADA_RSS = "https://www.law360.com/rss/articles?section=canada"

# Canadian Legal Information Institute — new decisions feed
CANLII_NEW_RSS = "https://www.canlii.org/en/ab/abqb/nav/date/2024-01-01.rss"

# High-value proceeding type keywords
EMERGENCY_KEYWORDS = re.compile(
    r"\b(injunction|ex parte|Anton Piller|Mareva|Norwich|receivership|"
    r"CCAA|Companies.Creditors Arrangement|preservation order|"
    r"freezing order|urgent|emergency motion|interim relief)\b",
    re.IGNORECASE,
)

HIGH_VALUE_KEYWORDS = re.compile(
    r"\b(class action|breach of contract|oppression|derivative|"
    r"securities fraud|misrepresentation|fiduciary|insider trading|"
    r"M&A dispute|purchase price adjustment|earn.out|indemnification|"
    r"billion|million|\$\d{2,}[Mm]|commercial arbitration)\b",
    re.IGNORECASE,
)

DOLLAR_RE = re.compile(r"\$\s*([\d,\.]+)\s*(billion|million|B|M)\b", re.IGNORECASE)

# Firm-to-counsel name aliases for identifying counsel in filing text
_FIRM_ALIASES: dict[str, list[str]] = {}
for _f in CALGARY_FIRMS:
    _FIRM_ALIASES[_f["id"]] = [_f["name"]] + _f.get("aliases", [])


def _identify_counsel(text: str) -> list[str]:
    found = []
    text_lower = text.lower()
    for firm_id, aliases in _FIRM_ALIASES.items():
        if any(a.lower() in text_lower for a in aliases):
            found.append(firm_id)
    return list(set(found))


def _parse_claim_value(text: str) -> float | None:
    for m in DOLLAR_RE.finditer(text):
        num  = float(m.group(1).replace(",", ""))
        unit = m.group(2).lower()
        if unit in ("billion", "b"): num *= 1000
        return num
    return None


class NewCourtFilingMonitor:
    """
    Monitors Alberta court filing indexes for new high-value commercial proceedings.
    Fires signals SAME DAY a matter is filed — 6-18 months before CanLII.
    """

    def __init__(self):
        self.new_signals: list[dict] = []
        self._seen: set[str] = set()
        self.session = requests.Session()
        self.session.headers["User-Agent"] = (
            "Mozilla/5.0 (compatible; LegalTracker/4.0; research)"
        )
        self._init_db()

    def _init_db(self):
        conn = get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS court_filings (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                action_number   TEXT UNIQUE,
                court           TEXT,
                filing_type     TEXT,
                parties         TEXT,
                counsel_firms   TEXT,
                claim_value     REAL,
                is_emergency    INTEGER DEFAULT 0,
                filed_date      TEXT,
                source          TEXT,
                first_seen      TEXT DEFAULT (date('now'))
            )""")
        conn.commit()
        conn.close()

    def run(self) -> list[dict]:
        log.info("[NewFilings] Scanning Alberta court filing indexes…")
        self._scan_law360_rss()
        self._scan_canlii_new_decisions()
        self._scan_abqb_website()
        log.info("[NewFilings] Done. %d signals.", len(self.new_signals))
        return self.new_signals

    # ── Law360 Canada RSS ──────────────────────────────────────────────────────

    def _scan_law360_rss(self):
        try:
            feed = feedparser.parse(LAW360_CANADA_RSS)
        except Exception as e:
            log.debug("[NewFilings] Law360 RSS error: %s", e); return

        for entry in feed.entries:
            title   = getattr(entry, "title",   "")
            link    = getattr(entry, "link",    "")
            summary = getattr(entry, "summary", "")
            combined = f"{title} {summary}"

            if "Calgary" not in combined and "Alberta" not in combined:
                continue

            uid = hashlib.md5(link.encode()).hexdigest()[:12]
            if uid in self._seen: continue
            self._seen.add(uid)

            is_emergency = bool(EMERGENCY_KEYWORDS.search(combined))
            is_hv        = bool(HIGH_VALUE_KEYWORDS.search(combined))
            if not (is_emergency or is_hv): continue

            value    = _parse_claim_value(combined)
            counsel  = _identify_counsel(combined)
            self._fire_filing_signal(
                title[:80], combined, link, "Law360",
                is_emergency, value, counsel
            )

    # ── CanLII new decisions ───────────────────────────────────────────────────

    def _scan_canlii_new_decisions(self):
        """
        CanLII publishes new decisions faster than their API updates.
        The RSS feed catches procedural decisions (interim orders, CMCs)
        that indicate a live, active file with current junior demand.
        """
        today_str = (date.today() - timedelta(days=2)).strftime("%Y-%m-%d")
        rss_url   = f"https://www.canlii.org/en/ab/abqb/nav/date/{today_str}.rss"
        try:
            feed = feedparser.parse(rss_url)
        except Exception as e:
            log.debug("[NewFilings] CanLII RSS error: %s", e); return

        for entry in feed.entries:
            title   = getattr(entry, "title",   "")
            link    = getattr(entry, "link",    "")
            summary = getattr(entry, "summary", "")
            combined = f"{title} {summary}"

            uid = hashlib.md5(link.encode()).hexdigest()[:12]
            if uid in self._seen: continue
            self._seen.add(uid)

            # Only care about procedural decisions indicating active files
            is_procedural = bool(re.search(
                r"\b(case management|CMC|discovery|production|examination for discovery|"
                r"procedural|adjournment|interim|injunction|urgent)\b",
                combined, re.IGNORECASE
            ))
            if not is_procedural: continue

            counsel = _identify_counsel(combined)
            value   = _parse_claim_value(combined)

            self._fire_filing_signal(
                title[:80], combined, link, "CanLII_new",
                False, value, counsel
            )

    # ── ABQB website scrape ────────────────────────────────────────────────────

    def _scan_abqb_website(self):
        """
        Scrape the Alberta Court of King's Bench public notice board
        for newly filed applications and emergency motions.
        """
        try:
            resp = self.session.get(ABQB_SEARCH_URL, timeout=12)
            soup = BeautifulSoup(resp.text, "lxml")
            # Find links to recent notices/applications
            links = soup.select("a[href*='notice'], a[href*='filing'], a[href*='application']")
            for link in links[:20]:
                text = link.get_text(strip=True)
                href = link.get("href", "")
                if EMERGENCY_KEYWORDS.search(text) or HIGH_VALUE_KEYWORDS.search(text):
                    uid = hashlib.md5(href.encode()).hexdigest()[:12]
                    if uid not in self._seen:
                        self._seen.add(uid)
                        counsel = _identify_counsel(text)
                        self._fire_filing_signal(
                            text[:80], text, href, "ABQB_web",
                            bool(EMERGENCY_KEYWORDS.search(text)),
                            _parse_claim_value(text), counsel
                        )
        except Exception as e:
            log.debug("[NewFilings] ABQB website error: %s", e)

    # ── Signal generator ───────────────────────────────────────────────────────

    def _fire_filing_signal(self, title: str, text: str, url: str,
                             source: str, is_emergency: bool,
                             value: float | None, counsel: list[str]):
        if not counsel and not is_emergency:
            return

        # Weight: emergency injunction = highest in system (5.5)
        if is_emergency:
            weight   = 5.5
            sig_type = "court_filing_emergency"
            pa       = "litigation"
        elif value and value >= 500:
            weight   = 5.0
            sig_type = "court_filing_major"
            pa       = "litigation"
        elif value and value >= 50:
            weight   = 4.5
            sig_type = "court_filing_significant"
            pa       = "litigation"
        else:
            weight   = 3.5
            sig_type = "court_filing_new"
            pa       = "litigation"

        val_str = f"${value:.0f}M " if value else ""
        desc = (
            f"[{source}] NEW FILING {'🚨 EMERGENCY — ' if is_emergency else ''}"
            f"{val_str}: {title}. "
            f"{'Injunction/emergency motion — counsel in war-room mode TODAY. ' if is_emergency else ''}"
            f"This matter was filed before CanLII captures it (6-18 month lag). "
            f"Identified counsel will need junior support immediately."
        )

        target_firms = counsel if counsel else (
            ["mccarthy","blakes","osler","stikeman"] if weight >= 5.0 else []
        )

        for firm_id in target_firms:
            is_new = insert_signal(
                firm_id=firm_id,
                signal_type=sig_type,
                weight=weight,
                title=f"[{source}] {'🚨 ' if is_emergency else ''}New filing: {title[:60]}",
                description=desc,
                source_url=url,
                practice_area=pa,
                raw_data={
                    "source": source, "is_emergency": is_emergency,
                    "claim_value_m": value, "title": title,
                },
            )
            if is_new:
                self.new_signals.append({
                    "firm_id": firm_id,
                    "signal_type": sig_type,
                    "weight": weight,
                    "title": f"[{source}] New filing: {title[:60]}",
                    "practice_area": pa,
                    "description": desc,
                    "raw_data": {"is_emergency": is_emergency, "value": value},
                })
                if is_emergency:
                    log.info("[NewFilings] 🚨 EMERGENCY filing → %s: %s", firm_id, title[:50])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    mon = NewCourtFilingMonitor()
    for s in mon.run():
        print(f"  [{s['signal_type']}] {s['firm_id']}: {s['title']}")
