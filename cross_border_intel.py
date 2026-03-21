"""
signals/cross_border_intel.py
──────────────────────────────
Two high-value signals in one module:

═══ A) SEC EDGAR Cross-Border Detector ═══════════════════════════════════════
Alberta companies cross-listing on US exchanges (Form 40-F, Form 20-F, F-1, F-3)
need BOTH US and Canadian securities counsel. The Canadian counsel firm is suddenly
handling a full US disclosure exercise — often the most associate-intensive work
that exists in Canadian securities law.

Source: SEC EDGAR full-text search API (free, no auth required)
  https://efts.sec.gov/LATEST/search-index?q=%22Alberta%22&dateRange=custom&...

Trigger: Alberta company files Form 40-F / 20-F → identify Canadian counsel in
         the prospectus → fire weight-5.0 signal same day

═══ B) Lateral Magnet Detector ══════════════════════════════════════════════
When a firm does 3+ lateral hires in a 60-day window, it's in aggressive growth
mode. Growth mode means:
  - They won a massive new mandate they need to staff
  - They poached a rainmaker partner and need to rebuild around them
  - They're about to open a new practice group

In ALL three cases, they will need 1-2 junior associates within 30-60 days.

Source: LinkedIn Proxycurl company feed + RSS press release monitoring
Track: "join" announcements on firm news pages and Canadian Lawyer lateral hires

Additionally detect the "Competitive Hire" pattern:
When Firm A hires from Firm B → Firm B has a capacity gap they haven't announced.
"""

import re
import time
import logging
from datetime import datetime, date, timedelta
from collections import defaultdict
from io import BytesIO

import requests
import feedparser

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config_calgary import (
    PROXYCURL_API_KEY, CALGARY_FIRMS, FIRM_BY_ID, FIRM_ALIASES,
    BIGLAW_FIRMS, SIGNAL_WEIGHTS,
)
from database.db import get_conn, insert_signal

log = logging.getLogger(__name__)

# ─── EDGAR API ────────────────────────────────────────────────────────────────

EDGAR_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
EDGAR_FORMS_40F  = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type=40-F&dateb=&owner=include&count=40&search_text="

# Cross-border forms that generate major Canadian legal work
CROSSBORDER_FORMS = ["40-F", "20-F", "F-1", "F-3", "6-K"]

# Alberta company indicators in SEC filings
AB_INDICATORS = re.compile(
    r"\b(Alberta|Calgary|Edmonton|AER|ABCA|TSX|TSX\.V|TSXV|"
    r"Alberta Securities Commission|Law Society of Alberta|"
    r"Canadian Securities|National Instrument)\b", re.I
)

COUNSEL_SECTION = re.compile(
    r"(legal counsel|Canadian counsel|counsel to|law firm|solicitors?|"
    r"as counsel|acting for)", re.I
)

# ─── Firm patterns ───────────────────────────────────────────────────────────

_FIRM_RE = {}
for _f in CALGARY_FIRMS:
    _tokens = [re.escape(a) for a in [_f["name"]] + _f["aliases"]]
    _FIRM_RE[_f["id"]] = re.compile("|".join(_tokens), re.IGNORECASE)

def find_calgary_firms(text: str) -> list[str]:
    return [fid for fid, pat in _FIRM_RE.items() if pat.search(text)]

# ─── Lateral detection patterns ──────────────────────────────────────────────

JOIN_KEYWORDS = re.compile(
    r"\b(join|joins|joined|welcome|pleased to welcome|addition to|"
    r"lateral|new partner|new associate|new counsel|expanding)\b", re.I
)
DEPART_KEYWORDS = re.compile(
    r"\b(formerly|previously|joins.{0,30}from|comes from|left|departed|"
    r"prior to joining|was at|previously at)\b", re.I
)


class SECEdgarCrossBorderTracker:
    """
    Monitors SEC EDGAR for new 40-F filings by Alberta companies.
    Alberta company doing a US cross-listing = massive junior associate work
    for the Canadian counsel firm.
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "LawFirmTracker/3.0 admin@example.com"
        self.new_signals: list[dict] = []

    def run(self) -> list[dict]:
        log.info("[EDGAR] Scanning for Alberta cross-border filings")
        for form_type in ["40-F", "20-F"]:
            self._scan_form_type(form_type)
        return self.new_signals

    def _scan_form_type(self, form_type: str):
        params = {
            "q":        '"Alberta"',
            "dateRange":"custom",
            "startdt":  (date.today() - timedelta(days=90)).isoformat(),
            "enddt":    date.today().isoformat(),
            "forms":    form_type,
        }
        try:
            resp = self.session.get(EDGAR_SEARCH_URL, params=params, timeout=20)
            data = resp.json()
        except Exception as e:
            log.debug("[EDGAR] Search error for %s: %s", form_type, e)
            return

        hits = data.get("hits", {}).get("hits", [])
        log.info("[EDGAR] %s: %d hits", form_type, len(hits))

        for hit in hits[:10]:
            src     = hit.get("_source", {})
            company = src.get("display_names", ["Unknown"])[0] if isinstance(src.get("display_names"), list) else str(src.get("display_names", "Unknown"))
            period  = src.get("period_of_report", "")
            link    = f"https://www.sec.gov/Archives/{src.get('file_date','')}"
            description_text = f"{company} {src.get('form_type','')} {period}"

            if not AB_INDICATORS.search(description_text):
                continue   # Not Alberta-related

            # Try to fetch the filing index to find Canadian counsel
            filing_url = src.get("_id", "")
            firms_found = self._extract_counsel_from_filing(filing_url, description_text)

            target_firms = firms_found if firms_found else []

            # If no firm found, alert the top 3 Calgary securities boutiques
            if not target_firms:
                target_firms = [f["id"] for f in CALGARY_FIRMS
                                if "securities" in f.get("focus", []) or
                                   "corporate" in f.get("focus", [])][:3]

            for firm_id in target_firms:
                firm   = FIRM_BY_ID.get(firm_id, {})
                weight = 5.0 if form_type == "40-F" else 4.5
                desc   = (
                    f"Alberta company '{company}' filed a {form_type} with the SEC — "
                    f"a US cross-listing that requires comprehensive Canadian securities counsel work: "
                    f"full disclosure review, GAAP reconciliation, NI 51-102 analysis, "
                    f"continuous disclosure obligations, and ongoing SEC compliance advisory. "
                    f"This is among the most associate-intensive mandates in Calgary securities practice."
                )
                is_new = insert_signal(
                    firm_id=firm_id, signal_type="sec_crossborder_filing",
                    weight=weight,
                    title=f"SEC {form_type}: {company[:50]} — Canadian counsel needed",
                    description=desc,
                    source_url=f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type={form_type}",
                    practice_area="securities",
                    raw_data={"company": company, "form_type": form_type, "period": period},
                )
                if is_new:
                    self.new_signals.append({
                        "firm_id": firm_id, "signal_type": "sec_crossborder_filing",
                        "weight": weight, "practice_area": "securities",
                        "title": f"SEC {form_type}: {company[:40]}",
                    })

    def _extract_counsel_from_filing(self, filing_id: str, fallback_text: str) -> list[str]:
        """Try to get the actual filing text and extract counsel names."""
        # For now, search in the metadata we already have
        return find_calgary_firms(fallback_text)


class LateralMagnetTracker:
    """
    Detects when a firm is in aggressive lateral hiring mode (3+ laterals in 60 days).
    Also detects "competitive hire" events: Firm A steals from Firm B → Firm B has gap.
    """

    def __init__(self):
        self.new_signals: list[dict] = []
        self._lateral_rss = [
            "https://www.canadianlawyermag.com/rss",
            "https://legalpost.financialpost.com/feed/",
            "https://www.lawtimesnews.com/rss",
        ]

    def run(self) -> list[dict]:
        log.info("[LateralMagnet] Scanning for lateral hire patterns")
        self._scan_rss()
        self._analyze_patterns()
        return self.new_signals

    def _scan_rss(self):
        """Parse RSS feeds for lateral hire announcements, store to DB."""
        for url in self._lateral_rss:
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries[:50]:
                    self._process_entry(entry)
            except Exception as e:
                log.debug("[LateralMagnet] RSS error %s: %s", url, e)
            time.sleep(0.5)

    def _process_entry(self, entry):
        title   = getattr(entry, "title", "")
        summary = getattr(entry, "summary", "")
        link    = getattr(entry, "link", "")
        text    = f"{title} {summary}"
        pub     = getattr(entry, "published", "")

        if not JOIN_KEYWORDS.search(text):
            return

        # Find which Calgary firm is the RECEIVING firm (magnet)
        receiving_firms = find_calgary_firms(text)
        if not receiving_firms:
            return

        # Try to detect the SOURCE firm (competitive hire)
        source_firm = None
        if DEPART_KEYWORDS.search(text):
            # Look for firm names in the "came from" context
            for fid, pat in _FIRM_RE.items():
                m = DEPART_KEYWORDS.search(text)
                if m:
                    after_depart = text[m.end():m.end()+200]
                    if pat.search(after_depart) and fid not in receiving_firms:
                        source_firm = fid
                        break

        # Store lateral event in DB
        conn = get_conn()
        for rfid in receiving_firms:
            conn.execute("""
                INSERT OR IGNORE INTO lateral_events
                    (firm_id, source_firm_id, headline, source_url, event_date)
                VALUES (?, ?, ?, ?, ?)
            """, (rfid, source_firm, title[:200], link, date.today().isoformat()))
        conn.commit()
        conn.close()

        # Fire competitive hire signal for source firm
        if source_firm:
            source = FIRM_BY_ID.get(source_firm, {})
            rfirm  = FIRM_BY_ID.get(receiving_firms[0], {})
            weight = 4.0
            desc   = (
                f"A lawyer departed {source.get('name', source_firm)} to join "
                f"{rfirm.get('name', receiving_firms[0])} as a lateral. "
                f"This departure creates an unadvertised capacity gap at {source.get('name', source_firm)} "
                f"— they lost institutional knowledge and billing capacity simultaneously. "
                f"The firm will need to backfill, likely at the associate level, within 30-60 days."
            )
            is_new = insert_signal(
                firm_id=source_firm, signal_type="competitive_hire_gap",
                weight=weight,
                title=f"Competitive hire gap: lawyer left {source.get('name', source_firm)} for {rfirm.get('name','?')}",
                description=desc,
                source_url=link,
                practice_area=source.get("focus", ["general"])[0],
                raw_data={"receiving_firm": rfirm.get("name", "?"), "headline": title},
            )
            if is_new:
                self.new_signals.append({
                    "firm_id": source_firm, "signal_type": "competitive_hire_gap",
                    "weight": weight,
                    "title": f"Lateral gap at {source.get('name', source_firm)}",
                    "practice_area": source.get("focus", ["general"])[0],
                })

    def _analyze_patterns(self):
        """Check DB for lateral magnet pattern: 3+ laterals to same firm in 60 days."""
        self._ensure_table()
        conn   = get_conn()
        rows   = conn.execute("""
            SELECT firm_id, count(*) as cnt
            FROM lateral_events
            WHERE date(event_date) >= date('now', '-60 days')
            GROUP BY firm_id
            HAVING cnt >= 3
            ORDER BY cnt DESC
        """).fetchall()
        conn.close()

        for row in rows:
            row    = dict(row)
            firm   = FIRM_BY_ID.get(row["firm_id"], {})
            cnt    = row["cnt"]
            weight = 3.8 + min((cnt - 3) * 0.3, 1.2)
            desc   = (
                f"{firm.get('name', row['firm_id'])} has made {cnt} lateral hires in the "
                f"past 60 days — a strong signal of aggressive growth mode. "
                f"Possible triggers: new major mandate, poached rainmaker partner, "
                f"new practice group launch, or office expansion. "
                f"In all scenarios, junior associate demand follows within 30-60 days. "
                f"The firm is clearly investing in capacity — they will need supporting juniors."
            )
            is_new = insert_signal(
                firm_id=row["firm_id"], signal_type="lateral_magnet",
                weight=weight,
                title=f"Lateral Magnet: {firm.get('name', row['firm_id'])} — {cnt} hires in 60 days",
                description=desc,
                source_url="",
                practice_area=firm.get("focus", ["general"])[0],
                raw_data={"lateral_count_60d": cnt},
            )
            if is_new:
                self.new_signals.append({
                    "firm_id": row["firm_id"], "signal_type": "lateral_magnet",
                    "weight": weight,
                    "title": f"Lateral Magnet: {cnt} hires in 60 days",
                    "practice_area": firm.get("focus", ["general"])[0],
                })
                log.info("[LateralMagnet] 🧲 %s: %d laterals in 60d → weight=%.1f",
                         firm.get("name", row["firm_id"]), cnt, weight)

    @staticmethod
    def _ensure_table():
        conn = get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lateral_events (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_id      TEXT NOT NULL,
                source_firm_id TEXT,
                headline     TEXT,
                source_url   TEXT,
                event_date   TEXT,
                UNIQUE(firm_id, headline)
            )""")
        conn.commit()
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    edgar  = SECEdgarCrossBorderTracker()
    magnet = LateralMagnetTracker()
    for sig in edgar.run() + magnet.run():
        print(f"  [{sig['signal_type']}] {sig['firm_id']}: {sig.get('title','')}")
