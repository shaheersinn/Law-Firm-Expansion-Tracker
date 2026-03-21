"""
signals/deep/fiscal_calendar.py
─────────────────────────────────
Signal 16 — Fiscal Calendar Pressure Predictor
            + Cross-Border US SEC Intelligence

═══════════════════════════════════════════════════════════════════════
PART A: Fiscal Calendar Pressure Predictor
═══════════════════════════════════════════════════════════════════════

Nobody thinks about this: law firm workload follows CLIENT fiscal calendars,
not law firm calendars. Calgary energy companies have predictable annual patterns:

  JAN-MAR: Annual reports, annual information forms (AIFs) on SEDAR+
           → securities lawyers slammed at Bennett Jones, Norton Rose, Blakes
  APR-MAY: Annual general meetings (AGMs), proxy circulars
           → corporate lawyers drafting proxies, M&A advisory
  JUN-JUL: Mid-year reserves reports, capital budget planning
           → regulatory and energy law boutiques
  AUG-SEP: Acquisition season (Q3 closes), articling starts
           → M&A closes, junior associates needed for post-close integration
  OCT-NOV: Year-end tax planning, income trust distributions
           → tax lawyers maxed out
  DEC:     Emergency closings, year-end transactions
           → deal lawyers work through holidays

This predicts SEASONAL DEMAND 4-8 weeks in advance. You can email Bennett Jones
in December knowing they'll be slammed in January.

═══════════════════════════════════════════════════════════════════════
PART B: Cross-Border US SEC Intelligence
═══════════════════════════════════════════════════════════════════════

Many Calgary energy companies are also SEC-registered (dual-listed on NYSE/TSX).
When they file a Form F-3 (shelf registration), Form 6-K (material event),
or Schedule 13D/G (activist investor), they need CANADIAN outside counsel
working IN PARALLEL with their US counsel.

The SEC EDGAR full-text search is FREE and indexes all filings.
A Form 6-K mentioning "Calgary", "Alberta", or "TSX" filed by a
dual-listed company = Canadian legal work about to begin.

Source: SEC EDGAR EFTS (full-text search)
URL: https://efts.sec.gov/LATEST/search-index?q=%22Calgary%22&dateRange=custom&startdt=...
"""

import re, logging, json
from datetime import date, datetime, timedelta
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import requests, feedparser

from database.db import insert_signal
from config_calgary import CALGARY_FIRMS, FIRM_BY_ID
from signals.advanced.aer_hearings import COMPANY_TO_COUNSEL

log = logging.getLogger(__name__)

EDGAR_EFTS_URL  = "https://efts.sec.gov/LATEST/search-index"
EDGAR_SEARCH    = "https://efts.sec.gov/LATEST/search-index?q=%22Calgary%22+%22Alberta%22&dateRange=custom&startdt={start}&forms={form}&hits.hits._source.period_of_report=true"

# Dual-listed Calgary companies on SEC EDGAR (CIK numbers)
DUAL_LISTED_COMPANIES = {
    "Cenovus Energy":          "0001510295",
    "Suncor Energy":           "0000101778",
    "Canadian Natural Resources":"0001175378",
    "TC Energy":               "0001022646",
    "Enbridge":                "0000880285",
    "Pembina Pipeline":        "0001018979",
    "TransAlta":               "0000096985",
    "Baytex Energy":           "0001053706",
    "MEG Energy":              "0001374789",
}

# Form types with high Canadian legal work content
HIGH_VALUE_FORMS = {
    "F-3":     ("shelf registration", 4.0, "securities"),
    "F-4":     ("business combination registration", 5.0, "corporate"),
    "6-K":     ("material change report", 3.5, "corporate"),
    "20-F":    ("annual report foreign private issuer", 3.0, "securities"),
    "SC 13D":  ("activist investor 5%+ stake", 5.0, "corporate"),
    "SC 13G":  ("passive investor 5%+ stake", 3.0, "securities"),
    "SC TO-T": ("tender offer", 5.5, "corporate"),
    "SC TO-C": ("issuer tender offer", 5.0, "corporate"),
    "F-80":    ("business combination proxy", 5.0, "corporate"),
}

DOLLAR_RE = re.compile(r"\$\s*([\d,\.]+)\s*(billion|million|B|M)\b", re.IGNORECASE)


class CrossBorderIntelligence:

    def __init__(self):
        self.new_signals: list[dict] = []
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "LawTracker/4.0 (research; admin@example.com)"

    def run(self) -> list[dict]:
        log.info("[SEC/EDGAR] Scanning EDGAR for Calgary-linked filings…")
        self._scan_edgar_filings()
        log.info("[SEC/EDGAR] Done. %d signals.", len(self.new_signals))
        return self.new_signals

    def _scan_edgar_filings(self):
        """Search EDGAR full-text search for recent Calgary-linked filings."""
        start = (date.today() - timedelta(days=3)).isoformat()
        for form, (desc, weight, pa) in HIGH_VALUE_FORMS.items():
            url = (
                f"https://efts.sec.gov/LATEST/search-index?"
                f"q=%22Calgary%22+%22Alberta%22&forms={form}"
                f"&dateRange=custom&startdt={start}"
            )
            try:
                resp = self.session.get(url, timeout=12)
                data = resp.json()
                hits = data.get("hits", {}).get("hits", [])
                for hit in hits[:10]:
                    source = hit.get("_source", {})
                    self._process_edgar_hit(source, form, desc, weight, pa)
            except Exception as e:
                log.debug("[EDGAR] %s scan error: %s", form, e)

    def _process_edgar_hit(self, source: dict, form: str, desc: str,
                            weight: float, pa: str):
        company  = source.get("entity_name", "")
        filed    = source.get("file_date", "")
        cik      = source.get("entity_id", "")
        acc_no   = source.get("file_num", "")
        url      = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type={form}"

        # Match to known Calgary companies
        counsel = []
        for co_name, co_counsel in {**COMPANY_TO_COUNSEL,
            **{k.lower(): [] for k in DUAL_LISTED_COMPANIES}}.items():
            if co_name.lower() in company.lower():
                counsel = co_counsel
                break

        # If no specific counsel known, use BigLaw default for large filings
        if not counsel and weight >= 4.5:
            counsel = ["blakes", "mccarthy", "osler", "bennett_jones", "norton_rose"]
        elif not counsel:
            counsel = ["norton_rose", "bennett_jones", "burnet"]

        signal_desc = (
            f"[SEC EDGAR] {company} filed {form} ({desc}) on {filed}. "
            f"This is a cross-border filing requiring Canadian outside counsel. "
            f"Calgary firms acting as Canadian counsel will be generating significant "
            f"hours on securities opinions, regulatory filings, and disclosure documents."
        )

        for firm_id in counsel[:4]:
            is_new = insert_signal(
                firm_id=firm_id,
                signal_type="sec_edgar_filing",
                weight=weight,
                title=f"[EDGAR {form}] {company[:50]} — {desc}",
                description=signal_desc,
                source_url=url,
                practice_area=pa,
                raw_data={"form": form, "company": company, "filed": filed, "cik": cik},
            )
            if is_new:
                self.new_signals.append({
                    "firm_id": firm_id,
                    "signal_type": "sec_edgar_filing",
                    "weight": weight,
                    "title": f"[EDGAR {form}] {company[:50]}",
                    "practice_area": pa,
                    "description": signal_desc,
                })


# ══════════════════════════════════════════════════════════════════════
# PART A: Fiscal Calendar Pressure Engine
# ══════════════════════════════════════════════════════════════════════

# Monthly pressure profiles per practice area
# Each month maps to a dict of {practice_area: (pressure_score, firms_affected)}
FISCAL_CALENDAR = {
    1:  [("securities", 5.0, ["bennett_jones","norton_rose","blakes","mccarthy"],
          "AIF/Annual Report season. SEDAR annual filings due. Securities lawyers maxed.")],
    2:  [("securities", 5.0, ["bennett_jones","norton_rose","blakes","mccarthy"],
          "AIF deadline crunch. Annual report filing season peak."),
         ("tax", 4.0, ["stikeman","osler","mccarthy"],
          "Year-end tax filings, deferred tax planning.")],
    3:  [("securities", 4.5, ["blakes","osler","torys","mccarthy"],
          "Q4 results, proxy season preparation begins."),
         ("corporate", 4.0, ["bennett_jones","burnet","norton_rose"],
          "Annual information form work, management proxy circulars.")],
    4:  [("corporate", 5.0, ["mccarthy","blakes","osler","bennett_jones"],
          "AGM proxy circular preparation. Peak junior demand for corporate review."),
         ("securities", 4.0, ["norton_rose","cassels","hamilton_law"],
          "TSXV quarterly compliance filings.")],
    5:  [("corporate", 4.5, ["blakes","mccarthy","bennett_jones"],
          "AGM season in full swing. Proxy solicitation and vote tracking.")],
    6:  [("energy", 4.0, ["burnet","field_law","parlee_mclaws"],
          "Mid-year reserves updates, capital budget amendments."),
         ("M&A", 4.0, ["osler","blakes","norton_rose"],
          "H1 close calendar — deals targeted for Q3 close begin structuring.")],
    7:  [("litigation", 3.5, ["mccarthy","parlee_mclaws","miller_thomson"],
          "Summer trial season — ABQB July sittings.")],
    8:  [("corporate", 4.5, ["blakes","mccarthy","osler","bennett_jones"],
          "Acquisition season. Q3 target deals close. Post-close integration work.")],
    9:  [("corporate", 5.0, ["blakes","mccarthy","osler","norton_rose"],
          "M&A peak season begins. Deals announced for Q4 close. Max junior demand."),
         ("energy", 4.5, ["burnet","field_law","bennett_jones"],
          "Capital budget season. Energy companies locking in capex for next year.")],
    10: [("tax", 5.0, ["stikeman","osler","mccarthy","fmc_law"],
          "Year-end tax planning peak. Income trust distributions. RRSP strategies."),
         ("corporate", 4.5, ["blakes","mccarthy"],
          "Q4 deal closings. Maximum deal pipeline pressure.")],
    11: [("restructuring", 4.5, ["mccarthy","norton_rose","burnet","miller_thomson"],
          "Year-end insolvency filings begin. CCAA proceedings spike in Nov-Dec."),
         ("tax", 4.5, ["stikeman","osler"],
          "Tax loss selling, year-end corporate restructuring.")],
    12: [("corporate", 5.5, ["blakes","mccarthy","osler","bennett_jones","norton_rose"],
          "EMERGENCY CLOSE SEASON. December year-end deals — firms work through holidays. "
          "Maximum junior demand. All hands on deck."),
         ("restructuring", 4.0, ["norton_rose","miller_thomson","burnet"],
          "Year-end CCAA filings and receivership appointments.")],
}


class FiscalCalendarPredictor:
    """
    Fires predictive workload signals based on the annual fiscal calendar.
    Run monthly to pre-seed the demand pipeline.
    """

    def run(self) -> list[dict]:
        new_signals = []
        today       = date.today()
        # Look ahead 4-8 weeks
        for weeks_ahead in [4, 6, 8]:
            target_month = (today + timedelta(weeks=weeks_ahead)).month
            pressures    = FISCAL_CALENDAR.get(target_month, [])
            for pa, pressure, firm_ids, rationale in pressures:
                for firm_id in firm_ids:
                    firm   = FIRM_BY_ID.get(firm_id, {})
                    is_new = insert_signal(
                        firm_id=firm_id,
                        signal_type="fiscal_pressure_incoming",
                        weight=min(pressure, 5.0) * 0.7,   # scaled down since predictive
                        title=f"Fiscal calendar: {pa.upper()} pressure in {weeks_ahead}w at {firm.get('name',firm_id)}",
                        description=(
                            f"FISCAL CALENDAR PREDICTOR (+{weeks_ahead} weeks): "
                            f"{rationale} "
                            f"Reach out to {firm.get('name',firm_id)} NOW — "
                            f"they'll be at peak capacity by "
                            f"{(today + timedelta(weeks=weeks_ahead)).strftime('%B %Y')}."
                        ),
                        source_url="",
                        practice_area=pa,
                        raw_data={
                            "target_month": target_month,
                            "weeks_ahead": weeks_ahead,
                            "pressure_score": pressure,
                        },
                    )
                    if is_new:
                        new_signals.append({
                            "firm_id": firm_id,
                            "signal_type": "fiscal_pressure_incoming",
                            "weight": pressure * 0.7,
                            "title": f"Fiscal calendar: {pa} pressure in {weeks_ahead}w",
                            "practice_area": pa,
                        })
        log.info("[FiscalCalendar] %d signals generated.", len(new_signals))
        return new_signals


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    cbi = CrossBorderIntelligence()
    for s in cbi.run():
        print(f"  [EDGAR] {s['firm_id']}: {s['title']}")
    fcp = FiscalCalendarPredictor()
    for s in fcp.run():
        print(f"  [FISCAL] {s['firm_id']}: {s['title']}")
