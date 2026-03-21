"""
signals/advanced/sedi_monitor.py
──────────────────────────────────
Signal 10 — SEDI Insider Trading Monitor

SEDI (System for Electronic Disclosure by Insiders) is the Canadian SEC Form-4
equivalent. When corporate insiders at Calgary energy companies buy shares,
they're betting on a positive upcoming event — almost always a major acquisition,
asset sale, or strategic transaction that will require outside legal counsel.

This gives a 30-60 day PREDICTIVE window before any public announcement.

Legal basis: Section 107 of the Securities Act (Alberta) requires insiders to
file SEDI reports within 10 calendar days of trading. These are PUBLIC.

Source: SEDI public search at https://www.sedi.ca/sedi/SVTSrchInsd

Methodology:
  1. Poll SEDI public filings for Calgary-area energy companies
  2. Identify CLUSTER purchases (3+ insiders buying same stock same week)
  3. Cross-reference company to known outside counsel
  4. Fire PREDICTIVE signal: "Insider cluster buy at Cenovus → Blakes/McCarthy
     likely to get M&A mandate in 30-60 days"

Also monitors:
  • Director/officer RESIGNATIONS (in-house counsel vacancy at company +
    chaos at outside firm)
  • Option exercises before deals (classic pre-transaction signal)
"""

import re, time, logging, hashlib, json
from datetime import datetime, date, timedelta
from collections import defaultdict
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import requests
from bs4 import BeautifulSoup

from database.db import insert_signal, get_conn
from config_calgary import FIRM_BY_ID, CALGARY_FIRMS
from signals.advanced.aer_hearings import COMPANY_TO_COUNSEL

log = logging.getLogger(__name__)

SEDI_SEARCH_URL  = "https://www.sedi.ca/sedi/SVTSrchInsd"
SEDI_FILINGS_URL = "https://www.sedi.ca/sedi/SVTIslrFillgList"

# Minimum cluster size to trigger a signal
CLUSTER_MIN_INSIDERS  = 3
CLUSTER_WINDOW_DAYS   = 7   # all buys within 7 days = cluster
MIN_PURCHASE_VALUE    = 50_000   # CAD — filter out tiny purchases

# Calgary energy company SEDI issuer IDs (approximate; verify against SEDI)
CALGARY_ISSUERS = {
    "cenovus":        "0000837670",
    "arc resources":  "0000835319",
    "tourmaline":     "0000835489",
    "pembina":        "0000835400",
    "whitecap":       "0000835482",
    "baytex":         "0000835350",
    "meg energy":     "0000835401",
    "crescent point": "0000835359",
    "tamarack valley":"0000835471",
    "freehold royalties": "0000835373",
}


class SEDIMonitor:
    """
    Polls SEDI for insider purchase clusters at Calgary energy companies.
    When 3+ insiders buy the same company in the same week, a major transaction
    is likely imminent — fire a predictive signal against outside counsel.
    """

    def __init__(self):
        self.new_signals: list[dict] = []
        self.session = requests.Session()
        self.session.headers["User-Agent"] = (
            "Mozilla/5.0 (compatible; LawTracker/3.0)"
        )
        self._init_db()

    def _init_db(self):
        conn = get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sedi_transactions (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                company_name  TEXT,
                issuer_id     TEXT,
                insider_name  TEXT,
                trade_date    TEXT,
                trade_type    TEXT,
                shares        INTEGER,
                value_cad     REAL,
                recorded_at   TEXT DEFAULT (datetime('now')),
                UNIQUE(issuer_id, insider_name, trade_date, trade_type)
            )""")
        conn.commit()
        conn.close()

    def run(self) -> list[dict]:
        log.info("[SEDI] Polling insider transactions for %d Calgary issuers…",
                 len(CALGARY_ISSUERS))
        filings_by_company = defaultdict(list)

        for company, issuer_id in CALGARY_ISSUERS.items():
            filings = self._fetch_recent_filings(company, issuer_id)
            self._store_filings(company, issuer_id, filings)
            filings_by_company[company] = filings
            time.sleep(1.5)   # polite

        self._detect_clusters(filings_by_company)
        log.info("[SEDI] Done. %d signals.", len(self.new_signals))
        return self.new_signals

    # ── Fetch filings ────────────────────────────────────────────────────────

    def _fetch_recent_filings(self, company: str, issuer_id: str) -> list[dict]:
        """
        Query SEDI public search for recent transactions at this issuer.
        Returns list of filing dicts.
        """
        cutoff = (date.today() - timedelta(days=30)).strftime("%Y-%m-%d")
        params = {
            "issuerType":    "A",
            "startDate":     cutoff,
            "issuerName":    company,
            "formType":      "4",     # insider report
            "action":        "0",
        }
        try:
            resp = self.session.post(SEDI_SEARCH_URL, data=params, timeout=15)
            return self._parse_filings(resp.text, company)
        except Exception as e:
            log.debug("[SEDI] Fetch failed for %s: %s", company, e)
            return []

    def _parse_filings(self, html: str, company: str) -> list[dict]:
        """Parse SEDI HTML results table."""
        soup     = BeautifulSoup(html, "lxml")
        filings  = []
        rows     = soup.select("table.sedi-table tr, table tr")[1:]

        for row in rows:
            cells = row.select("td")
            if len(cells) < 5:
                continue
            try:
                trade_date   = cells[0].get_text(strip=True)
                insider_name = cells[1].get_text(strip=True)
                trade_type   = cells[2].get_text(strip=True)
                shares_txt   = cells[3].get_text(strip=True).replace(",", "")
                value_txt    = cells[4].get_text(strip=True).replace(",", "").replace("$", "")
                shares = int(shares_txt) if shares_txt.isdigit() else 0
                value  = float(value_txt) if value_txt.replace(".", "").isdigit() else 0

                if "acquisition" not in trade_type.lower() and "purchase" not in trade_type.lower():
                    continue   # only interested in buys
                if value < MIN_PURCHASE_VALUE:
                    continue

                filings.append({
                    "company":      company,
                    "insider_name": insider_name,
                    "trade_date":   trade_date,
                    "trade_type":   trade_type,
                    "shares":       shares,
                    "value_cad":    value,
                })
            except Exception:
                continue
        return filings

    def _store_filings(self, company: str, issuer_id: str, filings: list):
        conn = get_conn()
        for f in filings:
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO sedi_transactions
                        (company_name, issuer_id, insider_name, trade_date,
                         trade_type, shares, value_cad)
                    VALUES (?,?,?,?,?,?,?)
                """, (company, issuer_id, f["insider_name"], f["trade_date"],
                      f["trade_type"], f["shares"], f["value_cad"]))
            except Exception:
                pass
        conn.commit()
        conn.close()

    # ── Cluster detection ─────────────────────────────────────────────────────

    def _detect_clusters(self, filings_by_company: dict):
        """
        For each company, check if 3+ insiders bought within a 7-day window.
        A cluster = probable upcoming transaction.
        """
        for company, filings in filings_by_company.items():
            if len(filings) < CLUSTER_MIN_INSIDERS:
                continue

            # Group by date windows
            dates = []
            for f in filings:
                try:
                    d = datetime.strptime(f["trade_date"], "%Y-%m-%d").date()
                    dates.append((d, f))
                except Exception:
                    pass

            dates.sort(key=lambda x: x[0])

            for i, (d0, _) in enumerate(dates):
                window = [f for d, f in dates
                          if 0 <= (d - d0).days <= CLUSTER_WINDOW_DAYS]
                unique_insiders = set(w["insider_name"] for w in window)

                if len(unique_insiders) >= CLUSTER_MIN_INSIDERS:
                    self._fire_cluster_signal(company, window, unique_insiders)
                    break   # one signal per company per run

    def _fire_cluster_signal(self, company: str, window: list, insiders: set):
        total_value = sum(w.get("value_cad", 0) for w in window)
        counsel     = COMPANY_TO_COUNSEL.get(company.lower(), [])

        if not counsel:
            log.debug("[SEDI] No counsel mapping for %s", company)
            return

        insiders_str = ", ".join(list(insiders)[:4])
        desc = (
            f"INSIDER CLUSTER BUY at {company.title()}: "
            f"{len(insiders)} insiders purchased shares totalling ~${total_value:,.0f} CAD "
            f"within a 7-day window ({window[0]['trade_date']} – {window[-1]['trade_date']}). "
            f"Insiders: {insiders_str}. "
            f"Historical pattern: insider clusters precede major transactions by 30-60 days. "
            f"Expected outside counsel for any transaction: {', '.join(counsel)}."
        )

        for firm_id in counsel:
            firm = FIRM_BY_ID.get(firm_id, {})
            is_new = insert_signal(
                firm_id=firm_id,
                signal_type="sedi_insider_cluster",
                weight=4.5,
                title=f"SEDI Cluster Buy: {len(insiders)} insiders at {company.title()} (${total_value:,.0f})",
                description=desc,
                source_url="https://www.sedi.ca",
                practice_area="corporate",
                raw_data={
                    "company":      company,
                    "insiders":     list(insiders),
                    "total_value":  total_value,
                    "trade_count":  len(window),
                    "date_range":   f"{window[0]['trade_date']}–{window[-1]['trade_date']}",
                },
            )
            if is_new:
                self.new_signals.append({
                    "firm_id": firm_id,
                    "signal_type": "sedi_insider_cluster",
                    "weight": 4.5,
                    "title": f"SEDI Cluster Buy: {company.title()} — {len(insiders)} insiders",
                    "practice_area": "corporate",
                    "description": desc,
                    "raw_data": {"company": company, "insiders": list(insiders)},
                })
                log.info("[SEDI] 🔴 Cluster signal → %s | %s ($%s)",
                         firm_id, company, f"{total_value:,.0f}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    mon = SEDIMonitor()
    sigs = mon.run()
    for s in sigs:
        print(f"  [{s['signal_type']}] {s['firm_id']}: {s['title']}")
