"""
signals/deep/corporate_registry.py
────────────────────────────────────
Signal 15 — Alberta Corporate Registry Entity Velocity

The most underused signal in Canadian legal intelligence.

When a company is ABOUT TO DO A DEAL, their lawyers begin incorporating
new entities BEFORE any public announcement:
  - Acquisition vehicles ("AcquireCo")
  - Holding structures ("HoldCo", "TopCo")
  - JV entities ("JVCo", "PartnerCo")
  - Special purpose vehicles ("SPV", "ProjectCo")

These appear in the Alberta Corporate Registry days to weeks before
the deal becomes public.

You can query the Alberta Corporate Registry public search at:
  https://www.alberta.ca/corporate-registry-search.aspx

What to look for:
  1. An energy company suddenly incorporates 3-5 new subsidiaries in one week
     (normal pace: 0-1/month) → deal structure being built
  2. New entities with generic names containing "Acquisition", "Merger",
     "Holdings", "Newco", "BidCo" → live M&A structure
  3. The SAME LAW FIRM appears as registered agent on multiple new entities
     → that firm is doing the deal work

Cross-reference: company → known outside counsel → fire signal

SOURCE: Alberta Corporate Registry public search (CORES system)
URL: https://www.alberta.ca/corporate-registry.aspx
Also: Canada Business Corporations Act registry (Corporations Canada)
URL: https://ised-isde.canada.ca/cc/lgcy/fdrlCrpDtls.html
"""

import re, time, logging, hashlib
from datetime import date, datetime, timedelta
from collections import defaultdict
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import requests
from bs4 import BeautifulSoup

from database.db import insert_signal, get_conn
from config_calgary import CALGARY_FIRMS, FIRM_BY_ID
from signals.advanced.aer_hearings import COMPANY_TO_COUNSEL

log = logging.getLogger(__name__)

CORES_SEARCH_URL   = "https://www.alberta.ca/corporate-registry-search.aspx"
CORPS_CANADA_URL   = "https://ised-isde.canada.ca/cc/lgcy/fdrlCrpDtls.html"

# Deal structure naming patterns
DEAL_ENTITY_RE = re.compile(
    r"\b(AcquireCo|BidCo|HoldCo|TopCo|NewCo|SPV|MergerCo|FinanceCo|"
    r"ProjectCo|JVCo|VehicleCo|PurchaseCo|TargetCo|Acquisition Corp|"
    r"Holdings Ltd|Holdings Inc|Acquisition Inc|Merger Sub)\b",
    re.IGNORECASE,
)

# Known Calgary energy company root names for entity clustering
ENERGY_ROOT_NAMES = [
    "cenovus", "suncor", "arc resources", "arc", "tourmaline",
    "tc energy", "enbridge", "pembina", "whitecap", "baytex",
    "meg energy", "transalta", "capital power", "keyera",
    "crescent point", "tamarack", "freehold", "birchcliff",
    "spartan delta", "peyto", "athabasca",
]

# Named registered agents known to be Calgary law firms
# (Registry-agent name → firm_id mapping)
REGISTERED_AGENT_MAP = {
    "mccarthy tetrault":        "mccarthy",
    "mccarthy tétrault":        "mccarthy",
    "blake cassels":            "blakes",
    "blakes":                   "blakes",
    "bennett jones":            "bennett_jones",
    "norton rose":              "norton_rose",
    "osler":                    "osler",
    "torys":                    "torys",
    "stikeman":                 "stikeman",
    "burnet duckworth":         "burnet",
    "bdp":                      "burnet",
    "field law":                "field_law",
    "miller thomson":           "miller_thomson",
    "gowling":                  "gowling",
    "borden ladner":            "borden_ladner",
    "dentons":                  "dentons",
    "fasken":                   "fmc_law",
    "hamilton cahoon":          "hamilton_law",
}


class CorporateRegistryMonitor:
    """
    Monitors the Alberta Corporate Registry for entity velocity spikes
    and deal-structure naming patterns.
    """

    def __init__(self):
        self.new_signals: list[dict] = []
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "Mozilla/5.0 (research)"
        self._init_db()

    def _init_db(self):
        conn = get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS registry_entities (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_name     TEXT NOT NULL,
                parent_company  TEXT,
                registered_agent TEXT,
                incorporation_date TEXT,
                province        TEXT DEFAULT 'AB',
                deal_pattern    INTEGER DEFAULT 0,
                first_seen      TEXT DEFAULT (date('now')),
                UNIQUE(entity_name, incorporation_date)
            )""")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS entity_velocity (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                parent_company  TEXT NOT NULL,
                week_start      TEXT NOT NULL,
                entity_count    INTEGER NOT NULL,
                PRIMARY KEY (parent_company, week_start)
                ON CONFLICT REPLACE
            )""")
        conn.commit()
        conn.close()

    def run(self) -> list[dict]:
        log.info("[Registry] Scanning Alberta Corporate Registry…")
        self._scan_registry()
        self._detect_velocity_spikes()
        log.info("[Registry] Done. %d signals.", len(self.new_signals))
        return self.new_signals

    # ── Registry scan ──────────────────────────────────────────────────────────

    def _scan_registry(self):
        """
        Search CORES for recent incorporations by parent company.
        Looks for deal-pattern names or velocity spikes.
        """
        for company in ENERGY_ROOT_NAMES:
            self._search_company(company)
            time.sleep(1.5)

    def _search_company(self, company: str):
        """Query the CORES registry for new entities containing this company name."""
        try:
            # CORES uses a POST form with search parameters
            payload = {
                "companyName":     company,
                "status":          "active",
                "incorporatedAfter": (date.today() - timedelta(days=30)).strftime("%Y-%m-%d"),
            }
            resp = self.session.post(CORES_SEARCH_URL, data=payload, timeout=15)
            if resp.status_code != 200:
                # Try GET with query string
                resp = self.session.get(
                    CORES_SEARCH_URL,
                    params={"q": company, "status": "Active"},
                    timeout=12,
                )
            self._parse_registry_results(resp.text, company)
        except Exception as e:
            log.debug("[Registry] %s search failed: %s", company, e)

    def _parse_registry_results(self, html: str, parent_company: str):
        soup   = BeautifulSoup(html, "lxml")
        # Try multiple table/list selectors used by different registry layouts
        rows   = soup.select(
            "table.results-table tr, .company-row, .result-row, "
            "[data-company-name], .search-result"
        )
        if not rows:
            rows = soup.select("tr")

        for row in rows[1:30]:
            cells = row.select("td, .cell")
            if len(cells) < 2:
                continue
            name     = cells[0].get_text(strip=True)
            reg_date = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            agent    = cells[2].get_text(strip=True) if len(cells) > 2 else ""

            if not name or len(name) < 3:
                continue

            # Detect deal pattern names
            is_deal_pattern = bool(DEAL_ENTITY_RE.search(name))
            # Check if registered agent is a known law firm
            agent_firm = self._map_agent_to_firm(agent)

            if not is_deal_pattern and not agent_firm:
                continue

            # Store entity
            conn = get_conn()
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO registry_entities
                        (entity_name, parent_company, registered_agent,
                         incorporation_date, deal_pattern)
                    VALUES (?,?,?,?,?)
                """, (name, parent_company.lower(), agent,
                      reg_date or date.today().isoformat(), int(is_deal_pattern)))
                conn.commit()
            except Exception:
                pass
            conn.close()

            # Fire immediate signal for deal-pattern entities
            if is_deal_pattern:
                counsel = COMPANY_TO_COUNSEL.get(parent_company.lower(), [])
                if agent_firm and agent_firm not in counsel:
                    counsel = [agent_firm] + counsel
                self._fire_deal_structure_signal(
                    name, parent_company, counsel, agent
                )

    # ── Velocity spike detection ───────────────────────────────────────────────

    def _detect_velocity_spikes(self):
        """
        For each parent company, check if the 30-day entity incorporation count
        is 3× or more above the company's baseline.
        """
        import numpy as np
        conn = get_conn()

        # Current 30-day count per company
        companies = conn.execute("""
            SELECT parent_company, count(*) as recent
            FROM registry_entities
            WHERE date(first_seen) >= date('now','-30 days')
            GROUP BY parent_company
        """).fetchall()

        for row in companies:
            company = row["parent_company"]
            recent  = row["recent"]

            # Baseline: prior 6 months average
            baseline_rows = conn.execute("""
                SELECT week_start, entity_count FROM entity_velocity
                WHERE parent_company = ?
                  AND week_start < date('now','-30 days')
                ORDER BY week_start DESC LIMIT 12
            """, (company,)).fetchall()

            if len(baseline_rows) < 4:
                continue

            baseline_avg = float(sum(r["entity_count"] for r in baseline_rows)) / len(baseline_rows)
            if baseline_avg == 0 or recent < 3:
                continue

            ratio = recent / baseline_avg
            if ratio >= 2.5:
                counsel = COMPANY_TO_COUNSEL.get(company, [])
                self._fire_velocity_signal(company, recent, baseline_avg, ratio, counsel)

        conn.close()

    # ── Signal generators ──────────────────────────────────────────────────────

    def _fire_deal_structure_signal(self, entity_name: str, parent: str,
                                     counsel: list, agent: str):
        desc = (
            f"DEAL STRUCTURE DETECTED: New entity '{entity_name}' incorporated under "
            f"{parent.title()} in Alberta Corporate Registry. "
            f"Named pattern ('AcquireCo', 'HoldCo', etc.) indicates active M&A structuring. "
            f"{'Registered agent: ' + agent + '. ' if agent else ''}"
            f"This entity was likely created 2-6 weeks before any public announcement."
        )
        for firm_id in counsel[:4]:
            is_new = insert_signal(
                firm_id=firm_id,
                signal_type="registry_deal_structure",
                weight=4.5,
                title=f"Registry: deal-pattern entity '{entity_name}' under {parent.title()}",
                description=desc,
                source_url=CORES_SEARCH_URL,
                practice_area="corporate",
                raw_data={"entity": entity_name, "parent": parent, "agent": agent},
            )
            if is_new:
                self.new_signals.append({
                    "firm_id": firm_id,
                    "signal_type": "registry_deal_structure",
                    "weight": 4.5,
                    "title": f"Registry: deal entity '{entity_name}' ({parent.title()})",
                    "practice_area": "corporate",
                    "description": desc,
                })

    def _fire_velocity_signal(self, company: str, recent: int,
                               baseline: float, ratio: float, counsel: list):
        desc = (
            f"ENTITY VELOCITY SPIKE: {company.title()} incorporated {recent} new entities "
            f"in the past 30 days vs baseline average of {baseline:.1f} ({ratio:.1f}× surge). "
            f"Rapid subsidiary formation is a strong pre-deal indicator. "
            f"A transaction announcement is likely 2-8 weeks away."
        )
        for firm_id in counsel[:4]:
            is_new = insert_signal(
                firm_id=firm_id,
                signal_type="registry_entity_velocity",
                weight=4.0,
                title=f"Registry spike: {company.title()} — {recent} new entities ({ratio:.1f}× normal)",
                description=desc,
                source_url=CORES_SEARCH_URL,
                practice_area="corporate",
                raw_data={"company": company, "recent": recent,
                          "baseline": baseline, "ratio": ratio},
            )
            if is_new:
                self.new_signals.append({
                    "firm_id": firm_id,
                    "signal_type": "registry_entity_velocity",
                    "weight": 4.0,
                    "title": f"Registry spike: {company.title()} — {recent} entities ({ratio:.1f}×)",
                    "practice_area": "corporate",
                    "description": desc,
                })

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _map_agent_to_firm(self, agent_text: str) -> str | None:
        if not agent_text:
            return None
        agent_lower = agent_text.lower()
        for key, firm_id in REGISTERED_AGENT_MAP.items():
            if key in agent_lower:
                return firm_id
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    mon = CorporateRegistryMonitor()
    for s in mon.run():
        print(f"  [{s['signal_type']}] {s['firm_id']}: {s['title']}")
