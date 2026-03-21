"""
signals/linkedin_turnover.py
─────────────────────────────
Strategy 3 — "Empty Chair" (Turnover Detection)

Builds and maintains a roster of 1st/2nd-year associates at 30 Calgary firms.
Runs a weekly cron job to detect when a junior associate changes employer.
When a departure is detected → unadvertised vacancy signal.

API: Proxycurl (https://nubela.co/proxycurl)
     or PhantomBuster as an alternative (configured by env var LINKEDIN_PROVIDER)

Cron: weekly (Sunday 05:00 UTC via GitHub Actions)

Ethics note: Proxycurl's API uses LinkedIn's public data in compliance with
             the hiQ v. LinkedIn ruling. Still — only use for individual
             professional data, not bulk harvesting of private content.
"""

import re
import time
import logging
from datetime import datetime, date, timedelta

import requests

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config_calgary import (
    PROXYCURL_API_KEY, CALGARY_FIRMS, SIGNAL_WEIGHTS,
)
from database.db import upsert_linkedin_associate, insert_signal, get_conn

log = logging.getLogger(__name__)

PROXYCURL_SEARCH_URL  = "https://nubela.co/proxycurl/api/v2/search/person"
PROXYCURL_PROFILE_URL = "https://nubela.co/proxycurl/api/v2/linkedin"
PROXYCURL_COMPANY_URL = "https://nubela.co/proxycurl/api/linkedin/company/employees/"

LINKEDIN_PROVIDER = os.getenv("LINKEDIN_PROVIDER", "proxycurl")

# Seniority terms that indicate 1st/2nd year associates
_JUNIOR_TITLE_RE = re.compile(
    r"\b(associate|articling student|articled student|student.at.law|"
    r"junior associate|first.year|1st.year|second.year|2nd.year|"
    r"law clerk|legal assistant|junior counsel)\b",
    re.IGNORECASE,
)

# Titles that indicate they've been promoted (not an empty-chair opportunity)
_SENIOR_TITLE_RE = re.compile(
    r"\b(partner|senior associate|counsel|principal|director|manager|"
    r"in.house|general counsel|GC|VP legal|chief legal)\b",
    re.IGNORECASE,
)


class ProxycurlClient:
    """Wrapper for the Proxycurl API with rate limiting."""

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("PROXYCURL_API_KEY not set. Get a key at nubela.co/proxycurl")
        self.api_key  = api_key
        self.session  = requests.Session()
        self.session.headers["Authorization"] = f"Bearer {api_key}"
        self._last    = 0.0
        self.delay    = 1.0   # 1 req/sec to stay within free-tier limits

    def _get(self, url: str, params: dict) -> dict:
        elapsed = time.time() - self._last
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        resp = self.session.get(url, params=params, timeout=20)
        self._last = time.time()
        if resp.status_code == 429:
            log.warning("[Proxycurl] Rate limited — sleeping 60s")
            time.sleep(60)
            return self._get(url, params)
        resp.raise_for_status()
        return resp.json()

    def search_employees(self, company_linkedin_url: str,
                         title_keywords: str = "associate") -> list[dict]:
        """
        Search for current employees of a company with a given title keyword.
        Returns list of profile stubs.
        """
        params = {
            "linkedin_company_profile_url": company_linkedin_url,
            "keyword_regex": title_keywords,
            "enrich_profiles": "skip",   # saves credits — we'll fetch separately
            "page_size": 50,
        }
        data = self._get(PROXYCURL_COMPANY_URL, params)
        return data.get("employees", [])

    def get_profile(self, linkedin_url: str) -> dict:
        """Fetch full profile for a LinkedIn URL."""
        return self._get(PROXYCURL_PROFILE_URL, {"url": linkedin_url})


def _is_junior_title(title: str) -> bool:
    return bool(_JUNIOR_TITLE_RE.search(title)) and not bool(_SENIOR_TITLE_RE.search(title))


def _company_url_from_slug(slug: str) -> str:
    return f"https://www.linkedin.com/company/{slug}/"


class LinkedInTurnoverTracker:
    """
    Builds and updates the junior associate roster.
    Detects departures and fires 'Empty Chair' signals.
    """

    def __init__(self):
        if not PROXYCURL_API_KEY:
            log.warning("[LinkedIn] No Proxycurl API key — tracker disabled")
            self.client = None
        else:
            self.client = ProxycurlClient(PROXYCURL_API_KEY)
        self.new_signals: list[dict] = []

    # ── Phase 1: Initial roster build (run once per firm) ────────────────────

    def build_roster(self, firm_id: str = None):
        """
        Fetch the current list of junior associates for all (or one) firm.
        Should be run once to bootstrap the roster, then weekly to update.
        """
        if not self.client:
            return
        firms = [f for f in CALGARY_FIRMS if f["id"] == firm_id] if firm_id else CALGARY_FIRMS

        for firm in firms:
            slug = firm.get("linkedin_slug", "")
            if not slug:
                continue
            co_url = _company_url_from_slug(slug)
            log.info("[LinkedIn] Building roster for %s (%s)", firm["name"], co_url)
            self._refresh_firm_roster(firm, co_url)

    def _refresh_firm_roster(self, firm: dict, co_url: str):
        try:
            employees = self.client.search_employees(co_url, title_keywords="associate|student")
        except Exception as e:
            log.error("[LinkedIn] Error fetching employees for %s: %s", firm["id"], e)
            return

        for emp in employees:
            title = emp.get("title", "")
            if not _is_junior_title(title):
                continue
            linkedin_url = emp.get("linkedin_profile_url", "")
            if not linkedin_url:
                continue
            upsert_linkedin_associate({
                "firm_id":      firm["id"],
                "linkedin_url": linkedin_url,
                "full_name":    emp.get("name", ""),
                "title":        title,
                "start_date":   "",
                "seniority":    self._classify_seniority(title),
                "last_checked": date.today().isoformat(),
                "is_active":    1,
                "left_date":    None,
                "new_employer": None,
            })

    # ── Phase 2: Weekly departure check ─────────────────────────────────────

    def check_departures(self):
        """
        For every associate in the roster, re-fetch their profile.
        If their current employer has changed → departure detected.
        """
        if not self.client:
            return []

        conn = get_conn()
        rows = conn.execute("""
            SELECT * FROM linkedin_roster
            WHERE is_active = 1
              AND (last_checked IS NULL OR date(last_checked) < date('now', '-6 days'))
        """).fetchall()
        conn.close()

        log.info("[LinkedIn] Checking %d active associates for departures", len(rows))

        for row in rows:
            self._check_one(dict(row))
            time.sleep(1.2)   # polite delay

        return self.new_signals

    def _check_one(self, row: dict):
        """Fetch latest profile and compare employer."""
        try:
            profile = self.client.get_profile(row["linkedin_url"])
        except Exception as e:
            log.debug("[LinkedIn] Profile fetch error for %s: %s", row["linkedin_url"], e)
            return

        # Current employer from profile
        experiences  = profile.get("experiences", [])
        current_exps = [e for e in experiences if not e.get("ends_at")]
        current_co   = current_exps[0].get("company", "") if current_exps else ""
        current_title = current_exps[0].get("title", "") if current_exps else ""

        firm         = next((f for f in CALGARY_FIRMS if f["id"] == row["firm_id"]), None)
        if not firm:
            return

        # Check if still at the tracked firm
        still_here   = any(
            alias.lower() in current_co.lower()
            for alias in [firm["name"]] + firm["aliases"]
        ) if current_co else True  # ambiguous — don't fire

        # Update last_checked
        conn = get_conn()
        conn.execute(
            "UPDATE linkedin_roster SET last_checked=? WHERE id=?",
            (date.today().isoformat(), row["id"])
        )

        if not still_here and current_co:
            # Departure confirmed
            log.info("[LinkedIn] DEPARTURE: %s left %s → %s",
                     row["full_name"], firm["name"], current_co)
            conn.execute("""
                UPDATE linkedin_roster
                SET is_active=0, left_date=?, new_employer=?
                WHERE id=?
            """, (date.today().isoformat(), current_co, row["id"]))
            conn.commit()
            conn.close()

            sig_type = "linkedin_turnover_detected"
            weight   = SIGNAL_WEIGHTS[sig_type]

            # Extra weight if they went in-house (higher urgency to fill)
            in_house_keywords = ["in-house", "counsel", "legal department",
                                 "Inc.", "Ltd.", "Corp.", "Energy", "Resources"]
            if any(kw.lower() in current_co.lower() for kw in in_house_keywords):
                weight += 0.5
                sig_type = "linkedin_turnover_detected"

            msg = (f"{row['full_name']} ({row['seniority'] or 'associate'}) has left "
                   f"{firm['name']} for {current_co} ({current_title}). "
                   f"Unadvertised vacancy likely — contact hiring partner immediately.")

            self.new_signals.append({
                "firm_id":     row["firm_id"],
                "signal_type": sig_type,
                "weight":      weight,
                "title":       f"Empty chair at {firm['name']}: {row['full_name']} departed",
                "description": msg,
                "source_url":  row["linkedin_url"],
                "raw_data":    {
                    "departed_name": row["full_name"],
                    "new_employer": current_co,
                    "new_title": current_title,
                },
            })
            insert_signal(
                firm_id=row["firm_id"],
                signal_type=sig_type,
                weight=weight,
                title=f"Empty chair at {firm['name']}: {row['full_name']} departed",
                description=msg,
                source_url=row["linkedin_url"],
                raw_data={
                    "departed_name": row["full_name"],
                    "new_employer": current_co,
                },
            )
        else:
            conn.commit()
            conn.close()

    @staticmethod
    def _classify_seniority(title: str) -> str:
        t = title.lower()
        if any(k in t for k in ["1st", "first", "articling", "student"]):
            return "1st year"
        if any(k in t for k in ["2nd", "second"]):
            return "2nd year"
        return "junior associate"

    # ── Roster summary ───────────────────────────────────────────────────────

    def get_roster_summary(self) -> list[dict]:
        conn = get_conn()
        rows = conn.execute("""
            SELECT firm_id, count(*) as active_juniors
            FROM linkedin_roster
            WHERE is_active = 1
            GROUP BY firm_id
            ORDER BY active_juniors DESC
        """).fetchall()
        conn.close()
        return [dict(r) for r in rows]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    tracker = LinkedInTurnoverTracker()
    # tracker.build_roster()    # run once to bootstrap
    signals = tracker.check_departures()
    for s in signals:
        print(f"  [{s['signal_type']}] {s['firm_id']}: {s['title']}")
