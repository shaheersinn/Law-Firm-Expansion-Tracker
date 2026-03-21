"""
signals/partner_clock.py
─────────────────────────
The "Partner Clock" Signal

When a lawyer is promoted to Partner (or Counsel/Senior Counsel), three
things happen within 90 days:
  1. The firm now has a senior-associate-shaped hole in the team
  2. The new partner is under pressure to originate business and NEEDS
     a junior to delegate to immediately
  3. Their former peer group is now understaffed at the associate level

This is one of the most reliable predictors of a junior hire opening.

Sources:
  A) LinkedIn title changes: "Associate" → "Partner" or "Counsel"
     detected via Proxycurl weekly profile sweep of known roster
  B) LSA member type changes: "Student/Articled Clerk" → "Lawyer" or
     "Lawyer" → "QC / KC" (King's Counsel appointment = seniority jump)
  C) Firm press releases / news pages: "proud to announce the promotion of..."
  D) Law Times / Canadian Lawyer announcements RSS

Composite signal weight: 4.2
Urgency: Within 3 days (partner needs team NOW)
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
    PROXYCURL_API_KEY, CALGARY_FIRMS, FIRM_ALIASES, FIRM_BY_ID,
    SIGNAL_WEIGHTS, LSA_SEARCH_URL,
)
from database.db import get_conn, insert_signal

log = logging.getLogger(__name__)

# Weight bonus for promotions at boutiques/mid-size (smaller team = bigger gap)
TIER_WEIGHT = {"boutique": 1.3, "mid": 1.15, "big": 1.0}

PROMOTION_KEYWORDS = re.compile(
    r"\b(partner|counsel|senior counsel|of counsel|KC|QC|"
    r"equity partner|income partner|special counsel|principal)\b",
    re.IGNORECASE,
)
PROMO_ANNOUNCE_RE = re.compile(
    r"\b(pleased to announce|proud to announce|promoted to|elevation to|"
    r"new partner|join.{0,20}as partner|appointed.{0,20}partner|"
    r"partnership|made partner)\b",
    re.IGNORECASE,
)

# RSS feeds that announce promotions
PROMO_RSS_FEEDS = [
    "https://www.canadianlawyermag.com/rss",
    "https://www.lawtimesnews.com/rss",
    "https://www.thelawyersdaily.ca/rss",
    "https://lso.ca/rss",
]

# Regex for names in announcement text: "Firstname Lastname has been promoted"
NAME_RE = re.compile(r"([A-Z][a-z]+ (?:[A-Z][a-z]+ )?[A-Z][a-z]+) (?:has been|was|is)")


class PartnerClockTracker:
    """
    Detects new partner promotions at tracked Calgary firms and fires
    a high-weight signal: "New partner → team-building need → hire junior."
    """

    def __init__(self):
        self.new_signals: list[dict] = []
        self._seen_promo_keys: set[str] = set()

    def run(self) -> list[dict]:
        log.info("[PartnerClock] Scanning for new partner promotions")
        self._scan_rss_feeds()
        self._scan_firm_news_pages()
        self._scan_linkedin_promotions()
        log.info("[PartnerClock] Done. %d signals.", len(self.new_signals))
        return self.new_signals

    # ── A) RSS promotion scanner ─────────────────────────────────────────────

    def _scan_rss_feeds(self):
        for url in PROMO_RSS_FEEDS:
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries[:40]:
                    self._check_entry(entry)
            except Exception as e:
                log.debug("[PartnerClock] RSS error %s: %s", url, e)
            time.sleep(0.5)

    def _check_entry(self, entry):
        title   = getattr(entry, "title", "")
        summary = getattr(entry, "summary", "")
        link    = getattr(entry, "link", "")
        text    = f"{title} {summary}"

        if not PROMO_ANNOUNCE_RE.search(text):
            return

        # Check if any Calgary firm is mentioned
        for firm in CALGARY_FIRMS:
            for alias in [firm["name"]] + firm["aliases"]:
                if alias.lower() in text.lower():
                    name_match = NAME_RE.search(text)
                    promoted_name = name_match.group(1) if name_match else "a lawyer"
                    key = f"{firm['id']}|{promoted_name}"
                    if key in self._seen_promo_keys:
                        continue
                    self._seen_promo_keys.add(key)
                    self._fire(firm, promoted_name, "news", title[:80], link)
                    break

    # ── B) Firm news page scanner ────────────────────────────────────────────

    def _scan_firm_news_pages(self):
        session = requests.Session()
        session.headers["User-Agent"] = "LawFirmTracker/3.0"

        for firm in CALGARY_FIRMS:
            news_url = firm.get("news_url", "")
            if not news_url:
                continue
            try:
                resp = session.get(news_url, timeout=10)
                soup = BeautifulSoup(resp.text, "lxml")
                text = soup.get_text(" ", strip=True)
                if PROMO_ANNOUNCE_RE.search(text):
                    names = NAME_RE.findall(text)
                    for promoted_name in names[:3]:
                        key = f"{firm['id']}|{promoted_name}"
                        if key in self._seen_promo_keys:
                            continue
                        self._seen_promo_keys.add(key)
                        self._fire(firm, promoted_name, "firm_news", "", news_url)
                time.sleep(1)
            except Exception as e:
                log.debug("[PartnerClock] News page error %s: %s", news_url, e)

    # ── C) LinkedIn promotion detection (Proxycurl roster sweep) ─────────────

    def _scan_linkedin_promotions(self):
        """
        For each known associate in the roster, check if their title
        now contains a partner-level keyword (meaning they were promoted).
        """
        if not PROXYCURL_API_KEY:
            return

        conn  = get_conn()
        rows  = conn.execute("""
            SELECT lr.*, lr.firm_id as fid
            FROM linkedin_roster lr
            WHERE lr.is_active = 1
              AND date(lr.last_checked) < date('now', '-6 days')
        """).fetchall()
        conn.close()

        import requests as req
        session = req.Session()
        session.headers["Authorization"] = f"Bearer {PROXYCURL_API_KEY}"

        for row in rows:
            row = dict(row)
            try:
                resp    = session.get(
                    "https://nubela.co/proxycurl/api/v2/linkedin",
                    params={"url": row["linkedin_url"]}, timeout=15
                )
                profile = resp.json()
                exps    = profile.get("experiences", [])
                current = [e for e in exps if not e.get("ends_at")]
                if not current:
                    continue
                new_title = current[0].get("title", "")
                old_title = row.get("title", "")

                # Was associate, now partner/counsel
                was_junior  = not PROMOTION_KEYWORDS.search(old_title)
                is_partner  = bool(PROMOTION_KEYWORDS.search(new_title))

                if was_junior and is_partner:
                    firm = FIRM_BY_ID.get(row["firm_id"], {})
                    key  = f"{row['firm_id']}|{row['full_name']}|partner"
                    if key not in self._seen_promo_keys:
                        self._seen_promo_keys.add(key)
                        self._fire(
                            firm, row["full_name"], "linkedin",
                            f"{row['full_name']} promoted to {new_title}",
                            row["linkedin_url"]
                        )
                time.sleep(1.2)
            except Exception as e:
                log.debug("[PartnerClock] LinkedIn error: %s", e)

    # ── Signal builder ────────────────────────────────────────────────────────

    def _fire(self, firm: dict, promoted_name: str, source: str,
              context: str, url: str):
        firm_id   = firm.get("id", "?")
        firm_name = firm.get("name", firm_id)
        tier      = firm.get("tier", "big")
        weight    = 4.2 * TIER_WEIGHT.get(tier, 1.0)
        focus     = firm.get("focus", ["law"])
        pa        = focus[0] if focus else "general"

        desc = (
            f"{promoted_name} has been promoted to Partner/Counsel at {firm_name}. "
            f"This creates an immediate 'partner clock' effect: "
            f"(1) A senior-associate desk is now empty, "
            f"(2) the new partner needs a junior to delegate to within 90 days, "
            f"(3) their former peer group is understaffed. "
            f"Context: {context}"
        )
        is_new = insert_signal(
            firm_id=firm_id,
            signal_type="partner_clock",
            weight=weight,
            title=f"Partner Clock: {promoted_name} promoted at {firm_name}",
            description=desc,
            source_url=url,
            practice_area=pa,
            raw_data={"promoted_name": promoted_name, "source": source, "context": context},
        )
        if is_new:
            self.new_signals.append({
                "firm_id": firm_id, "signal_type": "partner_clock",
                "weight": weight, "title": f"Partner Clock: {promoted_name} → {firm_name}",
                "description": desc, "source_url": url, "practice_area": pa,
            })
            log.info("[PartnerClock] 🔔 New promotion: %s @ %s", promoted_name, firm_name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    signals = PartnerClockTracker().run()
    for s in signals:
        print(f"  {s['firm_id']}: {s['title']}")
