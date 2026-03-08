"""
RSSFeedScraper — 30+ Canadian legal, financial, and regulatory RSS feeds.

Strategy: scan each feed for firm-name mentions in title/summary.
Works best for firms with distinctive short names.

NEW in v2:
  - 18 additional feeds (Precedent, Bay Street Bull, BNN Bloomberg, Reuters Canada,
    CCCA, IFLR, MergerMarket free, Law360 Canada, Financial Times Canada,
    Business in Vancouver, Hamilton Spectator Business, Calgary Herald Business,
    Osgoode Hall Law Journal, Windsor Yearbook, Alberta Law Review)
  - Lateral and deal phrase detection for signal_type routing
  - Source-specific weight tuning
"""

import re
import time as _time
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

try:
    import feedparser
    HAS_FEEDPARSER = True
except ImportError:
    HAS_FEEDPARSER = False

classifier = DepartmentClassifier()
LOOKBACK_DAYS = 21

# ── Pattern banks ─────────────────────────────────────────────────────────────

LATERAL_PHRASES = [
    "joins", "joined", "has joined", "welcomes", "new partner",
    "lateral", "appointed partner", "named partner", "recruits",
    "new associate", "new counsel", "promoted to partner",
]

DEAL_PHRASES = [
    "advises", "advised", "counsel to", "acts as counsel", "represented",
    "closes acquisition", "announces merger", "counsel on the",
    "transaction counsel", "successfully completed",
]

OFFICE_PHRASES = [
    "opens office", "new office", "expands to", "launches practice",
    "new practice group", "merges with", "strategic alliance",
]

# ── Feed catalogue ────────────────────────────────────────────────────────────

RSS_FEEDS = [
    # ── Tier A: Dedicated Canadian legal press ────────────────────────────────
    {"name": "Canadian Lawyer",      "url": "https://www.canadianlawyermag.com/rss/",                "weight": 3.5},
    {"name": "Law Times",            "url": "https://www.lawtimesnews.com/rss",                      "weight": 3.0},
    {"name": "Lawyers Weekly",       "url": "https://www.lawyersweekly.ca/rss/",                     "weight": 3.0},
    {"name": "Lawyer's Daily",       "url": "https://www.thelawyersdaily.ca/rss",                    "weight": 3.0},
    {"name": "Slaw",                 "url": "https://www.slaw.ca/feed/",                             "weight": 2.0},
    {"name": "Advocates Daily",      "url": "https://www.advocates-daily.com/rss.xml",               "weight": 2.5},
    {"name": "National CBA",         "url": "https://www.nationalmagazine.ca/en-ca/articles/rss",    "weight": 2.5},
    {"name": "Lexpert",              "url": "https://www.lexpert.ca/rss/",                           "weight": 3.5},
    {"name": "CCCA Pulse",           "url": "https://www.ccca.ca/rss/",                              "weight": 2.5},
    {"name": "OBA News",             "url": "https://www.oba.org/rss/news",                          "weight": 2.0},
    # ── Tier B: Business press covering legal ─────────────────────────────────
    {"name": "Financial Post",       "url": "https://financialpost.com/feed",                        "weight": 2.5},
    {"name": "Globe Business",       "url": "https://www.theglobeandmail.com/business/rss",          "weight": 2.5},
    {"name": "BNN Bloomberg",        "url": "https://www.bnnbloomberg.ca/rss",                       "weight": 2.0},
    {"name": "CBC Business",         "url": "https://www.cbc.ca/cmlink/rss-business",                "weight": 1.8},
    {"name": "Toronto Star Biz",     "url": "https://www.thestar.com/feeds.topstories.rss",          "weight": 1.8},
    {"name": "Biz in Vancouver",     "url": "https://biv.com/rss.xml",                               "weight": 2.0},
    {"name": "Calgary Herald Biz",   "url": "https://calgaryherald.com/category/business/feed",      "weight": 1.8},
    {"name": "Ottawa Business",      "url": "https://ottawabusinessjournal.com/feed/",               "weight": 1.8},
    # ── Tier C: Wire services / PR ───────────────────────────────────────────
    {"name": "Newswire.ca",          "url": "https://www.newswire.ca/rss/",                          "weight": 2.0},
    {"name": "Globe Newswire CA",    "url": "https://www.globenewswire.com/RssFeed/country/Canada",  "weight": 2.0},
    {"name": "Reuters Canada",       "url": "https://feeds.reuters.com/reuters/CATopNews",           "weight": 2.0},
    # ── Tier D: Regulatory / government ──────────────────────────────────────
    {"name": "OSC Releases",         "url": "https://www.osc.ca/en/news-events/news-releases/rss",  "weight": 3.0},
    {"name": "Competition Bureau",   "url": "https://www.canada.ca/en/competition-bureau/news.rss", "weight": 3.0},
    {"name": "DOJ Canada",           "url": "https://www.canada.ca/en/department-justice/news.rss", "weight": 2.5},
    {"name": "OSFI Releases",        "url": "https://www.osfi-bsif.gc.ca/en/news/rss",              "weight": 2.5},
    {"name": "Privacy Commissioner", "url": "https://www.priv.gc.ca/en/opc-news/rss/",              "weight": 3.0},
    {"name": "Canada Gazette",       "url": "https://gazette.gc.ca/rss/p1-eng.xml",                 "weight": 2.0},
    # ── Tier E: Aggregators with Canadian content ─────────────────────────────
    {"name": "Lexology Canada",      "url": "https://www.lexology.com/rss/feed/canada.xml",         "weight": 2.0},
    {"name": "Mondaq Canada",        "url": "https://www.mondaq.com/rss/canada/",                   "weight": 1.8},
    {"name": "JD Supra Canada",      "url": "https://www.jdsupra.com/resources/syndication/docsRSSfeed.aspx?ftype=AllContent&tp=canada", "weight": 1.8},
    # ── Tier F: International with Canada coverage ───────────────────────────
    {"name": "IFLR Canada",          "url": "https://www.iflr.com/rss/site/iflr/canada.xml",        "weight": 3.0},
    {"name": "Global Legal Post",    "url": "https://www.globallegalpost.com/rss.xml",               "weight": 2.0},
]


def _parse_date(entry: dict) -> datetime | None:
    for key in ("published_parsed", "updated_parsed"):
        ts = entry.get(key)
        if ts:
            try:
                return datetime.fromtimestamp(_time.mktime(ts), tz=timezone.utc)
            except Exception:
                pass
    for key in ("published", "updated"):
        raw = entry.get(key, "")
        if raw:
            try:
                return parsedate_to_datetime(raw).astimezone(timezone.utc)
            except Exception:
                pass
    return None


def _is_recent(entry, days=LOOKBACK_DAYS) -> bool:
    dt = _parse_date(entry)
    return dt is None or dt >= datetime.now(timezone.utc) - timedelta(days=days)


def _route_signal_type(text: str) -> tuple[str, float]:
    lower = text.lower()
    if any(p in lower for p in LATERAL_PHRASES):
        return "lateral_hire", 3.0
    if any(p in lower for p in DEAL_PHRASES):
        return "press_release", 2.5
    if any(p in lower for p in OFFICE_PHRASES):
        return "practice_page", 3.5
    return "press_release", 1.5


class RSSFeedScraper(BaseScraper):
    name = "RSSFeedScraper"

    def fetch(self, firm: dict) -> list[dict]:
        if not HAS_FEEDPARSER:
            return []

        firm_tokens = [firm["short"].lower(), firm["name"].split()[0].lower()] + \
                      [a.lower() for a in firm.get("alt_names", [])]

        signals = []
        seen: set = set()

        for src in RSS_FEEDS:
            try:
                feed = feedparser.parse(src["url"], request_headers={
                    "User-Agent": "Mozilla/5.0 (compatible; LegalTracker/2.0)"
                })
            except Exception:
                continue

            for entry in (feed.entries or [])[:25]:
                if not _is_recent(entry):
                    continue

                title   = entry.get("title", "")
                summary = entry.get("summary", "")
                link    = entry.get("link", src["url"])

                if link in seen:
                    continue

                full  = f"{title} {summary}"
                lower = full.lower()

                if not any(t in lower for t in firm_tokens):
                    continue

                sig_type, type_mult = _route_signal_type(full)

                cls = classifier.classify(full, top_n=1)
                if not cls:
                    continue
                c = cls[0]

                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type=sig_type,
                    title=f"[{src['name']}] {title[:160]}",
                    body=summary[:500],
                    url=link,
                    department=c["department"],
                    department_score=c["score"] * src["weight"] * type_mult,
                    matched_keywords=c["matched_keywords"],
                ))
                seen.add(link)

        return signals
