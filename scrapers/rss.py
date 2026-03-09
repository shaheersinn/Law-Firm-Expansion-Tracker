"""
RSS Feed Aggregator
=====================
Monitors 20+ legal news RSS feeds simultaneously for firm mentions.
Faster and lighter than HTML scraping — many outlets update their
RSS within minutes of publishing.

Sources include national legal press, regional outlets, financial press,
and trade publications that cover specific practice areas.

Why RSS matters: Lateral hire announcements and deal tombstones often
appear in RSS feeds within hours, days before the firm's own news page
is updated.
"""

import re
import time
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

try:
    import feedparser
    HAS_FEEDPARSER = True
except ImportError:
    HAS_FEEDPARSER = False

classifier = DepartmentClassifier()

RSS_FEEDS = [
    # ── Legal Press ────────────────────────────────────────────────────
    {"name": "Canadian Lawyer",       "url": "https://www.canadianlawyermag.com/rss/",                    "weight": 2.0},
    {"name": "The Lawyer's Daily",    "url": "https://www.thelawyersdaily.ca/rss",                         "weight": 2.0},
    {"name": "Law Times",             "url": "https://www.lawtimesnews.com/rss",                           "weight": 1.8},
    {"name": "Precedent",             "url": "https://www.precedentmagazine.com/feed/",                    "weight": 1.5},
    # ── Financial Press (deal coverage) ───────────────────────────────
    {"name": "Globe B&M",             "url": "https://www.theglobeandmail.com/business/rss",               "weight": 1.8},
    {"name": "Financial Post",        "url": "https://financialpost.com/feed",                             "weight": 1.8},
    {"name": "Bloomberg Canada",      "url": "https://feeds.bloomberg.com/markets/news.rss",               "weight": 2.0},
    # ── Practice-area specific ─────────────────────────────────────────
    {"name": "Privacy Law Blog",      "url": "https://www.privacylawblog.ca/feed/",                        "weight": 2.0},
    {"name": "Slaw.ca",               "url": "https://www.slaw.ca/feed/",                                  "weight": 1.5},
    {"name": "Lexology Canada",       "url": "https://www.lexology.com/rss/feed/canada.xml",               "weight": 1.5},
    {"name": "Mondaq Canada",         "url": "https://www.mondaq.com/rss/canada/rss",                      "weight": 1.5},
    # ── Deal Wires ────────────────────────────────────────────────────
    {"name": "Cision PR",             "url": "https://www.newswire.ca/rss/",                               "weight": 1.5},
    {"name": "CNW Group",             "url": "https://www.cnw.ca/rss/",                                    "weight": 1.5},
    # ── Litigation/Courts ──────────────────────────────────────────────
    {"name": "Advocates Daily",       "url": "https://www.advocates-daily.com/rss.xml",                    "weight": 2.0},
    # ── ESG/Energy ────────────────────────────────────────────────────
    {"name": "Daily Oil Bulletin",    "url": "https://www.dailyoilbulletin.com/rss",                       "weight": 2.0},
    {"name": "CBC Business",          "url": "https://www.cbc.ca/cmlink/rss-business",                     "weight": 1.5},
]

LATERAL_PHRASES = [
    "joins", "joined", "has joined", "welcomes", "new partner",
    "lateral hire", "expands team", "grows practice",
    "appointed partner", "named partner",
]

DEAL_PHRASES = [
    "advises", "advised", "counsel to", "acts as counsel", "represented",
    "successfully completed", "closes", "closed", "announces",
]


class RSSFeedScraper(BaseScraper):
    name = "RSSFeedScraper"

    def fetch(self, firm: dict) -> list[dict]:
        if not HAS_FEEDPARSER:
            self.logger.warning("feedparser not installed — skipping RSS scraper")
            return []

        signals = []
        firm_names = [firm["short"]] + firm.get("alt_names", []) + [firm["name"].split()[0]]

        for feed_meta in RSS_FEEDS:
            signals.extend(self._process_feed(firm, firm_names, feed_meta))

        self.logger.info(f"[{firm['short']}] RSS total: {len(signals)} signal(s)")
        return signals

    def _process_feed(self, firm: dict, firm_names: list, feed_meta: dict) -> list[dict]:
        signals = []
        try:
            feed = feedparser.parse(feed_meta["url"])
        except Exception as e:
            self.logger.debug(f"RSS parse error {feed_meta['url']}: {e}")
            return signals

        for entry in (feed.entries or [])[:25]:
            title   = entry.get("title",   "")
            summary = entry.get("summary", entry.get("description", ""))
            link    = entry.get("link",    feed_meta["url"])
            full    = f"{title} {summary}"
            lower   = full.lower()

            if not any(n.lower() in lower for n in firm_names):
                continue

            is_lateral = any(p in lower for p in LATERAL_PHRASES)
            is_deal    = any(p in lower for p in DEAL_PHRASES)

            if not (is_lateral or is_deal or len(full) > 100):
                continue

            sig_type   = "lateral_hire"  if is_lateral else "press_release"
            weight_mult = 2.5 if is_lateral else 1.0

            classifications = classifier.classify(full, top_n=1)
            if not classifications:
                continue

            cls = classifications[0]
            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type=sig_type,
                title=f"[{feed_meta['name']}] {title[:160]}",
                body=summary[:600],
                url=link,
                department=cls["department"],
                department_score=cls["score"] * feed_meta["weight"] * weight_mult,
                matched_keywords=cls["matched_keywords"],
            ))

        return signals
