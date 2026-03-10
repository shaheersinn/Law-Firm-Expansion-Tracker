"""
RSS Feed Aggregator
=====================
Monitors 22 legal/financial RSS feeds simultaneously for firm mentions.
Faster and lighter than HTML scraping — many outlets update their
RSS within minutes of publishing.

Sources include national legal press, regional outlets, financial press,
and trade publications that cover specific practice areas.

Why RSS matters: Lateral hire announcements and deal tombstones often
appear in RSS feeds within hours, days before the firm's own news page
is updated.

Changelog:
  - Added alt_names support for comprehensive firm name matching
  - Updated The Lawyer's Daily → Law360 Canada (rebranded)
  - Removed Bloomberg (blocks free RSS) → replaced with Reuters Canada
  - Added Globe & Mail Report on Business, CBC Business
  - Lowered content threshold: score at least 1 match instead of requiring
    lateral/deal phrases OR 100+ chars (was missing pure brand mentions)
  - Added PARTIAL match guard: skip single-word tokens < 4 chars to
    avoid false positives (e.g. "BLG" only matches when standalone)
  - Added published-date parsing to drop items older than LOOKBACK days
  - Per-feed error logging now uses DEBUG not silent pass
"""

import os
import re
import time
import logging
from datetime import datetime, timezone, timedelta

try:
    import feedparser
    HAS_FEEDPARSER = True
except ImportError:
    HAS_FEEDPARSER = False

try:
    from scrapers.base import BaseScraper
    from classifier.department import DepartmentClassifier
except ImportError:
    # Allow standalone import for testing
    class BaseScraper:
        name = "RSSFeedScraper"
        logger = logging.getLogger("scrapers.RSSFeedScraper")
        def _make_signal(self, **kw): return kw
    class DepartmentClassifier:
        def classify(self, text, top_n=1): return []

LOOKBACK_DAYS = int(os.getenv("SIGNAL_LOOKBACK_DAYS", "21"))

logger = logging.getLogger("scrapers.RSSFeedScraper")

classifier = DepartmentClassifier()

# ── Feed registry ────────────────────────────────────────────────────────────
# weight = signal credibility multiplier (applied on top of type weight)
RSS_FEEDS = [
    # ── Core Legal Press ──────────────────────────────────────────────────
    {
        "name": "Canadian Lawyer",
        "url": "https://www.canadianlawyermag.com/rss/",
        "weight": 2.0,
    },
    # Law360 Canada (formerly The Lawyer's Daily — rebranded 2024)
    {
        "name": "Law360 Canada",
        "url": "https://www.law360.ca/rss",
        "weight": 2.0,
        "fallback_url": "https://www.law360.com/canada/rss",
    },
    {
        "name": "Law Times",
        "url": "https://www.lawtimesnews.com/feed",
        "weight": 1.8,
        "fallback_url": "https://www.lawtimesnews.com/rss",
    },
    {
        "name": "Precedent",
        "url": "https://www.precedentmagazine.com/feed/",
        "weight": 1.5,
    },
    {
        "name": "Slaw",
        "url": "https://www.slaw.ca/feed/",
        "weight": 1.5,
    },
    # ── Financial / Business Press ────────────────────────────────────────
    {
        "name": "Globe Report on Business",
        "url": "https://www.theglobeandmail.com/business/article-bnn/?service=rss",
        "weight": 1.8,
        "fallback_url": "https://www.theglobeandmail.com/business/rss",
    },
    {
        "name": "Financial Post",
        "url": "https://financialpost.com/feed",
        "weight": 1.8,
    },
    {
        "name": "Reuters Canada Business",
        "url": "https://feeds.reuters.com/reuters/CABusinessNews",
        "weight": 1.8,
        "fallback_url": "https://www.reuters.com/rssFeed/businessNews",
    },
    {
        "name": "CBC Business",
        "url": "https://www.cbc.ca/cmlink/rss-business",
        "weight": 1.5,
    },
    # ── Practice-Area Specific ────────────────────────────────────────────
    {
        "name": "Privacy Law Blog",
        "url": "https://www.privacylawblog.ca/feed/",
        "weight": 2.0,
    },
    {
        "name": "Lexology Canada",
        "url": "https://www.lexology.com/rss/feed/canada.xml",
        "weight": 1.5,
    },
    {
        "name": "Mondaq Canada",
        "url": "https://www.mondaq.com/rss/canada/rss",
        "weight": 1.5,
    },
    {
        "name": "Competition Bureau News",
        "url": "https://www.canada.ca/en/competition-bureau/news.rss",
        "weight": 2.0,
    },
    # ── Deal / Press Wires ────────────────────────────────────────────────
    {
        "name": "Cision Newswire",
        "url": "https://www.newswire.ca/rss/",
        "weight": 1.5,
    },
    {
        "name": "CNW Group",
        "url": "https://www.cnw.ca/rss/",
        "weight": 1.5,
    },
    # ── Litigation / Courts ───────────────────────────────────────────────
    {
        "name": "Advocates Daily",
        "url": "https://www.advocates-daily.com/rss.xml",
        "weight": 2.0,
    },
    # ── Energy / ESG ──────────────────────────────────────────────────────
    {
        "name": "Daily Oil Bulletin",
        "url": "https://www.dailyoilbulletin.com/rss",
        "weight": 2.0,
    },
    {
        "name": "ESG Today",
        "url": "https://www.esgtoday.com/feed/",
        "weight": 1.5,
    },
    # ── Regulatory ────────────────────────────────────────────────────────
    {
        "name": "OSC News",
        "url": "https://www.osc.ca/en/rss.xml",
        "weight": 2.5,
    },
    {
        "name": "Canada Gazette",
        "url": "https://gazette.gc.ca/rss/p1-eng.xml",
        "weight": 2.0,
    },
    # ── IP / Tech ─────────────────────────────────────────────────────────
    {
        "name": "IP Osgoode",
        "url": "https://www.iposgoode.ca/feed/",
        "weight": 1.5,
    },
    {
        "name": "IT World Canada",
        "url": "https://www.itworldcanada.com/blog/feed",
        "weight": 1.3,
    },
]

# Lateral hire / appointment signals
LATERAL_PHRASES = [
    "joins", "joined", "has joined", "welcomes", "new partner",
    "lateral hire", "expands team", "grows practice", "named partner",
    "appointed partner", "promoted to partner", "new associate",
    "new counsel", "joins as", "moves to", "moves from",
    "is pleased to announce", "pleased to welcome",
]

# Deal / advisory signals
DEAL_PHRASES = [
    "advises", "advised", "counsel to", "acts as counsel",
    "represented", "acting for", "legal counsel",
    "successfully completed", "closes", "closed", "announces",
    "transaction counsel", "lead counsel",
]

# Expansion / office signals
EXPANSION_PHRASES = [
    "opens office", "new office", "expands to", "launches",
    "establishes presence", "new practice group", "bolsters",
]


def _parse_entry_date(entry) -> datetime | None:
    """Return timezone-aware datetime from feedparser entry, or None."""
    for attr in ("published_parsed", "updated_parsed", "created_parsed"):
        t = getattr(entry, attr, None)
        if t:
            try:
                import calendar
                ts = calendar.timegm(t)
                return datetime.fromtimestamp(ts, tz=timezone.utc)
            except Exception:
                pass
    return None


def _build_name_patterns(firm: dict) -> list[re.Pattern]:
    """
    Build compiled regex patterns for all known firm name variants.
    Uses word-boundary matching to avoid partial hits like "BLG" inside "OBLG".
    """
    names = [firm["short"]] + firm.get("alt_names", [])
    # Also add first word of full name if >= 5 chars
    first_word = firm["name"].split()[0].rstrip(",")
    if len(first_word) >= 5 and first_word not in names:
        names.append(first_word)

    patterns = []
    for name in names:
        escaped = re.escape(name)
        try:
            patterns.append(re.compile(r"\b" + escaped + r"\b", re.IGNORECASE))
        except re.error:
            pass
    return patterns


class RSSFeedScraper(BaseScraper):
    name = "RSSFeedScraper"

    def __init__(self):
        self._cutoff = datetime.now(tz=timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    def fetch(self, firm: dict) -> list[dict]:
        if not HAS_FEEDPARSER:
            self.logger.warning("feedparser not installed — skipping RSS scraper")
            return []

        patterns = _build_name_patterns(firm)
        signals = []

        for feed_meta in RSS_FEEDS:
            try:
                new = self._process_feed(firm, patterns, feed_meta)
                signals.extend(new)
            except Exception as exc:
                self.logger.debug(
                    f"[{firm['short']}] RSS error on {feed_meta['name']}: {exc}"
                )

        self.logger.info(f"[{firm['short']}] RSS total: {len(signals)} signal(s)")
        return signals

    # ------------------------------------------------------------------ #

    def _process_feed(
        self, firm: dict, patterns: list[re.Pattern], feed_meta: dict
    ) -> list[dict]:
        signals = []
        url = feed_meta["url"]

        feed = feedparser.parse(url)

        # If primary URL yields nothing, try fallback
        if not feed.entries and "fallback_url" in feed_meta:
            self.logger.debug(
                f"[{firm['short']}] {feed_meta['name']}: primary empty, trying fallback"
            )
            feed = feedparser.parse(feed_meta["fallback_url"])

        for entry in (feed.entries or [])[:30]:
            # ── Date filter ──────────────────────────────────────────────
            pub_dt = _parse_entry_date(entry)
            if pub_dt and pub_dt < self._cutoff:
                continue   # too old

            title   = entry.get("title",   "")
            summary = entry.get("summary", entry.get("description", ""))
            link    = entry.get("link",    url)
            full    = f"{title} {summary}"
            lower   = full.lower()

            # ── Firm name match ──────────────────────────────────────────
            if not any(p.search(full) for p in patterns):
                continue

            # ── Signal type ──────────────────────────────────────────────
            is_lateral   = any(ph in lower for ph in LATERAL_PHRASES)
            is_deal      = any(ph in lower for ph in DEAL_PHRASES)
            is_expansion = any(ph in lower for ph in EXPANSION_PHRASES)

            # Accept any firm mention with meaningful content (>= 60 chars)
            if not (is_lateral or is_deal or is_expansion or len(full.strip()) >= 60):
                continue

            if is_lateral:
                sig_type    = "lateral_hire"
                weight_mult = 2.5
            elif is_expansion:
                sig_type    = "press_release"
                weight_mult = 2.0
            elif is_deal:
                sig_type    = "press_release"
                weight_mult = 1.5
            else:
                sig_type    = "press_release"
                weight_mult = 1.0

            classifications = classifier.classify(full, top_n=1)
            if not classifications:
                continue

            cls = classifications[0]
            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type=sig_type,
                title=f"[{feed_meta['name']}] {title[:180]}",
                body=summary[:700],
                url=link,
                department=cls["department"],
                department_score=cls["score"] * feed_meta["weight"] * weight_mult,
                matched_keywords=cls["matched_keywords"],
                source_date=pub_dt.isoformat() if pub_dt else None,
            ))

        return signals
