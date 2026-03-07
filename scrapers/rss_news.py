"""
RSS Legal News Scraper
======================
Scrapes RSS/Atom feeds from major Canadian legal and business news outlets.
Detects firm name mentions in headlines and summaries — a high-velocity,
low-noise signal source that complements firm-direct scrapers.

Sources:
  • Law Times (lawttimes.ca)           — Canadian lawyer news
  • Canadian Lawyer Mag                — rankings, features, firm profiles  
  • Financial Post — Legal             — M&A counsel mentions
  • Globe and Mail — Business          — deal announcements, firm mentions
  • Lexology Canada                    — legal updates mentioning firms
  • Law360 Canada                      — deal/litigation news

Signal weight: press_release class (3.0 base) — external validation
"""

import re
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from scrapers.base import BaseScraper

logger = logging.getLogger("scrapers.RSSNewsScraper")

# ── Feed sources ────────────────────────────────────────────────────
RSS_FEEDS = [
    {
        "name": "Law Times",
        "url": "https://www.lawtimesnews.com/feed",
        "score_boost": 1.2,   # Specialist Canadian legal outlet = higher signal value
    },
    {
        "name": "Canadian Lawyer",
        "url": "https://www.canadianlawyermag.com/feed",
        "score_boost": 1.2,
    },
    {
        "name": "Financial Post Business",
        "url": "https://financialpost.com/business/feed",
        "score_boost": 1.0,
    },
    {
        "name": "Globe Business",
        "url": "https://www.theglobeandmail.com/business/feed",
        "score_boost": 1.0,
    },
    {
        "name": "Lexology Canada",
        "url": "https://www.lexology.com/rss.ashx?t=l&c=Canada",
        "score_boost": 1.1,
    },
    {
        "name": "Reuters Legal",
        "url": "https://feeds.reuters.com/reuters/companyNews",
        "score_boost": 0.9,
    },
]

# Phrases indicating a firm acted as counsel/advisor (higher signal value)
COUNSEL_PHRASES = [
    "acted as counsel", "as counsel to", "as legal advisor",
    "represented by", "advised by", "retained", "as counsel for",
    "legal counsel", "its lawyers", "its legal team", "lawyers from",
    "partner at", "counsel at", "associate at", "led by",
]

# Phrases that score lower (general mention, not specifically as counsel)
GENERAL_MENTION_PHRASES = [
    "law firm", "legal firm", "full-service", "boutique", "national firm",
]

# Keywords strongly suggesting expansion signal
EXPANSION_KEYWORDS = [
    "expand", "expan", "growth", "grow", "hire", "partner", "lateral",
    "opens", "office", "practice", "group", "team", "appoint", "senior",
    "join", "promote", "promoted", "promotion", "counsel", "advise",
    "merger", "acqui", "deal", "transaction", "M&A",
]


def _parse_feed(content: str, source_url: str) -> list[dict]:
    """
    Parse RSS/Atom feed XML into list of {title, url, summary, published} dicts.
    Uses simple regex to avoid external XML parser dependency.
    """
    items = []

    # Normalise CDATA
    content = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', content, flags=re.DOTALL)

    # Try both <item> (RSS) and <entry> (Atom)
    for tag in ("item", "entry"):
        pattern = rf"<{tag}[^>]*>(.*?)</{tag}>"
        for m in re.finditer(pattern, content, re.DOTALL):
            block = m.group(1)

            title_m   = re.search(r"<title[^>]*>(.*?)</title>",       block, re.DOTALL)
            link_m    = re.search(r"<link[^>]*>(.*?)</link>",          block, re.DOTALL)
            if not link_m:
                link_m = re.search(r"<link[^>]+href=['\"]([^'\"]+)['\"]", block)
            summary_m = re.search(r"<(?:description|summary)[^>]*>(.*?)</(?:description|summary)>",
                                  block, re.DOTALL)
            date_m    = re.search(r"<(?:pubDate|published|updated)[^>]*>(.*?)</(?:pubDate|published|updated)>",
                                  block, re.DOTALL)

            title   = re.sub(r"<[^>]+>", "", title_m.group(1)).strip()   if title_m   else ""
            link    = link_m.group(1).strip()                              if link_m    else source_url
            summary = re.sub(r"<[^>]+>", "", summary_m.group(1)).strip()  if summary_m else ""
            pub     = date_m.group(1).strip()                              if date_m    else ""

            if title:
                items.append({
                    "title":     title[:300],
                    "url":       link[:500],
                    "summary":   summary[:600],
                    "published": pub,
                })

    return items[:40]   # cap per feed


class RSSNewsScraper(BaseScraper):
    name = "RSSNewsScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        firm_names = [firm["name"], firm["short"]] + firm.get("alt_names", [])

        for feed in RSS_FEEDS:
            resp = self._get(feed["url"])
            if not resp:
                continue

            items = _parse_feed(resp.text, feed["url"])
            for item in items:
                full_text = f"{item['title']} {item['summary']}"
                full_lower = full_text.lower()

                # Must mention the firm
                if not any(n.lower() in full_lower for n in firm_names):
                    continue

                # Score the signal
                base_score = 2.0 * feed["score_boost"]

                # Boost if counsel phrase found
                if any(p in full_lower for p in COUNSEL_PHRASES):
                    base_score += 1.5

                # Boost if expansion keyword present
                expansion_hits = sum(1 for kw in EXPANSION_KEYWORDS if kw.lower() in full_lower)
                base_score += min(expansion_hits * 0.3, 1.2)

                # Deduplicate: skip if title mostly same as existing signals this run
                dedup = any(
                    s.get("title", "").lower()[:60] == item["title"].lower()[:60]
                    for s in signals
                )
                if dedup:
                    continue

                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="press_release",
                    title=f"[{feed['name']}] {item['title']}",
                    body=item["summary"],
                    url=item["url"],
                    department="",    # classifier will assign
                    department_score=base_score,
                    matched_keywords=[w for w in EXPANSION_KEYWORDS if w.lower() in full_lower][:5],
                ))

        if signals:
            logger.info(f"[{firm['short']}] RSS news: {len(signals)} signal(s)")
        return signals
