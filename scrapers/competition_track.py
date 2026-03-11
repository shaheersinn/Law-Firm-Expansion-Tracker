"""
CompetitionTrackScraper
=======================
Monitors Competition Bureau of Canada proceedings, merger reviews,
and enforcement actions. Firms acting as competition law counsel on
Bureau matters signal active Competition & Antitrust practice groups.

Sources:
  - Competition Bureau Canada newsroom RSS
    https://www.canada.ca/en/competition-bureau/news.rss
  - Canadian Lawyer RSS (covers competition law extensively)
  - Lexology Canada RSS
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

COMPETITION_FEEDS = [
    {"url": "https://www.canada.ca/en/competition-bureau/news.rss",
                                                    "name": "Competition Bureau"},
    {"url": "https://www.canadianlawyermag.com/rss/",   "name": "Canadian Lawyer"},
    {"url": "https://www.lexology.com/rss/feed/canada.xml", "name": "Lexology CA"},
]

COMPETITION_PHRASES = [
    "competition bureau", "merger review", "consent agreement",
    "abuse of dominance", "price fixing", "cartel", "deceptive marketing",
    "misleading advertising", "commissioner of competition",
    "Competition Act", "merger notification", "advance ruling certificate",
    "ARC", "SIR", "supplementary information request",
    "competition law", "antitrust", "market study",
]

COMPETITION_WEIGHT = 2.5


class CompetitionTrackScraper(BaseScraper):
    name = "CompetitionTrackScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in COMPETITION_FEEDS:
            try:
                feed = feedparser.parse(feed_meta["url"])
                for entry in (feed.entries or [])[:20]:
                    title   = entry.get("title", "")
                    summary = entry.get("summary", entry.get("description", ""))
                    link    = entry.get("link", feed_meta["url"])
                    pub     = entry.get("published", "")
                    full    = f"{title} {summary}"
                    lower   = full.lower()

                    # Competition Bureau feed: always relevant; others need firm check
                    if feed_meta["name"] != "Competition Bureau":
                        if not any(n.lower() in lower for n in firm_names):
                            continue
                    else:
                        # For Bureau feed: check if any tracked firm is counsel
                        if not any(n.lower() in lower for n in firm_names):
                            continue

                    if not any(p.lower() in lower for p in COMPETITION_PHRASES):
                        continue

                    dept, score, kw = _clf.top_department(full)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="court_record",
                        title=f"[{feed_meta['name']}] {title[:160]}",
                        body=summary[:600],
                        url=link,
                        department="Competition & Antitrust",
                        department_score=max(score, 1.5) * COMPETITION_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"CompetitionTrack {feed_meta['url']}: {e}")

        return signals[:10]
