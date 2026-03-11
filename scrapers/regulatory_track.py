"""
RegulatoryTrackScraper
======================
Tracks broader Canadian regulatory enforcement activities involving
tracked firms: FINTRAC, OSFI, CRTC, and federal/provincial energy
regulators. High regulatory activity = high financial services,
energy, or telecom practice group signal.

Sources:
  - OSFI newsroom RSS   https://www.osfi-bsif.gc.ca/en/news/rss
  - CRTC news RSS       https://www.crtc.gc.ca/eng/news/rss.xml
  - Canada Gazette RSS  https://gazette.gc.ca/rss/p1-eng.xml
  - Canadian Lawyer RSS
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

REGULATORY_FEEDS = [
    {"url": "https://www.osfi-bsif.gc.ca/en/news/rss",         "name": "OSFI"},
    {"url": "https://www.crtc.gc.ca/eng/news/rss.xml",         "name": "CRTC"},
    {"url": "https://gazette.gc.ca/rss/p1-eng.xml",            "name": "Canada Gazette"},
    {"url": "https://www.canadianlawyermag.com/rss/",          "name": "Canadian Lawyer"},
]

REGULATORY_PHRASES = [
    "regulatory", "enforcement", "compliance", "OSFI", "CRTC", "FINTRAC",
    "AML", "anti-money laundering", "sanctions", "penalties",
    "administrative monetary penalty", "notice of violation",
    "consent order", "regulatory counsel", "regulatory proceeding",
    "financial institution", "bank regulation", "insurance regulation",
    "NEB", "CER", "energy regulation", "environmental assessment",
    "environmental review",
]

REGULATORY_WEIGHT = 2.0


class RegulatoryTrackScraper(BaseScraper):
    name = "RegulatoryTrackScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        try:
            import feedparser
        except ImportError:
            return signals

        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for feed_meta in REGULATORY_FEEDS:
            try:
                feed = feedparser.parse(feed_meta["url"])
                for entry in (feed.entries or [])[:20]:
                    title   = entry.get("title", "")
                    summary = entry.get("summary", entry.get("description", ""))
                    link    = entry.get("link", feed_meta["url"])
                    pub     = entry.get("published", "")
                    full    = f"{title} {summary}"
                    lower   = full.lower()

                    if not any(n.lower() in lower for n in firm_names):
                        continue
                    if not any(p.lower() in lower for p in REGULATORY_PHRASES):
                        continue

                    dept, score, kw = _clf.top_department(full)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="court_record",
                        title=f"[{feed_meta['name']}] {title[:160]}",
                        body=summary[:600],
                        url=link,
                        department=dept or "Financial Services & Regulatory",
                        department_score=score * REGULATORY_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"RegulatoryTrack {feed_meta['url']}: {e}")

        return signals[:12]
