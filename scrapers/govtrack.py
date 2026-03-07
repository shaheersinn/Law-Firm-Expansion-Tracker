"""
GovTrackScraper — monitors Canada Gazette, Competition Bureau, CRTC, OSC,
OSFI, Privacy Commissioner, IRCC for regulatory filings and decisions
that name tracked firms as counsel or respondent.

Signals: court_record (weight 2.5–3.0)
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

GOV_SOURCES = [
    {
        "name": "Canada Gazette",
        "url": "https://gazette.gc.ca/rss/p1-eng.xml",
        "is_rss": True,
        "weight": 2.5,
        "dept_hint": "Competition",
    },
    {
        "name": "Competition Bureau",
        "url": "https://www.canada.ca/en/competition-bureau/news.rss",
        "is_rss": True,
        "weight": 3.0,
        "dept_hint": "Competition",
    },
    {
        "name": "OSC News",
        "url": "https://www.osc.ca/en/news-events/news/rss",
        "is_rss": True,
        "weight": 2.5,
        "dept_hint": "Capital Markets",
    },
    {
        "name": "OSFI",
        "url": "https://www.osfi-bsif.gc.ca/en/news/rss",
        "is_rss": True,
        "weight": 2.5,
        "dept_hint": "Financial Services",
    },
    {
        "name": "Privacy Commissioner",
        "url": "https://www.priv.gc.ca/en/opc-news/news-and-announcements/rss/",
        "is_rss": True,
        "weight": 3.0,
        "dept_hint": "Data Privacy",
    },
    {
        "name": "CRTC Decisions",
        "url": "https://crtc.gc.ca/eng/publications/reports/rss.xml",
        "is_rss": True,
        "weight": 2.5,
        "dept_hint": "Financial Services",
    },
    {
        "name": "IRCC News",
        "url": "https://www.canada.ca/en/immigration-refugees-citizenship/news.rss",
        "is_rss": True,
        "weight": 2.0,
        "dept_hint": "Immigration",
    },
]

try:
    import feedparser
    HAS_FEEDPARSER = True
except ImportError:
    HAS_FEEDPARSER = False


class GovTrackScraper(BaseScraper):
    name = "GovTrackScraper"

    def fetch(self, firm: dict) -> list[dict]:
        if not HAS_FEEDPARSER:
            return self._scrape_html_fallback(firm)

        signals = []
        firm_names = [firm["short"].lower()] + [n.lower() for n in firm.get("alt_names", [])]

        for source in GOV_SOURCES:
            try:
                import feedparser as fp
                feed = fp.parse(source["url"])
            except Exception:
                continue

            for entry in (feed.entries or [])[:20]:
                title   = entry.get("title",   "")
                summary = entry.get("summary", "")
                link    = entry.get("link",    source["url"])
                pub     = entry.get("published", "")

                if not self.is_recent(pub):
                    continue

                full  = f"{title} {summary}".lower()
                if not any(n in full for n in firm_names):
                    continue

                cls = classifier.classify(f"{title} {summary} {source['dept_hint']}", top_n=1)
                dept  = cls[0]["department"] if cls else source["dept_hint"]
                score = (cls[0]["score"] if cls else 1.0) * source["weight"]

                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="court_record",
                    title=f"[{source['name']}] {title[:160]}",
                    body=summary[:400],
                    url=link,
                    department=dept,
                    department_score=score,
                    matched_keywords=cls[0]["matched_keywords"] if cls else [],
                ))

        return signals

    def _scrape_html_fallback(self, firm: dict) -> list[dict]:
        """Fallback HTML scraping when feedparser unavailable."""
        return []
