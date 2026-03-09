"""
GovTrackScraper
Monitors federal and provincial regulatory sources for firm mentions,
law changes that will drive new mandates, and government legal contracts.

Sources:
  - Canada Gazette RSS
  - Competition Bureau news RSS
  - Privacy Commissioner of Canada RSS
  - OSC (Ontario Securities Commission) news
  - OSFI news
  - Federal buyandsell.gc.ca (government legal contracts)
  - IRCC regulatory news
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

GOV_WEIGHT = 2.5

GOV_FEEDS = [
    {
        "name": "Canada Gazette",
        "url":  "https://gazette.gc.ca/rss/p1-eng.xml",
        "dept_hint": None,
    },
    {
        "name": "Competition Bureau",
        "url":  "https://www.canada.ca/en/competition-bureau.atom.xml",
        "dept_hint": "Competition",
    },
    {
        "name": "Privacy Commissioner",
        "url":  "https://www.priv.gc.ca/en/opc-news/news-and-announcements/feed/",
        "dept_hint": "Data Privacy",
    },
    {
        "name": "OSFI",
        "url":  "https://www.osfi-bsif.gc.ca/Eng/osfi-bsif/med/Pages/list.aspx?Category=All",
        "dept_hint": "Financial Services",
    },
    {
        "name": "Federal Contracts",
        "url":  "https://buyandsell.gc.ca/procurement-data/award-notice/rss",
        "dept_hint": None,
    },
]

RELEVANT_TERMS = [
    "counsel", "legal services", "law firm", "legal advice",
    "regulatory", "compliance", "enforcement", "investigation",
    "amendment", "regulation", "act", "bill",
]


class GovTrackScraper(BaseScraper):
    name = "GovTrackScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        try:
            import feedparser
        except ImportError:
            return signals

        for src in GOV_FEEDS:
            try:
                feed = feedparser.parse(src["url"])
                for entry in (feed.entries or [])[:20]:
                    title   = entry.get("title", "")
                    summary = entry.get("summary", entry.get("description", ""))
                    link    = entry.get("link", src["url"])
                    pub     = entry.get("published", "")
                    full    = f"{title} {summary}"
                    lower   = full.lower()

                    # For firm-specific sources like contracts, filter by name
                    if src["name"] == "Federal Contracts":
                        if not any(n.lower() in lower for n in firm_names):
                            continue

                    if not any(t in lower for t in RELEVANT_TERMS):
                        continue

                    dept_hint = src.get("dept_hint")
                    if dept_hint:
                        dept, score, kw = dept_hint, 2.0, [dept_hint.lower()]
                    else:
                        dept, score, kw = _clf.top_department(full)

                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="court_record",
                        title=f"[{src['name']}] {title[:160]}",
                        body=summary[:500],
                        url=link,
                        department=dept,
                        department_score=score * GOV_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
                    if len(signals) >= 6:
                        return signals
            except Exception as e:
                self.logger.debug(f"GovTrack {src['name']}: {e}")

        return signals
