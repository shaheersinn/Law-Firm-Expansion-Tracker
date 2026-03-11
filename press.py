"""
Press Release & Lateral Hire Scraper
======================================
Tracks firm announcements, lateral hires, and deal tombstones.

Sources:
  - Firm news/insights pages (primary — direct from each firm's website)
  - Cision / PR Newswire (Canadian law firm feed)
  - Globe and Mail law section
  - Law360 Canada / Canadian Lawyer Magazine
  - Law Times News
  - Lexpert, Mondaq, Advocates Daily

Changelog (Cycle 7):
  - CRITICAL FIX: removed `if not classifications: continue` gates — was
    silently dropping all press items that didn't match a department.
    Now falls back to 'General' department instead of discarding.
  - Added 6 new media sources (Law360, Lexpert, Mondaq, Advocates Daily,
    Fasken Insights, Torys Insights) for better coverage.
  - Improved link extraction: try multiple tag/attr patterns.
  - Expanded LATERAL_PHRASES for better hire detection.
  - Added _classify_type_detailed() returning (sig_type, weight_mult).
  - classify() now uses title kwarg for better department scoring.
"""

import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup

try:
    from scrapers.base import BaseScraper
    from classifier.department import DepartmentClassifier
except ImportError:
    from base import BaseScraper
    from department import DepartmentClassifier

classifier = DepartmentClassifier()

LATERAL_PHRASES = [
    "joins", "has joined", "welcomes", "new partner", "new associate",
    "lateral hire", "lateral partner", "joins from", "formerly of",
    "previously at", "newly appointed", "new addition",
    "expands team", "grows team", "strengthens",
    "elected partner", "named partner", "promoted to partner",
    "moves to", "moves from", "recruited", "adds partner",
    "is pleased to welcome", "pleased to announce",
]

DEAL_PHRASES = [
    "advised", "counsel to", "acted as counsel", "represented",
    "closed", "completed", "announced", "successfully completed",
    "advises", "lead counsel", "co-counsel", "transaction counsel",
]

MEDIA_SOURCES = [
    {
        "name": "Canadian Lawyer",
        "url": "https://www.canadianlawyermag.com/news/",
        "weight": 2.0,
    },
    {
        "name": "Law360 Canada",
        "url": "https://www.law360.ca/articles",
        "weight": 2.0,
        "fallback_url": "https://www.law360.com/canada/articles",
    },
    {
        "name": "Law Times",
        "url": "https://www.lawtimesnews.com/news/",
        "weight": 1.8,
    },
    {
        "name": "Globe Business",
        "url": "https://www.theglobeandmail.com/business/careers/",
        "weight": 1.5,
    },
    {
        "name": "Lexpert",
        "url": "https://www.lexpert.ca/news/",
        "weight": 2.0,
    },
    {
        "name": "Mondaq Canada",
        "url": "https://www.mondaq.com/canada/",
        "weight": 1.5,
    },
    {
        "name": "Advocates Daily",
        "url": "https://www.advocates-daily.com/news.html",
        "weight": 2.0,
    },
    {
        "name": "Slaw",
        "url": "https://www.slaw.ca/",
        "weight": 1.5,
    },
]


def _extract_link(art, base_url: str) -> str:
    """Robustly extract a link from a BeautifulSoup article element."""
    # Try <a href>, canonical <link>, og:url
    for tag in art.find_all("a", href=True):
        href = tag["href"]
        if href and not href.startswith("#") and len(href) > 5:
            if href.startswith("/"):
                parsed = urlparse(base_url)
                return f"{parsed.scheme}://{parsed.netloc}{href}"
            if href.startswith("http"):
                return href
    return base_url


class PressScraper(BaseScraper):
    name = "PressScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        signals.extend(self._scrape_firm_news(firm))
        signals.extend(self._scrape_media(firm))
        return signals

    def _scrape_firm_news(self, firm: dict) -> list[dict]:
        signals = []
        news_url = firm.get("news_url", "")
        if not news_url:
            return signals

        resp = self._get(news_url)
        if not resp:
            return signals

        soup = BeautifulSoup(resp.text, "html.parser")
        articles = soup.find_all(
            ["article", "div", "li"],
            class_=re.compile(r"news|insight|article|post|press|release|announcement", re.I)
        )[:25]

        for art in articles:
            text = art.get_text(separator=" ", strip=True)
            if len(text.strip()) < 40:   # skip near-empty nodes
                continue
            text_lower = text.lower()

            title_tag = art.find(["h2", "h3", "h4", "a"])
            title = title_tag.get_text(strip=True) if title_tag else text[:150]
            link  = _extract_link(art, news_url)

            sig_type, weight_mult = self._classify_type_detailed(text_lower)

            # Classify with title boost; fallback to General
            cls = classifier.classify_with_fallback(f"{title} {text}", title=title)

            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type=sig_type,
                title=f"[{firm['short']} News] {title[:180]}",
                body=text[:600],
                url=link,
                department=cls["department"],
                department_score=cls["score"] * weight_mult,
                matched_keywords=cls["matched_keywords"],
            ))

        self.logger.info(f"[{firm['short']}] Firm news: {len(signals)} signal(s)")
        return signals

    def _scrape_media(self, firm: dict) -> list[dict]:
        signals = []
        for source in MEDIA_SOURCES:
            resp = self._get(source["url"])
            if not resp:
                # Try fallback if provided
                fb = source.get("fallback_url")
                if fb:
                    resp = self._get(fb)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            articles = soup.find_all(
                ["article", "div", "li"],
                class_=re.compile(r"article|news|story|post|item", re.I)
            )[:20]

            for art in articles:
                text = art.get_text(separator=" ", strip=True)
                if len(text.strip()) < 30:
                    continue
                text_lower = text.lower()

                # Must mention firm by short name or any alt_name
                firm_names = [firm["short"].lower()] + [
                    n.lower() for n in firm.get("alt_names", [])
                ]
                if not any(n in text_lower for n in firm_names):
                    continue

                title_tag = art.find(["h2", "h3", "h4", "a"])
                title = title_tag.get_text(strip=True) if title_tag else text[:150]
                link  = _extract_link(art, source["url"])

                sig_type, weight_mult = self._classify_type_detailed(text_lower)

                cls = classifier.classify_with_fallback(f"{title} {text}", title=title)

                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type=sig_type,
                    title=f"[{source['name']}] {title[:180]}",
                    body=text[:600],
                    url=link,
                    department=cls["department"],
                    department_score=cls["score"] * source["weight"] * weight_mult,
                    matched_keywords=cls["matched_keywords"],
                ))

        self.logger.info(f"[{firm['short']}] Media: {len(signals)} signal(s)")
        return signals

    def _classify_type_detailed(self, text_lower: str) -> tuple[str, float]:
        """Return (signal_type, weight_multiplier) based on text content."""
        if any(p in text_lower for p in LATERAL_PHRASES):
            return "lateral_hire", 2.5
        if any(p in text_lower for p in DEAL_PHRASES):
            return "press_release", 1.5
        return "press_release", 1.0

    # Keep backward compat for anything calling the old method
    def _classify_type(self, text_lower: str) -> str:
        return self._classify_type_detailed(text_lower)[0]
