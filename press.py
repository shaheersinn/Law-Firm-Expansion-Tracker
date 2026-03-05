"""
Press Release & Lateral Hire Scraper
======================================
Tracks firm announcements, lateral hires, and deal tombstones.

Sources:
  - Firm news/insights pages
  - Cision / PR Newswire (Canadian law firm feed)
  - Globe and Mail law section
  - The Lawyer Daily
  - Canadian Lawyer Magazine
  - Law Times News
"""

import re
from bs4 import BeautifulSoup
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

LATERAL_PHRASES = [
    "joins", "has joined", "welcomes", "new partner", "new associate",
    "lateral hire", "lateral partner", "joins from", "formerly of",
    "previously at", "newly appointed", "new addition",
    "expands team", "grows team", "strengthens",
]

DEAL_PHRASES = [
    "advised", "counsel to", "acted as counsel", "represented",
    "closed", "completed", "announced", "successfully completed",
]

MEDIA_SOURCES = [
    {
        "name": "Canadian Lawyer",
        "url": "https://www.canadianlawyermag.com/news/",
        "weight": 2.0,
    },
    {
        "name": "The Lawyer Daily",
        "url": "https://www.thelawyersdaily.ca/articles",
        "weight": 2.0,
    },
    {
        "name": "Law Times",
        "url": "https://www.lawtimesnews.com/news/",
        "weight": 1.5,
    },
    {
        "name": "Globe Business",
        "url": "https://www.theglobeandmail.com/business/careers/",
        "weight": 1.5,
    },
]


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
            text_lower = text.lower()

            title_tag = art.find(["h2", "h3", "h4", "a"])
            title = title_tag.get_text(strip=True) if title_tag else text[:150]

            link_tag = art.find("a", href=True)
            link = link_tag["href"] if link_tag else news_url
            if link.startswith("/"):
                from urllib.parse import urlparse
                base = urlparse(news_url)
                link = f"{base.scheme}://{base.netloc}{link}"

            sig_type = self._classify_type(text_lower)

            classifications = classifier.classify(f"{title} {text}", top_n=1)
            if not classifications:
                continue

            cls = classifications[0]
            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type=sig_type,
                title=f"[{firm['short']} News] {title}",
                body=text[:600],
                url=link,
                department=cls["department"],
                department_score=cls["score"],
                matched_keywords=cls["matched_keywords"],
            ))

        self.logger.info(f"[{firm['short']}] Firm news: {len(signals)} signal(s)")
        return signals

    def _scrape_media(self, firm: dict) -> list[dict]:
        signals = []
        for source in MEDIA_SOURCES:
            resp = self._get(source["url"])
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            articles = soup.find_all(
                ["article", "div", "li"],
                class_=re.compile(r"article|news|story|post|item", re.I)
            )[:20]

            for art in articles:
                text = art.get_text(separator=" ", strip=True)
                text_lower = text.lower()

                if firm["short"].lower() not in text_lower and \
                   not any(n.lower() in text_lower for n in firm.get("alt_names", [])):
                    continue

                title_tag = art.find(["h2", "h3", "h4", "a"])
                title = title_tag.get_text(strip=True) if title_tag else text[:150]

                sig_type = self._classify_type(text_lower)
                # Lateral hires in media = very high signal
                score_mult = 2.5 if sig_type == "lateral_hire" else 1.0

                classifications = classifier.classify(f"{title} {text}", top_n=1)
                if not classifications:
                    continue

                cls = classifications[0]
                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type=sig_type,
                    title=f"[{source['name']}] {title}",
                    body=text[:600],
                    url=source["url"],
                    department=cls["department"],
                    department_score=cls["score"] * source["weight"] * score_mult,
                    matched_keywords=cls["matched_keywords"],
                ))

        self.logger.info(f"[{firm['short']}] Media: {len(signals)} signal(s)")
        return signals

    def _classify_type(self, text_lower: str) -> str:
        if any(p in text_lower for p in LATERAL_PHRASES):
            return "lateral_hire"
        return "press_release"
