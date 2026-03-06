"""
Press Release & Lateral Hire Scraper
======================================
Lateral hire announcements are the single strongest expansion signal (3.0 weight).
A firm paying to bring in a partner from a competitor = real financial commitment.

What we track:
  1. Firm's own news/press release pages
  2. Canadian Lawyer magazine lateral moves section
  3. Law Times lateral hire announcements
  4. Lexpert news
  5. Google News RSS for "[Firm] joins" / "[Firm] announces"
"""

import re
from bs4 import BeautifulSoup
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

LATERAL_PHRASES = [
    "joins", "has joined", "lateral hire", "welcomes", "announces the addition",
    "new partner", "new counsel", "has been appointed", "hired as",
    "moves to", "joins from", "formerly of", "previously at",
]

PRESS_SOURCES = [
    {
        "name": "Canadian Lawyer",
        "url": "https://www.canadianlawyermag.com/news/general/",
        "rss": "https://www.canadianlawyermag.com/rss.xml",
    },
    {
        "name": "Law Times",
        "url": "https://www.lawtimesnews.com/news/",
        "rss": "https://www.lawtimesnews.com/rss.xml",
    },
    {
        "name": "Lexpert",
        "url": "https://www.lexpert.ca/news/",
    },
]


class PressScraper(BaseScraper):
    name = "PressScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        signals.extend(self._scrape_firm_news(firm))
        signals.extend(self._scrape_trade_press(firm))
        signals.extend(self._scrape_google_news_rss(firm))
        return self._deduplicate(signals)

    # ------------------------------------------------------------------ #
    #  Firm's own news page
    # ------------------------------------------------------------------ #

    def _scrape_firm_news(self, firm: dict) -> list[dict]:
        signals = []
        news_url = firm.get("news_url") or firm["website"].rstrip("/") + "/news"
        response = self._get(news_url)
        if not response:
            return signals

        soup = BeautifulSoup(response.text, "html.parser")
        articles = soup.find_all(
            ["article", "div", "li"],
            class_=re.compile(r"news|post|article|item|insight|publication", re.I)
        )[:25]

        for art in articles:
            text = art.get_text(separator=" ", strip=True)
            text_lower = text.lower()

            signal_type = "press_release"
            is_lateral = any(p in text_lower for p in LATERAL_PHRASES)
            if is_lateral:
                signal_type = "lateral_hire"

            title_tag = art.find(["h2", "h3", "h4", "a"])
            title = title_tag.get_text(strip=True) if title_tag else text[:120]
            if not title:
                continue

            classifications = classifier.classify(text, top_n=1)
            if not classifications:
                continue

            cls = classifications[0]
            boost = 1.5 if is_lateral else 1.0

            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type=signal_type,
                title=f"[Firm News] {title}",
                body=text[:600],
                url=news_url,
                department=cls["department"],
                department_score=cls["score"] * boost,
                matched_keywords=cls["matched_keywords"],
            ))

        self.logger.info(f"[{firm['short']}] Firm news: {len(signals)} signal(s)")
        return signals

    # ------------------------------------------------------------------ #
    #  Trade press sources
    # ------------------------------------------------------------------ #

    def _scrape_trade_press(self, firm: dict) -> list[dict]:
        signals = []
        for source in PRESS_SOURCES:
            url = source.get("rss") or source["url"]
            response = self._get(url)
            if not response:
                continue

            # Try RSS first
            is_rss = "rss" in url or "xml" in response.headers.get("content-type", "")
            if is_rss:
                items = self._parse_rss_items(response.text)
            else:
                items = self._parse_html_articles(response.text, url)

            for item_title, item_body, item_url, item_date in items:
                # Skip articles older than SIGNAL_LOOKBACK_DAYS (stops 2019+ content)
                if not self.is_recent(item_date):
                    continue
                combined = f"{item_title} {item_body}".lower()
                if (firm["short"].lower() not in combined
                        and firm["name"].split()[0].lower() not in combined):
                    continue

                is_lateral = any(p in combined for p in LATERAL_PHRASES)
                signal_type = "lateral_hire" if is_lateral else "press_release"
                boost = 2.0 if is_lateral else 1.0

                full_text = f"{item_title} {item_body}"
                classifications = classifier.classify(full_text, top_n=1)
                if not classifications:
                    continue

                cls = classifications[0]
                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type=signal_type,
                    title=f"[{source['name']}] {item_title}",
                    body=item_body[:600],
                    url=item_url or source["url"],
                    department=cls["department"],
                    department_score=cls["score"] * boost,
                    matched_keywords=cls["matched_keywords"],
                ))

        self.logger.info(f"[{firm['short']}] Trade press: {len(signals)} signal(s)")
        return signals

    # ------------------------------------------------------------------ #
    #  Google News RSS
    # ------------------------------------------------------------------ #

    def _scrape_google_news_rss(self, firm: dict) -> list[dict]:
        signals = []
        query = f'"{firm["short"]}" joins lawyer'
        encoded = query.replace(" ", "+").replace('"', "%22")
        url = f"https://news.google.com/rss/search?q={encoded}&hl=en-CA&gl=CA&ceid=CA:en"

        response = self._get(url)
        if not response:
            return signals

        items = self._parse_rss_items(response.text)
        for item_title, item_body, item_url, item_date in items[:15]:
            # Skip stale articles — Google News RSS sometimes returns multi-year-old items
            if not self.is_recent(item_date):
                continue
            combined = f"{item_title} {item_body}".lower()
            if (firm["short"].lower() not in combined
                    and firm["name"].split()[0].lower() not in combined):
                continue

            is_lateral = any(p in combined for p in LATERAL_PHRASES)
            signal_type = "lateral_hire" if is_lateral else "press_release"
            boost = 2.0 if is_lateral else 1.0

            full_text = f"{item_title} {item_body}"
            classifications = classifier.classify(full_text, top_n=1)
            if not classifications:
                continue

            cls = classifications[0]
            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type=signal_type,
                title=f"[Google News] {item_title}",
                body=item_body[:600],
                url=item_url,
                department=cls["department"],
                department_score=cls["score"] * boost,
                matched_keywords=cls["matched_keywords"],
            ))

        self.logger.info(f"[{firm['short']}] Google News: {len(signals)} signal(s)")
        return signals

    # ------------------------------------------------------------------ #
    #  Parsing helpers
    # ------------------------------------------------------------------ #

    def _parse_rss_items(self, xml_text: str) -> list[tuple[str, str, str, str]]:
        """Returns (title, body, url, pub_date) tuples."""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(xml_text, features="xml")
        if not soup.find("item"):
            soup = BeautifulSoup(xml_text, "html.parser")
        results = []
        for item in soup.find_all("item")[:30]:
            title   = item.find("title")
            desc    = item.find("description")
            link    = item.find("link")
            pubdate = item.find("pubDate") or item.find("published") or item.find("updated")
            t = title.get_text(strip=True)   if title   else ""
            b = BeautifulSoup(desc.get_text(strip=True) if desc else "", "html.parser").get_text()
            u = link.get_text(strip=True)    if link    else ""
            d = pubdate.get_text(strip=True) if pubdate else ""
            if t:
                results.append((t, b, u, d))
        return results

    def _parse_html_articles(self, html: str, base_url: str) -> list[tuple[str, str, str]]:
        soup = BeautifulSoup(html, "html.parser")
        results = []
        for tag in soup.find_all(
            ["article", "div", "li"],
            class_=re.compile(r"news|article|post|item", re.I)
        )[:20]:
            text = tag.get_text(separator=" ", strip=True)
            title_tag = tag.find(["h2", "h3", "a"])
            title = title_tag.get_text(strip=True) if title_tag else text[:80]
            if title:
                results.append((title, text, base_url))
        return results

    def _deduplicate(self, signals: list[dict]) -> list[dict]:
        seen = set()
        result = []
        for s in signals:
            key = s["title"].lower()[:80]
            if key not in seen:
                seen.add(key)
                result.append(s)
        return result
