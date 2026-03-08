"""
PublicationsScraper — firm thought leadership and external aggregators.

A spike in publications by a firm in a given practice area signals
deliberate investment in that area (hiring experts, chasing mandates).

Sources:
  Firm insights/blog pages   — direct scrape
  Lexology                   — Canada-filtered firm articles
  Mondaq                     — Canada section
  JD Supra                   — Canada tag
  Osgoode Hall (academic)    — cites firms
  SSRN Canada                — law review working papers

v2 improvements:
  - JD Supra direct firm-author search
  - Mondaq direct author search
  - Better title filtering (min quality threshold)
  - Dedup across sources
"""

import re
from urllib.parse import quote_plus, urljoin

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

MIN_TITLE_LEN   = 30
MIN_DEPT_SCORE  = 1.5

LEXOLOGY_URL = (
    "https://www.lexology.com/library/results.aspx"
    "?q={firm}&jurisdiction=Canada&sort=3&pageSize=20"
)
MONDAQ_URL  = "https://www.mondaq.com/search/{firm}?country=canada&sort=mostRecent"
JDSUPRA_URL = "https://www.jdsupra.com/law-news/?search={firm}&country=Canada&sort=date"


class PublicationsScraper(BaseScraper):
    name = "PublicationsScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        seen: set = set()

        # 1 — Firm's own insights page
        signals.extend(self._scrape_firm_insights(firm, seen))

        # 2 — Lexology
        signals.extend(self._scrape_lexology(firm, seen))

        # 3 — Mondaq
        signals.extend(self._scrape_mondaq(firm, seen))

        # 4 — JD Supra
        signals.extend(self._scrape_jdsupra(firm, seen))

        return signals[:20]

    def _scrape_firm_insights(self, firm, seen) -> list[dict]:
        url = firm.get("news_url", "")
        if not url:
            return []
        soup = self.get_soup(url)
        if not soup:
            return []

        signals = []
        for tag in soup.find_all(["a", "h2", "h3", "h4"], limit=80):
            text = tag.get_text(" ", strip=True)
            if len(text) < MIN_TITLE_LEN or len(text) > 300:
                continue

            cls = classifier.top_department(text)
            if not cls or cls["score"] < MIN_DEPT_SCORE:
                continue

            link = ""
            if tag.name == "a":
                href = tag.get("href", "")
                link = href if href.startswith("http") else urljoin(firm["website"], href)
            if link in seen:
                continue

            signals.append(self._make_signal(
                firm_id=firm["id"], firm_name=firm["name"],
                signal_type="publication",
                title=text[:200],
                url=link or url,
                department=cls["department"],
                department_score=cls["score"] * 1.2,
                matched_keywords=cls["matched_keywords"],
            ))
            seen.add(link or text[:80])

        # Dedup by title
        unique, titles = [], set()
        for s in signals:
            t = s["title"][:80]
            if t not in titles:
                titles.add(t)
                unique.append(s)
        return unique[:15]

    def _scrape_lexology(self, firm, seen) -> list[dict]:
        q   = quote_plus(firm["short"])
        url = LEXOLOGY_URL.format(firm=q)
        soup = self.get_soup(url)
        if not soup:
            return []

        signals = []
        for article in soup.find_all(["article", "div"], class_=re.compile(r"article|post|result", re.I))[:15]:
            title_tag = article.find(["h2", "h3", "a"])
            if not title_tag:
                continue
            title = title_tag.get_text(" ", strip=True)
            if len(title) < MIN_TITLE_LEN:
                continue

            author_tag = article.find(["span", "div"], class_=re.compile(r"author|firm", re.I))
            author = author_tag.get_text(" ", strip=True) if author_tag else ""

            # Must be authored by or mention the firm
            full = f"{title} {author}"
            if firm["short"].lower() not in full.lower() and \
               firm["name"].split()[0].lower() not in full.lower():
                continue

            link_tag = article.find("a", href=True)
            link = link_tag["href"] if link_tag else url
            if link in seen:
                continue

            cls = classifier.top_department(title)
            if not cls or cls["score"] < MIN_DEPT_SCORE:
                continue

            signals.append(self._make_signal(
                firm_id=firm["id"], firm_name=firm["name"],
                signal_type="publication",
                title=f"[Lexology] {title[:160]}",
                url=link,
                department=cls["department"],
                department_score=cls["score"] * 1.8,
                matched_keywords=cls["matched_keywords"],
            ))
            seen.add(link)
        return signals

    def _scrape_mondaq(self, firm, seen) -> list[dict]:
        q   = quote_plus(firm["short"])
        url = MONDAQ_URL.format(firm=q)
        soup = self.get_soup(url)
        if not soup:
            return []

        signals = []
        for tag in soup.find_all(["h2", "h3", "article"], limit=30):
            title_tag = tag if tag.name in ("h2", "h3") else tag.find(["h2", "h3"])
            if not title_tag:
                continue
            title = title_tag.get_text(" ", strip=True)
            if len(title) < MIN_TITLE_LEN:
                continue

            full_context = tag.get_text(" ", strip=True)
            if firm["short"].lower() not in full_context.lower():
                continue

            link_tag = tag.find("a", href=True) if tag.name != "a" else tag
            link = ""
            if link_tag:
                href = link_tag.get("href", "")
                link = href if href.startswith("http") else urljoin("https://www.mondaq.com", href)
            if link in seen:
                continue

            cls = classifier.top_department(title)
            if not cls or cls["score"] < MIN_DEPT_SCORE:
                continue

            signals.append(self._make_signal(
                firm_id=firm["id"], firm_name=firm["name"],
                signal_type="publication",
                title=f"[Mondaq] {title[:160]}",
                url=link or url,
                department=cls["department"],
                department_score=cls["score"] * 1.8,
                matched_keywords=cls["matched_keywords"],
            ))
            seen.add(link or title[:80])
        return signals[:8]

    def _scrape_jdsupra(self, firm, seen) -> list[dict]:
        q   = quote_plus(firm["short"])
        url = JDSUPRA_URL.format(firm=q)
        soup = self.get_soup(url)
        if not soup:
            return []

        signals = []
        for tag in soup.find_all(["h2", "h3", "article"], limit=30):
            title_tag = tag if tag.name in ("h2", "h3") else tag.find(["h2", "h3"])
            if not title_tag:
                continue
            title = title_tag.get_text(" ", strip=True)
            if len(title) < MIN_TITLE_LEN:
                continue

            link_tag = tag.find("a", href=True)
            link = ""
            if link_tag:
                href = link_tag.get("href", "")
                link = href if href.startswith("http") else urljoin("https://www.jdsupra.com", href)
            if link in seen:
                continue

            cls = classifier.top_department(title)
            if not cls or cls["score"] < MIN_DEPT_SCORE:
                continue

            signals.append(self._make_signal(
                firm_id=firm["id"], firm_name=firm["name"],
                signal_type="publication",
                title=f"[JD Supra] {title[:160]}",
                url=link or url,
                department=cls["department"],
                department_score=cls["score"] * 1.8,
                matched_keywords=cls["matched_keywords"],
            ))
            seen.add(link or title[:80])
        return signals[:8]
