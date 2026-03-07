"""
PressScraper — scrapes firm news pages and legal press for announcements.
Detects lateral hires with high precision using phrase matching.
"""

import re
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

LATERAL_SIGNALS = [
    r"joins\s+(?:the\s+firm|as\s+partner|as\s+counsel)",
    r"(?:has\s+)?joined\s+(?:the\s+firm|as)",
    r"welcomes\s+(?:new\s+)?(?:partner|counsel|associate)",
    r"new\s+partner\s+(?:at|joins)",
    r"lateral\s+hire",
    r"expands\s+(?:its\s+)?(?:team|practice|group)",
    r"appointed\s+(?:as\s+)?(?:partner|counsel)",
    r"named\s+(?:as\s+)?(?:partner|head|chair)",
    r"recruited?\s+(?:to|as)",
]
_LATERAL_RE = [re.compile(p, re.IGNORECASE) for p in LATERAL_SIGNALS]

EXTERNAL_SOURCES = [
    {"name": "Canadian Lawyer",    "url": "https://www.canadianlawyermag.com/news/",   "weight": 2.0},
    {"name": "Law Times",          "url": "https://www.lawtimesnews.com/news/",         "weight": 1.8},
    {"name": "The Lawyer's Daily", "url": "https://www.thelawyersdaily.ca/articles/",   "weight": 2.0},
]


class PressScraper(BaseScraper):
    name = "PressScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        signals.extend(self._scrape_firm_news(firm))
        signals.extend(self._scrape_external(firm))
        return signals

    def _scrape_firm_news(self, firm: dict) -> list[dict]:
        url = firm.get("news_url", "")
        if not url:
            return []

        soup = self.get_soup(url)
        if not soup:
            return []

        signals = []
        for tag in soup.find_all(["article", "div"], class_=re.compile(r"news|press|article|post", re.I))[:30]:
            title_tag = tag.find(["h2", "h3", "h4", "a"])
            if not title_tag:
                continue
            title = title_tag.get_text(" ", strip=True)
            if len(title) < 20:
                continue

            body_tag = tag.find("p")
            body = body_tag.get_text(" ", strip=True) if body_tag else ""
            full = f"{title} {body}"

            link_tag = tag.find("a", href=True)
            link = ""
            if link_tag:
                href = link_tag["href"]
                link = href if href.startswith("http") else firm["website"] + href

            is_lateral = any(r.search(full) for r in _LATERAL_RE)
            sig_type = "lateral_hire" if is_lateral else "press_release"
            weight_mult = 3.0 if is_lateral else 1.5

            cls = classifier.top_department(full)
            if not cls:
                continue

            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type=sig_type,
                title=title[:200],
                body=body[:600],
                url=link or url,
                department=cls["department"],
                department_score=cls["score"] * weight_mult,
                matched_keywords=cls["matched_keywords"],
            ))

        return signals[:15]

    def _scrape_external(self, firm: dict) -> list[dict]:
        signals = []
        name_variants = [firm["short"], firm["name"].split()[0]]

        for source in EXTERNAL_SOURCES:
            soup = self.get_soup(source["url"])
            if not soup:
                continue

            for tag in soup.find_all(["article", "h2", "h3"], limit=40):
                text = tag.get_text(" ", strip=True)
                if not any(n.lower() in text.lower() for n in name_variants):
                    continue
                if len(text) < 30:
                    continue

                link_tag = tag.find("a", href=True) if tag.name != "a" else tag
                link = ""
                if link_tag:
                    href = link_tag.get("href", "")
                    link = href if href.startswith("http") else "https://www.canadianlawyermag.com" + href

                is_lateral = any(r.search(text) for r in _LATERAL_RE)
                sig_type = "lateral_hire" if is_lateral else "press_release"
                weight_mult = 3.0 if is_lateral else 1.5

                cls = classifier.top_department(text)
                if not cls:
                    continue

                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type=sig_type,
                    title=f"[{source['name']}] {text[:160]}",
                    url=link or source["url"],
                    department=cls["department"],
                    department_score=cls["score"] * source["weight"] * weight_mult,
                    matched_keywords=cls["matched_keywords"],
                ))

        return signals[:10]
