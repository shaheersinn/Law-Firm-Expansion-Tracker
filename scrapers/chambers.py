"""
ChambersScraper — scrapes Chambers Canada and Legal 500 for firm rankings.
New or improved rankings = high-confidence expansion signal (weight 3.0).
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

CHAMBERS_URL = "https://chambers.com/law-firm/{slug}/canada/6"
LEGAL500_URL  = "https://www.legal500.com/c/canada/"


class ChambersScraper(BaseScraper):
    name = "ChambersScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        signals.extend(self._scrape_chambers(firm))
        signals.extend(self._scrape_legal500(firm))
        return signals

    def _scrape_chambers(self, firm: dict) -> list[dict]:
        slug = firm.get("linkedin_slug", firm["id"].replace("_", "-"))
        url  = CHAMBERS_URL.format(slug=slug)
        soup = self.get_soup(url)
        if not soup:
            return []

        signals = []
        # Look for band/ranking mentions
        for tag in soup.find_all(["div", "p", "span"], limit=100):
            text = tag.get_text(" ", strip=True)
            if not any(w in text.lower() for w in ["band", "ranked", "leading", "recommended", "notable"]):
                continue
            if len(text) < 20 or len(text) > 400:
                continue

            cls = classifier.top_department(text)
            dept = cls["department"] if cls else "Corporate/M&A"
            score = (cls["score"] if cls else 1.0) * 3.0

            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="ranking",
                title=f"[Chambers] {text[:160]}",
                url=url,
                department=dept,
                department_score=score,
                matched_keywords=cls["matched_keywords"] if cls else [],
            ))

        return signals[:5]

    def _scrape_legal500(self, firm: dict) -> list[dict]:
        soup = self.get_soup(LEGAL500_URL)
        if not soup:
            return []

        firm_names = [firm["short"].lower(), firm["name"].lower()[:20]]
        signals = []

        for tag in soup.find_all(["td", "li", "p"], limit=200):
            text = tag.get_text(" ", strip=True)
            lower = text.lower()
            if not any(n in lower for n in firm_names):
                continue
            if len(text) < 20:
                continue

            cls = classifier.top_department(text)
            dept = cls["department"] if cls else "Corporate/M&A"

            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="ranking",
                title=f"[Legal 500] {text[:160]}",
                url=LEGAL500_URL,
                department=dept,
                department_score=(cls["score"] if cls else 1.0) * 3.0,
                matched_keywords=cls["matched_keywords"] if cls else [],
            ))

        return signals[:5]
