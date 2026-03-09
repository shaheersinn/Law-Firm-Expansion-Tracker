"""
RecruiterScraper
Monitors legal recruitment firms for Canadian law firm placements.
Legal recruiters often know about lateral moves before the firms announce them.

Sources:
  - ZSA Legal Recruitment (zsa.ca)
  - Caldwell Partners Canada
  - BCG Attorney Search Canada
  - IQ Partners Legal
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

RECRUITER_WEIGHT = 2.5

RECRUITER_SOURCES = [
    {
        "name": "ZSA Legal",
        "url": "https://www.zsa.ca/placements/",
        "alt_url": "https://www.zsa.ca/market-intelligence/",
    },
    {
        "name": "Caldwell",
        "url": "https://www.caldwellpartners.com/news/",
        "alt_url": None,
    },
]

PLACEMENT_KEYWORDS = [
    "placed", "placement", "joins", "appointed", "partner",
    "associate", "counsel", "lateral", "move", "hire",
]


class RecruiterScraper(BaseScraper):
    name = "RecruiterScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        firm_names = [firm["short"]] + firm.get("alt_names", [])

        for src in RECRUITER_SOURCES:
            for url in filter(None, [src["url"], src.get("alt_url")]):
                soup = self._soup(url, timeout=15)
                if not soup:
                    continue
                # Gather text nodes that mention the firm
                for tag in soup.find_all(["p", "li", "h2", "h3", "h4"])[:80]:
                    text = self._clean(tag.get_text())
                    lower = text.lower()
                    if not any(n.lower() in lower for n in firm_names):
                        continue
                    if not any(k in lower for k in PLACEMENT_KEYWORDS):
                        continue
                    if len(text) < 20:
                        continue

                    # Find nearest link
                    link = url
                    parent = tag.find_parent("a") or tag.find("a")
                    if parent and parent.get("href"):
                        href = parent["href"]
                        if href.startswith("http"):
                            link = href
                        else:
                            from urllib.parse import urljoin
                            link = urljoin(url, href)

                    dept, score, kw = _clf.top_department(text)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="lateral_hire",
                        title=f"[{src['name']}] {text[:160]}",
                        body=text[:500],
                        url=link,
                        department=dept,
                        department_score=score * RECRUITER_WEIGHT,
                        matched_keywords=kw,
                    ))
                break  # first working URL per recruiter is enough

        return signals[:8]
