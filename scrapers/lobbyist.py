"""
LobbyistScraper — queries the Federal Lobbyist Registry for firms acting
as lobbyist or legal consultant. Lobbying registrations are strong
signals of regulatory practice expansion.
Weight: 3.0
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

LOBBYIST_SEARCH_URL = (
    "https://lobbycanada.gc.ca/app/secure/ocl/lrs/do/guest?"
    "lang=eng&fn={query}"
)

LOBBYIST_API_URL = (
    "https://lobbycanada.gc.ca/app/secure/ocl/lrs/do/srchSmry?"
    "srchType=1&lang=eng&txnType=R&dtRange=L12M&keyword={query}"
)


class LobbyistScraper(BaseScraper):
    name = "LobbyistScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []

        for name_variant in [firm["short"], firm["name"][:30]]:
            url  = LOBBYIST_SEARCH_URL.format(query=name_variant.replace(" ", "+"))
            soup = self.get_soup(url)
            if not soup:
                continue

            firm_names = [firm["short"].lower(), firm["name"].lower()[:20]]

            for tag in soup.find_all(["tr", "li", "div"], limit=100):
                text = tag.get_text(" ", strip=True)
                lower = text.lower()

                if not any(n in lower for n in firm_names):
                    continue
                if len(text) < 25 or len(text) > 500:
                    continue

                link_tag = tag.find("a", href=True)
                link = ""
                if link_tag:
                    href = link_tag["href"]
                    link = href if href.startswith("http") else "https://lobbycanada.gc.ca" + href

                cls = classifier.top_department(text)
                dept = cls["department"] if cls else "Competition"

                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="court_record",
                    title=f"[Lobbyist Registry] {text[:160]}",
                    url=link or url,
                    department=dept,
                    department_score=(cls["score"] if cls else 1.0) * 3.0,
                    matched_keywords=cls["matched_keywords"] if cls else [],
                ))

        return signals[:6]
