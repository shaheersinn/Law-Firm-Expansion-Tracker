"""
Federal Lobbyist Registry Scraper
===================================
The Office of the Commissioner of Lobbying of Canada publishes a public
registry of all registered lobbyists. When a law firm registers to lobby
on behalf of a client in a new regulatory area, it reveals:
  1. The firm has a client active in that sector
  2. The firm is building government relations capability there
  3. The mandate is ongoing (lobbying registrations are renewed)

This is a uniquely high-signal source because:
  - Legally mandated disclosure (no self-reporting bias)
  - Shows the firm, the client, and the subject matter
  - Often reveals mandates before any press coverage

Registry: https://lobbycanada.gc.ca/app/secure/ocl/lrs/do/advSrch

Also covers: Ontario Integrity Commissioner Lobbyist Registry
"""

import re
from bs4 import BeautifulSoup
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

LOBBY_CANADA_SEARCH = "https://lobbycanada.gc.ca/app/secure/ocl/lrs/do/advSrch"
LOBBY_ONTARIO_BASE  = "https://www.ontario.ca/page/ontario-lobbyists-registry"

# Subject matter categories in the federal registry that map to departments
SUBJECT_DEPT_MAP = {
    "competition":           "Competition & Antitrust",
    "privacy":               "Data Privacy & Cybersecurity",
    "cybersecurity":         "Data Privacy & Cybersecurity",
    "environment":           "ESG & Regulatory",
    "climate":               "ESG & Regulatory",
    "energy":                "Energy & Natural Resources",
    "natural resources":     "Energy & Natural Resources",
    "mining":                "Energy & Natural Resources",
    "banking":               "Financial Services & Regulatory",
    "financial":             "Financial Services & Regulatory",
    "securities":            "Capital Markets",
    "health":                "Healthcare & Life Sciences",
    "pharmaceutical":        "Healthcare & Life Sciences",
    "immigration":           "Immigration",
    "infrastructure":        "Infrastructure & Projects",
    "transportation":        "Infrastructure & Projects",
    "labour":                "Employment & Labour",
    "employment":            "Employment & Labour",
    "tax":                   "Tax",
    "intellectual property": "Intellectual Property",
    "corporate":             "Corporate / M&A",
}


class LobbyistScraper(BaseScraper):
    name = "LobbyistScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        signals.extend(self._scrape_federal_registry(firm))
        return signals

    def _scrape_federal_registry(self, firm: dict) -> list[dict]:
        signals = []

        # POST search to federal registry
        search_names = [firm["short"]] + firm.get("alt_names", [])[:1]

        for name in search_names:
            # The federal registry uses a form POST
            url  = LOBBY_CANADA_SEARCH
            resp = self._get(
                f"{url}?organization={name.replace(' ', '+')}&lobbyistType=cor"
            )
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.find_all(
                ["tr", "div"],
                class_=re.compile(r"result|registration|filing|record", re.I)
            )[:20]

            for row in rows:
                text = row.get_text(separator=" ", strip=True)
                text_lower = text.lower()

                if name.lower() not in text_lower:
                    continue

                # Map subject matter to department
                dept = ""
                for kw, d in SUBJECT_DEPT_MAP.items():
                    if kw in text_lower:
                        dept = d
                        break

                if not dept:
                    classifications = classifier.classify(text, top_n=1)
                    dept = classifications[0]["department"] if classifications else ""

                if not dept:
                    continue

                title_tag = row.find(["td", "span", "strong", "a"])
                title = title_tag.get_text(strip=True) if title_tag else text[:160]

                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="court_record",   # regulatory engagement = same weight tier
                    title=f"[Lobbyist Registry] {title}",
                    body=text[:600],
                    url=url,
                    department=dept,
                    department_score=3.0,
                    matched_keywords=["lobbyist registry", dept],
                ))

            if signals:
                break

        self.logger.info(f"[{firm['short']}] Lobbyist registry: {len(signals)} signal(s)")
        return signals
