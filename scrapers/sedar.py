"""
SEDAR+ Regulatory Filings Scraper
===================================
SEDAR+ (System for Electronic Document Analysis and Retrieval) is Canada's
mandatory securities filing platform. When a law firm appears as "counsel to
the issuer/underwriter" in a prospectus, AIF, or material change report,
it's direct evidence of capital markets mandates.

This is uniquely high-confidence: SEDAR filings are legally mandated,
third-party verified, and publicly searchable. No press release needed.

What we track:
  - Firm appearing as counsel in prospectus filings (IPOs, bought deals)
  - Firm appearing as counsel in takeover bid circulars
  - Frequency of appearances → capital markets deal flow signal

API: SEDAR+ has a public search endpoint.
URL: https://www.sedarplus.ca/csa-party/party/search.html
"""

import re
import time
from bs4 import BeautifulSoup
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

SEDAR_SEARCH = "https://www.sedarplus.ca/csa-party/party/search.html"

# Filing types that indicate counsel involvement
HIGH_VALUE_FILING_TYPES = [
    "prospectus", "short form prospectus", "preliminary prospectus",
    "annual information form", "take-over bid", "circular",
    "management information circular", "material change",
    "business acquisition report",
]

COUNSEL_INDICATORS = [
    "legal counsel", "counsel to", "counsel for", "legal advisors",
    "solicitors", "acted as counsel", "advised by",
]


class SedarScraper(BaseScraper):
    name = "SedarScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        signals.extend(self._search_sedar(firm))
        return signals

    def _search_sedar(self, firm: dict) -> list[dict]:
        signals = []

        # SEDAR+ public text search
        search_terms = [firm["short"], firm["name"].split()[0]]

        for term in search_terms[:1]:
            url = f"{SEDAR_SEARCH}?search={term.replace(' ', '+')}&category=allfilings"
            resp = self._get(url)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            filing_rows = soup.find_all(
                ["tr", "div", "li"],
                class_=re.compile(r"filing|result|document|row", re.I)
            )[:20]

            for row in filing_rows:
                text = row.get_text(separator=" ", strip=True)
                text_lower = text.lower()

                # Confirm firm is mentioned as counsel
                firm_mentioned = (
                    firm["short"].lower() in text_lower or
                    firm["name"].split()[0].lower() in text_lower
                )
                if not firm_mentioned:
                    continue

                counsel_found = any(p in text_lower for p in COUNSEL_INDICATORS)
                if not counsel_found:
                    continue

                # Determine filing type and score
                filing_type = "securities_filing"
                base_score  = 3.0
                for ft in HIGH_VALUE_FILING_TYPES:
                    if ft in text_lower:
                        filing_type = ft.replace(" ", "_")
                        base_score  = 5.0 if "prospectus" in ft or "take-over" in ft else 3.5
                        break

                title_tag = row.find(["h3", "h4", "strong", "a"])
                title = title_tag.get_text(strip=True) if title_tag else text[:150]

                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="court_record",   # reuse weight class
                    title=f"[SEDAR+] {title}",
                    body=text[:600],
                    url=url,
                    department="Capital Markets",
                    department_score=base_score,
                    matched_keywords=["SEDAR", filing_type, term],
                ))

            time.sleep(1.5)
            if signals:
                break

        self.logger.info(f"[{firm['short']}] SEDAR+: {len(signals)} signal(s)")
        return signals
