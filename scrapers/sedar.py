"""
SedarScraper — searches SEDAR+ for securities filings that mention tracked firms.
High-value signals: M&A filings, prospectuses, material change reports.
Weight: 2.5–5.0 depending on filing type.
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

SEDAR_SEARCH_URL = "https://efts.sedar.com/regsvcweb/services/filing/searchFilings"

# Filing types and their weights
FILING_WEIGHTS = {
    "material change report": 5.0,
    "prospectus":             4.5,
    "merger":                 4.5,
    "acquisition":            4.0,
    "management proxy":       3.0,
    "annual information form":2.5,
    "rights offering":        3.5,
}


class SedarScraper(BaseScraper):
    name = "SedarScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        # SEDAR+ public search endpoint
        for name_variant in [firm["short"], firm["name"][:30]]:
            results = self._search_sedar(name_variant, firm)
            signals.extend(results)

        # Deduplicate
        seen = set()
        unique = []
        for s in signals:
            if s["title"] not in seen:
                seen.add(s["title"])
                unique.append(s)
        return unique[:8]

    def _search_sedar(self, query: str, firm: dict) -> list[dict]:
        # SEDAR+ full-text search via their public API
        url = (
            f"https://efts.sedar.com/regsvcweb/services/filing/searchFilings"
            f"?searchText={query.replace(' ', '+')}&dateFrom=&dateTo=&lang=EN"
        )
        resp = self.get(url, headers={"Accept": "application/json"})
        if not resp:
            return []

        try:
            data = resp.json()
        except Exception:
            return []

        filings = data.get("filings", data.get("results", []))
        signals = []

        for filing in filings[:10]:
            title   = filing.get("description", filing.get("title", ""))
            issuer  = filing.get("issuerName", "")
            date    = filing.get("receivedDate", filing.get("date", ""))
            doc_url = filing.get("url", "https://www.sedar.com")

            if not self.is_recent(date):
                continue

            full = f"{title} {issuer} {firm['short']}"
            filing_type_lower = title.lower()

            # Pick weight based on filing type
            weight = 2.5
            for ftype, w in FILING_WEIGHTS.items():
                if ftype in filing_type_lower:
                    weight = w
                    break

            cls = classifier.top_department(full)
            dept = cls["department"] if cls else "Capital Markets"

            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="court_record",
                title=f"[SEDAR+] {issuer[:60]} — {title[:100]}",
                body=f"Filing date: {date}",
                url=doc_url,
                department=dept,
                department_score=(cls["score"] if cls else 1.0) * weight,
                matched_keywords=cls["matched_keywords"] if cls else [],
            ))

        return signals
