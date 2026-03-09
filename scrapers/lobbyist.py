"""
LobbyistScraper
Monitors the Office of the Commissioner of Lobbying of Canada
for registrations that name tracked firms as counsel or as lobbyist employer.

Registry search: https://lobbycanada.gc.ca/app/secure/ocl/lrs/do/srchSmpl
Public data is downloadable as XML/JSON via Open Canada.
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

LOBBY_WEIGHT = 3.0

# Open Canada bulk lobbyist registry
LOBBY_API = "https://open.canada.ca/data/en/api/3/action/datastore_search"
LOBBY_RESOURCE_ID = "e213ab31-2e73-48a9-b02d-83ce7d965b14"


class LobbyistScraper(BaseScraper):
    name = "LobbyistScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        firm_names = [firm["short"]] + firm.get("alt_names", [])

        # Query Open Canada lobbyist dataset for firm mentions
        for name in firm_names[:2]:
            try:
                resp = self._get(
                    LOBBY_API,
                    params={
                        "resource_id": LOBBY_RESOURCE_ID,
                        "q": name,
                        "limit": 10,
                    },
                    timeout=15,
                )
                if not resp:
                    continue
                data = resp.json()
                records = data.get("result", {}).get("records", [])
                for rec in records:
                    # Field names vary; try common ones
                    subject = (
                        rec.get("subject_matter", "")
                        or rec.get("SubjectMatter", "")
                        or str(rec)[:200]
                    )
                    registrant = (
                        rec.get("registrant_name", "")
                        or rec.get("RegistrantName", "")
                        or ""
                    )
                    title = f"[Lobbyist] {registrant or name}: {subject[:120]}"
                    dept, score, kw = _clf.top_department(subject)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="court_record",
                        title=title,
                        body=subject[:400],
                        url="https://lobbycanada.gc.ca",
                        department=dept,
                        department_score=score * LOBBY_WEIGHT,
                        matched_keywords=kw + ["lobbying", "regulatory"],
                    ))
                    if len(signals) >= 3:
                        return signals
            except Exception as e:
                self.logger.debug(f"Lobbyist {name}: {e}")

        return signals
