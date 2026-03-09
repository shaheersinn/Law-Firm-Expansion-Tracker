"""
ConferenceScraper
Monitors legal conference agendas for firm speaking engagements
and sponsorships.

Signal research insight:
  "When a firm has three or four lawyers speaking at the same conference,
   that practice group is almost certainly growing."
  "Conference speaking = market considers them a leader = client work flowing."

Sources:
  - OBA (Ontario Bar Association) PD programs
  - LSO CPD programs
  - Osgoode Professional Development
  - Federated Press / Insight conferences
  - PDAC (mining law — Calgary/Vancouver)
  - CAPL (energy law)
  - IAPP Canada (privacy)
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

CONF_WEIGHT = 2.0

CONFERENCE_SOURCES = [
    {
        "name": "OBA Institute",
        "url": "https://www.oba.org/Professional-Development",
        "rss": None,
        "dept_hint": None,
    },
    {
        "name": "Osgoode PD",
        "url": "https://www.osgoode.yorku.ca/programs/professional-development/",
        "rss": None,
        "dept_hint": None,
    },
    {
        "name": "IAPP Canada",
        "url": "https://iapp.org/conference/",
        "rss": None,
        "dept_hint": "Data Privacy",
    },
]

SPEAKING_KEYWORDS = [
    "speaker", "speakers", "panelist", "keynote", "moderator",
    "chair", "presents", "featured", "agenda", "faculty",
]

SPONSORSHIP_KEYWORDS = [
    "sponsor", "presenting sponsor", "gold sponsor", "silver sponsor",
    "platinum sponsor", "event partner",
]


class ConferenceScraper(BaseScraper):
    name = "ConferenceScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for src in CONFERENCE_SOURCES:
            soup = self._soup(src["url"], timeout=15)
            if not soup:
                continue
            for tag in soup.find_all(["li", "p", "div", "td"])[:80]:
                text = self._clean(tag.get_text())
                lower = text.lower()
                if not any(n.lower() in lower for n in firm_names):
                    continue
                if len(text) < 15:
                    continue

                is_speaking    = any(k in lower for k in SPEAKING_KEYWORDS)
                is_sponsorship = any(k in lower for k in SPONSORSHIP_KEYWORDS)
                if not (is_speaking or is_sponsorship):
                    continue

                sig_type = "bar_speaking" if is_speaking else "bar_sponsorship"
                weight   = CONF_WEIGHT * (1.5 if is_sponsorship else 1.0)

                # Find URL
                a = tag.find("a", href=True)
                link = src["url"]
                if a and a.get("href"):
                    href = a["href"]
                    link = href if href.startswith("http") else src["url"]

                dept_hint = src.get("dept_hint")
                if dept_hint:
                    dept, score, kw = dept_hint, 2.0, [dept_hint.lower()]
                else:
                    dept, score, kw = _clf.top_department(text)

                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type=sig_type,
                    title=f"[{src['name']}] {text[:160]}",
                    body=text[:400],
                    url=link,
                    department=dept,
                    department_score=score * weight,
                    matched_keywords=kw,
                ))
                if len(signals) >= 6:
                    return signals

        return signals
