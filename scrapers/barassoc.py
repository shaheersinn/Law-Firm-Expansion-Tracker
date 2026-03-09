"""
BarAssociationScraper
Monitors bar association leadership appointments and speaking engagements.
Bar leadership roles are Tier-1 signals: firm made a reputational commitment.

Sources:
  - CBA (Canadian Bar Association) — sections list + news
  - OBA (Ontario Bar Association) — news + sections
  - LSO (Law Society of Ontario) — news
  - Advocates' Society — news
  - CCCA (Canadian Corporate Counsel Association) — news
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

BAR_SOURCES = [
    {
        "name": "CBA",
        "news_url": "https://www.cba.org/News-Media/News-and-Articles",
        "rss": None,
    },
    {
        "name": "OBA",
        "news_url": "https://www.oba.org/News",
        "rss": None,
    },
    {
        "name": "Advocates Society",
        "news_url": "https://www.advocates.ca/news",
        "rss": None,
    },
]

LEADERSHIP_KEYWORDS = [
    "chair", "president", "vice-president", "elected", "appointed",
    "executive", "section head", "committee chair", "board member",
    "treasurer", "secretary", "director",
]

SPEAKING_KEYWORDS = [
    "speaker", "panelist", "keynote", "moderator", "presents",
    "presenting", "speaking at", "featured speaker",
]

BAR_LEADERSHIP_WEIGHT = 3.5
BAR_SPEAKING_WEIGHT   = 2.0


class BarAssociationScraper(BaseScraper):
    name = "BarAssociationScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        for src in BAR_SOURCES:
            soup = self._soup(src["news_url"], timeout=15)
            if not soup:
                continue
            for tag in soup.find_all(["li", "p", "article", "div"])[:60]:
                text = self._clean(tag.get_text())
                lower = text.lower()
                if not any(n.lower() in lower for n in firm_names):
                    continue
                if len(text) < 20:
                    continue

                is_leadership = any(k in lower for k in LEADERSHIP_KEYWORDS)
                is_speaking   = any(k in lower for k in SPEAKING_KEYWORDS)
                if not (is_leadership or is_speaking):
                    continue

                # Find URL
                a = tag.find("a", href=True) or tag.find_parent("a")
                link = src["news_url"]
                if a and a.get("href"):
                    href = a["href"]
                    link = href if href.startswith("http") else f"https://{href.lstrip('/')}"

                sig_type = "bar_leadership" if is_leadership else "bar_speaking"
                weight   = BAR_LEADERSHIP_WEIGHT if is_leadership else BAR_SPEAKING_WEIGHT

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
                if len(signals) >= 8:
                    return signals

        return signals
