"""
Conference & CLE Presentation Scraper
=======================================
Speaking at an external conference is a firm's public declaration that it
has expertise in that area. Sponsoring one means business development spend.

Why conferences matter beyond bar associations:
  - PDAC (mining/resources) sponsorship → energy/natural resources push
  - CIPP/C (privacy) speaking → data privacy depth
  - SuperConference (employment) → labour expansion
  - IAPP Global Privacy Summit → data privacy investment
  - Osgoode PD, Law Society CPD → prestigious practice signals

Sources:
  1. Osgoode Professional Development
  2. Law Society of Ontario CPD (lso.ca/cpd)
  3. PDAC Convention (mining/resources)
  4. CAPP (Canadian Association of Petroleum Producers)
  5. Prospectors & Developers Association of Canada
  6. Canadian Institute conferences
  7. Insight Information / Clarion conferences
  8. CIBC Whistler Institutional Investor Conference speakers
  9. Stikeman Institute (tax CPD)
  10. OBA Institute
"""

import re
from bs4 import BeautifulSoup
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

CONFERENCE_SOURCES = [
    {
        "name": "LSO CPD",
        "url": "https://lso.ca/lawyers/cpd/programs",
        "department_hint": "",
        "weight": 2.5,
    },
    {
        "name": "OBA Institute",
        "url": "https://www.oba.org/OBA/Events/Institute",
        "department_hint": "",
        "weight": 2.5,
    },
    {
        "name": "Osgoode PD",
        "url": "https://www.osgoodepd.ca/programs/",
        "department_hint": "",
        "weight": 2.0,
    },
    {
        "name": "Canadian Institute",
        "url": "https://www.canadianinstitute.com/conferences/",
        "department_hint": "",
        "weight": 2.0,
    },
    {
        "name": "PDAC",
        "url": "https://www.pdac.ca/convention/speakers",
        "department_hint": "Energy & Natural Resources",
        "weight": 2.5,
    },
    {
        "name": "IAPP Canada",
        "url": "https://iapp.org/conference/canada/",
        "department_hint": "Data Privacy & Cybersecurity",
        "weight": 2.5,
    },
    {
        "name": "HR Law Canada",
        "url": "https://hrlawcanada.com/speakers/",
        "department_hint": "Employment & Labour",
        "weight": 2.0,
    },
    {
        "name": "CBA National",
        "url": "https://www.cba.org/CBA-National-Summit",
        "department_hint": "",
        "weight": 2.0,
    },
]

SPEAKER_PHRASES = [
    "speaker", "panelist", "panellist", "moderator", "keynote",
    "presenter", "faculty", "chair", "co-chair", "facilitator",
    "presenting", "speaking", "discussion leader",
]

SPONSOR_PHRASES = [
    "sponsor", "presenting sponsor", "gold sponsor", "silver sponsor",
    "platinum sponsor", "supporting sponsor", "associate sponsor",
]


class ConferenceScraper(BaseScraper):
    name = "ConferenceScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        for source in CONFERENCE_SOURCES:
            signals.extend(self._scrape_conference(firm, source))
        return signals

    def _scrape_conference(self, firm: dict, source: dict) -> list[dict]:
        signals = []
        resp = self._get(source["url"])
        if not resp:
            return signals

        soup = BeautifulSoup(resp.text, "html.parser")
        page_text = soup.get_text(separator=" ")

        firm_names = [firm["short"]] + firm.get("alt_names", [])
        if not any(n.lower() in page_text.lower() for n in firm_names):
            return signals

        # Find sections mentioning the firm
        items = soup.find_all(
            ["div", "li", "article", "section", "tr"],
            class_=re.compile(r"speaker|sponsor|panelist|faculty|presenter|schedule|agenda", re.I)
        )[:30]

        if not items:
            # Fall back to paragraphs/list items
            items = soup.find_all(["p", "li"])[:50]

        for item in items:
            text = item.get_text(separator=" ", strip=True)
            text_lower = text.lower()

            if len(text) < 15:
                continue

            if not any(n.lower() in text_lower for n in firm_names):
                continue

            is_speaker  = any(p in text_lower for p in SPEAKER_PHRASES)
            is_sponsor  = any(p in text_lower for p in SPONSOR_PHRASES)

            if not (is_speaker or is_sponsor):
                continue

            signal_type = "bar_speaking" if is_speaker else "bar_sponsorship"
            weight_mult = 1.2 if is_speaker else 0.8

            title_tag = item.find(["h2", "h3", "h4", "strong", "a"])
            title = title_tag.get_text(strip=True) if title_tag else text[:160]

            # Use department hint if given, otherwise classify from text
            dept = source.get("department_hint", "")
            score = source["weight"] * weight_mult
            matched = [source["name"]]

            if not dept:
                full_context = f"{source['name']} {title} {text}"
                classifications = classifier.classify(full_context, top_n=1)
                if classifications:
                    dept  = classifications[0]["department"]
                    score = max(score, classifications[0]["score"] * source["weight"] * weight_mult)
                    matched = classifications[0]["matched_keywords"]

            if not dept:
                continue

            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type=signal_type,
                title=f"[{source['name']}] {title}",
                body=text[:600],
                url=source["url"],
                department=dept,
                department_score=score,
                matched_keywords=matched,
            ))

        if signals:
            self.logger.info(f"[{firm['short']}] {source['name']}: {len(signals)} signal(s)")
        return signals
