"""
Government & Regulatory Proceedings Tracker
=============================================
Tracks Canadian federal and provincial regulatory proceedings, consultations,
and government relations activity — a strong signal for ESG, Energy,
Competition, Financial Services, and Infrastructure expansion.

Sources:
  1. Canada Gazette — proposed regulations and final rules
  2. Competition Bureau proceedings and consent agreements
  3. CRTC proceedings
  4. Federal Register of Lobbyists (firms registering on behalf of clients)
  5. Ontario Regulatory Registry
  6. Investment Canada Act notifications (public register)

Signal logic:
  Firm appearing as intervenor/counsel in a regulatory proceeding →
  firm is building depth in that regulatory area before it shows up
  in rankings or job postings.
"""

import re
from bs4 import BeautifulSoup
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

GOV_SOURCES = [
    {
        "name": "Canada Gazette",
        "url": "https://www.gazette.gc.ca/rp-pr/p1/index-eng.html",
        "department": "ESG & Regulatory",
        "weight": 2.5,
    },
    {
        "name": "Competition Bureau",
        "url": "https://www.canada.ca/en/competition-bureau/news/all-news.html",
        "department": "Competition & Antitrust",
        "weight": 3.0,
    },
    {
        "name": "CRTC Decisions",
        "url": "https://crtc.gc.ca/eng/all/telcom.htm",
        "department": "Financial Services & Regulatory",
        "weight": 2.5,
    },
    {
        "name": "OSC Proceedings",
        "url": "https://www.osc.ca/en/news-events/news/administrative-proceedings",
        "department": "Capital Markets",
        "weight": 3.0,
    },
    {
        "name": "Investment Canada",
        "url": "https://www.ic.gc.ca/app/iba/index?lang=eng",
        "department": "Corporate / M&A",
        "weight": 3.0,
    },
    {
        "name": "National Energy Board",
        "url": "https://www.cer-rec.gc.ca/en/applications-hearings/",
        "department": "Energy & Natural Resources",
        "weight": 3.0,
    },
    {
        "name": "Privacy Commissioner",
        "url": "https://www.priv.gc.ca/en/opc-news/news-and-announcements/",
        "department": "Data Privacy & Cybersecurity",
        "weight": 3.0,
    },
    {
        "name": "OSFI",
        "url": "https://www.osfi-bsif.gc.ca/en/news-events/news-releases",
        "department": "Financial Services & Regulatory",
        "weight": 2.5,
    },
    {
        "name": "Health Canada",
        "url": "https://www.canada.ca/en/health-canada/news/all.html",
        "department": "Healthcare & Life Sciences",
        "weight": 2.0,
    },
    {
        "name": "IRCC",
        "url": "https://www.canada.ca/en/immigration-refugees-citizenship/news/notices.html",
        "department": "Immigration",
        "weight": 2.0,
    },
]


class GovTrackScraper(BaseScraper):
    name = "GovTrackScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        for source in GOV_SOURCES:
            signals.extend(self._scrape_source(firm, source))
        return signals

    def _scrape_source(self, firm: dict, source: dict) -> list[dict]:
        signals = []
        resp = self._get(source["url"])
        if not resp:
            return signals

        soup = BeautifulSoup(resp.text, "html.parser")
        items = soup.find_all(
            ["article", "div", "li", "tr"],
            class_=re.compile(r"news|notice|result|item|decision|proceeding|announcement", re.I)
        )[:20]

        for item in items:
            text = item.get_text(separator=" ", strip=True)
            text_lower = text.lower()

            # Must mention the firm as participant/counsel
            firm_mentioned = (
                firm["short"].lower() in text_lower or
                firm["name"].split()[0].lower() in text_lower or
                any(n.lower() in text_lower for n in firm.get("alt_names", []))
            )
            if not firm_mentioned:
                continue

            title_tag = item.find(["h2", "h3", "h4", "a", "strong"])
            title = title_tag.get_text(strip=True) if title_tag else text[:150]

            # Try to classify more precisely from content
            classifications = classifier.classify(text, top_n=1)
            department = source["department"]
            score = source["weight"]
            matched = [source["name"]]

            if classifications:
                cls = classifications[0]
                if cls["score"] > 2.0:
                    department = cls["department"]
                    score = max(score, cls["score"])
                    matched = cls["matched_keywords"]

            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="court_record",
                title=f"[{source['name']}] {title}",
                body=text[:600],
                url=source["url"],
                department=department,
                department_score=score,
                matched_keywords=matched,
            ))

        if signals:
            self.logger.info(f"[{firm['short']}] {source['name']}: {len(signals)} signal(s)")
        return signals
