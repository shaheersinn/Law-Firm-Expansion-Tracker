"""
Bar Association Announcements Scraper
=======================================
Bar association activity is a strong proxy for where lawyers are concentrating
their professional energy and where firms are building credibility:

  - A firm's lawyer becoming Chair of a bar section = firm is asserting
    leadership in that practice area
  - Multiple lawyers from one firm joining a new section committee = firm
    is building bench depth in that area
  - Firm sponsoring a bar event in a new practice area = business development
    investment in expanding into that area
  - CBA / LSUC CPD programs: which topics are lawyers from this firm presenting on

Sources:
  1. Canadian Bar Association (CBA) — section news, committee appointments
  2. Law Society of Ontario (LSO) — CPD presenters, committee appointments
  3. Ontario Bar Association (OBA) — section updates, event speakers
  4. Law Society of BC (LSBC)
  5. Law Society of Alberta (LSA)
  6. The Advocates' Society
  7. Canadian Corporate Counsel Association (CCCA)
  8. Women's Law Association of Ontario (WLAO) — equity/diversity leadership signals

Key signals:
  NEW CHAIR / CO-CHAIR appointment → highest weight (3.5)
  New committee member listing   → medium weight (2.0)
  Speaking at a bar section event → lower weight (1.5)
  Sponsoring a section event      → awareness signal (1.0)
"""

import re
from bs4 import BeautifulSoup
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

BAR_SOURCES = [
    {
        "name": "CBA",
        "full_name": "Canadian Bar Association",
        "news_url": "https://www.cba.org/Sections-Divisions",
        "rss_url": "https://www.cba.org/rss/news",
        "weight": 3.0,
    },
    {
        "name": "OBA",
        "full_name": "Ontario Bar Association",
        "news_url": "https://www.oba.org/Sections",
        "rss_url": "https://www.oba.org/rss",
        "weight": 2.5,
    },
    {
        "name": "LSO",
        "full_name": "Law Society of Ontario",
        "news_url": "https://lso.ca/news-events/news",
        "weight": 2.0,
    },
    {
        "name": "Advocates Society",
        "full_name": "The Advocates' Society",
        "news_url": "https://www.advocates.ca/news",
        "weight": 2.5,
    },
    # CCCA (ccca-caj.ca) — REMOVED: DNS resolution failure, domain appears dead
    {
        "name": "ACCA",
        "full_name": "Association of Corporate Counsel",
        "news_url": "https://www.acc.com/chapters/canada",
        "weight": 1.5,
    },
]

# Leadership signal phrases
LEADERSHIP_PHRASES = [
    "elected chair", "appointed chair", "new chair", "co-chair",
    "elected president", "appointed president", "section executive",
    "section council", "committee chair", "committee member",
    "board of directors", "executive committee", "appointed to",
    "elected to", "section leader", "practice group leader",
    "incoming chair", "past chair",
]

# Speaking/presenting signal phrases  
SPEAKING_PHRASES = [
    "present", "speaker", "panelist", "moderator", "keynote",
    "presenting on", "speaking at", "hosted by", "chaired by",
    "organized by", "sponsored by",
]

# Sponsorship signals
SPONSOR_PHRASES = [
    "proudly sponsor", "presenting sponsor", "gold sponsor",
    "silver sponsor", "platinum sponsor", "event sponsor",
]


class BarAssociationScraper(BaseScraper):
    name = "BarAssociationScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        for source in BAR_SOURCES:
            signals.extend(self._scrape_bar_source(firm, source))
        signals.extend(self._scrape_cba_sections(firm))
        signals.extend(self._scrape_oba_sections(firm))
        return self._deduplicate(signals)

    # ------------------------------------------------------------------ #
    #  Generic bar source scraper
    # ------------------------------------------------------------------ #

    def _scrape_bar_source(self, firm: dict, source: dict) -> list[dict]:
        signals = []
        url = source.get("rss_url") or source.get("news_url", "")
        if not url:
            return signals

        response = self._get(url)
        if not response:
            return signals

        # Try RSS first, fall back to HTML
        content_type = response.headers.get("content-type", "")
        if "xml" in content_type or "rss" in url:
            signals.extend(self._parse_rss(firm, source, response.text))
        else:
            signals.extend(self._parse_html(firm, source, response.text, url))

        return signals

    def _parse_rss(self, firm: dict, source: dict, xml_text: str) -> list[dict]:
        signals = []
        soup = BeautifulSoup(xml_text, features="xml")
        if not soup.find("item"):
            soup = BeautifulSoup(xml_text, "html.parser")

        for item in soup.find_all("item")[:20]:
            title_tag = item.find("title")
            desc_tag = item.find("description")
            link_tag = item.find("link")

            title = title_tag.get_text(strip=True) if title_tag else ""
            body = desc_tag.get_text(strip=True) if desc_tag else ""
            link = link_tag.get_text(strip=True) if link_tag else source.get("news_url", "")

            if not title:
                continue

            combined = f"{title} {body}".lower()

            # Must mention the firm (or one of its lawyers — harder to detect)
            firm_mentioned = (
                firm["short"].lower() in combined
                or firm["name"].split()[0].lower() in combined
            )
            if not firm_mentioned:
                continue

            signal_type, weight_mult = self._classify_signal_type(combined)
            full_text = f"{title} {body}"
            classifications = classifier.classify(full_text, top_n=1)
            if not classifications:
                continue

            cls = classifications[0]
            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type=signal_type,
                title=f"[{source['name']}] {title}",
                body=body[:600],
                url=link,
                department=cls["department"],
                department_score=cls["score"] * source["weight"] * weight_mult,
                matched_keywords=cls["matched_keywords"],
            ))

        return signals

    def _parse_html(self, firm: dict, source: dict, html: str, url: str) -> list[dict]:
        signals = []
        soup = BeautifulSoup(html, "html.parser")

        articles = soup.find_all(
            ["article", "div", "li"],
            class_=re.compile(r"news|article|post|item|result|announcement", re.I)
        )[:25]

        for tag in articles:
            text = tag.get_text(separator=" ", strip=True)
            text_lower = text.lower()

            # Must mention the firm
            if firm["short"].lower() not in text_lower and \
               firm["name"].split()[0].lower() not in text_lower:
                continue

            signal_type, weight_mult = self._classify_signal_type(text_lower)

            title_tag = tag.find(["h2", "h3", "h4", "strong", "a"])
            title = title_tag.get_text(strip=True) if title_tag else text[:120]

            classifications = classifier.classify(text, top_n=1)
            if not classifications:
                continue

            cls = classifications[0]
            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type=signal_type,
                title=f"[{source['name']}] {title}",
                body=text[:600],
                url=url,
                department=cls["department"],
                department_score=cls["score"] * source["weight"] * weight_mult,
                matched_keywords=cls["matched_keywords"],
            ))

        return signals

    # ------------------------------------------------------------------ #
    #  CBA Sections — enumerate all sections and search for firm mentions
    # ------------------------------------------------------------------ #

    def _scrape_cba_sections(self, firm: dict) -> list[dict]:
        signals = []
        url = "https://www.cba.org/Sections-Divisions"
        response = self._get(url)
        if not response:
            return signals

        soup = BeautifulSoup(response.text, "html.parser")

        # Find section links
        section_links = [
            a for a in soup.find_all("a", href=True)
            if "/Sections" in a.get("href", "") and len(a.get_text(strip=True)) > 3
        ]

        for link in section_links[:25]:
            section_name = link.get_text(strip=True)
            section_url = "https://www.cba.org" + link["href"] if link["href"].startswith("/") else link["href"]

            section_response = self._get(section_url)
            if not section_response:
                continue

            section_soup = BeautifulSoup(section_response.text, "html.parser")
            section_text = section_soup.get_text(separator=" ")

            if firm["short"].lower() not in section_text.lower() and \
               firm["name"].split()[0].lower() not in section_text.lower():
                continue

            # Firm is present in this section page
            # Look for leadership listings
            for leader_tag in section_soup.find_all(
                class_=re.compile(r"executive|council|officer|chair|leader|director", re.I)
            ):
                leader_text = leader_tag.get_text(separator=" ", strip=True)
                if firm["short"].lower() in leader_text.lower() or \
                   firm["name"].split()[0].lower() in leader_text.lower():

                    dept = self._classify_section(section_name)
                    if dept:
                        signals.append(self._make_signal(
                            firm_id=firm["id"],
                            firm_name=firm["name"],
                            signal_type="bar_leadership",
                            title=f"[CBA] {firm['short']} in {section_name} leadership",
                            body=leader_text[:400],
                            url=section_url,
                            department=dept,
                            department_score=9.0,  # leadership = very high signal
                            matched_keywords=["CBA", "section chair", section_name],
                        ))

        self.logger.info(f"[{firm['short']}] CBA sections: {len(signals)} signal(s)")
        return signals

    # ------------------------------------------------------------------ #
    #  OBA Sections
    # ------------------------------------------------------------------ #

    def _scrape_oba_sections(self, firm: dict) -> list[dict]:
        signals = []
        url = "https://www.oba.org/Sections"
        response = self._get(url)
        if not response:
            return signals

        soup = BeautifulSoup(response.text, "html.parser")
        section_links = [
            a for a in soup.find_all("a", href=True)
            if "section" in a.get("href", "").lower() and len(a.get_text(strip=True)) > 3
        ]

        for link in section_links[:20]:
            section_name = link.get_text(strip=True)
            href = link.get("href", "")
            section_url = ("https://www.oba.org" + href) if href.startswith("/") else href

            section_response = self._get(section_url)
            if not section_response:
                continue

            section_soup = BeautifulSoup(section_response.text, "html.parser")
            section_text = section_soup.get_text(separator=" ")

            if firm["short"].lower() not in section_text.lower() and \
               firm["name"].split()[0].lower() not in section_text.lower():
                continue

            dept = self._classify_section(section_name)
            if not dept:
                dept_result = classifier.classify(f"{section_name} {section_text[:300]}", top_n=1)
                dept = dept_result[0]["department"] if dept_result else ""

            if dept:
                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="bar_leadership",
                    title=f"[OBA] {firm['short']} active in {section_name}",
                    body=section_text[:400],
                    url=section_url,
                    department=dept,
                    department_score=6.0,
                    matched_keywords=["OBA", section_name],
                ))

        self.logger.info(f"[{firm['short']}] OBA sections: {len(signals)} signal(s)")
        return signals

    # ------------------------------------------------------------------ #
    #  Helpers
    # ------------------------------------------------------------------ #

    def _classify_signal_type(self, text_lower: str) -> tuple[str, float]:
        """Return (signal_type, weight_multiplier)."""
        if any(p in text_lower for p in LEADERSHIP_PHRASES):
            return "bar_leadership", 1.5
        if any(p in text_lower for p in SPEAKING_PHRASES):
            return "bar_speaking", 1.0
        if any(p in text_lower for p in SPONSOR_PHRASES):
            return "bar_sponsorship", 0.7
        return "bar_mention", 0.5

    def _classify_section(self, section_name: str) -> str:
        """Map a bar association section name to a department."""
        section_lower = section_name.lower()
        section_map = {
            "corporate":            "Corporate / M&A",
            "m&a":                  "Corporate / M&A",
            "private equity":       "Private Equity",
            "capital markets":      "Capital Markets",
            "securities":           "Capital Markets",
            "litigation":           "Litigation & Disputes",
            "dispute":              "Litigation & Disputes",
            "arbitration":          "Litigation & Disputes",
            "insolvency":           "Restructuring & Insolvency",
            "restructur":           "Restructuring & Insolvency",
            "real estate":          "Real Estate",
            "property":             "Real Estate",
            "tax":                  "Tax",
            "employment":           "Employment & Labour",
            "labour":               "Employment & Labour",
            "labor":                "Employment & Labour",
            "intellectual property":"Intellectual Property",
            "privacy":              "Data Privacy & Cybersecurity",
            "cyber":                "Data Privacy & Cybersecurity",
            "environment":          "ESG & Regulatory",
            "esg":                  "ESG & Regulatory",
            "energy":               "Energy & Natural Resources",
            "mining":               "Energy & Natural Resources",
            "banking":              "Financial Services & Regulatory",
            "financial":            "Financial Services & Regulatory",
            "competition":          "Competition & Antitrust",
            "antitrust":            "Competition & Antitrust",
            "health":               "Healthcare & Life Sciences",
            "pharma":               "Healthcare & Life Sciences",
            "immigration":          "Immigration",
            "infrastructure":       "Infrastructure & Projects",
            "construction":         "Infrastructure & Projects",
        }
        for key, dept in section_map.items():
            if key in section_lower:
                return dept
        return ""

    def _deduplicate(self, signals: list[dict]) -> list[dict]:
        seen = set()
        result = []
        for s in signals:
            if s["title"] not in seen:
                seen.add(s["title"])
                result.append(s)
        return result
