"""
Chambers & Legal 500 Rankings Scraper
======================================
Chambers Canada and Legal 500 Canada are the two authoritative third-party
rankings for Canadian law firms. A firm moving up in a new practice group,
or appearing in a new band for the first time, is a lagging-but-reliable
confirmation of expansion that has already happened.

What we track:
  - New practice areas a firm has been ranked in (didn't exist last year)
  - Band improvements within an existing practice area
  - New individual lawyer rankings in a department (signals firm depth)
  - "Firms to watch" and "Rising stars" designations

Why it matters:
  Chambers rankings require 12-18 months of demonstrated activity — so a
  NEW ranking = the firm has been building that capability for at least a year.
  It's a high-confidence, lower-frequency signal.

Sources:
  1. Chambers Canada: https://chambers.com/guide/canada
  2. Legal 500 Canada: https://www.legal500.com/c/canada/
  3. Canadian Lawyer Top 10 rankings (annual)
  4. Benchmark Canada
"""

import re
from bs4 import BeautifulSoup
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

CHAMBERS_BASE = "https://chambers.com"
LEGAL500_BASE = "https://www.legal500.com/c/canada"

# Chambers practice area URL slugs → department mapping
CHAMBERS_PRACTICE_MAP = {
    "corporate-commercial":             "Corporate / M&A",
    "mergers-acquisitions":             "Corporate / M&A",
    "private-equity":                   "Private Equity",
    "capital-markets":                  "Capital Markets",
    "securities":                       "Capital Markets",
    "litigation":                       "Litigation & Disputes",
    "dispute-resolution":               "Litigation & Disputes",
    "arbitration":                      "Litigation & Disputes",
    "restructuring-insolvency":         "Restructuring & Insolvency",
    "real-estate":                      "Real Estate",
    "tax":                              "Tax",
    "employment":                       "Employment & Labour",
    "labour":                           "Employment & Labour",
    "intellectual-property":            "Intellectual Property",
    "privacy-data":                     "Data Privacy & Cybersecurity",
    "cybersecurity":                    "Data Privacy & Cybersecurity",
    "environment":                      "ESG & Regulatory",
    "esg":                              "ESG & Regulatory",
    "energy":                           "Energy & Natural Resources",
    "mining":                           "Energy & Natural Resources",
    "natural-resources":                "Energy & Natural Resources",
    "banking-finance":                  "Financial Services & Regulatory",
    "financial-services":               "Financial Services & Regulatory",
    "fintech":                          "Financial Services & Regulatory",
    "competition-antitrust":            "Competition & Antitrust",
    "healthcare":                       "Healthcare & Life Sciences",
    "life-sciences":                    "Healthcare & Life Sciences",
    "immigration":                      "Immigration",
    "infrastructure":                   "Infrastructure & Projects",
    "projects":                         "Infrastructure & Projects",
    "construction":                     "Infrastructure & Projects",
}

# Signals that a ranking is NEW or IMPROVED
POSITIVE_RANKING_SIGNALS = [
    "new entry", "newly ranked", "first time", "band 1", "tier 1",
    "recommended", "highly recommended", "rising star", "firms to watch",
    "next generation", "promoted", "band 2", "band 3",
]


class ChambersScraper(BaseScraper):
    name = "ChambersScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        signals.extend(self._scrape_chambers(firm))
        signals.extend(self._scrape_legal500(firm))
        signals.extend(self._scrape_canadian_lawyer_rankings(firm))
        if not signals:
            self.logger.info(
                f"[{firm['short']}] Chambers/Legal500: 0 rankings found "
                "(site may require JS/login — consider adding manual ranking data)"
            )
        return signals

    # ------------------------------------------------------------------ #
    #  Chambers Canada
    # ------------------------------------------------------------------ #

    def _scrape_chambers(self, firm: dict) -> list[dict]:
        signals = []
        # Chambers firm profile page
        firm_slug = self._to_slug(firm["name"])
        url = f"{CHAMBERS_BASE}/law-firm/{firm_slug}/canada"

        response = self._get(url)
        if not response:
            # Try alternate slug format
            short_slug = self._to_slug(firm["short"])
            url = f"{CHAMBERS_BASE}/law-firm/{short_slug}/canada"
            response = self._get(url)

        if not response:
            self.logger.debug(f"[{firm['short']}] Chambers profile not found")
            return signals

        soup = BeautifulSoup(response.text, "html.parser")

        # Find ranked practice areas
        practice_sections = soup.find_all(
            ["div", "section", "li"],
            class_=re.compile(r"practice|ranking|band|department|area", re.I)
        )

        for section in practice_sections[:30]:
            section_text = section.get_text(separator=" ", strip=True)
            if len(section_text) < 10:
                continue

            # Check for positive signals (new rankings, band improvements)
            has_positive = any(sig in section_text.lower() for sig in POSITIVE_RANKING_SIGNALS)
            if not has_positive:
                continue

            # Extract band/tier label
            band_match = re.search(r'(Band\s*\d|Tier\s*\d|Band\s*[A-Z])', section_text, re.I)
            band_label = band_match.group(0) if band_match else "Ranked"

            # Map to department
            dept = self._map_section_to_dept(section_text)
            if not dept:
                continue

            title_tag = section.find(["h2", "h3", "h4", "strong", "a"])
            title = title_tag.get_text(strip=True) if title_tag else section_text[:100]

            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="ranking",
                title=f"[Chambers] {title} — {band_label}",
                body=section_text[:500],
                url=url,
                department=dept,
                department_score=8.0,  # rankings = very high confidence
                matched_keywords=[band_label, "Chambers ranking"],
            ))

        # Also parse lawyer-level rankings (individual rankings = team depth signal)
        lawyer_sections = soup.find_all(
            class_=re.compile(r"lawyer|individual|ranked-lawyer|associate-to-watch", re.I)
        )
        for section in lawyer_sections[:20]:
            text = section.get_text(separator=" ", strip=True)
            dept = self._map_section_to_dept(text)
            if dept:
                name_tag = section.find(["strong", "h3", "h4", "a"])
                name = name_tag.get_text(strip=True) if name_tag else text[:80]
                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="ranking",
                    title=f"[Chambers] Individual ranking: {name}",
                    body=text[:400],
                    url=url,
                    department=dept,
                    department_score=5.0,
                    matched_keywords=["Chambers", "individual ranking"],
                ))

        self.logger.info(f"[{firm['short']}] Chambers: {len(signals)} signal(s)")
        return signals

    # ------------------------------------------------------------------ #
    #  Legal 500 Canada
    # ------------------------------------------------------------------ #

    def _scrape_legal500(self, firm: dict) -> list[dict]:
        signals = []
        firm_slug = self._to_slug(firm["name"])
        url = f"https://www.legal500.com/firms/{firm_slug}/canada/"

        response = self._get(url)
        if not response:
            return signals

        soup = BeautifulSoup(response.text, "html.parser")

        # Legal 500 uses tier tables — find rows with tier designations
        for row in soup.find_all(["tr", "div", "li"], class_=re.compile(r"tier|ranking|practice|band", re.I)):
            text = row.get_text(separator=" ", strip=True)
            if len(text) < 10:
                continue

            tier_match = re.search(r'Tier\s*(\d)', text, re.I)
            if not tier_match:
                continue

            tier_num = int(tier_match.group(1))
            dept = self._map_section_to_dept(text)
            if not dept:
                continue

            # Tier 1 and 2 = strong signal; Tier 3+ = weaker
            score = max(10.0 - (tier_num * 2), 2.0)

            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="ranking",
                title=f"[Legal 500] {dept} — Tier {tier_num}",
                body=text[:400],
                url=url,
                department=dept,
                department_score=score,
                matched_keywords=[f"Tier {tier_num}", "Legal 500"],
            ))

        self.logger.info(f"[{firm['short']}] Legal 500: {len(signals)} signal(s)")
        return signals

    # ------------------------------------------------------------------ #
    #  Canadian Lawyer Top 10 / Benchmark
    # ------------------------------------------------------------------ #

    def _scrape_canadian_lawyer_rankings(self, firm: dict) -> list[dict]:
        signals = []
        # Canadian Lawyer Top 10 regional lists
        url = f"https://www.canadianlawyermag.com/rankings/top-10-regional-firms/"

        response = self._get(url)
        if not response:
            return signals

        soup = BeautifulSoup(response.text, "html.parser")
        page_text = soup.get_text(separator=" ")

        # Check if firm is mentioned
        if firm["short"].lower() not in page_text.lower() and \
           firm["name"].split()[0].lower() not in page_text.lower():
            return signals

        # Find context around firm mention
        idx = page_text.lower().find(firm["short"].lower())
        if idx == -1:
            idx = page_text.lower().find(firm["name"].split()[0].lower())

        if idx > 0:
            context = page_text[max(0, idx-200):idx+400]
            classifications = classifier.classify(context, top_n=2)
            for cls in classifications:
                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="ranking",
                    title=f"[Canadian Lawyer] {firm['short']} mentioned in rankings",
                    body=context.strip(),
                    url=url,
                    department=cls["department"],
                    department_score=cls["score"],
                    matched_keywords=cls["matched_keywords"],
                ))

        return signals

    # ------------------------------------------------------------------ #
    #  Helpers
    # ------------------------------------------------------------------ #

    def _map_section_to_dept(self, text: str) -> str:
        text_lower = text.lower()
        for slug, dept in CHAMBERS_PRACTICE_MAP.items():
            if slug.replace("-", " ") in text_lower or slug.replace("-", "&") in text_lower:
                return dept
        # Fallback to NLP classifier
        results = classifier.classify(text, top_n=1)
        return results[0]["department"] if results else ""

    def _to_slug(self, name: str) -> str:
        slug = name.lower()
        slug = re.sub(r'[^a-z0-9\s-]', '', slug)
        slug = re.sub(r'\s+', '-', slug.strip())
        slug = re.sub(r'-+', '-', slug)
        return slug
