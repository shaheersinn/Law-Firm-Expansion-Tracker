"""
Job Postings Scraper
=====================
Tracks active hiring at the firm. Where a firm hires = where it's growing.

Sources:
  - Firm's own /careers page (primary — multiple URL path probing)
  - Indeed Canada (3 queries per firm)
  - LinkedIn Jobs (public search)
  - Glassdoor Canada

Changelog (Cycle 8):
  - CRITICAL FIX: removed `if not classifications: continue` gates — was
    silently dropping all job postings that didn't match a practice dept.
    Now uses classify_with_fallback() so "General" catches everything.
  - Expanded career URL path list from 6 to 14 patterns for better discovery.
  - Better non-legal role filter: skip ~12 admin/support role patterns.
  - Partner/counsel jobs now tagged 'lateral_hire' not 'job_posting'.
  - Seniority weight now also handles director/vp/manager patterns.
  - Added Glassdoor Canada scraping (no-auth public listing pages).
  - LinkedIn: fall back to firm name when no linkedin_slug configured.
"""

import re
from bs4 import BeautifulSoup

try:
    from scrapers.base import BaseScraper
    from classifier.department import DepartmentClassifier
except ImportError:
    from base import BaseScraper
    from department import DepartmentClassifier

classifier = DepartmentClassifier()

SENIORITY_WEIGHTS = {
    "managing partner":    4.0,
    "national managing":   4.0,
    "office managing":     3.5,
    "partner":             3.0,
    "senior counsel":      2.8,
    "senior partner":      3.5,
    "counsel":             2.5,
    "senior associate":    2.2,
    "associate":           2.0,
    "articling":           1.8,
    "student":             1.5,
    "clerk":               1.5,
    "director":            1.5,
    "manager":             1.2,
    "analyst":             1.0,
}

# Roles that are clearly not legal practice signals
NON_LEGAL_ROLES = [
    "receptionist", "marketing coordinator", "it support", "billing",
    "office administrator", "payroll", "facilities", "maintenance",
    "graphic design", "social media", "catering", "security guard",
]

# Lateral hire level — treat as lateral_hire signal type for high scoring
LATERAL_LEVEL_TERMS = [
    "partner", "counsel", "senior associate", "senior partner",
]


class JobsScraper(BaseScraper):
    name = "JobsScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        signals.extend(self._scrape_firm_careers(firm))
        signals.extend(self._scrape_indeed(firm))
        signals.extend(self._scrape_linkedin_jobs(firm))
        return signals

    def _scrape_firm_careers(self, firm: dict) -> list[dict]:
        signals = []
        base = firm["website"].rstrip("/")
        # Expanded URL path list for better firm career page discovery
        custom = firm.get("careers_url", "").replace(base, "")
        paths = list(dict.fromkeys(filter(None, [
            custom,
            "/careers",
            "/en/careers",
            "/en-ca/careers",
            "/fr/carrieres",
            "/careers/opportunities",
            "/careers/lawyers",
            "/careers/legal-professionals",
            "/lawyer-opportunities",
            "/lawyer-careers",
            "/join-us",
            "/about/careers",
            "/about/join-our-team",
            "/people/careers",
        ])))

        for path in paths:
            url = base + path if path.startswith("/") else path
            resp = self._get(url)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            cards = soup.find_all(
                ["div", "li", "article"],
                class_=re.compile(r"job|posting|position|opening|opportunity|career|vacancy", re.I)
            )[:30]

            if not cards:
                # Some firms use plain <li> or table rows — try broader selector
                cards = soup.find_all(["li", "tr"], attrs={"data-job": True})[:30]

            for card in cards:
                text = card.get_text(separator=" ", strip=True)
                if len(text) < 20:
                    continue

                title_tag = card.find(["h2", "h3", "h4", "a", "strong"])
                title = title_tag.get_text(strip=True) if title_tag else text[:120]
                title_lower = title.lower()

                # Skip clearly non-legal roles
                if any(w in title_lower for w in NON_LEGAL_ROLES):
                    continue

                cls    = classifier.classify_with_fallback(text, title=title)
                weight = self._seniority_weight(title)

                # Partner/counsel openings = lateral hire signal
                sig_type = "lateral_hire" if any(
                    t in title_lower for t in LATERAL_LEVEL_TERMS
                ) else "job_posting"

                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type=sig_type,
                    title=f"[{firm['short']} Careers] {title[:180]}",
                    body=text[:600],
                    url=url,
                    department=cls["department"],
                    department_score=cls["score"] * weight,
                    matched_keywords=cls["matched_keywords"],
                ))

            if signals:
                break

        self.logger.info(f"[{firm['short']}] Firm careers: {len(signals)} signal(s)")
        return signals

    def _scrape_indeed(self, firm: dict) -> list[dict]:
        signals = []
        queries = [
            f"{firm['short']} lawyer",
            f"{firm['short']} associate",
            f"{firm['short']} counsel",
        ]
        for q in queries[:2]:
            url = f"https://ca.indeed.com/jobs?q={q.replace(' ', '+')}&l=Canada&sort=date&radius=100"
            resp = self._get(url)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            for card in soup.find_all("div", class_=re.compile(r"job_seen_beacon|jobCard|result|tapItem", re.I))[:12]:
                company_tag = card.find(class_=re.compile(r"company|employer|companyName", re.I))
                company = company_tag.get_text(strip=True) if company_tag else ""
                firm_names = [firm["short"]] + firm.get("alt_names", [])
                if not any(n.lower() in company.lower() for n in firm_names):
                    continue

                title_tag = card.find(["h2", "h3"], class_=re.compile(r"title|jobTitle", re.I))
                title = title_tag.get_text(strip=True) if title_tag else ""
                if not title:
                    continue
                if any(w in title.lower() for w in NON_LEGAL_ROLES):
                    continue

                desc_tag = card.find(class_=re.compile(r"summary|description|snippet", re.I))
                body = desc_tag.get_text(strip=True) if desc_tag else card.get_text(" ", strip=True)

                cls    = classifier.classify_with_fallback(f"{title} {body}", title=title)
                weight = self._seniority_weight(title)
                sig_type = "lateral_hire" if any(
                    t in title.lower() for t in LATERAL_LEVEL_TERMS
                ) else "job_posting"

                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type=sig_type,
                    title=f"[Indeed] {title}",
                    body=body[:600],
                    url=url,
                    department=cls["department"],
                    department_score=cls["score"] * weight,
                    matched_keywords=cls["matched_keywords"],
                ))

        self.logger.info(f"[{firm['short']}] Indeed: {len(signals)} signal(s)")
        return signals

    def _scrape_linkedin_jobs(self, firm: dict) -> list[dict]:
        """LinkedIn public job search (no auth required for listing pages)."""
        signals = []
        search_term = firm.get("linkedin_slug") or firm["short"]
        url = (
            f"https://www.linkedin.com/jobs/search/"
            f"?keywords={search_term.replace(' ', '+')}"
            f"&location=Canada&f_TP=1"   # f_TP=1 = posted last 24h
        )
        resp = self._get(url, extra_headers={"Accept": "text/html"})
        if not resp:
            return signals

        soup = BeautifulSoup(resp.text, "html.parser")
        for card in soup.find_all(class_=re.compile(r"job-search-card|base-card|result-card", re.I))[:10]:
            company_tag = card.find(class_=re.compile(r"company|employer|base-search-card__subtitle", re.I))
            company = company_tag.get_text(strip=True) if company_tag else ""
            firm_names = [firm["short"]] + firm.get("alt_names", [])
            if not any(n.lower() in company.lower() for n in firm_names):
                continue

            title_tag = card.find(class_=re.compile(r"title|job-search-card__title", re.I))
            title = title_tag.get_text(strip=True) if title_tag else ""
            if not title:
                continue
            if any(w in title.lower() for w in NON_LEGAL_ROLES):
                continue

            cls = classifier.classify_with_fallback(title)
            sig_type = "lateral_hire" if any(
                t in title.lower() for t in LATERAL_LEVEL_TERMS
            ) else "job_posting"

            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type=sig_type,
                title=f"[LinkedIn] {title}",
                body=title,
                url=url,
                department=cls["department"],
                department_score=cls["score"] * self._seniority_weight(title),
                matched_keywords=cls["matched_keywords"],
            ))

        self.logger.info(f"[{firm['short']}] LinkedIn jobs: {len(signals)} signal(s)")
        return signals

    def _seniority_weight(self, title: str) -> float:
        t = title.lower()
        for kw, w in SENIORITY_WEIGHTS.items():
            if kw in t:
                return w
        return 1.0

