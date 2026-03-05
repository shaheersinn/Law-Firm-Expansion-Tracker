"""
Job Postings Scraper
=====================
Tracks active hiring at the firm. Where a firm hires = where it's growing.

Sources:
  - Firm's own /careers page
  - Indeed Canada
  - LinkedIn Jobs (public search)
  - WorkopolicaCA
  - Glassdoor Canada job listings
"""

import re
from bs4 import BeautifulSoup
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

SENIORITY_WEIGHTS = {
    "partner":         3.0,
    "counsel":         2.5,
    "senior counsel":  2.5,
    "associate":       2.0,
    "student":         1.5,
    "clerk":           1.5,
    "analyst":         1.0,
}


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
        paths = [
            firm.get("careers_url", "").replace(base, "") or "/careers",
            "/en/careers", "/en-ca/careers", "/careers/opportunities",
            "/careers/lawyers", "/lawyer-opportunities",
        ]

        for path in paths:
            url = base + path if path.startswith("/") else path
            resp = self._get(url)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            cards = soup.find_all(
                ["div", "li", "article"],
                class_=re.compile(r"job|posting|position|opening|opportunity|career", re.I)
            )[:30]

            for card in cards:
                text = card.get_text(separator=" ", strip=True)
                if len(text) < 20:
                    continue

                title_tag = card.find(["h2", "h3", "h4", "a", "strong"])
                title = title_tag.get_text(strip=True) if title_tag else text[:120]

                # Skip clearly non-legal roles
                if any(w in title.lower() for w in ["receptionist", "marketing coordinator", "it support", "billing"]):
                    continue

                classifications = classifier.classify(text, top_n=1)
                if not classifications:
                    continue

                cls    = classifications[0]
                weight = self._seniority_weight(title)

                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="job_posting",
                    title=f"[{firm['short']} Careers] {title}",
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
                if not any(n.lower() in company.lower() for n in [firm["short"]] + firm.get("alt_names", [])):
                    continue

                title_tag = card.find(["h2", "h3"], class_=re.compile(r"title|jobTitle", re.I))
                title = title_tag.get_text(strip=True) if title_tag else ""
                if not title:
                    continue

                desc_tag = card.find(class_=re.compile(r"summary|description|snippet", re.I))
                body = desc_tag.get_text(strip=True) if desc_tag else card.get_text(" ", strip=True)

                classifications = classifier.classify(f"{title} {body}", top_n=1)
                if not classifications:
                    continue

                cls    = classifications[0]
                weight = self._seniority_weight(title)
                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="job_posting",
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
        slug = firm.get("linkedin_slug", "")
        if not slug:
            return signals

        url = f"https://www.linkedin.com/jobs/search/?keywords={firm['short'].replace(' ','+')}&f_C=&location=Canada"
        resp = self._get(url, extra_headers={"Accept": "text/html"})
        if not resp:
            return signals

        soup = BeautifulSoup(resp.text, "html.parser")
        for card in soup.find_all(class_=re.compile(r"job-search-card|base-card|result-card", re.I))[:10]:
            company_tag = card.find(class_=re.compile(r"company|employer|base-search-card__subtitle", re.I))
            company = company_tag.get_text(strip=True) if company_tag else ""
            if not any(n.lower() in company.lower() for n in [firm["short"]] + firm.get("alt_names", [])):
                continue

            title_tag = card.find(class_=re.compile(r"title|job-search-card__title", re.I))
            title = title_tag.get_text(strip=True) if title_tag else ""
            if not title:
                continue

            classifications = classifier.classify(title, top_n=1)
            if not classifications:
                continue

            cls = classifications[0]
            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="job_posting",
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
