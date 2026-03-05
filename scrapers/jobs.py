"""
Job Postings Scraper
=====================
Active job postings are a Tier-2 signal — the firm is *currently* hiring
in a practice area, which means they've already secured client demand
and need headcount to service it.

Sources:
  1. Firm's own /careers page (most reliable)
  2. LinkedIn Jobs (firm-specific search)
  3. Indeed Canada (firm + city search)
  4. ZipRecruiter Canada fallback
"""

import re
from bs4 import BeautifulSoup
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

# Role-level keywords that identify lawyer/legal-professional postings
LAWYER_KEYWORDS = [
    "associate", "partner", "counsel", "solicitor", "barrister",
    "lawyer", "attorney", "legal", "articling", "law clerk",
    "paralegal", "law student",
]

# Seniority keywords — senior/partner hires = stronger signal
SENIOR_KEYWORDS = ["partner", "senior counsel", "senior associate", "of counsel", "director"]


class JobsScraper(BaseScraper):
    name = "JobsScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        signals.extend(self._scrape_firm_careers_page(firm))
        signals.extend(self._scrape_indeed(firm))
        return self._deduplicate(signals)

    # ------------------------------------------------------------------ #
    #  Firm's own careers page
    # ------------------------------------------------------------------ #

    def _scrape_firm_careers_page(self, firm: dict) -> list[dict]:
        signals = []
        base = firm.get("careers_url") or firm["website"].rstrip("/") + "/careers"

        response = self._get(base)
        if not response:
            return signals

        soup = BeautifulSoup(response.text, "html.parser")

        # Find job listing cards
        job_cards = soup.find_all(
            ["div", "li", "article", "tr"],
            class_=re.compile(r"job|position|opening|vacancy|career|posting|listing", re.I)
        )

        # Fallback: look for anchor links that mention "apply" or "job"
        if not job_cards:
            job_cards = soup.find_all("a", href=re.compile(r"job|career|apply|position", re.I))

        for card in job_cards[:30]:
            text = card.get_text(separator=" ", strip=True)
            text_lower = text.lower()

            # Must be a legal professional role
            if not any(kw in text_lower for kw in LAWYER_KEYWORDS):
                continue

            title_tag = card.find(["h2", "h3", "h4", "strong", "a"])
            title = title_tag.get_text(strip=True) if title_tag else text[:120]

            # Determine seniority — boosts score
            is_senior = any(kw in text_lower for kw in SENIOR_KEYWORDS)
            boost = 1.5 if is_senior else 1.0

            classifications = classifier.classify(text, top_n=1)
            if not classifications:
                continue

            cls = classifications[0]
            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="job_posting",
                title=f"[Careers] {title}",
                body=text[:600],
                url=base,
                department=cls["department"],
                department_score=cls["score"] * boost,
                matched_keywords=cls["matched_keywords"],
            ))

        self.logger.info(f"[{firm['short']}] Firm careers: {len(signals)} signal(s)")
        return signals

    # ------------------------------------------------------------------ #
    #  Indeed Canada
    # ------------------------------------------------------------------ #

    def _scrape_indeed(self, firm: dict) -> list[dict]:
        signals = []
        firm_name_q = firm["name"].split()[0]
        url = (
            f"https://ca.indeed.com/jobs"
            f"?q={firm_name_q}+lawyer+associate&l=Canada&sort=date&fromage=30"
        )

        response = self._get(url)
        if not response:
            return signals

        soup = BeautifulSoup(response.text, "html.parser")

        for card in soup.find_all("div", class_=re.compile(r"job_seen|jobCard|resultContent", re.I))[:15]:
            title_tag = card.find(["h2", "h3"], class_=re.compile(r"jobTitle|title", re.I))
            company_tag = card.find(class_=re.compile(r"companyName|company", re.I))
            if not title_tag:
                continue

            title = title_tag.get_text(strip=True)
            company = company_tag.get_text(strip=True) if company_tag else ""

            # Filter to this firm only
            if (firm["short"].lower() not in company.lower()
                    and firm["name"].split()[0].lower() not in company.lower()):
                continue

            desc = card.find(class_=re.compile(r"summary|description|snippet", re.I))
            body = desc.get_text(strip=True) if desc else ""
            full_text = f"{title} {body}"

            if not any(kw in full_text.lower() for kw in LAWYER_KEYWORDS):
                continue

            classifications = classifier.classify(full_text, top_n=1)
            if not classifications:
                continue

            cls = classifications[0]
            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="job_posting",
                title=f"[Indeed] {title}",
                body=body[:500],
                url=url,
                department=cls["department"],
                department_score=cls["score"],
                matched_keywords=cls["matched_keywords"],
            ))

        self.logger.info(f"[{firm['short']}] Indeed: {len(signals)} signal(s)")
        return signals

    def _deduplicate(self, signals: list[dict]) -> list[dict]:
        seen = set()
        result = []
        for s in signals:
            key = s["title"].lower()[:80]
            if key not in seen:
                seen.add(key)
                result.append(s)
        return result
