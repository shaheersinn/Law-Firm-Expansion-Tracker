"""
JobsScraper — scrapes firm careers pages and Indeed for job postings.
Signals: new practice-area roles signal expansion intent.
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

# Indeed search URL template
INDEED_URL = "https://ca.indeed.com/jobs?q={query}&l=Canada&sort=date&fromage=14"


class JobsScraper(BaseScraper):
    name = "JobsScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        signals.extend(self._scrape_careers_page(firm))
        signals.extend(self._scrape_indeed(firm))
        return signals

    def _scrape_careers_page(self, firm: dict) -> list[dict]:
        url = firm.get("careers_url", "")
        if not url:
            return []

        soup = self.get_soup(url)
        if not soup:
            return []

        signals = []
        # Generic: find all <a> tags with job-like text
        job_keywords = ["associate", "partner", "counsel", "lawyer", "attorney",
                        "paralegal", "clerk", "student"]

        for tag in soup.find_all(["a", "h2", "h3", "h4", "li"], limit=150):
            text = tag.get_text(" ", strip=True)
            if len(text) < 15 or len(text) > 300:
                continue
            if not any(kw in text.lower() for kw in job_keywords):
                continue

            link = ""
            if tag.name == "a":
                href = tag.get("href", "")
                link = href if href.startswith("http") else firm["website"] + href

            cls = classifier.top_department(text)
            if not cls:
                continue

            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="job_posting",
                title=text[:200],
                url=link or url,
                department=cls["department"],
                department_score=cls["score"] * 2.0,
                matched_keywords=cls["matched_keywords"],
            ))

        # Deduplicate by title
        seen = set()
        unique = []
        for s in signals:
            if s["title"] not in seen:
                seen.add(s["title"])
                unique.append(s)

        return unique[:20]

    def _scrape_indeed(self, firm: dict) -> list[dict]:
        short = firm["short"].replace(" ", "+")
        url = INDEED_URL.format(query=f'"{short}"+lawyer')
        soup = self.get_soup(url)
        if not soup:
            return []

        signals = []
        for card in soup.find_all("div", class_=lambda c: c and "job_seen_beacon" in c)[:10]:
            title_tag = card.find("h2", class_=lambda c: c and "jobTitle" in str(c))
            if not title_tag:
                continue
            title = title_tag.get_text(" ", strip=True)
            link_tag = card.find("a", href=True)
            link = "https://ca.indeed.com" + link_tag["href"] if link_tag else url

            full = f"{firm['short']} {title}"
            cls = classifier.top_department(full)
            if not cls:
                continue

            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="job_posting",
                title=f"[Indeed] {title}",
                url=link,
                department=cls["department"],
                department_score=cls["score"] * 2.0,
                matched_keywords=cls["matched_keywords"],
            ))

        return signals
