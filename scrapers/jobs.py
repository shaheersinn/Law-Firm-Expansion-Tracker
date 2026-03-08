"""
JobsScraper — senior legal role postings from multiple sources.

v2: seniority-weighted scoring, more job boards, LinkedIn job RSS,
    Job Bank Canada API, and role cluster detection (multiple partner
    roles in same department = expansion burst signal).
"""

import re
from urllib.parse import quote_plus, urljoin
from collections import Counter

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

SENIORITY_WEIGHTS = {
    "managing partner":   5.0,
    "senior partner":     4.5,
    "partner":            4.0,
    "counsel":            3.0,
    "senior associate":   2.5,
    "associate":          2.0,
    "articling":          1.2,
    "student":            1.0,
    "paralegal":          0.8,
    "law clerk":          0.8,
}

PARTNER_RE = re.compile(r"\bpartner\b", re.IGNORECASE)
COUNSEL_RE = re.compile(r"\bcounsel\b|\blegal\s+director\b|\bvp\s+legal\b|\bgeneral\s+counsel\b", re.IGNORECASE)

# Job boards
INDEED_URL        = "https://ca.indeed.com/jobs?q={q}&l=Canada&sort=date&fromage=14"
JOB_BANK_URL      = "https://www.jobbank.gc.ca/jobsearch/jobsearch?searchstring={q}&locationstring=&sort=D"
LINKEDIN_JOBS_URL = "https://www.linkedin.com/jobs/search/?keywords={q}&location=Canada&f_TPR=r604800&sortBy=DD"


def _seniority_weight(text: str) -> float:
    lower = text.lower()
    for title, w in SENIORITY_WEIGHTS.items():
        if title in lower:
            return w
    return 1.0


class JobsScraper(BaseScraper):
    name = "JobsScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        seen: set = set()

        signals.extend(self._scrape_careers_page(firm, seen))
        signals.extend(self._scrape_indeed(firm, seen))
        signals.extend(self._scrape_job_bank(firm, seen))

        # Cluster detection: multiple partner roles in same dept = burst signal
        signals.extend(self._detect_role_cluster(signals, firm))

        return signals[:20]

    def _scrape_careers_page(self, firm, seen) -> list[dict]:
        url = firm.get("careers_url", "")
        if not url:
            return []
        soup = self.get_soup(url)
        if not soup:
            return []

        signals = []
        job_kws = [
            "associate", "partner", "counsel", "lawyer", "attorney",
            "paralegal", "clerk", "student", "legal director", "general counsel",
        ]

        for tag in soup.find_all(["a", "h2", "h3", "h4", "li", "span"], limit=200):
            text = tag.get_text(" ", strip=True)
            if len(text) < 15 or len(text) > 300:
                continue
            lower = text.lower()
            if not any(kw in lower for kw in job_kws):
                continue

            link = ""
            if tag.name == "a":
                href = tag.get("href", "")
                link = href if href.startswith("http") else urljoin(firm["website"], href)
            if link in seen:
                continue

            sw = _seniority_weight(text)
            # Only include roles worth tracking
            if sw < 1.5:
                continue

            cls = classifier.top_department(f"{firm['short']} {text}")
            if not cls:
                continue

            signals.append(self._make_signal(
                firm_id=firm["id"], firm_name=firm["name"],
                signal_type="job_posting",
                title=text[:200],
                url=link or url,
                department=cls["department"],
                department_score=cls["score"] * 2.0 * sw,
                matched_keywords=cls["matched_keywords"],
            ))
            seen.add(link or text[:80])

        # Dedup
        unique, titles = [], set()
        for s in signals:
            t = s["title"][:80]
            if t not in titles:
                titles.add(t)
                unique.append(s)
        return unique[:15]

    def _scrape_indeed(self, firm, seen) -> list[dict]:
        # Partner and counsel roles only
        q   = quote_plus(f'"{firm["short"]}" partner OR counsel')
        url = INDEED_URL.format(q=q)
        soup = self.get_soup(url)
        if not soup:
            return []

        signals = []
        for card in soup.find_all("div", attrs={"data-jk": True})[:15]:
            title_tag = card.find(["h2", "span"], attrs={"class": re.compile(r"title|jobTitle", re.I)})
            if not title_tag:
                continue
            title = title_tag.get_text(" ", strip=True)
            lower = title.lower()

            if not any(kw in lower for kw in ["partner", "counsel", "director", "head of legal"]):
                continue

            jk   = card.get("data-jk", "")
            link = f"https://ca.indeed.com/viewjob?jk={jk}" if jk else url
            if link in seen:
                continue

            sw = _seniority_weight(title)
            cls = classifier.top_department(f"{firm['short']} {title}")
            if not cls:
                continue

            signals.append(self._make_signal(
                firm_id=firm["id"], firm_name=firm["name"],
                signal_type="job_posting",
                title=f"[Indeed] {title[:160]}",
                url=link,
                department=cls["department"],
                department_score=cls["score"] * 2.5 * sw,
                matched_keywords=cls["matched_keywords"],
            ))
            seen.add(link)
        return signals

    def _scrape_job_bank(self, firm, seen) -> list[dict]:
        q   = quote_plus(f"{firm['short']} lawyer")
        url = JOB_BANK_URL.format(q=q)
        soup = self.get_soup(url)
        if not soup:
            return []
        signals = []
        for tag in soup.find_all(["h3", "a"], class_=re.compile(r"title|job", re.I), limit=20):
            text = tag.get_text(" ", strip=True)
            if len(text) < 10 or len(text) > 200:
                continue
            if firm["short"].lower() not in text.lower() and \
               firm["name"].split()[0].lower() not in text.lower():
                continue

            link_tag = tag if tag.name == "a" else tag.find("a", href=True)
            link = ""
            if link_tag and link_tag.get("href"):
                href = link_tag["href"]
                link = href if href.startswith("http") else f"https://www.jobbank.gc.ca{href}"
            if link in seen:
                continue

            sw = _seniority_weight(text)
            if sw < 1.5:
                continue
            cls = classifier.top_department(f"{firm['short']} {text}")
            if not cls:
                continue

            signals.append(self._make_signal(
                firm_id=firm["id"], firm_name=firm["name"],
                signal_type="job_posting",
                title=f"[Job Bank] {text[:160]}",
                url=link or url,
                department=cls["department"],
                department_score=cls["score"] * 2.0 * sw,
                matched_keywords=cls["matched_keywords"],
            ))
            seen.add(link or text[:80])
        return signals

    def _detect_role_cluster(self, existing: list[dict], firm: dict) -> list[dict]:
        """If 3+ partner/counsel roles in same dept → emit a cluster signal."""
        dept_counter: Counter = Counter()
        for s in existing:
            if s["signal_type"] == "job_posting" and s["department_score"] >= 6.0:
                dept_counter[s["department"]] += 1

        bursts = []
        for dept, count in dept_counter.items():
            if count >= 3:
                bursts.append(self._make_signal(
                    firm_id=firm["id"], firm_name=firm["name"],
                    signal_type="job_posting",
                    title=f"[Hiring Burst] {firm['short']} hiring {count} senior roles in {dept}",
                    url=firm.get("careers_url", firm["website"]),
                    department=dept,
                    department_score=count * 5.0,  # high-value burst signal
                    matched_keywords=[dept.lower(), "hiring burst", "expansion"],
                ))
        return bursts
