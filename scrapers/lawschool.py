"""
Law School Recruiting Signals Scraper
======================================
Law firm recruiting is a leading indicator of expansion — firms commit to
articling students and summer associates 12-18 months before those students
arrive, which reflects where the firm expects to be growing.

Key insight: When a firm suddenly increases its OCI (On-Campus Interview)
presence at a particular school, or when their articling postings mention
new practice areas, it signals *planned* expansion — not just current state.

What we track:
  1. OCI / recruit postings on law school career portals
  2. Articling student postings on firm websites and job boards
  3. Practice area mentions in student recruitment materials
  4. Summer associate intake announcements

Sources:
  - University of Toronto Law (utlaw.ca/recruit)
  - Osgoode Hall Law School (osgoode.yorku.ca/programs/jd/career-development)
  - University of Alberta Law
  - University of British Columbia Law
  - McGill Law Faculty
  - University of Ottawa Law
  - Indeed / LinkedIn for "articling student" + firm name
  - GreatStudentJobs (student legal job board)
  - lawrecruits.com (Canadian law student recruitment platform)
"""

import re
from bs4 import BeautifulSoup
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

# Law school career portals
LAW_SCHOOL_SOURCES = [
    {
        "name": "U of T Law",
        "url": "https://ultravires.ca/jobs/",
        "secondary": "https://ultravires.ca/recruit/",
    },
    # osgoode.yorku.ca — REMOVED: persistent SSL cert verification failure
    # lawrecruits.com  — REMOVED: persistent connection timeout
    {
        "name": "GreatStudentJobs Legal",
        "url": "https://www.greatstudentjobs.com/jobs/?area=law",
    },
]

# Terms that identify student/recruit postings
RECRUIT_KEYWORDS = [
    "articling student", "articling clerk", "summer student", "summer associate",
    "law student", "1L", "2L", "recruit", "OCI", "on-campus interview",
    "summer program", "articling program", "articling positions",
    "student recruitment", "future associate",
]

# Practice area mentions in student materials are especially valuable —
# they represent where the firm PLANS to grow
FORWARD_LOOKING_PHRASES = [
    "growing practice", "expanding team", "new practice group",
    "building our", "launching", "establishing", "develop our",
    "increasing demand", "high growth", "emerging area",
    "invest in", "strategic priority", "firm's focus",
]


class LawSchoolScraper(BaseScraper):
    name = "LawSchoolScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        signals.extend(self._scrape_law_school_boards(firm))
        signals.extend(self._scrape_indeed_recruit(firm))
        signals.extend(self._scrape_recruit_postings_on_firm_site(firm))
        return signals

    # ------------------------------------------------------------------ #
    #  Law school career portals
    # ------------------------------------------------------------------ #

    def _scrape_law_school_boards(self, firm: dict) -> list[dict]:
        signals = []

        for source in LAW_SCHOOL_SOURCES:
            url = source["url"]
            response = self._get(url)
            if not response:
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            page_text = soup.get_text(separator=" ")

            # Check if firm appears on this page at all
            firm_mentioned = (
                firm["short"].lower() in page_text.lower()
                or firm["name"].split()[0].lower() in page_text.lower()
            )
            if not firm_mentioned:
                continue

            # Find job/posting elements
            job_cards = soup.find_all(
                ["div", "li", "article", "tr"],
                class_=re.compile(r"job|posting|listing|result|position|opening", re.I)
            )

            for card in job_cards[:30]:
                card_text = card.get_text(separator=" ", strip=True)
                card_lower = card_text.lower()

                # Must mention the firm
                if firm["short"].lower() not in card_lower and \
                   firm["name"].split()[0].lower() not in card_lower:
                    continue

                # Must mention a recruit keyword
                if not any(kw in card_lower for kw in RECRUIT_KEYWORDS):
                    continue

                title_tag = card.find(["h2", "h3", "h4", "a", "strong"])
                title = title_tag.get_text(strip=True) if title_tag else card_text[:120]

                # Score higher if forward-looking phrases present
                has_forward = any(p in card_lower for p in FORWARD_LOOKING_PHRASES)
                boost = 2.0 if has_forward else 1.0

                classifications = classifier.classify(card_text, top_n=2)
                if not classifications:
                    continue

                for cls in classifications[:1]:
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="recruit_posting",
                        title=f"[{source['name']}] {title}",
                        body=card_text[:800],
                        url=url,
                        department=cls["department"],
                        department_score=cls["score"] * boost,
                        matched_keywords=cls["matched_keywords"],
                    ))

        self.logger.info(f"[{firm['short']}] Law school boards: {len(signals)} signal(s)")
        return signals

    # ------------------------------------------------------------------ #
    #  Indeed Canada — articling/student postings
    # ------------------------------------------------------------------ #

    def _scrape_indeed_recruit(self, firm: dict) -> list[dict]:
        signals = []
        firm_encoded = firm["name"].split()[0]
        url = f"https://ca.indeed.com/jobs?q={firm_encoded}+articling+student&l=Canada&sort=date"

        response = self._get(url)
        if not response:
            return signals

        soup = BeautifulSoup(response.text, "html.parser")

        for card in soup.find_all("div", class_=re.compile(r"job_seen_beacon|jobCard|result", re.I))[:15]:
            title_tag = card.find(["h2", "h3"], class_=re.compile(r"title|jobTitle", re.I))
            company_tag = card.find(class_=re.compile(r"company|employer", re.I))
            title = title_tag.get_text(strip=True) if title_tag else ""
            company = company_tag.get_text(strip=True) if company_tag else ""

            if not title:
                continue

            # Filter to this firm only
            if firm["short"].lower() not in company.lower() and \
               firm["name"].split()[0].lower() not in company.lower():
                continue

            desc_tag = card.find(class_=re.compile(r"summary|description|snippet", re.I))
            body = desc_tag.get_text(strip=True) if desc_tag else card.get_text(separator=" ", strip=True)
            full_text = f"{title} {body}"

            # Look for forward-looking phrases — big signal
            has_forward = any(p in full_text.lower() for p in FORWARD_LOOKING_PHRASES)

            classifications = classifier.classify(full_text, top_n=1)
            if not classifications:
                continue

            cls = classifications[0]
            boost = 2.5 if has_forward else 1.0

            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="recruit_posting",
                title=f"[Indeed Recruit] {title}",
                body=body[:600],
                url=url,
                department=cls["department"],
                department_score=cls["score"] * boost,
                matched_keywords=cls["matched_keywords"],
            ))

        self.logger.info(f"[{firm['short']}] Indeed recruit: {len(signals)} signal(s)")
        return signals

    # ------------------------------------------------------------------ #
    #  Firm's own student recruitment page
    # ------------------------------------------------------------------ #

    def _scrape_recruit_postings_on_firm_site(self, firm: dict) -> list[dict]:
        signals = []
        base = firm["website"].rstrip("/")

        recruit_paths = [
            "/careers/students",
            "/careers/articling",
            "/careers/student-programs",
            "/students",
            "/en/careers/students",
            "/en-ca/careers/students",
            "/articling",
            "/summer-students",
            "/recruitment",
        ]

        for path in recruit_paths:
            url = base + path
            response = self._get(url)
            if not response:
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            page_text = soup.get_text(separator=" ", strip=True)

            if len(page_text) < 100:
                continue

            # Extract practice areas mentioned on the student recruitment page
            # These are deliberately chosen by the firm to attract students
            practice_sections = soup.find_all(
                ["div", "section", "p", "li"],
                class_=re.compile(r"practice|area|group|department|team", re.I)
            )

            for section in practice_sections[:20]:
                text = section.get_text(separator=" ", strip=True)
                if len(text) < 20:
                    continue

                has_forward = any(p in text.lower() for p in FORWARD_LOOKING_PHRASES)
                classifications = classifier.classify(text, top_n=1)
                if not classifications:
                    continue

                cls = classifications[0]
                if cls["score"] < 2.0:
                    continue

                boost = 2.0 if has_forward else 1.0
                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="recruit_posting",
                    title=f"[Student Recruit] {firm['short']} — {cls['department']}",
                    body=text[:600],
                    url=url,
                    department=cls["department"],
                    department_score=cls["score"] * boost,
                    matched_keywords=cls["matched_keywords"],
                ))

            if signals:
                break  # found a working student page

        self.logger.info(f"[{firm['short']}] Firm recruit page: {len(signals)} signal(s)")
        return signals
