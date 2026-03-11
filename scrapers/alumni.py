"""
AlumniTrackScraper — law school alumni and recruit-season intelligence.

Signals:
  1. Law school recruit pages listing which firms are hiring (OCI/recruit season)
     → multiple articling offers in a dept = expansion signal
  2. Law school alumni newsletters mentioning specific firms
  3. Law school "alumni of note" sections (partner appointments, notable moves)
  4. Firm-specific articling class sizes (posted by firms in fall)

Sources:
  Osgoode Hall         osgoode.yorku.ca/careers/recruit
  U of T Law           law.utoronto.ca/students/careers
  Queens Law           law.queensu.ca/students/careers
  UBC Law              allard.ubc.ca/students/careers
  U of Calgary Law     law.ucalgary.ca/students/careers
  Dalhousie Law        dal.ca/law/careers
  McGill Law           law.mcgill.ca/students
  Ottawa Law           commonlaw.uottawa.ca/students
  University of Alberta law.ualberta.ca/students/careers
  Firm own recruit page (articling/student section)

Recruit-season queries via Google News (August-November peak):
  "{firm}" articling recruit 2025 site:law.*.ca
"""

import re
from urllib.parse import quote_plus

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

import feedparser

classifier = DepartmentClassifier()

LAW_SCHOOL_RECRUIT_PAGES = [
    {"school": "Osgoode",    "url": "https://www.osgoode.yorku.ca/programs/jd-program/career-development/recruit/"},
    {"school": "U of T",     "url": "https://www.law.utoronto.ca/students/careers-professional-development"},
    {"school": "Queens",     "url": "https://law.queensu.ca/students/careers"},
    {"school": "UBC Allard", "url": "https://allard.ubc.ca/students/careers"},
    {"school": "U Calgary",  "url": "https://law.ucalgary.ca/current-students/career-services"},
    {"school": "Dal Law",    "url": "https://www.dal.ca/faculty/law/current-students/jd-program/career.html"},
    {"school": "McGill Law", "url": "https://www.mcgill.ca/law/students/careers"},
    {"school": "Ottawa Law", "url": "https://www.uottawa.ca/faculty-law/students/careers"},
    {"school": "U Alberta",  "url": "https://law.ualberta.ca/students/career-services"},
]

ARTICLING_RE = re.compile(
    r"articling|articled\s+clerk|student|summer\s+student|recruit\s+202[3-9]"
    r"|summer\s+associate|oci|on-campus\s+interview",
    re.IGNORECASE,
)

PARTNER_APPT_RE = re.compile(
    r"alumna?\s+(?:named|appointed|promoted|named)\s+partner"
    r"|partner\s+at\s+\w+"
    r"|alumna?\s+joins\s+\w+\s+(?:as|to)",
    re.IGNORECASE,
)

GOOG = "https://news.google.com/rss/search?q={q}&hl=en-CA&gl=CA&ceid=CA:en"


class AlumniTrackScraper(BaseScraper):
    name = "AlumniTrackScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        seen: set = set()
        firm_tokens = [firm["short"].lower(), firm["name"].split()[0].lower()] + \
                      [a.lower() for a in firm.get("alt_names", [])]

        # 1 — Law school recruit pages
        signals.extend(self._scrape_recruit_pages(firm, firm_tokens, seen))

        # 2 — Firm's own student/articling page
        signals.extend(self._scrape_firm_recruit(firm, seen))

        # 3 — Google News: recruit season articles
        signals.extend(self._google_recruit_news(firm, firm_tokens, seen))

        return signals[:12]

    def _scrape_recruit_pages(self, firm, firm_tokens, seen) -> list[dict]:
        signals = []
        for src in LAW_SCHOOL_RECRUIT_PAGES:
            soup = self.get_soup(src["url"])
            if not soup:
                continue
            text = soup.get_text(" ", strip=True)
            if not any(t in text.lower() for t in firm_tokens):
                continue

            # Count mentions (multiple mentions = larger articling class)
            count = sum(text.lower().count(t) for t in firm_tokens)

            # Find surrounding context
            for tag in soup.find_all(["li", "td", "p", "span"], limit=200):
                chunk = tag.get_text(" ", strip=True)
                if not any(t in chunk.lower() for t in firm_tokens):
                    continue
                if len(chunk) < 10 or len(chunk) > 400:
                    continue

                key = chunk[:80]
                if key in seen:
                    continue

                cls = classifier.classify(f"{firm['short']} articling student law", top_n=1)
                dept = cls[0]["department"] if cls else "Corporate/M&A"

                # Score scales with count: more school placements = more signals
                score = min(count * 0.8, 5.0)

                signals.append(self._make_signal(
                    firm_id=firm["id"], firm_name=firm["name"],
                    signal_type="job_posting",
                    title=f"[{src['school']} Recruit] {firm['short']} — {chunk[:120]}",
                    url=src["url"],
                    department=dept,
                    department_score=score,
                    matched_keywords=["articling", "recruit", "student"],
                ))
                seen.add(key)
                break  # one signal per school per firm

        return signals

    def _scrape_firm_recruit(self, firm, seen) -> list[dict]:
        """Scrape firm's articling/student program page for class details."""
        base = firm["website"].rstrip("/")
        candidate_urls = [
            f"{base}/en/careers/students",
            f"{base}/careers/students",
            f"{base}/en/students",
            f"{base}/students",
            f"{base}/en/careers/articling",
        ]
        for url in candidate_urls:
            soup = self.get_soup(url)
            if not soup:
                continue
            text = soup.get_text(" ", strip=True)
            if not ARTICLING_RE.search(text):
                continue

            # Extract headcount clues: "We hire X students per year"
            count_match = re.search(r"(\d+)\s+(?:articling\s+)?students?", text, re.I)
            count = int(count_match.group(1)) if count_match else 1

            cls = classifier.classify(text[:500], top_n=1)
            dept = cls[0]["department"] if cls else "Corporate/M&A"

            return [self._make_signal(
                firm_id=firm["id"], firm_name=firm["name"],
                signal_type="job_posting",
                title=f"[Student Recruit] {firm['short']} articling program — {count} students",
                url=url,
                department=dept,
                department_score=min(count * 0.5, 4.0),
                matched_keywords=["articling", "student", "recruit"],
            )]
        return []

    def _google_recruit_news(self, firm, firm_tokens, seen) -> list[dict]:
        q   = f'"{firm["short"]}" articling recruit OR "summer student" law school'
        url = GOOG.format(q=quote_plus(q))
        signals = []
        try:
            feed = feedparser.parse(url, request_headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalTracker/2.0)"
            })
        except Exception:
            return []

        for entry in (feed.entries or [])[:10]:
            title   = entry.get("title", "") or ""
            summary = entry.get("summary", "") or ""
            link    = entry.get("link", url) or url
            if link in seen:
                continue
            full = f"{title} {summary}"
            if not any(t in full.lower() for t in firm_tokens):
                continue

            is_partner = bool(PARTNER_APPT_RE.search(full))
            sig_type   = "lateral_hire" if is_partner else "job_posting"
            w = 3.5 if is_partner else 1.5

            cls = classifier.classify(full, top_n=1)
            if not cls:
                continue
            c = cls[0]

            signals.append(self._make_signal(
                firm_id=firm["id"], firm_name=firm["name"],
                signal_type=sig_type,
                title=f"[Alumni/Recruit] {title[:160]}",
                body=summary[:400],
                url=link,
                department=c["department"],
                department_score=c["score"] * w,
                matched_keywords=c["matched_keywords"],
            ))
            seen.add(link)
        return signals
