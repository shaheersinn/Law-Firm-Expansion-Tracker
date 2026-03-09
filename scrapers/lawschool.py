"""
LawSchoolScraper
Monitors law school job boards and firm student recruit pages.

Signal research insight:
  "Firm increases articling class size year-over-year → growing practice groups."
  "Firm recruits at more law schools than prior year → expanding hiring funnel."

Sources:
  - Ultra Vires (uoftlaw student newspaper)
  - GreatStudentJobs Canada
  - Firm student/articling pages
  - Law school career postings (Osgoode, UofT, UBC, Western, Queens)
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

LAWSCHOOL_WEIGHT = 2.0

STUDENT_SOURCES = [
    {
        "name": "Ultra Vires",
        "url": "https://ultravires.ca/",
        "rss": "https://ultravires.ca/feed/",
    },
    {
        "name": "GreatStudentJobs",
        "url": "https://www.greatstudentjobs.com/law-jobs/",
        "rss": None,
    },
]

STUDENT_KEYWORDS = [
    "articling", "summer student", "2l recruit", "1l recruit",
    "law student", "summer associate", "student program",
    "recruit", "articling position", "articling applications",
    "interview week", "oci", "on-campus interview",
]

POSITIVE_SIGNALS = [
    "increase", "expanded", "additional", "new", "more positions",
    "growing", "larger class", "additional spots",
]


class LawSchoolScraper(BaseScraper):
    name = "LawSchoolScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        firm_names = [firm["short"], firm["name"].split()[0]] + firm.get("alt_names", [])

        # ── Ultra Vires RSS ────────────────────────────────────────────
        try:
            import feedparser
            for src in STUDENT_SOURCES:
                if not src.get("rss"):
                    continue
                try:
                    feed = feedparser.parse(src["rss"])
                    for entry in (feed.entries or [])[:25]:
                        title   = entry.get("title", "")
                        summary = entry.get("summary", "")
                        link    = entry.get("link", src["url"])
                        pub     = entry.get("published", "")
                        full    = f"{title} {summary}"
                        lower   = full.lower()

                        if not any(n.lower() in lower for n in firm_names):
                            continue
                        if not any(k in lower for k in STUDENT_KEYWORDS):
                            continue

                        dept, score, kw = _clf.top_department(full)
                        signals.append(self._make_signal(
                            firm_id=firm["id"],
                            firm_name=firm["name"],
                            signal_type="recruit_posting",
                            title=f"[{src['name']}] {title[:160]}",
                            body=summary[:400],
                            url=link,
                            department=dept,
                            department_score=score * LAWSCHOOL_WEIGHT,
                            matched_keywords=kw,
                            published_at=pub,
                        ))
                except Exception as e:
                    self.logger.debug(f"LawSchool {src['name']}: {e}")
        except ImportError:
            pass

        # ── Firm's own student/articling page ──────────────────────────
        for suffix in ["/students", "/articling", "/careers/students",
                       "/en/students", "/summer-students"]:
            url = firm["website"].rstrip("/") + suffix
            soup = self._soup(url, timeout=15)
            if not soup:
                continue
            text = soup.get_text(separator=" ")
            lower = text.lower()
            if not any(k in lower for k in STUDENT_KEYWORDS):
                break
            dept, score, kw = _clf.top_department(text[:1000])
            boost = any(p in lower for p in POSITIVE_SIGNALS)
            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="recruit_posting",
                title=f"[{firm['short']}] Articling/student page",
                body=text[:600],
                url=url,
                department=dept,
                department_score=score * LAWSCHOOL_WEIGHT * (1.5 if boost else 1.0),
                matched_keywords=kw,
            ))
            break

        return signals[:6]
