"""
JobsScraper
Monitors firm careers pages and job aggregators for associate,
articling, and counsel postings.

Sources:
  - Firm careers page (direct scrape)
  - Indeed Canada (firm name search)
  - LinkedIn Jobs RSS
"""

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier
from urllib.parse import quote_plus

_clf = DepartmentClassifier()

JOBS_WEIGHT = 2.0

LEGAL_JOB_TITLES = [
    "associate", "articling", "student", "counsel", "lawyer",
    "solicitor", "barrister", "legal", "partner", "clerk",
]

SENIORITY_BOOST = {
    "partner": 3.0,
    "counsel": 2.5,
    "associate": 2.0,
    "articling": 2.0,
    "student": 1.8,
}


class JobsScraper(BaseScraper):
    name = "JobsScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []

        # ── Firm's own careers page ─────────────────────────────────────
        careers_url = firm.get("careers_url", "")
        if careers_url:
            soup = self._soup(careers_url, timeout=20)
            if soup:
                for a in (soup.find_all("a", href=True) or [])[:80]:
                    text = self._clean(a.get_text())
                    lower = text.lower()
                    if not any(t in lower for t in LEGAL_JOB_TITLES):
                        continue
                    if len(text) < 10:
                        continue

                    href = a["href"]
                    if not href.startswith("http"):
                        from urllib.parse import urljoin
                        href = urljoin(careers_url, href)

                    # Estimate weight from seniority
                    weight = JOBS_WEIGHT
                    for title, w in SENIORITY_BOOST.items():
                        if title in lower:
                            weight = max(weight, w)
                            break

                    dept, score, kw = _clf.top_department(text)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="job_posting",
                        title=f"[{firm['short']}] {text[:160]}",
                        body=text,
                        url=href,
                        department=dept,
                        department_score=score * weight,
                        matched_keywords=kw,
                    ))
                    if len(signals) >= 8:
                        return signals

        # ── Indeed Canada RSS ──────────────────────────────────────────
        try:
            import feedparser
            q = quote_plus(f'"{firm["short"]}" lawyer OR associate OR articling')
            indeed_url = f"https://www.indeed.com/rss?q={q}&l=Canada&sort=date"
            try:
                feed = feedparser.parse(indeed_url)
                for entry in (feed.entries or [])[:10]:
                    title   = entry.get("title", "")
                    summary = entry.get("summary", "")
                    link    = entry.get("link", indeed_url)
                    pub     = entry.get("published", "")
                    full    = f"{title} {summary}"
                    lower   = full.lower()
                    if not any(t in lower for t in LEGAL_JOB_TITLES):
                        continue
                    dept, score, kw = _clf.top_department(full)
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="job_posting",
                        title=f"[Indeed] {title[:160]}",
                        body=summary[:400],
                        url=link,
                        department=dept,
                        department_score=score * JOBS_WEIGHT,
                        matched_keywords=kw,
                        published_at=pub,
                    ))
            except Exception as e:
                self.logger.debug(f"Indeed RSS: {e}")
        except ImportError:
            pass

        return signals[:10]
