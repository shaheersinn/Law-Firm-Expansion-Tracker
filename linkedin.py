"""
LinkedIn Profile & Company Feed Scraper
=========================================
LinkedIn is the single richest real-time signal for lateral hires and
practice expansion. Even without API access, the public company feed and
people search pages expose meaningful signals.

What we track:
  1. Public firm company page posts → new hire announcements, deal tombstones
  2. "People Also Viewed" on firm pages → competitor movement signals
  3. Public profile job-title changes (via Google cache search)

Approach:
  - Company feed: /company/{slug}/posts/ (public, no login required for basic)
  - Google site: search for "[firm name] site:linkedin.com/in lawyer joined"
    This finds cached snippets of updated LinkedIn profiles without login.

Signal weight boost:
  Partner-level hires get 3.5× (highest in the system)
  Associate-level: 2.0×
  Student/articling: 1.5×
"""

import re
from bs4 import BeautifulSoup
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

LINKEDIN_BASE = "https://www.linkedin.com"

HIRE_PHRASES = [
    "delighted to welcome", "excited to announce", "pleased to welcome",
    "joins our", "has joined", "joining our team", "welcome to the team",
    "new partner", "new associate", "new counsel", "new addition",
    "expanding our", "growing our", "strengthening our",
]

SENIORITY = {
    "partner":        3.5,
    "counsel":        2.5,
    "senior counsel": 2.5,
    "associate":      2.0,
    "articling":      1.5,
    "student":        1.5,
    "law clerk":      1.5,
}


class LinkedInScraper(BaseScraper):
    name = "LinkedInScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        signals.extend(self._scrape_company_feed(firm))
        signals.extend(self._scrape_google_profile_cache(firm))
        return signals

    # ── Company feed (public posts) ────────────────────────────────────
    def _scrape_company_feed(self, firm: dict) -> list[dict]:
        signals = []
        slug = firm.get("linkedin_slug", "")
        if not slug:
            return signals

        url = f"{LINKEDIN_BASE}/company/{slug}/posts/?feedView=all"
        resp = self._get(url, extra_headers={
            "Accept": "text/html,application/xhtml+xml",
            "Referer": "https://www.linkedin.com/",
        })
        if not resp:
            return signals

        soup = BeautifulSoup(resp.text, "html.parser")
        posts = soup.find_all(
            ["div", "article", "section"],
            class_=re.compile(r"feed-shared|update-components|post|entity", re.I)
        )[:15]

        for post in posts:
            text = post.get_text(separator=" ", strip=True)
            text_lower = text.lower()

            if not any(p in text_lower for p in HIRE_PHRASES):
                continue

            title_tag = post.find(["span", "p", "div"], class_=re.compile(r"commentary|text|body", re.I))
            title = title_tag.get_text(strip=True)[:160] if title_tag else text[:160]

            sig_type = "lateral_hire" if any(p in text_lower for p in HIRE_PHRASES) else "press_release"
            weight   = self._seniority_weight(text_lower)

            classifications = classifier.classify(text, top_n=1)
            if not classifications:
                continue

            cls = classifications[0]
            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type=sig_type,
                title=f"[LinkedIn] {title}",
                body=text[:600],
                url=url,
                department=cls["department"],
                department_score=cls["score"] * weight,
                matched_keywords=cls["matched_keywords"],
            ))

        self.logger.info(f"[{firm['short']}] LinkedIn feed: {len(signals)} signal(s)")
        return signals

    # ── Google cache of LinkedIn profiles (no login needed) ────────────
    def _scrape_google_profile_cache(self, firm: dict) -> list[dict]:
        """
        Query Google for recent LinkedIn profile mentions tied to the firm.
        Pattern: site:linkedin.com/in "[firm short name]" (lawyer|partner|associate)
        Google returns cached snippets which often reveal recent job changes.
        """
        signals = []
        query = f'site:linkedin.com/in "{firm["short"]}" lawyer OR partner OR associate'
        url = f"https://www.google.com/search?q={query.replace(' ', '+')}&num=10&hl=en"

        resp = self._get(url, extra_headers={
            "Accept": "text/html",
            "Referer": "https://www.google.com/",
        })
        if not resp:
            return signals

        soup = BeautifulSoup(resp.text, "html.parser")
        results = soup.find_all("div", class_=re.compile(r"^g$|result|tF2Cxc", re.I))[:10]

        for result in results:
            text = result.get_text(separator=" ", strip=True)
            text_lower = text.lower()

            if firm["short"].lower() not in text_lower:
                continue

            # Look for signals of recently added firm affiliation
            joined_signals = [
                "joined", "now at", "currently at", "new role",
                "started new position", "excited to share",
            ]
            if not any(p in text_lower for p in joined_signals):
                continue

            title_tag = result.find(["h3", "div"], class_=re.compile(r"title|LC20lb|DKV0Md", re.I))
            title = title_tag.get_text(strip=True) if title_tag else text[:150]

            weight = self._seniority_weight(text_lower)
            classifications = classifier.classify(text, top_n=1)
            if not classifications:
                continue

            cls = classifications[0]
            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="lateral_hire",
                title=f"[LinkedIn/Google] {title}",
                body=text[:600],
                url="https://www.linkedin.com",
                department=cls["department"],
                department_score=cls["score"] * weight,
                matched_keywords=cls["matched_keywords"],
            ))

        self.logger.info(f"[{firm['short']}] LinkedIn/Google cache: {len(signals)} signal(s)")
        return signals

    def _seniority_weight(self, text: str) -> float:
        for kw, w in SENIORITY.items():
            if kw in text:
                return w
        return 1.5
