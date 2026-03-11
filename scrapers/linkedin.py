"""
LinkedIn Scraper — RapidAPI Backend
=====================================
Uses RapidAPI instead of scraping LinkedIn directly (which blocks bots).

Three data sources, all via RapidAPI:

  1. PEOPLE SEARCH  (Fresh LinkedIn Profile Data)
     → Find lawyers who recently changed jobs to/from tracked firms
     → Endpoint: /get-profile-data-by-url or /search-employees
     → Best signal: lateral_hire (3.5× weight for partners)

  2. JOB POSTINGS  (LinkedIn Jobs Search)
     → Active job postings by firm (where they're hiring = where they're growing)
     → Endpoint: /search-jobs
     → Signals: job_posting / lateral_hire (for partner-level roles)

  3. COMPANY POSTS  (LinkedIn Company Updates)
     → Company page announcements (hire announcements, deals, office openings)
     → Endpoint: /company-updates
     → Signals: lateral_hire, press_release

RapidAPI key setup:
  1. Go to https://rapidapi.com and create a free account
  2. Subscribe to the APIs below (each has a free tier)
  3. Add RAPIDAPI_KEY to GitHub Secrets
  4. That's it — the key is shared across all RapidAPI endpoints

API subscriptions needed (all have free tiers):
  - Fresh LinkedIn Profile Data: https://rapidapi.com/freshdata-freshdata-default/api/fresh-linkedin-profile-data
  - LinkedIn Jobs Search:        https://rapidapi.com/jaypat87/api/linkedin-jobs-search
  - LinkedIn Company Updates:    https://rapidapi.com/vahoora/api/linkedin-company-updates

Cost estimate for 26 firms × daily run:
  - People search: ~52 calls/day (2/firm) → free tier is usually 100–200/month
  - Job search:    ~26 calls/day          → typically free
  - Company posts: ~26 calls/day          → typically free

Changelog:
  - Initial version: replaces direct LinkedIn HTML scraping with RapidAPI
  - Graceful degradation: if RAPIDAPI_KEY is empty, logs a warning and returns []
  - Rate-limit aware: 1s sleep between API calls
"""

import os
import re
import time
import logging

try:
    from scrapers.base import BaseScraper
    from classifier.department import DepartmentClassifier
except ImportError:
    from base import BaseScraper
    from department import DepartmentClassifier

classifier = DepartmentClassifier()
logger     = logging.getLogger("scrapers.LinkedInScraper")

RAPIDAPI_KEY  = os.getenv("RAPIDAPI_KEY", "")
RAPIDAPI_HOST_PROFILES = "fresh-linkedin-profile-data.p.rapidapi.com"
RAPIDAPI_HOST_JOBS     = "linkedin-jobs-search.p.rapidapi.com"
RAPIDAPI_HOST_COMPANY  = "linkedin-company-updates.p.rapidapi.com"

# ── Seniority multipliers ─────────────────────────────────────────────────────
SENIORITY = {
    "managing partner":    4.0,
    "national managing":   4.0,
    "senior partner":      3.8,
    "partner":             3.5,
    "senior counsel":      2.8,
    "counsel":             2.5,
    "senior associate":    2.2,
    "associate":           2.0,
    "articling":           1.8,
    "student":             1.5,
    "law clerk":           1.5,
    "director":            1.5,
}

# Phrases that confirm this is a hire announcement
HIRE_PHRASES = [
    "delighted to welcome", "excited to announce", "pleased to welcome",
    "joins our", "has joined", "joining our team", "welcome to the team",
    "new partner", "new associate", "new counsel", "new addition",
    "expanding our", "growing our", "strengthening our",
    "we are thrilled", "proud to welcome",
]

# Roles that are partner/counsel level — tag as lateral_hire not job_posting
LATERAL_LEVEL = [
    "partner", "counsel", "senior partner", "senior associate",
    "senior counsel", "managing partner",
]

# Non-legal roles to skip from job postings
NON_LEGAL_ROLES = [
    "receptionist", "marketing coordinator", "it support", "billing",
    "office administrator", "payroll", "facilities", "graphic design",
]


def _rapid_headers(host: str) -> dict:
    return {
        "x-rapidapi-key":  RAPIDAPI_KEY,
        "x-rapidapi-host": host,
        "Content-Type":    "application/json",
    }


class LinkedInScraper(BaseScraper):
    name = "LinkedInScraper"

    def fetch(self, firm: dict) -> list[dict]:
        if not RAPIDAPI_KEY:
            self.logger.warning(
                "RAPIDAPI_KEY not set — LinkedIn scraper disabled. "
                "Add it to GitHub Secrets to enable rich LinkedIn signals."
            )
            return []

        signals = []
        signals.extend(self._scrape_people_search(firm))
        signals.extend(self._scrape_job_postings(firm))
        signals.extend(self._scrape_company_posts(firm))
        return signals

    # ── 1. PEOPLE SEARCH — lateral hire detection ────────────────────────────

    def _scrape_people_search(self, firm: dict) -> list[dict]:
        """
        Search for employees at this firm. Filter for recent job changes.
        API: Fresh LinkedIn Profile Data — /search-employees
        """
        signals = []
        firm_names = [firm["short"]] + firm.get("alt_names", [])

        for firm_name in firm_names[:2]:   # limit API calls
            try:
                url = f"https://{RAPIDAPI_HOST_PROFILES}/search-employees"
                params = {
                    "company_name": firm_name,
                    "keyword":      "lawyer OR partner OR counsel OR associate",
                    "page":         "1",
                }
                resp = self._get(
                    url,
                    params=params,
                    extra_headers=_rapid_headers(RAPIDAPI_HOST_PROFILES),
                )
                if not resp:
                    continue

                data = resp.json()
                employees = data.get("data", data.get("employees", data.get("results", [])))

                for emp in (employees or [])[:20]:
                    full_name  = emp.get("full_name", "") or emp.get("name", "")
                    headline   = emp.get("headline", "")
                    position   = emp.get("current_position", {}) or {}
                    title      = position.get("title", "") or emp.get("title", headline)
                    started    = position.get("start_date", "")
                    profile_url = emp.get("profile_url", "") or emp.get("linkedin_url", "")

                    if not title:
                        continue
                    if any(w in title.lower() for w in NON_LEGAL_ROLES):
                        continue

                    text = f"{full_name} {title} {headline} {firm_name}"
                    cls  = classifier.classify_with_fallback(text, title=title)
                    weight = self._seniority_weight(title.lower())

                    sig_type = "lateral_hire" if any(
                        t in title.lower() for t in LATERAL_LEVEL
                    ) else "job_posting"

                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type=sig_type,
                        title=f"[LinkedIn] {full_name} — {title} at {firm['short']}",
                        body=f"{full_name} | {title} | {headline} | Started: {started}",
                        url=profile_url or f"https://www.linkedin.com/search/results/people/?keywords={firm_name.replace(' ', '+')}",
                        department=cls["department"],
                        department_score=cls["score"] * weight,
                        matched_keywords=cls["matched_keywords"],
                    ))

                time.sleep(1.0)   # respect rate limits

            except Exception as exc:
                self.logger.debug(f"[{firm['short']}] LinkedIn people search error: {exc}")

        self.logger.info(f"[{firm['short']}] LinkedIn people: {len(signals)} signal(s)")
        return signals

    # ── 2. JOB POSTINGS ──────────────────────────────────────────────────────

    def _scrape_job_postings(self, firm: dict) -> list[dict]:
        """
        Active job postings at this firm.
        API: LinkedIn Jobs Search — /search-jobs
        """
        signals = []
        try:
            url = f"https://{RAPIDAPI_HOST_JOBS}/search-jobs"
            params = {
                "query":    f"{firm['short']} lawyer",
                "location": "Canada",
                "datePosted": "pastWeek",
                "sort":     "mostRecent",
            }
            resp = self._get(
                url,
                params=params,
                extra_headers=_rapid_headers(RAPIDAPI_HOST_JOBS),
            )
            if not resp:
                return signals

            data = resp.json()
            jobs = data.get("data", data.get("jobs", []))

            for job in (jobs or [])[:15]:
                company = job.get("company", {})
                company_name = company.get("name", "") if isinstance(company, dict) else str(company)
                firm_names = [firm["short"]] + firm.get("alt_names", [])
                if not any(n.lower() in company_name.lower() for n in firm_names):
                    continue

                title       = job.get("title", "")
                description = job.get("description", "")
                job_url     = job.get("url", job.get("applyUrl", ""))
                location    = job.get("location", "")

                if not title:
                    continue
                if any(w in title.lower() for w in NON_LEGAL_ROLES):
                    continue

                text   = f"{title} {description[:300]} {location}"
                cls    = classifier.classify_with_fallback(text, title=title)
                weight = self._seniority_weight(title.lower())

                sig_type = "lateral_hire" if any(
                    t in title.lower() for t in LATERAL_LEVEL
                ) else "job_posting"

                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type=sig_type,
                    title=f"[LinkedIn Jobs] {title} — {firm['short']}",
                    body=f"{title} | {location}\n{description[:500]}",
                    url=job_url or f"https://www.linkedin.com/jobs/search/?keywords={firm['short'].replace(' ', '+')}",
                    department=cls["department"],
                    department_score=cls["score"] * weight,
                    matched_keywords=cls["matched_keywords"],
                ))

            time.sleep(1.0)

        except Exception as exc:
            self.logger.debug(f"[{firm['short']}] LinkedIn jobs error: {exc}")

        self.logger.info(f"[{firm['short']}] LinkedIn jobs: {len(signals)} signal(s)")
        return signals

    # ── 3. COMPANY PAGE POSTS ────────────────────────────────────────────────

    def _scrape_company_posts(self, firm: dict) -> list[dict]:
        """
        Company page announcements — hire news, deal tombstones, office openings.
        API: LinkedIn Company Updates — /company-updates
        """
        signals = []
        slug = firm.get("linkedin_slug", "")
        if not slug:
            return signals

        try:
            url = f"https://{RAPIDAPI_HOST_COMPANY}/company-updates"
            params = {
                "company_slug": slug,
                "page":         "1",
            }
            resp = self._get(
                url,
                params=params,
                extra_headers=_rapid_headers(RAPIDAPI_HOST_COMPANY),
            )
            if not resp:
                return signals

            data  = resp.json()
            posts = data.get("data", data.get("posts", data.get("updates", [])))

            for post in (posts or [])[:20]:
                text = (
                    post.get("text", "")
                    or post.get("commentary", "")
                    or post.get("description", "")
                )
                if not text or len(text.strip()) < 30:
                    continue

                text_lower = text.lower()
                post_url   = post.get("url", post.get("postUrl", ""))

                # Only take hire/expansion/deal announcements
                is_hire      = any(p in text_lower for p in HIRE_PHRASES)
                is_expansion = any(p in text_lower for p in [
                    "new office", "opens office", "expands to", "new practice group",
                    "launches", "proud to announce", "pleased to announce",
                ])
                is_deal = any(p in text_lower for p in [
                    "advised", "counsel to", "represented", "successfully completed",
                    "closed", "transaction",
                ])

                if not (is_hire or is_expansion or is_deal):
                    continue

                title = text[:160].replace("\n", " ")

                if is_hire:
                    sig_type = "lateral_hire"
                    weight_mult = 2.5
                elif is_expansion:
                    sig_type = "press_release"
                    weight_mult = 2.0
                else:
                    sig_type = "press_release"
                    weight_mult = 1.5

                seniority = self._seniority_weight(text_lower)
                cls = classifier.classify_with_fallback(text, title=title)

                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type=sig_type,
                    title=f"[LinkedIn Post] {title}",
                    body=text[:700],
                    url=post_url or f"https://www.linkedin.com/company/{slug}/posts/",
                    department=cls["department"],
                    department_score=cls["score"] * weight_mult * seniority,
                    matched_keywords=cls["matched_keywords"],
                ))

            time.sleep(1.0)

        except Exception as exc:
            self.logger.debug(f"[{firm['short']}] LinkedIn company posts error: {exc}")

        self.logger.info(f"[{firm['short']}] LinkedIn posts: {len(signals)} signal(s)")
        return signals

    def _seniority_weight(self, text: str) -> float:
        for kw, w in SENIORITY.items():
            if kw in text:
                return w
        return 1.5
