"""
RecruiterScraper — legal recruiter signals for Canadian law firms.

Legal recruiters actively publish lateral move announcements and
"hot jobs" lists that directly reveal firm expansion intent.

Sources:
  ZSA Legal Recruitment      zsa.ca/news/               — Canada's top legal recruiter
  Counsel Network            thecounselnetwork.com       — Toronto-focused
  Osler Hoskin (not the firm!) — generic legal job boards
  LegalLeaders.ca            legallyjobs.com             — Canadian legal jobs
  Google News (recruiter filter)                         — press coverage of moves
  Lateral Link Canada        laterallink.com             — lateral hire aggregator
  BCG Attorney Search        bcgsearch.com/canada        — national coverage
  Major Lindsey & Africa     mlaglobal.com               — senior lateral coverage
  Recruiting for Good        recruitingforgood.com       — boutique/specialized
  Robert Half Legal          roberthalf.ca               — volume signal
  Indeed Canada (senior)     ca.indeed.com               — senior associate / partner jobs

Signal types:
  lateral_hire   — recruiter explicitly placing someone at firm
  job_posting    — open roles at firm (weighted by seniority)
"""

import re
from urllib.parse import quote_plus, urljoin

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

try:
    import feedparser
    HAS_FEEDPARSER = True
except ImportError:
    HAS_FEEDPARSER = False

classifier = DepartmentClassifier()

SENIORITY_WEIGHTS = {
    "managing partner":  4.5,
    "senior partner":    4.0,
    "partner":           3.5,
    "counsel":           2.5,
    "senior associate":  2.0,
    "associate":         1.5,
    "articling":         1.0,
    "student":           0.8,
}

SENIORITY_RE = re.compile(
    r"(managing\s+partner|senior\s+partner|partner|senior\s+counsel"
    r"|of\s+counsel|counsel|senior\s+associate|associate|articling\s+student)",
    re.IGNORECASE,
)

# Google News queries that surface recruiter-placed lateral moves
LATERAL_QUERIES = [
    '"{short}" "joins" OR "joined" law firm partner counsel',
    '"{short}" lateral hire partner lawyer Canada',
    '"{short}" "new partner" OR "appointed partner" legal',
    '"{short}" recruits OR "has hired" lawyer partner',
]

GOOG = "https://news.google.com/rss/search?q={q}&hl=en-CA&gl=CA&ceid=CA:en"

# Recruiter job board HTML pages (we extract job titles)
RECRUITER_JOB_PAGES = [
    {
        "name": "ZSA",
        "search_url": "https://www.zsa.ca/jobs/?s={short}",
        "base": "https://www.zsa.ca",
        "weight": 3.5,
    },
    {
        "name": "Counsel Network",
        "search_url": "https://www.thecounselnetwork.com/jobs/?search={short}",
        "base": "https://www.thecounselnetwork.com",
        "weight": 3.0,
    },
    {
        "name": "Lateral Link",
        "search_url": "https://www.laterallink.com/jobs/?q={short}&location=Canada",
        "base": "https://www.laterallink.com",
        "weight": 3.0,
    },
    {
        "name": "BCG Search",
        "search_url": "https://www.bcgsearch.com/jobs/?q={short}+canada",
        "base": "https://www.bcgsearch.com",
        "weight": 3.0,
    },
]

# ZSA news page has explicit placement announcements
ZSA_NEWS_URL = "https://www.zsa.ca/news/"

# Indeed Canada senior legal roles
INDEED_SENIOR_URL = (
    "https://ca.indeed.com/jobs"
    "?q={query}+partner+OR+counsel&l=Canada&sort=date&fromage=14&sc=0kf%3Ajt(permanent)%3B"
)

JOB_TITLE_KWS = [
    "partner", "counsel", "associate", "lawyer", "attorney",
    "articling", "student", "legal director", "chief legal",
    "general counsel", "deputy general counsel", "head of legal",
]


def _weight_for_title(text: str) -> float:
    lower = text.lower()
    for title, w in SENIORITY_WEIGHTS.items():
        if title in lower:
            return w
    return 1.2


class RecruiterScraper(BaseScraper):
    name = "RecruiterScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        seen: set = set()
        firm_tokens = [firm["short"].lower(), firm["name"].split()[0].lower()] + \
                      [a.lower() for a in firm.get("alt_names", [])]

        # 1 — ZSA News (explicit placement announcements)
        signals.extend(self._scrape_zsa_news(firm, firm_tokens, seen))

        # 2 — Recruiter job boards (firm-specific searches)
        for src in RECRUITER_JOB_PAGES:
            signals.extend(self._scrape_job_board(firm, src, firm_tokens, seen))

        # 3 — Google News lateral queries
        if HAS_FEEDPARSER:
            signals.extend(self._google_lateral(firm, firm_tokens, seen))

        # 4 — Indeed Canada (senior-only filter)
        signals.extend(self._scrape_indeed_senior(firm, firm_tokens, seen))

        return signals[:15]

    def _scrape_zsa_news(self, firm, firm_tokens, seen) -> list[dict]:
        soup = self.get_soup(ZSA_NEWS_URL)
        if not soup:
            return []
        signals = []
        for article in soup.find_all(["article", "div"], class_=re.compile(r"post|news|entry", re.I))[:30]:
            title_tag = article.find(["h2", "h3", "a"])
            if not title_tag:
                continue
            title = title_tag.get_text(" ", strip=True)
            if len(title) < 15:
                continue
            lower = title.lower()
            if not any(t in lower for t in firm_tokens):
                continue

            link_tag = article.find("a", href=True)
            link = urljoin(ZSA_NEWS_URL, link_tag["href"]) if link_tag else ZSA_NEWS_URL
            if link in seen:
                continue

            w = _weight_for_title(title)
            is_lateral = bool(re.search(r"join|hire|appoint|place|recruit", lower))
            sig_type = "lateral_hire" if is_lateral else "job_posting"

            cls = classifier.classify(title, top_n=1)
            dept = cls[0]["department"] if cls else "Corporate/M&A"
            score = (cls[0]["score"] if cls else 1.0) * 3.5 * w

            signals.append(self._make_signal(
                firm_id=firm["id"], firm_name=firm["name"],
                signal_type=sig_type,
                title=f"[ZSA] {title[:160]}",
                url=link,
                department=dept,
                department_score=score,
                matched_keywords=cls[0]["matched_keywords"] if cls else [],
            ))
            seen.add(link)
        return signals

    def _scrape_job_board(self, firm, src, firm_tokens, seen) -> list[dict]:
        url = src["search_url"].format(short=quote_plus(firm["short"]))
        soup = self.get_soup(url)
        if not soup:
            return []
        signals = []
        for tag in soup.find_all(["h2", "h3", "h4", "a", "li"], limit=40):
            text = tag.get_text(" ", strip=True)
            if len(text) < 15 or len(text) > 250:
                continue
            lower = text.lower()
            if not any(kw in lower for kw in JOB_TITLE_KWS):
                continue
            if not any(t in lower for t in firm_tokens):
                continue

            link_tag = tag if tag.name == "a" else tag.find("a", href=True)
            link = ""
            if link_tag and link_tag.get("href"):
                href = link_tag["href"]
                link = href if href.startswith("http") else urljoin(src["base"], href)
            if link in seen:
                continue

            w = _weight_for_title(text)
            cls = classifier.classify(text, top_n=1)
            dept = cls[0]["department"] if cls else "Corporate/M&A"

            signals.append(self._make_signal(
                firm_id=firm["id"], firm_name=firm["name"],
                signal_type="job_posting",
                title=f"[{src['name']}] {text[:160]}",
                url=link or url,
                department=dept,
                department_score=(cls[0]["score"] if cls else 1.0) * src["weight"] * w,
                matched_keywords=cls[0]["matched_keywords"] if cls else [],
            ))
            seen.add(link or text[:80])
        return signals[:5]

    def _google_lateral(self, firm, firm_tokens, seen) -> list[dict]:
        signals = []
        import time as _time
        from datetime import timezone, timedelta
        from email.utils import parsedate_to_datetime

        for q_tpl in LATERAL_QUERIES[:2]:  # limit to 2 queries
            q   = q_tpl.format(short=firm["short"])
            url = GOOG.format(q=quote_plus(q))
            try:
                feed = feedparser.parse(url, request_headers={
                    "User-Agent": "Mozilla/5.0 (compatible; LegalTracker/2.0)"
                })
            except Exception:
                continue

            for entry in (feed.entries or [])[:10]:
                title   = entry.get("title", "")
                summary = entry.get("summary", "")
                link    = entry.get("link", url)
                if link in seen:
                    continue
                full  = f"{title} {summary}"
                lower = full.lower()
                if not any(t in lower for t in firm_tokens):
                    continue
                if not re.search(r"join|hire|lateral|partner|counsel|appoint|recruit", lower):
                    continue

                w = _weight_for_title(full)
                cls = classifier.classify(full, top_n=1)
                if not cls:
                    continue
                c = cls[0]

                signals.append(self._make_signal(
                    firm_id=firm["id"], firm_name=firm["name"],
                    signal_type="lateral_hire",
                    title=f"[Recruiter News] {title[:160]}",
                    body=summary[:400],
                    url=link,
                    department=c["department"],
                    department_score=c["score"] * 4.0 * w,
                    matched_keywords=c["matched_keywords"],
                ))
                seen.add(link)
        return signals

    def _scrape_indeed_senior(self, firm, firm_tokens, seen) -> list[dict]:
        """Indeed Canada — partner/senior counsel postings only."""
        short_enc = quote_plus(f'"{firm["short"]}"')
        url = INDEED_SENIOR_URL.format(query=short_enc)
        soup = self.get_soup(url)
        if not soup:
            return []
        signals = []
        for card in soup.find_all("div", attrs={"data-jk": True})[:10]:
            title_tag = card.find(["h2", "span"], attrs={"class": re.compile(r"title|jobTitle", re.I)})
            if not title_tag:
                continue
            title = title_tag.get_text(" ", strip=True)
            lower = title.lower()
            if not any(kw in lower for kw in ["partner", "counsel", "director", "head of"]):
                continue

            jk = card.get("data-jk", "")
            link = f"https://ca.indeed.com/viewjob?jk={jk}" if jk else url
            if link in seen:
                continue

            w = _weight_for_title(title)
            cls = classifier.classify(f"{firm['short']} {title}", top_n=1)
            dept = cls[0]["department"] if cls else "Corporate/M&A"

            signals.append(self._make_signal(
                firm_id=firm["id"], firm_name=firm["name"],
                signal_type="job_posting",
                title=f"[Indeed Senior] {title[:160]}",
                url=link,
                department=dept,
                department_score=(cls[0]["score"] if cls else 1.0) * 2.5 * w,
                matched_keywords=cls[0]["matched_keywords"] if cls else [],
            ))
            seen.add(link)
        return signals
