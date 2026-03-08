"""
PressScraper — firm news pages + targeted external sources.

Improvements in v2:
  - Much richer lateral/expansion phrase detection
  - Firm news page scraping with multiple article container patterns
  - Precedent + Canadian Lawyer Google News queries (complement media.py)
  - Office opening / new practice detection
  - Firm-specific Newswire.ca search
"""

import re
from urllib.parse import urljoin, quote_plus

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

try:
    import feedparser
    HAS_FEEDPARSER = True
except ImportError:
    HAS_FEEDPARSER = False

classifier = DepartmentClassifier()

# ── Detection patterns ────────────────────────────────────────────────────────

LATERAL_RE = re.compile(
    r"join(?:s|ed|ing)\s+(?:the\s+firm|as\s+partner|as\s+counsel|as\s+associate)"
    r"|has\s+joined"
    r"|welcome[sd]\s+(?:new\s+)?(?:partner|counsel|associate|lawyer)"
    r"|new\s+(?:senior\s+)?partner\s+(?:at|joins|to)"
    r"|lateral\s+(?:hire|move|partner)"
    r"|appointed\s+(?:as\s+)?(?:partner|managing\s+partner|counsel|head)"
    r"|named\s+(?:as\s+)?(?:partner|head|chair|co-chair|managing)"
    r"|expands?\s+(?:its\s+)?(?:team|practice|group|bench)"
    r"|recruits?\s+(?:partner|counsel|lawyer)"
    r"|promoted\s+to\s+partner",
    re.IGNORECASE,
)

EXPANSION_RE = re.compile(
    r"opens?\s+(?:new\s+)?(?:office|practice\s+group|group)"
    r"|launch(?:es|ed|ing)\s+(?:new\s+)?(?:practice|group|desk)"
    r"|new\s+office\s+in"
    r"|expands?\s+(?:to|into)\s+"
    r"|merges?\s+with\s+"
    r"|strategic\s+(?:alliance|merger)",
    re.IGNORECASE,
)

DEAL_RE = re.compile(
    r"advises?\s+|acted\s+as\s+counsel|counsel\s+to|represents?\s+"
    r"|successfully\s+completed|transaction\s+counsel",
    re.IGNORECASE,
)

# ── External sources (HTML scrape) ────────────────────────────────────────────

EXTERNAL_SOURCES = [
    {
        "name":    "Newswire.ca",
        "base":    "https://www.newswire.ca",
        "search":  "https://www.newswire.ca/news-releases/?s={short}",
        "weight":  2.5,
    },
    {
        "name":    "Globe Newswire",
        "base":    "https://www.globenewswire.com",
        "search":  "https://www.globenewswire.com/Search/Keyword/{short}?country=Canada",
        "weight":  2.5,
    },
]

GOOG = "https://news.google.com/rss/search?q={q}&hl=en-CA&gl=CA&ceid=CA:en"


class PressScraper(BaseScraper):
    name = "PressScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        seen: set = set()

        # 1 — Firm's own news page
        signals.extend(self._scrape_firm_news(firm, seen))

        # 2 — External wire services
        for src in EXTERNAL_SOURCES:
            signals.extend(self._scrape_external(firm, src, seen))

        # 3 — Google News (firm name + news)
        if HAS_FEEDPARSER:
            signals.extend(self._google_news(firm, seen))

        return signals[:20]

    def _scrape_firm_news(self, firm, seen) -> list[dict]:
        url = firm.get("news_url", "")
        if not url:
            return []
        soup = self.get_soup(url)
        if not soup:
            return []

        signals = []
        # Multiple container patterns for different firm site layouts
        containers = (
            soup.find_all(["article"], limit=40) or
            soup.find_all("div", class_=re.compile(r"news|press|article|post|item|card|insight", re.I), limit=40)
        )

        for tag in containers:
            title_tag = tag.find(["h2", "h3", "h4"]) or tag.find("a", href=True)
            if not title_tag:
                continue
            title = title_tag.get_text(" ", strip=True)
            if len(title) < 20 or len(title) > 350:
                continue

            body_tag = tag.find("p")
            body = body_tag.get_text(" ", strip=True)[:500] if body_tag else ""
            full = f"{title} {body}"

            link_tag = tag.find("a", href=True)
            link = ""
            if link_tag:
                href = link_tag["href"]
                link = href if href.startswith("http") else urljoin(firm["website"], href)
            if link in seen:
                continue

            # Route signal type
            if LATERAL_RE.search(full):
                sig_type, w_mult = "lateral_hire", 4.0
            elif EXPANSION_RE.search(full):
                sig_type, w_mult = "practice_page", 3.5
            elif DEAL_RE.search(full):
                sig_type, w_mult = "press_release", 3.0
            else:
                sig_type, w_mult = "press_release", 1.5

            cls = classifier.top_department(full)
            if not cls:
                continue

            signals.append(self._make_signal(
                firm_id=firm["id"], firm_name=firm["name"],
                signal_type=sig_type,
                title=title[:200], body=body,
                url=link or url,
                department=cls["department"],
                department_score=cls["score"] * w_mult,
                matched_keywords=cls["matched_keywords"],
            ))
            seen.add(link or title[:80])

        unique, titles = [], set()
        for s in signals:
            t = s["title"][:80]
            if t not in titles:
                titles.add(t)
                unique.append(s)
        return unique[:15]

    def _scrape_external(self, firm, src, seen) -> list[dict]:
        url = src["search"].format(short=quote_plus(firm["short"]))
        soup = self.get_soup(url)
        if not soup:
            return []

        signals = []
        firm_tokens = [firm["short"].lower(), firm["name"].split()[0].lower()]

        for tag in soup.find_all(["article", "h2", "h3", "li"], limit=30):
            text = tag.get_text(" ", strip=True)
            if len(text) < 25 or len(text) > 400:
                continue
            if not any(t in text.lower() for t in firm_tokens):
                continue

            link_tag = tag.find("a", href=True)
            link = ""
            if link_tag:
                href = link_tag.get("href", "")
                link = href if href.startswith("http") else urljoin(src["base"], href)
            if link in seen:
                continue

            if LATERAL_RE.search(text):
                sig_type, w_mult = "lateral_hire", 4.0
            elif EXPANSION_RE.search(text):
                sig_type, w_mult = "practice_page", 3.5
            elif DEAL_RE.search(text):
                sig_type, w_mult = "press_release", 3.0
            else:
                sig_type, w_mult = "press_release", 1.5

            cls = classifier.top_department(text)
            if not cls:
                continue

            signals.append(self._make_signal(
                firm_id=firm["id"], firm_name=firm["name"],
                signal_type=sig_type,
                title=f"[{src['name']}] {text[:160]}",
                url=link or url,
                department=cls["department"],
                department_score=cls["score"] * src["weight"] * w_mult,
                matched_keywords=cls["matched_keywords"],
            ))
            seen.add(link or text[:80])
        return signals[:8]

    def _google_news(self, firm, seen) -> list[dict]:
        """Targeted Google News for firm-specific news."""
        import time as _t
        from datetime import timezone, timedelta
        from email.utils import parsedate_to_datetime

        q   = f'"{firm["short"]}" law firm'
        url = GOOG.format(q=quote_plus(q))
        signals = []
        try:
            feed = feedparser.parse(url, request_headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalTracker/2.0)"
            })
        except Exception:
            return []

        firm_tokens = [firm["short"].lower(), firm["name"].split()[0].lower()]
        cutoff = __import__("datetime").datetime.now(__import__("datetime").timezone.utc) - \
                 __import__("datetime").timedelta(days=21)

        for entry in (feed.entries or [])[:15]:
            title   = entry.get("title", "")
            summary = entry.get("summary", "")
            link    = entry.get("link", url)
            if link in seen:
                continue
            full  = f"{title} {summary}"
            lower = full.lower()
            if not any(t in lower for t in firm_tokens):
                continue

            if LATERAL_RE.search(full):
                sig_type, w_mult = "lateral_hire", 4.0
            elif EXPANSION_RE.search(full):
                sig_type, w_mult = "practice_page", 3.5
            elif DEAL_RE.search(full):
                sig_type, w_mult = "press_release", 3.0
            else:
                sig_type, w_mult = "press_release", 1.5

            cls = classifier.top_department(full)
            if not cls:
                continue

            signals.append(self._make_signal(
                firm_id=firm["id"], firm_name=firm["name"],
                signal_type=sig_type,
                title=f"[Google News] {title[:160]}",
                body=summary[:400],
                url=link,
                department=cls["department"],
                department_score=cls["score"] * 2.0 * w_mult,
                matched_keywords=cls["matched_keywords"],
            ))
            seen.add(link)
        return signals[:8]
