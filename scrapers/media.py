"""
MediaScraper — dedicated Canadian legal media scraper.

Sources (all firm-mention filtered):
  Precedent Magazine    precedentmag.com          — dealmakers, career moves, profiles
  Canadian Lawyer       canadianlawyermag.com     — lateral hires, rankings, top-25 lists
  Lexpert Magazine      lexpert.ca/magazine/      — transactional deals, rankings
  Law Times             lawtimesnews.com           — profession news, bar news
  The Lawyer's Daily    thelawyersdaily.ca         — court, regulatory, profession
  Slaw.ca               slaw.ca                    — Canadian law commentary
  Advocates' Daily      advocates-daily.com        — litigation, appellate
  National (CBA)        nationalmagazine.ca        — bar association, profession trends
  Above the Law CA      abovethelaw.com (filtered) — lateral hires touching Canada
  Bay Street Bull       baystreetbull.com          — Bay Street culture, hiring
  OBA Communiqué        oba.org                    — Ontario Bar events/notices
  CCCA Pulse            ccca.ca                    — in-house counsel moves

Signal types produced:
  lateral_hire          — joins, appointed partner, new partner
  press_release         — firm mentioned in news
  ranking               — awards, lists, rankings
  practice_page         — new practice group / office launch
"""

import re
import time as _time
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import quote_plus, urljoin

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

try:
    import feedparser
    HAS_FEEDPARSER = True
except ImportError:
    HAS_FEEDPARSER = False

classifier = DepartmentClassifier()
LOOKBACK_DAYS = 21

# ── Patterns ─────────────────────────────────────────────────────────────────

LATERAL_RE = re.compile(
    r"join(?:s|ed|ing)\s+(?:the\s+firm|as\s+partner|as\s+counsel|as\s+associate)"
    r"|has\s+joined"
    r"|welcome[sd]\s+(?:new\s+)?(?:partner|counsel|associate|lawyer)"
    r"|new\s+partner\s+(?:at|joins|to)"
    r"|lateral\s+(?:hire|move|partner)"
    r"|appointed\s+(?:as\s+)?(?:partner|managing\s+partner|counsel|head)"
    r"|named\s+(?:as\s+)?(?:partner|head|chair|co-chair|managing)"
    r"|expands?\s+(?:its\s+)?(?:team|practice|group|bench)"
    r"|recruit(?:s|ed|ing)\s+(?:partner|counsel|lawyer)"
    r"|promote[sd]\s+to\s+partner",
    re.IGNORECASE,
)

EXPANSION_RE = re.compile(
    r"opens?\s+(?:new\s+)?(?:office|practice|group)"
    r"|launch(?:es|ed|ing)\s+(?:new\s+)?(?:practice|group|desk|team)"
    r"|establishes?\s+(?:new\s+)?(?:office|practice|presence)"
    r"|expands?\s+to\s+"
    r"|new\s+office\s+in"
    r"|strategic\s+(?:alliance|merger|combination|partnership)",
    re.IGNORECASE,
)

RANKING_RE = re.compile(
    r"top\s+\d+\s+(?:law\s+firm|lawyer|counsel)"
    r"|ranked\s+(?:band\s+\d|tier\s+\d|first|top|leading)"
    r"|most\s+influential"
    r"|dealmaker\s+of\s+the\s+year"
    r"|lawyer\s+of\s+the\s+year"
    r"|best\s+(?:law\s+firm|lawyer|counsel)"
    r"|award(?:ed|s)\s+(?:for|to)"
    r"|recognized\s+(?:as|for)",
    re.IGNORECASE,
)

# ── RSS feeds with firm-mention filtering ─────────────────────────────────────

RSS_SOURCES = [
    {
        "name":    "Canadian Lawyer",
        "url":     "https://www.canadianlawyermag.com/rss/",
        "base":    "https://www.canadianlawyermag.com",
        "weight":  3.5,
        "scrape_url": "https://www.canadianlawyermag.com/news/general/",
    },
    {
        "name":    "Law Times",
        "url":     "https://www.lawtimesnews.com/rss",
        "base":    "https://www.lawtimesnews.com",
        "weight":  3.0,
        "scrape_url": "https://www.lawtimesnews.com/news/",
    },
    {
        "name":    "Slaw",
        "url":     "https://www.slaw.ca/feed/",
        "base":    "https://www.slaw.ca",
        "weight":  2.0,
    },
    {
        "name":    "Advocates Daily",
        "url":     "https://www.advocates-daily.com/rss.xml",
        "base":    "https://www.advocates-daily.com",
        "weight":  2.5,
    },
    {
        "name":    "Lexpert",
        "url":     "https://www.lexpert.ca/rss/",
        "base":    "https://www.lexpert.ca",
        "weight":  3.5,
        "scrape_url": "https://www.lexpert.ca/rankings/",
    },
    {
        "name":    "National CBA",
        "url":     "https://www.nationalmagazine.ca/en-ca/articles/rss",
        "base":    "https://www.nationalmagazine.ca",
        "weight":  2.5,
    },
    {
        "name":    "OBA Communiqué",
        "url":     "https://www.oba.org/rss/news",
        "base":    "https://www.oba.org",
        "weight":  2.0,
    },
    {
        "name":    "Mondaq Canada",
        "url":     "https://www.mondaq.com/rss/canada/",
        "base":    "https://www.mondaq.com",
        "weight":  1.8,
    },
    {
        "name":    "JD Supra Canada",
        "url":     "https://www.jdsupra.com/resources/syndication/docsRSSfeed.aspx?ftype=AllContent&tp=canada",
        "base":    "https://www.jdsupra.com",
        "weight":  1.8,
    },
    {
        "name":    "Lawyers Weekly",
        "url":     "https://www.lawyersweekly.ca/rss/",
        "base":    "https://www.lawyersweekly.ca",
        "weight":  3.0,
    },
    {
        "name":    "Canadian Lawyer Top25",
        "url":     "https://www.canadianlawyermag.com/tag/top-25-most-influential-lawyers/rss",
        "base":    "https://www.canadianlawyermag.com",
        "weight":  4.0,
    },
]

# ── Precedent Magazine (no RSS — HTML + Google News) ─────────────────────────

PRECEDENT_SECTIONS = [
    "https://precedentmag.com/category/careers/",
    "https://precedentmag.com/category/people/",
    "https://precedentmag.com/category/deals/",
    "https://precedentmag.com/category/firms/",
    "https://precedentmag.com/dealmakers/",
]

GOOG = "https://news.google.com/rss/search?q={q}&hl=en-CA&gl=CA&ceid=CA:en"


def _parse_date(entry: dict) -> datetime | None:
    for key in ("published_parsed", "updated_parsed"):
        ts = entry.get(key)
        if ts:
            try:
                return datetime.fromtimestamp(_time.mktime(ts), tz=timezone.utc)
            except Exception:
                pass
    for key in ("published", "updated"):
        raw = entry.get(key, "")
        if raw:
            try:
                return parsedate_to_datetime(raw).astimezone(timezone.utc)
            except Exception:
                pass
    return None


def _is_recent(entry, days=LOOKBACK_DAYS) -> bool:
    dt = _parse_date(entry)
    return dt is None or dt >= datetime.now(timezone.utc) - timedelta(days=days)


def _signal_type(text: str) -> tuple[str, float]:
    """Return (signal_type, weight_multiplier)."""
    if LATERAL_RE.search(text):
        return "lateral_hire", 4.0
    if EXPANSION_RE.search(text):
        return "practice_page", 3.5
    if RANKING_RE.search(text):
        return "ranking", 3.5
    return "press_release", 1.5


class MediaScraper(BaseScraper):
    name = "MediaScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        seen: set = set()
        firm_tokens = self._name_tokens(firm)

        # 1 — RSS feeds
        if HAS_FEEDPARSER:
            for src in RSS_SOURCES:
                signals.extend(self._from_rss(firm, src, firm_tokens, seen))

        # 2 — Precedent Magazine HTML scrape
        signals.extend(self._scrape_precedent(firm, firm_tokens, seen))

        # 3 — Canadian Lawyer HTML scrape (supplements RSS)
        signals.extend(self._scrape_canadian_lawyer(firm, firm_tokens, seen))

        # 4 — Lexpert HTML scrape (rankings + news)
        signals.extend(self._scrape_lexpert(firm, firm_tokens, seen))

        # 5 — Google News targeted to legal media
        if HAS_FEEDPARSER:
            signals.extend(self._google_legal_media(firm, firm_tokens, seen))

        return signals[:20]

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _name_tokens(self, firm: dict) -> list[str]:
        tokens = [firm["short"].lower(), firm["name"].split()[0].lower()]
        for alt in firm.get("alt_names", []):
            tokens.append(alt.lower())
        return list(dict.fromkeys(tokens))  # dedup, preserve order

    def _from_rss(self, firm, src, firm_tokens, seen) -> list[dict]:
        signals = []
        try:
            feed = feedparser.parse(src["url"], request_headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalTracker/2.0)"
            })
        except Exception:
            return []

        for entry in (feed.entries or [])[:30]:
            if not _is_recent(entry):
                continue
            title   = entry.get("title", "")
            summary = entry.get("summary", "")
            link    = entry.get("link", src["url"])
            if link in seen:
                continue
            full  = f"{title} {summary}"
            lower = full.lower()
            if not any(t in lower for t in firm_tokens):
                continue

            sig_type, weight_mult = _signal_type(full)
            cls = classifier.classify(full, top_n=1)
            if not cls:
                continue
            c = cls[0]

            signals.append(self._make_signal(
                firm_id=firm["id"], firm_name=firm["name"],
                signal_type=sig_type,
                title=f"[{src['name']}] {title[:160]}",
                body=summary[:500],
                url=link,
                department=c["department"],
                department_score=c["score"] * src["weight"] * weight_mult,
                matched_keywords=c["matched_keywords"],
            ))
            seen.add(link)
        return signals

    def _scrape_precedent(self, firm, firm_tokens, seen) -> list[dict]:
        """Scrape Precedent Magazine — careers, deals, people, firm sections."""
        signals = []
        for url in PRECEDENT_SECTIONS:
            soup = self.get_soup(url)
            if not soup:
                continue
            for article in soup.find_all(["article", "div"], class_=re.compile(
                r"post|entry|item|card", re.I
            ))[:25]:
                title_tag = article.find(["h2", "h3", "h4", "a"])
                if not title_tag:
                    continue
                title = title_tag.get_text(" ", strip=True)
                if len(title) < 20:
                    continue

                body_tag = article.find("p")
                body = body_tag.get_text(" ", strip=True)[:400] if body_tag else ""
                full  = f"{title} {body}"
                lower = full.lower()

                if not any(t in lower for t in firm_tokens):
                    continue

                link_tag = article.find("a", href=True)
                link = urljoin("https://precedentmag.com", link_tag["href"]) if link_tag else url
                if link in seen:
                    continue

                sig_type, weight_mult = _signal_type(full)
                cls = classifier.classify(full, top_n=1)
                if not cls:
                    continue
                c = cls[0]

                signals.append(self._make_signal(
                    firm_id=firm["id"], firm_name=firm["name"],
                    signal_type=sig_type,
                    title=f"[Precedent] {title[:160]}",
                    body=body,
                    url=link,
                    department=c["department"],
                    department_score=c["score"] * 4.0 * weight_mult,
                    matched_keywords=c["matched_keywords"],
                ))
                seen.add(link)
        return signals

    def _scrape_canadian_lawyer(self, firm, firm_tokens, seen) -> list[dict]:
        """Direct HTML scrape of Canadian Lawyer news and people pages."""
        signals = []
        pages = [
            "https://www.canadianlawyermag.com/news/general/",
            "https://www.canadianlawyermag.com/news/people/",
            "https://www.canadianlawyermag.com/news/firms/",
            "https://www.canadianlawyermag.com/rankings/",
        ]
        for url in pages:
            soup = self.get_soup(url)
            if not soup:
                continue
            for tag in soup.find_all(["h2", "h3", "article"], limit=40):
                title_tag = tag if tag.name in ("h2", "h3") else tag.find(["h2", "h3"])
                if not title_tag:
                    continue
                title = title_tag.get_text(" ", strip=True)
                if len(title) < 20:
                    continue

                lower = title.lower()
                if not any(t in lower for t in firm_tokens):
                    continue

                link_tag = tag.find("a", href=True) if tag.name not in ("h2", "h3") else tag.find_parent("a")
                if not link_tag:
                    link_tag = title_tag.find_parent("a") or title_tag.find("a")
                link = ""
                if link_tag and link_tag.get("href"):
                    href = link_tag["href"]
                    link = href if href.startswith("http") else urljoin("https://www.canadianlawyermag.com", href)
                if link in seen:
                    continue

                sig_type, weight_mult = _signal_type(title)
                cls = classifier.classify(title, top_n=1)
                if not cls:
                    continue
                c = cls[0]

                signals.append(self._make_signal(
                    firm_id=firm["id"], firm_name=firm["name"],
                    signal_type=sig_type,
                    title=f"[Canadian Lawyer] {title[:160]}",
                    url=link or url,
                    department=c["department"],
                    department_score=c["score"] * 3.5 * weight_mult,
                    matched_keywords=c["matched_keywords"],
                ))
                seen.add(link or title)
        return signals

    def _scrape_lexpert(self, firm, firm_tokens, seen) -> list[dict]:
        """Scrape Lexpert news and rankings pages."""
        signals = []
        pages = [
            "https://www.lexpert.ca/news/",
            "https://www.lexpert.ca/rankings/",
            "https://www.lexpert.ca/special-editions/",
        ]
        for url in pages:
            soup = self.get_soup(url)
            if not soup:
                continue
            for tag in soup.find_all(["h2", "h3", "article", "li"], limit=40):
                text = tag.get_text(" ", strip=True)
                if len(text) < 20 or len(text) > 400:
                    continue
                if not any(t in text.lower() for t in firm_tokens):
                    continue

                link_tag = tag.find("a", href=True) or (tag if tag.name == "a" else None)
                link = ""
                if link_tag and link_tag.get("href"):
                    href = link_tag["href"]
                    link = href if href.startswith("http") else urljoin("https://www.lexpert.ca", href)
                if link in seen:
                    continue

                sig_type, weight_mult = _signal_type(text)
                cls = classifier.classify(text, top_n=1)
                if not cls:
                    continue
                c = cls[0]

                signals.append(self._make_signal(
                    firm_id=firm["id"], firm_name=firm["name"],
                    signal_type=sig_type,
                    title=f"[Lexpert] {text[:160]}",
                    url=link or url,
                    department=c["department"],
                    department_score=c["score"] * 3.5 * weight_mult,
                    matched_keywords=c["matched_keywords"],
                ))
                seen.add(link or text[:80])
        return signals

    def _google_legal_media(self, firm, firm_tokens, seen) -> list[dict]:
        """Google News restricted to top Canadian legal media domains."""
        signals = []
        site_filter = (
            "site:canadianlawyermag.com OR site:precedentmag.com OR "
            "site:lexpert.ca OR site:lawtimesnews.com OR site:thelawyersdaily.ca OR "
            "site:slaw.ca OR site:lawyersweekly.ca OR site:nationalmagazine.ca"
        )
        q   = f'"{firm["short"]}" ({site_filter})'
        url = GOOG.format(q=quote_plus(q))
        try:
            feed = feedparser.parse(url, request_headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalTracker/2.0)"
            })
        except Exception:
            return []

        for entry in (feed.entries or [])[:15]:
            if not _is_recent(entry):
                continue
            title   = entry.get("title", "")
            summary = entry.get("summary", "")
            link    = entry.get("link", url)
            if link in seen:
                continue
            full = f"{title} {summary}"
            if not any(t in full.lower() for t in firm_tokens):
                continue

            sig_type, weight_mult = _signal_type(full)
            cls = classifier.classify(full, top_n=1)
            if not cls:
                continue
            c = cls[0]

            # Identify source from URL
            src_name = "Legal Media"
            for domain, label in [
                ("precedentmag", "Precedent"), ("canadianlawyermag", "Canadian Lawyer"),
                ("lexpert", "Lexpert"), ("lawtimesnews", "Law Times"),
                ("thelawyersdaily", "Lawyer's Daily"), ("lawyersweekly", "Lawyers Weekly"),
                ("nationalmagazine", "National CBA"),
            ]:
                if domain in link:
                    src_name = label
                    break

            signals.append(self._make_signal(
                firm_id=firm["id"], firm_name=firm["name"],
                signal_type=sig_type,
                title=f"[{src_name}] {title[:160]}",
                body=summary[:400],
                url=link,
                department=c["department"],
                department_score=c["score"] * 3.5 * weight_mult,
                matched_keywords=c["matched_keywords"],
            ))
            seen.add(link)
        return signals
