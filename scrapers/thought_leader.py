"""
ThoughtLeaderScraper — tracks firm lawyers as quoted experts in media.

When journalists quote a firm's lawyer as an expert on a legal topic,
it signals that firm is recognized as a practice leader in that area.
Repeated expert quotes = deliberate BD strategy = expansion intent.

Sources:
  Globe & Mail law coverage     (site: filter via Google News)
  Financial Post legal          (site: filter)
  National Post legal           (site: filter)
  Toronto Star legal            (site: filter)
  CBC News law/business         (site: filter)
  CTV News legal analysis       (site: filter)
  iPolitics legal               (iPolitics.ca)
  Policy Options                (policyoptions.irpp.org)
  Lawyers Weekly commentary     (lawyersweekly.ca)
  SLAW commentary               (slaw.ca)

Detection: article mentions firm + "said", "told", "according to", "noted",
"explained", "argued", "commented" — classic expert-quote patterns.
"""

import re
from urllib.parse import quote_plus, urljoin

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

import feedparser

classifier = DepartmentClassifier()

EXPERT_QUOTE_RE = re.compile(
    r"\b(?:said|says|told|according\s+to|noted|explained|argued|commented|"
    r"stated|observed|suggests?|warns?|advises?)\b",
    re.IGNORECASE,
)

EXPERT_CONTEXT_RE = re.compile(
    r"(?:lawyer|partner|counsel|attorney|legal\s+expert|"
    r"specialist|practitioner)\s+(?:at|with|from)\s+\w+",
    re.IGNORECASE,
)

# Google News domain-restricted searches
MEDIA_DOMAINS = [
    "site:theglobeandmail.com",
    "site:financialpost.com",
    "site:nationalpost.com",
    "site:thestar.com",
    "site:cbc.ca",
    "site:slaw.ca",
    "site:lawyersweekly.ca",
    "site:policyoptions.irpp.org",
    "site:ipolitics.ca",
]

RSS_SOURCES = [
    {"name": "Slaw Commentary",    "url": "https://www.slaw.ca/feed/",             "weight": 3.0},
    {"name": "Lawyers Weekly",     "url": "https://www.lawyersweekly.ca/rss/",      "weight": 3.0},
    {"name": "Policy Options",     "url": "https://policyoptions.irpp.org/feed/",   "weight": 2.5},
    {"name": "iPolitics",          "url": "https://ipolitics.ca/feed/",             "weight": 2.5},
    {"name": "Globe Business",     "url": "https://www.theglobeandmail.com/business/rss", "weight": 2.0},
    {"name": "Financial Post",     "url": "https://financialpost.com/feed",         "weight": 2.0},
    {"name": "National Post Biz",  "url": "https://nationalpost.com/category/news/business/feed", "weight": 2.0},
    {"name": "CBC Law",            "url": "https://www.cbc.ca/cmlink/rss-law",      "weight": 2.0},
]

GOOG = "https://news.google.com/rss/search?q={q}&hl=en-CA&gl=CA&ceid=CA:en"


class ThoughtLeaderScraper(BaseScraper):
    name = "ThoughtLeaderScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        seen: set = set()
        firm_tokens = [firm["short"].lower(), firm["name"].split()[0].lower()] + \
                      [a.lower() for a in firm.get("alt_names", [])]

        # 1 — RSS: scan for expert quotes mentioning firm
        for src in RSS_SOURCES:
            try:
                feed = feedparser.parse(src["url"], request_headers={
                    "User-Agent": "Mozilla/5.0 (compatible; LegalTracker/2.0)"
                })
            except Exception:
                continue

            for entry in (feed.entries or [])[:30]:
                title   = entry.get("title", "") or ""
                summary = entry.get("summary", "") or ""
                link    = entry.get("link", src["url"]) or src["url"]
                if link in seen:
                    continue
                full  = f"{title} {summary}"
                lower = full.lower()
                if not any(t in lower for t in firm_tokens):
                    continue
                if not (EXPERT_QUOTE_RE.search(full) or EXPERT_CONTEXT_RE.search(full)):
                    continue

                cls = classifier.classify(full, top_n=1)
                if not cls:
                    continue
                c = cls[0]

                signals.append(self._make_signal(
                    firm_id=firm["id"], firm_name=firm["name"],
                    signal_type="press_release",
                    title=f"[Expert Quote — {src['name']}] {title[:160]}",
                    body=summary[:500],
                    url=link,
                    department=c["department"],
                    department_score=c["score"] * src["weight"],
                    matched_keywords=c["matched_keywords"],
                ))
                seen.add(link)

        # 2 — Google News: firm quoted in mainstream press
        domains_filter = " OR ".join(MEDIA_DOMAINS[:4])
        q   = f'"{firm["short"]}" lawyer OR partner ({domains_filter})'
        url = GOOG.format(q=quote_plus(q))
        try:
            feed = feedparser.parse(url, request_headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalTracker/2.0)"
            })
        except Exception:
            feed = type("F", (), {"entries": []})()

        for entry in (feed.entries or [])[:12]:
            title   = entry.get("title", "") or ""
            summary = entry.get("summary", "") or ""
            link    = entry.get("link", url) or url
            if link in seen:
                continue
            full  = f"{title} {summary}"
            if not any(t in full.lower() for t in firm_tokens):
                continue
            if not (EXPERT_QUOTE_RE.search(full) or EXPERT_CONTEXT_RE.search(full)):
                continue

            cls = classifier.classify(full, top_n=1)
            if not cls:
                continue
            c = cls[0]

            signals.append(self._make_signal(
                firm_id=firm["id"], firm_name=firm["name"],
                signal_type="press_release",
                title=f"[Expert Quote — Media] {title[:160]}",
                body=summary[:400],
                url=link,
                department=c["department"],
                department_score=c["score"] * 2.5,
                matched_keywords=c["matched_keywords"],
            ))
            seen.add(link)

        return signals[:12]
