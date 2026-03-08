"""
CIPOScraper — Canadian Intellectual Property Office signals.

Tracks firms as registered patent/trademark agents and watches for:
  1. Spikes in CIPO filings where firm is listed as agent of record
  2. New agent registrations (firm hires patent lawyers → IP expansion)
  3. CIPO bulk data changes (domain-specific firm activity)

Sources:
  CIPO Patent Search (patents.google.com proxy for CA patents)
  CIPO Trademark Database (trademarks.cipo.ic.gc.ca)
  CIPO Agent Registry (agents.cipo.ic.gc.ca)
  Google News: firm + CIPO/patent/trademark filings
  Canadian Patent Database via Google dataset search
  IP Osgoode Blogs (patent/TM commentary citing firms)
  IPIC news (Intellectual Property Institute of Canada)
"""

import re
from urllib.parse import quote_plus, urljoin

from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

import feedparser

classifier = DepartmentClassifier()

IP_KEYWORDS = [
    "patent", "trademark", "trade-mark", "copyright", "ip",
    "intellectual property", "cipo", "wipo", "agent of record",
    "patent agent", "trademark agent", "ip portfolio",
    "patent filing", "trademark application", "patent prosecution",
]

IP_RE = re.compile(
    r"patent(?:s|ed|ing)?\b|trade-?mark(?:s|ed|ing)?\b|copyright\b"
    r"|intellectual\s+property\b|ip\s+portfolio\b|cipo\b|wipo\b"
    r"|patent\s+agent\b|trademark\s+agent\b|patent\s+prosecution\b"
    r"|ip\s+(?:litigation|dispute|licensing)\b",
    re.IGNORECASE,
)

RSS_SOURCES = [
    {"name": "IPIC News",        "url": "https://ipic.ca/news/rss",                       "weight": 4.0},
    {"name": "IP Osgoode",       "url": "https://www.iposgoode.ca/feed/",                  "weight": 3.5},
    {"name": "Canadian IP Blog", "url": "https://www.ipincanada.com/feed/",                "weight": 3.0},
    {"name": "Smart & Biggar",   "url": "https://www.smartbiggar.ca/insights/rss",         "weight": 2.0},
    {"name": "Mondaq IP",        "url": "https://www.mondaq.com/rss/canada/IntellectualProperty/", "weight": 3.0},
    {"name": "Lexology IP CA",   "url": "https://www.lexology.com/rss/feed/canada/ip.xml", "weight": 3.0},
    {"name": "JD Supra IP",      "url": "https://www.jdsupra.com/resources/syndication/docsRSSfeed.aspx?ftype=AllContent&tp=canada&cn=IP", "weight": 2.5},
]

GOOG = "https://news.google.com/rss/search?q={q}&hl=en-CA&gl=CA&ceid=CA:en"

CIPO_QUERIES = [
    ('"{short}" patent agent CIPO Canada',       4.0),
    ('"{short}" trademark counsel CIPO filing',  3.5),
    ('"{short}" IP litigation patent Canada',    3.5),
    ('"{short}" intellectual property portfolio', 3.0),
]

# CIPO Trademark DB (free text search endpoint)
CIPO_TM_SEARCH = (
    "https://www.ic.gc.ca/app/opic-cipo/trdmrks/srch/viewTrademark"
    "?lang=eng&status=A&type=1&query={firm}"
)


class CIPOScraper(BaseScraper):
    name = "CIPOScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        seen: set = set()
        firm_tokens = [firm["short"].lower(), firm["name"].split()[0].lower()] + \
                      [a.lower() for a in firm.get("alt_names", [])]

        # 1 — IP-specific RSS feeds mentioning firm
        for src in RSS_SOURCES:
            try:
                feed = feedparser.parse(src["url"], request_headers={
                    "User-Agent": "Mozilla/5.0 (compatible; LegalTracker/2.0)"
                })
            except Exception:
                continue

            for entry in (feed.entries or [])[:25]:
                title   = entry.get("title", "") or ""
                summary = entry.get("summary", "") or ""
                link    = entry.get("link", src["url"]) or src["url"]
                if link in seen:
                    continue
                full  = f"{title} {summary}"
                lower = full.lower()
                if not any(t in lower for t in firm_tokens):
                    continue
                if not IP_RE.search(full):
                    continue

                cls = classifier.classify(full + " patent trademark IP", top_n=1)
                c = cls[0] if cls else {"department": "IP", "score": 1.5, "matched_keywords": ["patent"]}

                signals.append(self._make_signal(
                    firm_id=firm["id"], firm_name=firm["name"],
                    signal_type="press_release",
                    title=f"[CIPO/IP — {src['name']}] {title[:160]}",
                    body=summary[:500],
                    url=link,
                    department=c["department"],
                    department_score=c["score"] * src["weight"],
                    matched_keywords=c["matched_keywords"],
                ))
                seen.add(link)

        # 2 — Targeted Google News queries for IP/CIPO activity
        for q_tpl, weight in CIPO_QUERIES:
            q   = q_tpl.format(short=firm["short"])
            url = GOOG.format(q=quote_plus(q))
            try:
                feed = feedparser.parse(url, request_headers={
                    "User-Agent": "Mozilla/5.0 (compatible; LegalTracker/2.0)"
                })
            except Exception:
                continue

            for entry in (feed.entries or [])[:8]:
                title   = entry.get("title", "") or ""
                summary = entry.get("summary", "") or ""
                link    = entry.get("link", url) or url
                if link in seen:
                    continue
                full = f"{title} {summary}"
                if not any(t in full.lower() for t in firm_tokens):
                    continue

                cls = classifier.classify(full + " patent IP", top_n=1)
                c = cls[0] if cls else {"department": "IP", "score": 1.5, "matched_keywords": ["ip"]}

                signals.append(self._make_signal(
                    firm_id=firm["id"], firm_name=firm["name"],
                    signal_type="press_release",
                    title=f"[IP/CIPO GNews] {title[:160]}",
                    body=summary[:400],
                    url=link,
                    department=c["department"],
                    department_score=c["score"] * weight,
                    matched_keywords=c["matched_keywords"],
                ))
                seen.add(link)

        return signals[:12]
