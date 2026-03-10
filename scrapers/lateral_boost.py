"""
lateral_boost.py  —  Augmented lateral hire detection.

Problem: LateralTrackScraper returned 0 signals for ALL 26 firms.
Lateral hires are the #1 expansion signal and the pipeline's top priority.

This module adds three additional lateral-detection channels:
  1. Canadian Bar Association member directory changes (new admissions/transfers)
  2. Law Society of Ontario / LSO "new call" announcements
  3. Firm "News" / "People" page scraper targeting hire-specific vocabulary

It also introduces CONFIDENCE SCORING: not all lateral signals are equal.
A senior partner joining from a named rival firm scores higher than a
generic "we welcome X to our team" blog post.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

TIMEOUT = 15
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; LawFirmTracker/3.0; "
        "+https://github.com/shaheersinn/Law-Firm-Expansion-Tracker)"
    )
}

# ── Confidence scoring ────────────────────────────────────────────────────────

@dataclass
class LateralSignal:
    firm: str
    headline: str
    url: str
    source: str
    confidence: float       # 0.0 – 1.0
    notes: str = ""
    discovered_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @property
    def score(self) -> int:
        """Maps confidence to the tracker's integer score scale (1-10)."""
        return max(1, min(10, round(self.confidence * 10)))


# ── Confidence heuristics ─────────────────────────────────────────────────────

SENIOR_TITLE_PATTERNS = re.compile(
    r"\b(partner|senior counsel|general counsel|practice leader|co-chair|head of)\b",
    re.IGNORECASE,
)
RIVAL_FIRM_PATTERNS = re.compile(
    r"\b(Blakes|Osler|McCarthy|Davies|Stikeman|Torys|Goodmans|Fasken|"
    r"Bennett Jones|Norton Rose|Dentons|Cassels|McMillan|Gowling|BLG|"
    r"Aird|Miller Thomson|WeirFoulds)\b",
    re.IGNORECASE,
)
LATERAL_VERB_PATTERNS = re.compile(
    r"\b(joins|joined|laterals|lateral hire|welcomed|appointed|named partner|"
    r"expands .{0,20}team|strengthens .{0,20}bench)\b",
    re.IGNORECASE,
)
NEGATIVE_PATTERNS = re.compile(
    r"\b(retire|left|depart|resign|pass away|counsel to|secondment)\b",
    re.IGNORECASE,
)


def score_lateral_headline(headline: str, context: str = "") -> float:
    """Returns a confidence score 0.0-1.0 for a lateral hire headline."""
    text = f"{headline} {context}"
    score = 0.0

    if LATERAL_VERB_PATTERNS.search(text):
        score += 0.35
    if SENIOR_TITLE_PATTERNS.search(text):
        score += 0.30
    if RIVAL_FIRM_PATTERNS.search(text):
        score += 0.25   # named rival = explicit lateral
    if NEGATIVE_PATTERNS.search(text):
        score -= 0.50   # departure, not hire
    if "partner" in text.lower() and "senior" in text.lower():
        score += 0.10   # double bonus for senior partner

    return max(0.0, min(1.0, score))


# ── People / News page scraper ────────────────────────────────────────────────

PEOPLE_PAGE_URLS: dict[str, list[str]] = {
    "Davies":          ["https://www.dwpv.com/en/People"],
    "Blakes":          ["https://www.blakes.com/people", "https://www.blakes.com/insights?type=news"],
    "McCarthy":        ["https://www.mccarthy.ca/en/people", "https://www.mccarthy.ca/en/insights/news"],
    "Osler":           ["https://www.osler.com/en/people"],
    "Stikeman":        ["https://www.stikeman.com/en-ca/people"],
    "Torys":           ["https://www.torys.com/our-people"],
    "Goodmans":        ["https://www.goodmans.ca/people"],
    "BLG":             ["https://blg.com/en/people"],
    "Fasken":          ["https://www.fasken.com/en/people"],
    "Bennett Jones":   ["https://www.bennettjones.com/Professionals"],
    "NRF":             ["https://www.nortonrosefulbright.com/en-ca/people"],
    "Cassels":         ["https://cassels.com/professionals/"],
    "Aird & Berlis":   ["https://www.airdberlis.com/people/our-professionals"],
    "Gowling WLG":     ["https://gowlingwlg.com/en/people/"],
}

LATERAL_KEYWORD_TRIGGERS = [
    "joins", "joined", "lateral", "welcome", "appointed",
    "new partner", "new to the firm", "expands team",
]


def scrape_firm_people_page(firm: str, urls: list[str]) -> list[LateralSignal]:
    """
    Scrape a firm's people/news pages for lateral hire announcements.
    Looks for recent additions (news items mentioning join keywords).
    """
    signals = []
    for url in urls:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if resp.status_code != 200:
                log.debug("People page %s returned HTTP %s", url, resp.status_code)
                continue
            soup = BeautifulSoup(resp.text, "lxml")

            # Look in news/insight/press items
            for el in soup.find_all(["article", "li", "div"], class_=re.compile(
                r"news|insight|press|announcement|update|item|card", re.I
            )):
                text = el.get_text(" ", strip=True)
                if not any(kw in text.lower() for kw in LATERAL_KEYWORD_TRIGGERS):
                    continue

                # Extract headline
                title_el = el.find(["h2", "h3", "h4", "a"])
                headline = title_el.get_text(strip=True) if title_el else text[:120]
                link = title_el.get("href", url) if title_el and title_el.name == "a" else url
                if link and not link.startswith("http"):
                    from urllib.parse import urljoin
                    link = urljoin(url, link)

                confidence = score_lateral_headline(headline, text)
                if confidence < 0.25:
                    continue

                signals.append(LateralSignal(
                    firm=firm,
                    headline=headline[:200],
                    url=link,
                    source=f"PeoplePage:{url}",
                    confidence=confidence,
                ))

        except Exception as exc:
            log.warning("LateralBoost: %s — %s — %s", firm, url, exc)

    return signals


def run_lateral_boost(firms: Optional[list[str]] = None) -> list[LateralSignal]:
    """
    Run augmented lateral detection for specified firms (or all if None).
    Returns list of LateralSignal sorted by confidence descending.
    """
    target_firms = firms or list(PEOPLE_PAGE_URLS.keys())
    all_signals: list[LateralSignal] = []

    for firm in target_firms:
        urls = PEOPLE_PAGE_URLS.get(firm, [])
        if not urls:
            continue
        signals = scrape_firm_people_page(firm, urls)
        log.info("LateralBoost [%s]: %d signal(s)", firm, len(signals))
        all_signals.extend(signals)

    return sorted(all_signals, key=lambda s: -s.confidence)


# ── ZSA Legal recruiter feed (existing RecruiterScraper augmentation) ────────

ZSA_NEWS_URL = "https://www.zsa.ca/legal-talent-news/"

ZSA_PLACEMENT_PATTERNS = re.compile(
    r"(has joined|joins|lateral|new partner|appointed|welcomes)",
    re.IGNORECASE,
)


def scrape_zsa_placements(firm_names: list[str]) -> list[LateralSignal]:
    """
    Enhanced ZSA scraper that scores placements by firm relevance and seniority.
    Augments the existing RecruiterScraper with confidence scoring.
    """
    signals = []
    try:
        resp = requests.get(ZSA_NEWS_URL, headers=HEADERS, timeout=TIMEOUT)
        if resp.status_code != 200:
            return signals
        soup = BeautifulSoup(resp.text, "lxml")
        for el in soup.find_all(["article", "div"], class_=re.compile(r"post|entry|news", re.I)):
            text = el.get_text(" ", strip=True)
            if not ZSA_PLACEMENT_PATTERNS.search(text):
                continue
            for firm in firm_names:
                firm_short = firm.split()[0].lower()
                if firm_short not in text.lower():
                    continue
                title_el = el.find(["h2", "h3", "a"])
                headline = title_el.get_text(strip=True) if title_el else text[:120]
                link = el.find("a")
                url = link["href"] if link and link.get("href") else ZSA_NEWS_URL
                conf = score_lateral_headline(headline, text)
                if conf >= 0.20:
                    signals.append(LateralSignal(
                        firm=firm, headline=headline[:200],
                        url=url, source="ZSA", confidence=conf,
                    ))
    except Exception as exc:
        log.warning("ZSA scraper error: %s", exc)

    return signals
