"""
Website Scraper — Practice Area Pages & Attorney Profiles
===========================================================
A firm adding a new practice area page, or updating lawyer bios with a
new practice area, signals deliberate marketing investment in that area.

Two signal types:
  practice_page    (2.5) — firm created/updated a dedicated practice page
  attorney_profile (1.0) — individual bio now lists a new practice area
  website_snapshot (0.0) — content hash stored for change detection next run

Change detection:
  We SHA-256 hash each practice page. If the hash changes run-to-run, we
  know the firm updated that page (new cases listed, new description, etc.).
"""

import re
import hashlib
from bs4 import BeautifulSoup
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

classifier = DepartmentClassifier()

# Practice area page path patterns — what to look for in a firm's nav
PRACTICE_PATH_PATTERNS = [
    "/services", "/practice-areas", "/practices", "/expertise",
    "/en/expertise", "/en/services", "/en-ca/services",
    "/our-services", "/what-we-do",
]

# Attorney listing path patterns
ATTORNEY_PATH_PATTERNS = [
    "/lawyers", "/attorneys", "/people", "/our-team",
    "/professionals", "/en/people", "/en-ca/people",
]

# Minimum page text length to be considered a real practice page
MIN_PAGE_LENGTH = 300


class WebsiteScraper(BaseScraper):
    name = "WebsiteScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []
        signals.extend(self._scrape_practice_pages(firm))
        signals.extend(self._scrape_attorney_profiles(firm))
        return signals

    # ------------------------------------------------------------------ #
    #  Practice area pages
    # ------------------------------------------------------------------ #

    def _scrape_practice_pages(self, firm: dict) -> list[dict]:
        signals = []
        base = firm["website"].rstrip("/")

        for path in PRACTICE_PATH_PATTERNS:
            url = base + path
            response = self._get(url)
            if not response or len(response.text) < 200:
                continue

            soup = BeautifulSoup(response.text, "html.parser")

            # Find all sub-practice links on this hub page
            practice_links = [
                a for a in soup.find_all("a", href=True)
                if any(p in a.get("href", "").lower() for p in
                       ["service", "practice", "expertise", "area", "group"])
                and len(a.get_text(strip=True)) > 3
            ][:5]   # cap sub-fetches per hub — 30 was causing GitHub Actions timeout

            for link in practice_links:
                href = link.get("href", "")
                link_url = (base + href) if href.startswith("/") else href
                link_text = link.get_text(strip=True)

                # Quick classification from the link text alone
                pre_cls = classifier.classify(link_text, top_n=1)
                if not pre_cls:
                    continue

                dept = pre_cls[0]["department"]
                score = pre_cls[0]["score"]

                # Snapshot hash for change detection (zero-weight signal)
                page_resp = self._get(link_url)
                if page_resp and len(page_resp.text) > MIN_PAGE_LENGTH:
                    page_text = BeautifulSoup(page_resp.text, "html.parser").get_text(separator=" ")
                    content_hash = hashlib.sha256(page_text.encode()).hexdigest()

                    # Store snapshot
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="website_snapshot",
                        title=f"[Snapshot] {firm['short']} — {link_text}",
                        body=content_hash,  # stored as body for hash comparison
                        url=link_url,
                        department=dept,
                        department_score=0.0,
                        matched_keywords=[],
                    ))

                    # Full page classification for the practice_page signal
                    full_cls = classifier.classify(page_text[:2000], top_n=1)
                    if full_cls and full_cls[0]["score"] >= 1.5:
                        signals.append(self._make_signal(
                            firm_id=firm["id"],
                            firm_name=firm["name"],
                            signal_type="practice_page",
                            title=f"[Practice Page] {firm['short']} — {link_text}",
                            body=page_text[:600],
                            url=link_url,
                            department=full_cls[0]["department"],
                            department_score=full_cls[0]["score"],
                            matched_keywords=full_cls[0]["matched_keywords"],
                        ))
                else:
                    # No sub-page — use link text only
                    signals.append(self._make_signal(
                        firm_id=firm["id"],
                        firm_name=firm["name"],
                        signal_type="practice_page",
                        title=f"[Practice Link] {firm['short']} — {link_text}",
                        body=link_text,
                        url=link_url,
                        department=dept,
                        department_score=score,
                        matched_keywords=pre_cls[0]["matched_keywords"],
                    ))

            if any(s["signal_type"] == "practice_page" for s in signals):
                break  # found a working practice hub

        self.logger.info(f"[{firm['short']}] Practice pages: {len(signals)} signal(s)")
        return signals

    # ------------------------------------------------------------------ #
    #  Attorney profiles
    # ------------------------------------------------------------------ #

    def _scrape_attorney_profiles(self, firm: dict) -> list[dict]:
        signals = []
        base = firm["website"].rstrip("/")

        for path in ATTORNEY_PATH_PATTERNS:
            url = base + path
            response = self._get(url)
            if not response or len(response.text) < 200:
                continue

            soup = BeautifulSoup(response.text, "html.parser")

            # Find individual bio cards
            bio_cards = soup.find_all(
                ["div", "article", "li"],
                class_=re.compile(r"lawyer|attorney|person|profile|bio|team-member|professional", re.I)
            )[:40]

            for card in bio_cards:
                text = card.get_text(separator=" ", strip=True)
                if len(text) < 30:
                    continue

                classifications = classifier.classify(text, top_n=1)
                if not classifications or classifications[0]["score"] < 1.0:
                    continue

                cls = classifications[0]
                name_tag = card.find(["h2", "h3", "h4", "strong"])
                name = name_tag.get_text(strip=True) if name_tag else text[:60]

                signals.append(self._make_signal(
                    firm_id=firm["id"],
                    firm_name=firm["name"],
                    signal_type="attorney_profile",
                    title=f"[Attorney Profile] {name} — {cls['department']}",
                    body=text[:400],
                    url=url,
                    department=cls["department"],
                    department_score=cls["score"],
                    matched_keywords=cls["matched_keywords"],
                ))

            if signals:
                break

        self.logger.info(f"[{firm['short']}] Attorney profiles: {len(signals)} signal(s)")
        return signals
