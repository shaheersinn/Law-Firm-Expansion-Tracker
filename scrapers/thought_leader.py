"""
ThoughtLeaderScraper
Monitors publication velocity as a practice-group growth proxy.

Signal research insight:
  "A firm that suddenly starts publishing weekly alerts on AI regulation,
   trade law, or insolvency is almost certainly building a practice and
   winning mandates in that space."
  "Subscribe to firm mailing lists — Most Canadian firms have free
   subscription portals for client updates by practice area."

We count how many recent publications a firm has per department and flag
any department showing an above-average publication burst.
"""

from collections import Counter
from scrapers.base import BaseScraper
from classifier.department import DepartmentClassifier

_clf = DepartmentClassifier()

TL_WEIGHT = 1.5

# Hot 2026 topics that, when published about frequently, signal active practices
HOT_TOPICS = [
    "ai regulation", "artificial intelligence", "privacy", "trade tariff",
    "u.s. tariff", "employment rights act", "insolvency", "restructuring",
    "cybersecurity", "climate", "esg", "data breach", "budget 2025",
    "employment law", "competition act", "immigration",
]


class ThoughtLeaderScraper(BaseScraper):
    name = "ThoughtLeaderScraper"

    def fetch(self, firm: dict) -> list[dict]:
        signals = []

        insights_url = firm.get("news_url", "")
        if not insights_url:
            return signals

        soup = self._soup(insights_url, timeout=15)
        if not soup:
            return signals

        dept_counter: Counter = Counter()
        topic_hits: dict[str, list[str]] = {}

        # Gather recent article titles and classify them
        texts = []
        for tag in soup.find_all(["a", "h2", "h3"])[:60]:
            text = self._clean(tag.get_text())
            if len(text) < 20:
                continue
            texts.append(text)

        for text in texts:
            lower = text.lower()
            for topic in HOT_TOPICS:
                if topic in lower:
                    dept, score, kw = _clf.top_department(text)
                    dept_counter[dept] += 1
                    topic_hits.setdefault(dept, [])
                    topic_hits[dept].append(text[:80])

        # Surface departments with 3+ recent articles (velocity signal)
        for dept, count in dept_counter.most_common(3):
            if count < 2:
                continue
            examples = topic_hits.get(dept, [])[:3]
            signals.append(self._make_signal(
                firm_id=firm["id"],
                firm_name=firm["name"],
                signal_type="thought_leadership",
                title=f"[TL Velocity] {firm['short']} — {dept}: {count} recent pieces",
                body=" | ".join(examples),
                url=insights_url,
                department=dept,
                department_score=count * TL_WEIGHT,
                matched_keywords=[dept.lower(), "content_velocity"],
            ))

        return signals[:3]
