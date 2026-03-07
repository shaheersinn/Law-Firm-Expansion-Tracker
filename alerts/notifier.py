"""
Telegram notifier — one polished message per run.

What it sends:
  - Signal count summary (clean type labels, no internal names)
  - Expansion alerts grouped by firm (multiple departments merged under one entry)
  - Practice page changes (grouped by firm, linked cleanly)
  - Dashboard + run log links
"""

import os
import re
import logging
import requests
from collections import defaultdict
from datetime import datetime, timezone
from config import Config

logger = logging.getLogger("notifier")


# ── Department display ────────────────────────────────────────────────────────

DEPT_EMOJI = {
    "Corporate / M&A":                 "🤝",
    "Private Equity":                  "💰",
    "Capital Markets":                 "📈",
    "Litigation & Disputes":           "⚖️",
    "Restructuring & Insolvency":      "🔄",
    "Real Estate":                     "🏢",
    "Tax":                             "🧾",
    "Employment & Labour":             "👷",
    "Intellectual Property":           "💡",
    "Data Privacy & Cybersecurity":    "🔒",
    "ESG & Regulatory":                "🌿",
    "Energy & Natural Resources":      "⚡",
    "Financial Services & Regulatory": "🏦",
    "Competition & Antitrust":         "🔍",
    "Healthcare & Life Sciences":      "🏥",
    "Immigration":                     "🛂",
    "Infrastructure & Projects":       "🏗️",
}


# ── Signal labels ─────────────────────────────────────────────────────────────

SIGNAL_LABEL = {
    "lateral_hire":     "👤 Lateral hire",
    "job_posting":      "📋 Job posting",
    "press_release":    "📰 Press release",
    "publication":      "✍️ Publication",
    "practice_page":    "🌐 Practice page",
    "attorney_profile": "👔 Attorney profile",
    "bar_leadership":   "🏅 Bar leadership",
    "ranking":          "🏆 Ranking",
    "court_record":     "⚖️ Court record",
    "recruit_posting":  "🎓 Articling recruit",
    "bar_speaking":     "🎤 Speaking engagement",
    "bar_sponsorship":  "🤝 Bar sponsorship",
    "bar_mention":      "📌 Bar mention",
}

# Human-readable plural labels for the header summary
# None = hide from header (internal / noise types)
SIGNAL_TYPE_PLURAL = {
    "lateral_hire":     "lateral hires",
    "job_posting":      "job postings",
    "press_release":    "press releases",
    "publication":      "publications",
    "practice_page":    "practice pages",
    "attorney_profile": "attorney profiles",
    "bar_leadership":   "bar leadership",
    "ranking":          "rankings",
    "court_record":     "court records",
    "recruit_posting":  "recruit postings",
    "website_snapshot": None,   # internal — never shown to user
}

# Readable singular/plural for breakdown line
BREAKDOWN_SINGULAR = {
    "lateral_hire":    "lateral hire",
    "job_posting":     "job posting",
    "press_release":   "press release",
    "publication":     "publication",
    "practice_page":   "practice page",
    "attorney_profile":"attorney profile",
    "bar_leadership":  "bar leadership",
    "ranking":         "ranking",
    "court_record":    "court record",
    "recruit_posting": "recruit posting",
}


# ── URL / env setup ───────────────────────────────────────────────────────────

_REPO  = os.environ.get("GITHUB_REPOSITORY", "")
_RUN   = os.environ.get("GITHUB_RUN_ID", "")
_OWNER = _REPO.split("/")[0] if "/" in _REPO else ""
_RNAME = _REPO.split("/")[1] if "/" in _REPO else "law-firm-tracker"

DASHBOARD_URL = os.environ.get(
    "DASHBOARD_URL",
    f"https://{_OWNER}.github.io/{_RNAME}/" if _OWNER else f"https://github.com/{_REPO}",
)
RUN_URL = (
    f"https://github.com/{_REPO}/actions/runs/{_RUN}"
    if (_REPO and _RUN) else ""
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _p(n: int, singular: str, plural: str = None) -> str:
    """Proper plural. _p(1,'signal') → '1 signal', _p(3,'signal') → '3 signals'"""
    word = plural if (plural and n != 1) else (singular + ("s" if n != 1 else ""))
    return f"{n} {word}"


_MINOR_WORDS = {"and", "or", "of", "the", "in", "on", "at", "to", "a", "an",
               "for", "with", "by", "from", "but", "nor", "yet", "so"}

def _smart_title(s: str) -> str:
    """Title-case that keeps minor words lowercase (except the first word)."""
    words = s.split()
    return " ".join(
        w.capitalize() if (i == 0 or w.lower() not in _MINOR_WORDS) else w.lower()
        for i, w in enumerate(words)
    )


def _clean_title(raw: str, max_len: int = 72) -> str:
    """
    Strip internal source-tag prefixes and artifact words, then truncate with '…'.

    "[Practice Page] Goodmans — Capital Markets"              → "Capital Markets"
    "[Firm News] PublicationThe rise of class actionsCanada"  → "The rise of class actions Canada"
    "[Google News] Foundations for settlement: A guide to…"   → "Foundations for settlement: A guide to…"
    """
    t = raw.strip()

    # 1. Strip [Source Tag] prefix
    t = re.sub(r"^\[.*?\]\s*", "", t)

    # 2. Strip artifact label words HTML parsers sometimes glue to the real title
    t = re.sub(
        r"^(Publication|Article|Insights?|News|Update|Blog Post|Alert|Press Release|Release|Event)\s*",
        "", t, flags=re.IGNORECASE
    ).strip()

    # 3. Strip "FirmName — " prefix ONLY on em-dash (not colons — colons appear in real subtitles)
    t = re.sub(r"^[\w\s&,\.']+\s*—\s*", "", t).strip()

    # 4. Split camelCase artifacts (e.g. "actionsCanada" → "actions Canada")
    t = re.sub(r"([a-z])([A-Z])", r"\1 \2", t)

    # 5. Collapse whitespace
    t = re.sub(r"\s+", " ", t).strip()

    if not t:
        t = re.sub(r"\[.*?\]", "", raw).strip()

    # 6. Truncate cleanly with ellipsis
    if len(t) > max_len:
        t = t[:max_len].rsplit(" ", 1)[0].rstrip(",;:") + "…"

    return t or raw[:max_len]


def _fmt_breakdown(breakdown: dict) -> str:
    """
    Human-readable signal breakdown.
    {press_release:3, publication:2, practice_page:4} → "3 press releases, 2 publications, 4 practice pages"
    """
    if not breakdown:
        return ""
    parts = []
    for k, v in breakdown.items():
        if v <= 0:
            continue
        singular = BREAKDOWN_SINGULAR.get(k, k.replace("_", " "))
        plural   = singular + "s"
        parts.append(f"{v} {singular if v == 1 else plural}")
    return ", ".join(parts)


def _strength_badge(score: float, is_spike: bool) -> str:
    """Visual strength indicator — replaces the raw numeric score."""
    if is_spike:
        return "🚨 Spike"
    if score >= 40:
        return "🔴 Very strong"
    if score >= 20:
        return "🟠 Strong"
    if score >= 10:
        return "🟡 Moderate"
    return "🟢 Emerging"


_URL_SKIP = {"expertise-detail", "expertise", "services", "en", "en-ca",
             "areas-of-law", "practice", "our", "about", "insights"}

def _page_name_from_url(url: str) -> str:
    """
    Extract a human-readable page name from a URL path, skipping generic segments.
    "https://osler.com/en/expertise/services/capital-markets/"    → "Capital Markets"
    "https://goodmans.ca/expertise-detail/banking-and-financial-services" → "Banking and Financial Services"
    """
    try:
        parts = [p for p in url.rstrip("/").split("/") if p]
        slug  = parts[-1]   # start with last segment
        # Walk backwards to find the most meaningful slug
        for part in reversed(parts):
            if part.lower() not in _URL_SKIP and len(part) > 3:
                slug = part
                break
        name = _smart_title(slug.replace("-", " ").replace("_", " "))
        return name.strip() or slug
    except Exception:
        return url


# ── Main class ────────────────────────────────────────────────────────────────

class Notifier:
    def __init__(self, config: Config):
        self.config = config
        self.base_url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}"

    def send_combined_digest(
        self,
        alerts: list[dict],
        website_changes: list[dict],
        new_signals: list = None,
    ):
        if not self.config.TELEGRAM_BOT_TOKEN or not self.config.TELEGRAM_CHAT_ID:
            logger.warning("Telegram not configured — skipping")
            return
        msg = self._build_message(alerts, website_changes, new_signals or [])
        self._send(msg)

    def send_weekly_digest(self, alerts, website_changes, **kwargs):
        self.send_combined_digest(alerts, website_changes, kwargs.get("new_signals", []))

    # ── Message builder ───────────────────────────────────────────────────────

    def _build_message(
        self,
        alerts: list[dict],
        website_changes: list[dict],
        new_signals: list,
    ) -> str:
        ts = datetime.now(timezone.utc).strftime("%b %d %Y · %H:%M UTC")

        lines = [
            "📊 <b>Law Firm Expansion Tracker</b>",
            f"<i>{ts}</i>",
            "─" * 30,
        ]

        # ── Signal summary ────────────────────────────────────────────────────
        if new_signals:
            n_sigs    = len(new_signals)
            n_firms   = len(set(s["firm_id"] for s in new_signals))

            by_type: dict[str, int] = {}
            for s in new_signals:
                by_type[s["signal_type"]] = by_type.get(s["signal_type"], 0) + 1

            visible_types = [
                (k, v) for k, v in sorted(by_type.items(), key=lambda x: -x[1])
                if SIGNAL_TYPE_PLURAL.get(k) is not None   # hides website_snapshot
            ]
            type_parts = [
                f"{v} {SIGNAL_TYPE_PLURAL.get(k, k.replace('_',' '))}"
                for k, v in visible_types[:5]
            ]

            lines.append(
                f"🆕 <b>{_p(n_sigs, 'new signal')}</b> across {_p(n_firms, 'firm')}"
            )
            if type_parts:
                lines.append(f"   <i>{' · '.join(type_parts)}</i>")
            lines.append("")

        # ── Expansion alerts (grouped by firm) ────────────────────────────────
        if alerts:
            # Group alert buckets by firm_id
            by_firm: dict[str, list] = defaultdict(list)
            for a in alerts:
                by_firm[a["firm_id"]].append(a)

            n_alert_firms = len(by_firm)
            n_buckets     = len(alerts)

            lines.append(
                f"🔥 <b>{_p(n_buckets, 'expansion alert')}</b> "
                f"across {_p(n_alert_firms, 'firm')}"
            )
            lines.append("")

            # Sort firms by their highest-scoring bucket
            sorted_firms = sorted(
                by_firm.items(),
                key=lambda kv: max(a["expansion_score"] for a in kv[1]),
                reverse=True,
            )

            for i, (firm_id, firm_alerts) in enumerate(sorted_firms[:10], 1):
                firm_name = firm_alerts[0]["firm_name"]

                # Sort this firm's buckets by score
                firm_alerts = sorted(firm_alerts, key=lambda a: a["expansion_score"], reverse=True)
                top_bucket  = firm_alerts[0]
                total_signals = sum(a["signal_count"] for a in firm_alerts)

                # Strength badge uses the top bucket score
                strength = _strength_badge(
                    top_bucket["expansion_score"],
                    top_bucket.get("is_spike", False),
                )
                spike_tag = ""
                if top_bucket.get("is_spike") and top_bucket.get("spike_ratio"):
                    spike_tag = f" · ↑{top_bucket['spike_ratio']}× vs baseline"

                # Department list for this firm
                dept_tags = " · ".join(
                    f"{DEPT_EMOJI.get(a['department'], '⚖️')} {a['department']}"
                    for a in firm_alerts[:3]
                )

                lines.append(f"{i}. 🏛 <b>{firm_name}</b>")
                lines.append(f"   {dept_tags}")
                lines.append(f"   {strength}{spike_tag} · {_p(total_signals, 'signal')}")

                # Aggregate breakdown across all this firm's buckets
                merged_breakdown: dict[str, int] = defaultdict(int)
                for a in firm_alerts:
                    for k, v in (a.get("signal_breakdown") or {}).items():
                        merged_breakdown[k] += v
                bd_str = _fmt_breakdown(dict(merged_breakdown))
                if bd_str:
                    lines.append(f"   <i>{bd_str}</i>")

                # Top evidence: best signals across all buckets for this firm
                all_top = []
                for a in firm_alerts:
                    all_top.extend(a.get("top_signals", []))
                # De-dup by url/title, keep highest-weight
                seen_urls: set[str] = set()
                deduped = []
                for sig in all_top:
                    key = sig.get("url") or sig["title"][:60]
                    if key not in seen_urls:
                        seen_urls.add(key)
                        deduped.append(sig)

                for sig in deduped[:3]:
                    label = SIGNAL_LABEL.get(sig["signal_type"], sig["signal_type"])
                    title = _clean_title(sig["title"])
                    url   = sig.get("url", "")
                    if url:
                        lines.append(f"   • {label}: <a href='{url}'>{title}</a>")
                    else:
                        lines.append(f"   • {label}: <i>{title}</i>")

                lines.append("")
        else:
            lines.append("ℹ️ No expansion alerts this run.")
            lines.append("")

        # ── Practice page changes ─────────────────────────────────────────────
        if website_changes:
            total = len(website_changes)

            # Group by firm
            changes_by_firm: dict[str, list] = defaultdict(list)
            for ch in website_changes:
                changes_by_firm[ch["firm_name"]].append(ch)

            lines.append(f"🌐 <b>Practice page changes</b> ({total})")

            shown = 0
            MAX   = 8
            for firm_name, changes in changes_by_firm.items():
                if shown >= MAX:
                    rem = total - shown
                    lines.append(f"   <i>+{rem} more — see dashboard</i>")
                    break
                firm_max = min(3, MAX - shown)
                for ch in changes[:firm_max]:
                    url  = ch.get("url", "")
                    name = _page_name_from_url(url) if url else ch.get("message", "")[:50]
                    if url:
                        lines.append(f"   • <b>{firm_name}</b> — <a href='{url}'>{name}</a>")
                    else:
                        lines.append(f"   • <b>{firm_name}</b> — {name}")
                    shown += 1
                extra = len(changes) - firm_max
                if extra > 0 and shown < MAX:
                    lines.append(f"   <i>  +{extra} more page{'s' if extra != 1 else ''} for {firm_name}</i>")
            lines.append("")

        # ── Footer ────────────────────────────────────────────────────────────
        lines.append("─" * 30)
        footer_links = [f"<a href='{DASHBOARD_URL}'>📈 Dashboard</a>"]
        if RUN_URL:
            footer_links.append(f"<a href='{RUN_URL}'>📋 Run logs</a>")
        lines.append("  ·  ".join(footer_links))

        full = "\n".join(lines)

        # Trim to Telegram's 4096-char limit by dropping lowest-ranked alert firms
        if len(full) > 4000 and len(alerts) > 1:
            return self._build_message(alerts[:-1], website_changes, new_signals)

        return full

    # ── HTTP ──────────────────────────────────────────────────────────────────

    def _send(self, text: str):
        try:
            resp = requests.post(
                f"{self.base_url}/sendMessage",
                json={
                    "chat_id":                  self.config.TELEGRAM_CHAT_ID,
                    "text":                     text,
                    "parse_mode":               "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=15,
            )
            if resp.ok:
                logger.info("Telegram combined digest sent (1 message)")
            else:
                logger.error(f"Telegram error {resp.status_code}: {resp.text[:200]}")
        except requests.RequestException as e:
            logger.error(f"Telegram request failed: {e}")
