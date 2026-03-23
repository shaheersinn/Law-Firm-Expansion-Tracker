"""
Multi-agent signal verification for scraped data.

The verifier uses a panel of specialized agents to independently score each
signal, then combines their opinions into a final verification verdict.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Optional
from urllib.parse import urlparse

from config_calgary import FIRM_BY_ID
from database.db import get_conn

log = logging.getLogger(__name__)

CONFIDENCE_FLOOR = 0.45
HIGH_CONFIDENCE_FLOOR = 0.78
MAX_VERIFICATION_AGE_DAYS = 90

BOILERPLATE_RE = re.compile(
    r"^(website snapshot|placeholder|test signal|untitled|n/a|none|unknown|\s*)$",
    re.IGNORECASE,
)
CALGARY_TERMS = re.compile(
    r"\b(calgary|alberta|ab|edmonton|energy|oil|gas|pipeline|aer|auc|"
    r"sedar|canlii|tsxv|tsx\.v|transactions?|litigation|hearing|court|"
    r"m&a|financing|prospectus|merger|acquisition)\b",
    re.IGNORECASE,
)
DATE_TOKEN_RE = re.compile(r"\b(20\d{2})[-/](\d{1,2})[-/](\d{1,2})\b")
MONEY_TOKEN_RE = re.compile(r"\$\s?([0-9][0-9,]*(?:\.[0-9]+)?)\s*(billion|million|bn|mn|m|b)?", re.IGNORECASE)
WORD_RE = re.compile(r"[A-Za-z][A-Za-z&'.-]+")

SOURCE_TRUST = {
    "canlii.org": 0.97,
    "api.canlii.org": 0.99,
    "sedarplus.ca": 0.98,
    "www.sedarplus.ca": 0.98,
    "sec.gov": 0.96,
    "www.sec.gov": 0.96,
    "lawsociety.ab.ca": 0.95,
    "www.lawsociety.ab.ca": 0.95,
    "albertacourts.ca": 0.94,
    "www.albertacourts.ca": 0.94,
    "linkedin.com": 0.66,
    "www.linkedin.com": 0.66,
    "newswire.ca": 0.76,
    "www.newswire.ca": 0.76,
    "businesswire.com": 0.78,
    "www.businesswire.com": 0.78,
    "globenewswire.com": 0.78,
    "www.globenewswire.com": 0.78,
}
DEFAULT_SOURCE_TRUST = 0.62
WEIGHT_RANGES = {
    "sedar_major_deal": (3.0, 6.0),
    "biglaw_spillage_predicted": (3.0, 5.5),
    "canlii_appearance_spike": (2.5, 5.0),
    "linkedin_turnover_detected": (3.5, 5.5),
    "lsa_student_not_retained": (2.0, 4.0),
    "lateral_hire": (2.0, 4.0),
    "job_posting": (1.0, 3.0),
    "macro_ma_wave_incoming": (3.0, 5.0),
    "macro_demand_surge": (2.5, 4.5),
    "fiscal_pressure_incoming": (2.0, 4.0),
    "sec_edgar_filing": (1.5, 3.5),
    "web_signal": (1.0, 3.0),
}


@dataclass
class AgentFinding:
    agent: str
    score: float
    summary: str
    evidence: list[str]


@dataclass
class VerificationResult:
    confidence_score: float
    verdict: str
    summary: str
    low_confidence: bool
    findings: list[AgentFinding]

    def as_payload(self) -> dict[str, Any]:
        return {
            "confidence_score": self.confidence_score,
            "verdict": self.verdict,
            "summary": self.summary,
            "low_confidence": self.low_confidence,
            "agents": [asdict(finding) for finding in self.findings],
        }


class VerificationAgent:
    name = "base"

    def evaluate(self, signal: dict[str, Any], conn: Optional[sqlite3.Connection] = None) -> AgentFinding:
        raise NotImplementedError


class SourceReliabilityAgent(VerificationAgent):
    name = "source_reliability"

    def evaluate(self, signal: dict[str, Any], conn: Optional[sqlite3.Connection] = None) -> AgentFinding:
        url = (signal.get("source_url") or "").strip()
        domain = urlparse(url).netloc.lower()
        trust = SOURCE_TRUST.get(domain, DEFAULT_SOURCE_TRUST)
        evidence = [f"domain={domain or 'missing'}", f"base_trust={trust:.2f}"]

        if not url:
            trust -= 0.22
            evidence.append("missing source URL")
        elif domain.endswith(".gov"):
            trust += 0.06
            evidence.append("government domain bonus")
        elif domain.endswith(".ca"):
            trust += 0.02
            evidence.append("canadian domain bonus")

        title = signal.get("title") or ""
        if title and domain and any(token in title.lower() for token in domain.replace("www.", "").split(".")):
            trust += 0.03
            evidence.append("title echoes source domain")

        score = max(0.0, min(1.0, trust))
        return AgentFinding(self.name, round(score, 3), "Assesses whether the source domain is authoritative.", evidence)


class EntityConsistencyAgent(VerificationAgent):
    name = "entity_consistency"

    def evaluate(self, signal: dict[str, Any], conn: Optional[sqlite3.Connection] = None) -> AgentFinding:
        firm_id = signal.get("firm_id") or ""
        if firm_id == "market":
            return AgentFinding(self.name, 0.82, "Market-wide signals are allowed without firm-name matching.", ["firm_id=market"])

        firm = FIRM_BY_ID.get(firm_id)
        if not firm:
            return AgentFinding(self.name, 0.18, "Unknown firm ID was not found in tracked firms.", [f"firm_id={firm_id}"])

        haystack = " ".join(
            str(signal.get(key) or "") for key in ("title", "description", "source_url", "raw_data")
        ).lower()
        aliases = {firm.get("name", "").lower(), firm.get("short", "").lower(), *[a.lower() for a in firm.get("aliases", [])], *[a.lower() for a in firm.get("alt_names", [])]}
        aliases = {alias for alias in aliases if alias}
        alias_hits = sorted(alias for alias in aliases if alias in haystack)

        score = 0.45 + min(0.4, 0.12 * len(alias_hits))
        evidence = [f"firm={firm.get('name', firm_id)}", f"alias_hits={len(alias_hits)}"]
        if alias_hits:
            evidence.append("matched=" + ", ".join(alias_hits[:4]))
        else:
            score -= 0.22
            evidence.append("no firm alias detected in scraped text")

        if CALGARY_TERMS.search(haystack):
            score += 0.08
            evidence.append("regional context detected")

        return AgentFinding(self.name, round(max(0.0, min(1.0, score)), 3), "Checks whether the scraped text consistently refers to the intended firm/context.", evidence)


class TemporalIntegrityAgent(VerificationAgent):
    name = "temporal_integrity"

    def evaluate(self, signal: dict[str, Any], conn: Optional[sqlite3.Connection] = None) -> AgentFinding:
        detected_at = signal.get("detected_at")
        score = 0.72
        evidence = []
        now = datetime.now(timezone.utc)

        if detected_at:
            try:
                detected = datetime.fromisoformat(str(detected_at).replace("Z", "+00:00"))
                age_days = (now - detected).days
                evidence.append(f"age_days={age_days}")
                if age_days <= 7:
                    score += 0.14
                elif age_days <= 30:
                    score += 0.08
                elif age_days > MAX_VERIFICATION_AGE_DAYS:
                    score -= 0.42
                    evidence.append("outside lookback window")
                if detected > now:
                    score -= 0.35
                    evidence.append("future timestamp")
            except ValueError:
                score -= 0.25
                evidence.append("invalid detected_at timestamp")
        else:
            score -= 0.18
            evidence.append("missing detected_at")

        text = f"{signal.get('title') or ''} {signal.get('description') or ''}"
        years = []
        for year, month, day in DATE_TOKEN_RE.findall(text):
            try:
                parsed = datetime(int(year), int(month), int(day), tzinfo=timezone.utc)
                years.append(parsed.year)
                if parsed > now:
                    score -= 0.08
            except ValueError:
                score -= 0.03
        if years:
            evidence.append(f"embedded_years={sorted(set(years))[:4]}")

        return AgentFinding(self.name, round(max(0.0, min(1.0, score)), 3), "Validates timestamps and lookback-window recency.", evidence or ["no temporal anomalies"])


class ContentIntegrityAgent(VerificationAgent):
    name = "content_integrity"

    def evaluate(self, signal: dict[str, Any], conn: Optional[sqlite3.Connection] = None) -> AgentFinding:
        title = (signal.get("title") or "").strip()
        description = (signal.get("description") or "").strip()
        combined = f"{title} {description}".strip()
        words = WORD_RE.findall(combined)

        score = 0.58
        evidence = [f"word_count={len(words)}"]
        if not title or BOILERPLATE_RE.match(title):
            score -= 0.34
            evidence.append("title is blank or boilerplate")
        elif len(title) >= 24:
            score += 0.12
            evidence.append("descriptive title")

        if len(words) >= 18:
            score += 0.15
            evidence.append("rich supporting text")
        elif len(words) < 6:
            score -= 0.20
            evidence.append("too little supporting text")

        if CALGARY_TERMS.search(combined):
            score += 0.08
            evidence.append("practice/region vocabulary present")

        if combined.count("http") > 1:
            score -= 0.05
            evidence.append("content appears link-heavy")

        return AgentFinding(self.name, round(max(0.0, min(1.0, score)), 3), "Measures whether the scraped content is specific enough to trust.", evidence)


class NumericalSanityAgent(VerificationAgent):
    name = "numerical_sanity"

    def evaluate(self, signal: dict[str, Any], conn: Optional[sqlite3.Connection] = None) -> AgentFinding:
        weight = float(signal.get("weight") or 0)
        signal_type = signal.get("signal_type") or ""
        low, high = WEIGHT_RANGES.get(signal_type, (0.5, 6.0))
        score = 0.68
        evidence = [f"weight={weight:.2f}", f"expected_range=({low:.1f},{high:.1f})"]

        if low <= weight <= high:
            score += 0.16
            evidence.append("weight in expected range")
        elif weight < 0 or weight > high * 1.75:
            score -= 0.30
            evidence.append("weight is implausible")
        else:
            score -= 0.08
            evidence.append("weight is slightly unusual")

        combined = f"{signal.get('title') or ''} {signal.get('description') or ''}"
        money_mentions = MONEY_TOKEN_RE.findall(combined)
        if money_mentions:
            evidence.append(f"money_mentions={len(money_mentions)}")
            score += min(0.08, 0.03 * len(money_mentions))

        return AgentFinding(self.name, round(max(0.0, min(1.0, score)), 3), "Validates score weights and numeric hints in the evidence.", evidence)


class ConsensusAgent(VerificationAgent):
    name = "cross_signal_consensus"

    def evaluate(self, signal: dict[str, Any], conn: Optional[sqlite3.Connection] = None) -> AgentFinding:
        if conn is None:
            return AgentFinding(self.name, 0.5, "Consensus agent skipped because no database connection was provided.", ["db=missing"])

        firm_id = signal.get("firm_id")
        signal_type = signal.get("signal_type")
        title = (signal.get("title") or "").strip().lower()
        score = 0.48
        evidence = []

        corroboration = conn.execute(
            """
            SELECT COUNT(DISTINCT source_url)
            FROM signals
            WHERE firm_id = ?
              AND signal_type = ?
              AND date(detected_at) >= date('now', '-7 days')
              AND COALESCE(source_url, '') != ''
            """,
            (firm_id, signal_type),
        ).fetchone()[0]
        evidence.append(f"distinct_sources_7d={corroboration}")
        if corroboration >= 3:
            score += 0.34
        elif corroboration == 2:
            score += 0.22
        elif corroboration == 1:
            score += 0.08

        if title:
            duplicate_count = conn.execute(
                """
                SELECT COUNT(*)
                FROM signals
                WHERE firm_id = ?
                  AND signal_type = ?
                  AND lower(title) = ?
                  AND date(detected_at) >= date('now', '-2 days')
                """,
                (firm_id, signal_type, title),
            ).fetchone()[0]
            evidence.append(f"exact_title_matches_2d={duplicate_count}")
            if duplicate_count > 3:
                score -= 0.18
            elif duplicate_count == 2:
                score += 0.05

        neighboring_types = conn.execute(
            """
            SELECT COUNT(DISTINCT signal_type)
            FROM signals
            WHERE firm_id = ?
              AND date(detected_at) >= date('now', '-7 days')
            """,
            (firm_id,),
        ).fetchone()[0]
        evidence.append(f"distinct_signal_types_7d={neighboring_types}")
        if neighboring_types >= 3:
            score += 0.10

        return AgentFinding(self.name, round(max(0.0, min(1.0, score)), 3), "Looks for corroboration or suspicious duplication across recent signals.", evidence)


class VerificationOrchestrator:
    def __init__(self, agents: Optional[Iterable[VerificationAgent]] = None):
        self.agents = list(agents or [
            SourceReliabilityAgent(),
            EntityConsistencyAgent(),
            TemporalIntegrityAgent(),
            ContentIntegrityAgent(),
            NumericalSanityAgent(),
            ConsensusAgent(),
        ])
        self.weights = {
            "source_reliability": 0.24,
            "entity_consistency": 0.20,
            "temporal_integrity": 0.15,
            "content_integrity": 0.16,
            "numerical_sanity": 0.10,
            "cross_signal_consensus": 0.15,
        }

    def verify(self, signal: dict[str, Any], conn: Optional[sqlite3.Connection] = None) -> VerificationResult:
        findings = [agent.evaluate(signal, conn=conn) for agent in self.agents]
        confidence = sum(self.weights.get(f.agent, 0.0) * f.score for f in findings)
        confidence = round(max(0.0, min(1.0, confidence)), 3)

        if confidence >= HIGH_CONFIDENCE_FLOOR:
            verdict = "verified"
        elif confidence >= CONFIDENCE_FLOOR:
            verdict = "review"
        else:
            verdict = "rejected"

        sorted_findings = sorted(findings, key=lambda f: f.score, reverse=True)
        best = sorted_findings[0]
        worst = sorted(findings, key=lambda f: f.score)[0]
        summary = f"Top support: {best.agent} ({best.score:.2f}); main risk: {worst.agent} ({worst.score:.2f})."
        return VerificationResult(
            confidence_score=confidence,
            verdict=verdict,
            summary=summary,
            low_confidence=confidence < CONFIDENCE_FLOOR,
            findings=findings,
        )


ORCHESTRATOR = VerificationOrchestrator()


def _ensure_schema(conn: sqlite3.Connection):
    cols = {row[1] for row in conn.execute("PRAGMA table_info(signals)")}
    migrations = {
        "confidence_score": "ALTER TABLE signals ADD COLUMN confidence_score REAL",
        "low_confidence": "ALTER TABLE signals ADD COLUMN low_confidence INTEGER DEFAULT 0",
        "verification_status": "ALTER TABLE signals ADD COLUMN verification_status TEXT DEFAULT 'pending'",
        "verification_summary": "ALTER TABLE signals ADD COLUMN verification_summary TEXT",
        "verification_payload": "ALTER TABLE signals ADD COLUMN verification_payload TEXT",
    }
    for col, ddl in migrations.items():
        if col not in cols:
            conn.execute(ddl)
    conn.commit()


def _normalize_signal_row(row: sqlite3.Row) -> dict[str, Any]:
    signal = dict(row)
    raw_data = signal.get("raw_data")
    if isinstance(raw_data, str) and raw_data.strip():
        try:
            signal["raw_data"] = json.loads(raw_data)
        except json.JSONDecodeError:
            signal["raw_data"] = {"raw_text": raw_data}
    else:
        signal["raw_data"] = signal.get("raw_data") or {}
    return signal


def compute_confidence(
    firm_id: str,
    signal_type: str,
    title: str,
    description: str,
    weight: float,
    source_url: str,
    detected_at: Optional[str] = None,
    raw_data: Optional[dict[str, Any]] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> float:
    signal = {
        "firm_id": firm_id,
        "signal_type": signal_type,
        "title": title,
        "description": description,
        "weight": weight,
        "source_url": source_url,
        "detected_at": detected_at,
        "raw_data": raw_data or {},
    }
    return ORCHESTRATOR.verify(signal, conn=conn).confidence_score


def verify_recent_signals(days: int = 1):
    conn = get_conn()
    _ensure_schema(conn)

    rows = conn.execute(
        """
        SELECT id, firm_id, signal_type, title, description, weight,
               source_url, raw_data, detected_at
        FROM signals
        WHERE (
                confidence_score IS NULL
             OR verification_status IS NULL
             OR verification_status = 'pending'
              )
          AND date(detected_at) >= date('now', ? || ' days')
        ORDER BY id DESC
        """,
        (f"-{days}",),
    ).fetchall()

    if not rows:
        log.debug("[Verifier] No unverified signals.")
        conn.close()
        return

    log.info("[Verifier] Verifying %d signals with %d custom agents.", len(rows), len(ORCHESTRATOR.agents))
    counts = {"verified": 0, "review": 0, "rejected": 0}

    for row in rows:
        signal = _normalize_signal_row(row)
        result = ORCHESTRATOR.verify(signal, conn=conn)
        counts[result.verdict] += 1
        conn.execute(
            """
            UPDATE signals
            SET confidence_score = ?,
                low_confidence = ?,
                verification_status = ?,
                verification_summary = ?,
                verification_payload = ?
            WHERE id = ?
            """,
            (
                result.confidence_score,
                1 if result.low_confidence else 0,
                result.verdict,
                result.summary,
                json.dumps(result.as_payload(), ensure_ascii=False),
                signal["id"],
            ),
        )

    conn.commit()
    conn.close()
    log.info(
        "[Verifier] Done. verified=%d review=%d rejected=%d confidence_floor=%.0f%%",
        counts["verified"], counts["review"], counts["rejected"], CONFIDENCE_FLOOR * 100,
    )


def get_verified_signals_for_dashboard(days: int = 90) -> list[dict[str, Any]]:
    conn = get_conn()
    _ensure_schema(conn)
    rows = conn.execute(
        """
        SELECT *
        FROM signals
        WHERE date(detected_at) >= date('now', ? || ' days')
        ORDER BY
            CASE verification_status
                WHEN 'verified' THEN 0
                WHEN 'review' THEN 1
                ELSE 2
            END,
            COALESCE(confidence_score, 0) DESC,
            weight DESC,
            detected_at DESC
        """,
        (f"-{days}",),
    ).fetchall()
    conn.close()
    return [_normalize_signal_row(row) for row in rows]
