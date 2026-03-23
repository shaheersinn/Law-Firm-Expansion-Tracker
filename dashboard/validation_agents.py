from __future__ import annotations

"""Dashboard data verification and validation agents.

These agents run immediately before signals are injected into the dashboard
payload. Their job is to verify record shape, validate business rules, and
reject malformed uploads while preserving a detailed audit trail.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

from config_calgary import FIRM_BY_ID

REQUIRED_SIGNAL_FIELDS = {
    "firm_id",
    "signal_type",
    "weight",
    "title",
    "detected_at",
}

SIGNAL_WEIGHT_RANGES: dict[str, tuple[float, float]] = {
    "sedar_major_deal": (3.0, 6.0),
    "biglaw_spillage_predicted": (3.0, 5.5),
    "canlii_appearance_spike": (2.5, 5.0),
    "canlii_new_large_file": (2.0, 5.0),
    "linkedin_turnover_detected": (3.5, 5.5),
    "linkedin_new_vacancy": (2.5, 5.0),
    "lsa_retention_gap": (2.0, 4.0),
    "lsa_student_not_retained": (2.0, 4.0),
    "job_posting": (1.0, 3.0),
    "lateral_hire": (2.0, 4.0),
    "ranking": (0.5, 3.0),
    "web_signal": (1.0, 3.0),
}


@dataclass
class ValidationIssue:
    severity: str
    code: str
    message: str


@dataclass
class ValidationResult:
    accepted: bool = True
    issues: list[ValidationIssue] = field(default_factory=list)

    def error(self, code: str, message: str):
        self.accepted = False
        self.issues.append(ValidationIssue("error", code, message))

    def warn(self, code: str, message: str):
        self.issues.append(ValidationIssue("warning", code, message))


class DashboardValidationAgent:
    name = "base-agent"

    def review(self, record: dict[str, Any]) -> ValidationResult:
        raise NotImplementedError


class SchemaValidationAgent(DashboardValidationAgent):
    name = "schema_validation"

    def review(self, record: dict[str, Any]) -> ValidationResult:
        result = ValidationResult()
        missing = sorted(field for field in REQUIRED_SIGNAL_FIELDS if field not in record)
        if missing:
            result.error("missing_fields", f"Missing required fields: {', '.join(missing)}")
            return result

        title = (record.get("title") or "").strip()
        if not title:
            result.error("blank_title", "Signal title is blank.")

        try:
            float(record.get("weight", 0))
        except (TypeError, ValueError):
            result.error("invalid_weight", "Signal weight must be numeric.")

        detected_at = record.get("detected_at") or ""
        try:
            datetime.fromisoformat(str(detected_at).replace("Z", "+00:00"))
        except ValueError:
            result.error("invalid_timestamp", "detected_at is not valid ISO-8601.")

        return result


class FirmValidationAgent(DashboardValidationAgent):
    name = "firm_validation"

    def review(self, record: dict[str, Any]) -> ValidationResult:
        result = ValidationResult()
        firm_id = record.get("firm_id")
        if firm_id == "market":
            return result
        if firm_id not in FIRM_BY_ID:
            result.error("unknown_firm", f"Unknown firm_id '{firm_id}'.")
            return result

        firm_meta = FIRM_BY_ID[firm_id]
        if not firm_meta.get("name"):
            result.error("firm_name_missing", f"Configured firm '{firm_id}' is missing a name.")
        if firm_meta.get("tier") not in {"boutique", "mid", "big"}:
            result.warn("unexpected_tier", f"Firm '{firm_id}' has an unexpected tier value.")
        return result


class SignalQualityAgent(DashboardValidationAgent):
    name = "signal_quality"

    def review(self, record: dict[str, Any]) -> ValidationResult:
        result = ValidationResult()
        signal_type = record.get("signal_type") or ""
        title = (record.get("title") or "").strip()
        weight = float(record.get("weight", 0) or 0)
        source_url = (record.get("source_url") or "").strip()
        confidence = record.get("confidence_score")

        low, high = SIGNAL_WEIGHT_RANGES.get(signal_type, (0.5, 6.0))
        if not (low <= weight <= high):
            result.warn(
                "weight_outlier",
                f"Weight {weight:g} is outside the expected range for {signal_type}."
            )

        if len(title) < 8:
            result.warn("short_title", "Signal title is unusually short.")

        if source_url:
            parsed = urlparse(source_url)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                result.error("invalid_source_url", "source_url is not a valid absolute URL.")
        else:
            result.warn("missing_source_url", "Signal has no source URL for auditability.")

        if confidence is not None:
            try:
                confidence_value = float(confidence)
            except (TypeError, ValueError):
                result.error("invalid_confidence", "confidence_score must be numeric when present.")
            else:
                if confidence_value < 0 or confidence_value > 1:
                    result.error("confidence_range", "confidence_score must be between 0 and 1.")
                elif confidence_value < 0.45:
                    result.warn("low_confidence", "Signal confidence is below the dashboard comfort threshold.")

        return result


class DuplicateDetectionAgent(DashboardValidationAgent):
    name = "duplicate_detection"

    def __init__(self):
        self._seen_keys: set[tuple[str, str, str, str]] = set()

    def review(self, record: dict[str, Any]) -> ValidationResult:
        result = ValidationResult()
        dedup_hash = record.get("dedup_hash")
        detected_day = str(record.get("detected_at") or "")[:10]
        key = (
            str(record.get("firm_id") or ""),
            str(record.get("signal_type") or ""),
            str(dedup_hash or (record.get("title") or "").strip().lower()),
            detected_day,
        )
        if key in self._seen_keys:
            result.error("duplicate_signal", "Duplicate signal detected in dashboard upload batch.")
        else:
            self._seen_keys.add(key)
        return result


def validate_dashboard_records(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    agents: list[DashboardValidationAgent] = [
        SchemaValidationAgent(),
        FirmValidationAgent(),
        SignalQualityAgent(),
        DuplicateDetectionAgent(),
    ]

    accepted_records: list[dict[str, Any]] = []
    rejected_records: list[dict[str, Any]] = []
    agent_stats = {
        agent.name: {"errors": 0, "warnings": 0, "rejected": 0}
        for agent in agents
    }

    for record in records:
        record_issues: list[dict[str, str]] = []
        accepted = True
        for agent in agents:
            result = agent.review(record)
            if not result.accepted:
                accepted = False
                agent_stats[agent.name]["rejected"] += 1
            for issue in result.issues:
                agent_stats[agent.name][f"{issue.severity}s"] += 1
                record_issues.append({
                    "agent": agent.name,
                    "severity": issue.severity,
                    "code": issue.code,
                    "message": issue.message,
                })

        enriched = {**record, "validation_issues": record_issues, "validated_for_dashboard": accepted}
        if accepted:
            accepted_records.append(enriched)
        else:
            rejected_records.append(enriched)

    summary = {
        "validated_at": datetime.utcnow().isoformat() + "Z",
        "input_records": len(records),
        "accepted_records": len(accepted_records),
        "rejected_records": len(rejected_records),
        "warning_records": sum(1 for r in accepted_records if r["validation_issues"]),
        "agents": agent_stats,
        "rejected_examples": [
            {
                "firm_id": r.get("firm_id"),
                "signal_type": r.get("signal_type"),
                "title": r.get("title"),
                "issues": r.get("validation_issues", [])[:5],
            }
            for r in rejected_records[:10]
        ],
    }
    return accepted_records, summary
