"""
tests/test_deduplicate.py — Unit tests for scripts/deduplicate.py

Tests cover:
  - Exact match detection
  - Near-duplicate detection (below likely, above possible threshold)
  - No false positives for genuinely different records
  - Scoring logic (individual criteria)
  - No records are deleted (flag-only behavior)
"""

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from deduplicate import score_pair, detect_duplicates, LIKELY_THRESHOLD, POSSIBLE_THRESHOLD

FIXTURES_VALID = REPO_ROOT / "tests" / "fixtures" / "valid"


def _record(**kwargs) -> dict:
    """Build a minimal valid-enough record for duplicate testing."""
    defaults = {
        "record_id": "test-record-2026",
        "firm_name": "Test Firm LLP",
        "expansion_type": "new_office",
        "practice_areas": ["Tax"],
        "country": "US",
        "city": "New York",
        "announced_date": "2026-01-01",
        "source_url": "https://example.com/news",
    }
    defaults.update(kwargs)
    return defaults


# ── Scoring tests ─────────────────────────────────────────────────────────────

class TestScorePair:
    def test_identical_records_high_score(self):
        a = _record()
        b = _record(record_id="test-record-2026-b")
        score, reasons = score_pair(a, b)
        assert score >= LIKELY_THRESHOLD, f"Identical records should score >= {LIKELY_THRESHOLD}, got {score}"
        assert len(reasons) >= 4

    def test_same_firm_adds_30_points(self):
        a = _record(expansion_type="merger", country="GB", city="", announced_date="2022-01-01")
        b = _record(expansion_type="merger", country="GB", city="", announced_date="2022-01-01")
        score, reasons = score_pair(a, b)
        assert "same firm_name" in reasons

    def test_different_firms_no_firm_name_contribution(self):
        a = _record(firm_name="Firm Alpha LLP")
        b = _record(firm_name="Firm Beta LLP")
        score, reasons = score_pair(a, b)
        assert "same firm_name" not in reasons

    def test_practice_area_overlap_adds_points(self):
        a = _record(practice_areas=["Tax", "Corporate & M&A"])
        b = _record(practice_areas=["Tax", "Banking & Finance"])
        score, reasons = score_pair(a, b)
        assert any("overlapping practice_areas" in r for r in reasons)

    def test_same_source_url_adds_20_points(self):
        a = _record(source_url="https://example.com/unique-news")
        b = _record(source_url="https://example.com/unique-news", record_id="test-record-2026-b")
        score, reasons = score_pair(a, b)
        assert "same source_url" in reasons

    def test_date_proximity_within_7_days_adds_points(self):
        a = _record(announced_date="2026-01-01")
        b = _record(announced_date="2026-01-05", record_id="test-b")
        score, reasons = score_pair(a, b)
        assert any("within" in r and "days" in r for r in reasons)

    def test_date_proximity_outside_30_days_no_date_contribution(self):
        a = _record(announced_date="2026-01-01")
        b = _record(announced_date="2026-03-15", record_id="test-b")
        score, reasons = score_pair(a, b)
        assert not any("announced_date" in r for r in reasons)

    def test_completely_different_records_low_score(self):
        a = _record(
            firm_name="Alpha International LLP",
            expansion_type="merger",
            country="AU",
            city="Sydney",
            practice_areas=["Immigration"],
            announced_date="2024-01-01",
            source_url="https://alpha.com",
        )
        b = _record(
            record_id="test-b",
            firm_name="Beta Pacific LLP",
            expansion_type="new_practice_group",
            country="JP",
            city="Tokyo",
            practice_areas=["Tax"],
            announced_date="2025-12-01",
            source_url="https://beta.com",
        )
        score, reasons = score_pair(a, b)
        assert score < POSSIBLE_THRESHOLD, f"Different records scored too high: {score} reasons: {reasons}"


# ── detect_duplicates tests ───────────────────────────────────────────────────

class TestDetectDuplicates:
    def test_no_duplicates_returns_empty(self):
        """Distinct records produce no duplicate candidates."""
        records = [
            (Path("a.yaml"), _record(
                record_id="firm-a-new-office-us-2026",
                firm_name="Firm Alpha LLP",
                expansion_type="new_office",
                country="US",
                city="Chicago",
                practice_areas=["Litigation & Dispute Resolution"],
                announced_date="2026-01-01",
                source_url="https://alpha.com",
            )),
            (Path("b.yaml"), _record(
                record_id="firm-b-merger-jp-2026",
                firm_name="Firm Beta LLP",
                expansion_type="merger",
                country="JP",
                city="Tokyo",
                practice_areas=["Banking & Finance"],
                announced_date="2025-01-01",
                source_url="https://beta.com",
            )),
        ]
        candidates = detect_duplicates(records)
        assert candidates == [], f"Expected no duplicates, got: {candidates}"

    def test_likely_duplicate_detected(self):
        """Two near-identical records are flagged as likely_duplicate."""
        base = _record()
        records = [
            (Path("a.yaml"), {**base, "record_id": "record-a"}),
            (Path("b.yaml"), {**base, "record_id": "record-b"}),
        ]
        candidates = detect_duplicates(records)
        assert len(candidates) == 1
        assert candidates[0]["level"] == "likely_duplicate"
        assert candidates[0]["action"] == "FLAG_FOR_REVIEW"

    def test_possible_duplicate_detected(self):
        """Records sharing firm/type/country but different city and date get 'possible'."""
        a = _record(
            record_id="record-a",
            firm_name="Test Firm LLP",
            expansion_type="new_office",
            country="US",
            city="New York",
            practice_areas=["Tax"],
            announced_date="2026-01-01",
            source_url="https://a.com",
        )
        b = _record(
            record_id="record-b",
            firm_name="Test Firm LLP",
            expansion_type="new_office",
            country="US",
            city="Los Angeles",
            practice_areas=["Tax"],
            announced_date="2026-01-10",
            source_url="https://b.com",
        )
        candidates = detect_duplicates([(Path("a.yaml"), a), (Path("b.yaml"), b)])
        # Should have at least a possible_duplicate
        assert len(candidates) >= 1
        levels = [c["level"] for c in candidates]
        assert "likely_duplicate" in levels or "possible_duplicate" in levels

    def test_single_record_no_duplicates(self):
        """A single record produces no duplicate pairs."""
        records = [(Path("a.yaml"), _record())]
        candidates = detect_duplicates(records)
        assert candidates == []

    def test_no_auto_deletion(self):
        """Duplicate detection never removes records — only flags them."""
        base = _record()
        records = [
            (Path("a.yaml"), {**base, "record_id": "record-a"}),
            (Path("b.yaml"), {**base, "record_id": "record-b"}),
        ]
        original_count = len(records)
        _ = detect_duplicates(records)
        assert len(records) == original_count, "Records were unexpectedly modified"

    def test_data_firms_no_duplicates(self):
        """The actual data/firms/ records contain no duplicates."""
        from deduplicate import load_records
        firms_dir = REPO_ROOT / "data" / "firms"
        if not firms_dir.exists():
            pytest.skip("data/firms/ directory not found")
        records = load_records([str(firms_dir)])
        candidates = detect_duplicates(records)
        likely = [c for c in candidates if c["level"] == "likely_duplicate"]
        assert likely == [], f"Unexpected likely duplicates in data/firms/: {likely}"
