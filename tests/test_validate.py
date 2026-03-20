"""
tests/test_validate.py — Unit tests for scripts/validate.py

Tests cover:
  - Valid records pass without errors
  - Edge-case records produce expected warnings
  - Invalid records produce expected errors
  - Custom rules: self-reference, unverified+published, date logic, etc.
"""

import json
import sys
import os
from pathlib import Path
from datetime import date

import pytest

# Add scripts directory to path
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from validate import (
    load_schema,
    load_record,
    validate_record,
    run_validation,
    collect_yaml_files,
)

FIXTURES_VALID = REPO_ROOT / "tests" / "fixtures" / "valid"
FIXTURES_INVALID = REPO_ROOT / "tests" / "fixtures" / "invalid"
FIXTURES_EDGE = REPO_ROOT / "tests" / "fixtures" / "edge_cases"


@pytest.fixture(scope="module")
def schema():
    return load_schema()


# ── Schema loading ────────────────────────────────────────────────────────────

def test_schema_loads():
    """Schema file exists and is valid JSON with expected keys."""
    s = load_schema()
    assert s["type"] == "object"
    assert "properties" in s
    assert "required" in s


# ── Valid record tests ────────────────────────────────────────────────────────

class TestValidRecords:
    def test_valid_complete_passes(self, schema):
        """A fully complete valid record has no errors."""
        path = FIXTURES_VALID / "valid_complete.yaml"
        record, err = load_record(path)
        assert err is None, f"Load error: {err}"
        errors, warnings = validate_record(record, schema, path)
        assert errors == [], f"Unexpected errors: {errors}"

    def test_all_data_firms_valid(self, schema):
        """All records in data/firms/ pass validation (no errors)."""
        firms_dir = REPO_ROOT / "data" / "firms"
        if not firms_dir.exists():
            pytest.skip("data/firms/ directory not found")
        files = collect_yaml_files([str(firms_dir)])
        assert len(files) > 0, "No YAML files found in data/firms/"
        for f in files:
            record, err = load_record(f)
            assert err is None, f"Load error in {f}: {err}"
            errors, _ = validate_record(record, schema, f)
            assert errors == [], f"Errors in {f}: {errors}"

    def test_valid_record_with_optional_fields(self, schema):
        """A record with all optional fields populated passes."""
        record = {
            "record_id": "test-firm-new-office-us-2026",
            "firm_name": "Test Firm LLP",
            "expansion_type": "new_office",
            "practice_areas": ["Corporate & M&A", "Tax"],
            "country": "US",
            "region": "New York",
            "city": "New York",
            "announced_date": "2026-01-01",
            "effective_date": "2026-06-01",
            "source_url": "https://www.testfirm.com/news",
            "source_type": "firm_press_release",
            "confidence": "confirmed",
            "status": "published",
            "headcount": 10,
            "related_records": [],
            "tags": ["test", "expansion"],
            "notes": "A test note.",
            "created_at": "2026-01-01T00:00:00Z",
            "last_modified": "2026-01-01T00:00:00Z",
            "schema_version": "1.0.0",
            "created_by": "testuser",
        }
        errors, warnings = validate_record(record, schema, Path("test"))
        assert errors == [], f"Unexpected errors: {errors}"


# ── Invalid record tests ──────────────────────────────────────────────────────

class TestInvalidRecords:
    def test_missing_required_fields_produces_errors(self, schema):
        """Records missing required fields produce schema errors."""
        path = FIXTURES_INVALID / "invalid_missing_required.yaml"
        record, err = load_record(path)
        assert err is None
        errors, _ = validate_record(record, schema, path)
        assert len(errors) > 0, "Expected errors for missing required fields"
        # Should report missing firm_name, practice_areas, source_url
        combined = " ".join(errors)
        assert "firm_name" in combined or "practice_areas" in combined or "source_url" in combined

    def test_bad_enum_values_produce_errors(self, schema):
        """Records with invalid enum values produce schema errors."""
        path = FIXTURES_INVALID / "invalid_bad_enums.yaml"
        record, err = load_record(path)
        assert err is None
        errors, _ = validate_record(record, schema, path)
        assert len(errors) > 0, "Expected errors for bad enum values"

    def test_invalid_record_id_pattern_error(self, schema):
        """Uppercase or special-character record_id is rejected."""
        record = {
            "record_id": "UPPERCASE_ID",
            "firm_name": "Test Firm",
            "expansion_type": "new_office",
            "practice_areas": ["Tax"],
            "country": "US",
            "announced_date": "2026-01-01",
            "source_url": "https://example.com",
            "source_type": "firm_press_release",
            "confidence": "confirmed",
            "status": "draft",
            "created_at": "2026-01-01T00:00:00Z",
            "last_modified": "2026-01-01T00:00:00Z",
            "schema_version": "1.0.0",
        }
        errors, _ = validate_record(record, schema, Path("test"))
        assert any("record_id" in e for e in errors), f"Expected record_id error, got: {errors}"

    def test_invalid_country_code_pattern(self, schema):
        """Three-letter or lowercase country code is rejected."""
        record = {
            "record_id": "test-record-2026",
            "firm_name": "Test Firm",
            "expansion_type": "new_office",
            "practice_areas": ["Tax"],
            "country": "USA",  # invalid — must be 2 uppercase letters
            "announced_date": "2026-01-01",
            "source_url": "https://example.com",
            "source_type": "firm_press_release",
            "confidence": "confirmed",
            "status": "draft",
            "created_at": "2026-01-01T00:00:00Z",
            "last_modified": "2026-01-01T00:00:00Z",
            "schema_version": "1.0.0",
        }
        errors, _ = validate_record(record, schema, Path("test"))
        assert any("country" in e for e in errors), f"Expected country error, got: {errors}"

    def test_malformed_source_url_error(self, schema):
        """A source_url without http:// or https:// is rejected."""
        record = {
            "record_id": "test-record-2026",
            "firm_name": "Test Firm",
            "expansion_type": "new_office",
            "practice_areas": ["Tax"],
            "country": "US",
            "announced_date": "2026-01-01",
            "source_url": "not-a-url",
            "source_type": "firm_press_release",
            "confidence": "confirmed",
            "status": "draft",
            "created_at": "2026-01-01T00:00:00Z",
            "last_modified": "2026-01-01T00:00:00Z",
            "schema_version": "1.0.0",
        }
        errors, _ = validate_record(record, schema, Path("test"))
        assert any("source_url" in e for e in errors), f"Expected source_url error, got: {errors}"

    def test_empty_practice_areas_error(self, schema):
        """An empty practice_areas array is rejected."""
        record = {
            "record_id": "test-record-2026",
            "firm_name": "Test Firm",
            "expansion_type": "new_office",
            "practice_areas": [],  # must have at least 1
            "country": "US",
            "announced_date": "2026-01-01",
            "source_url": "https://example.com",
            "source_type": "firm_press_release",
            "confidence": "confirmed",
            "status": "draft",
            "created_at": "2026-01-01T00:00:00Z",
            "last_modified": "2026-01-01T00:00:00Z",
            "schema_version": "1.0.0",
        }
        errors, _ = validate_record(record, schema, Path("test"))
        assert any("practice_areas" in e for e in errors), f"Expected practice_areas error, got: {errors}"

    def test_self_reference_in_related_records(self, schema):
        """A record that references itself in related_records is rejected."""
        path = FIXTURES_INVALID / "invalid_self_reference.yaml"
        record, err = load_record(path)
        assert err is None
        errors, _ = validate_record(record, schema, path)
        assert any("self" in e.lower() or "related_records" in e for e in errors), \
            f"Expected self-reference error, got: {errors}"

    def test_unknown_additional_properties_rejected(self, schema):
        """Records with unknown fields are rejected (additionalProperties: false)."""
        record = {
            "record_id": "test-record-2026",
            "firm_name": "Test Firm",
            "expansion_type": "new_office",
            "practice_areas": ["Tax"],
            "country": "US",
            "announced_date": "2026-01-01",
            "source_url": "https://example.com",
            "source_type": "firm_press_release",
            "confidence": "confirmed",
            "status": "draft",
            "created_at": "2026-01-01T00:00:00Z",
            "last_modified": "2026-01-01T00:00:00Z",
            "schema_version": "1.0.0",
            "unknown_field_xyz": "should fail",
        }
        errors, _ = validate_record(record, schema, Path("test"))
        assert len(errors) > 0, "Expected error for unknown field"

    def test_headcount_less_than_one_rejected(self, schema):
        """headcount of 0 is rejected (minimum is 1)."""
        record = {
            "record_id": "test-record-2026",
            "firm_name": "Test Firm",
            "expansion_type": "new_office",
            "practice_areas": ["Tax"],
            "country": "US",
            "announced_date": "2026-01-01",
            "source_url": "https://example.com",
            "source_type": "firm_press_release",
            "confidence": "confirmed",
            "status": "draft",
            "headcount": 0,
            "created_at": "2026-01-01T00:00:00Z",
            "last_modified": "2026-01-01T00:00:00Z",
            "schema_version": "1.0.0",
        }
        errors, _ = validate_record(record, schema, Path("test"))
        assert any("headcount" in e for e in errors), f"Expected headcount error, got: {errors}"


# ── Warning tests ─────────────────────────────────────────────────────────────

class TestWarnings:
    def test_unverified_plus_published_warns(self, schema):
        """confidence=unverified + status=published generates a warning."""
        record = {
            "record_id": "test-record-2026",
            "firm_name": "Test Firm",
            "expansion_type": "new_office",
            "practice_areas": ["Tax"],
            "country": "US",
            "announced_date": "2026-01-01",
            "source_url": "https://example.com",
            "source_type": "firm_press_release",
            "confidence": "unverified",
            "status": "published",
            "created_at": "2026-01-01T00:00:00Z",
            "last_modified": "2026-01-01T00:00:00Z",
            "schema_version": "1.0.0",
        }
        errors, warnings = validate_record(record, schema, Path("test"))
        assert errors == [], f"Unexpected errors: {errors}"
        assert any("unverified" in w and "published" in w for w in warnings), \
            f"Expected unverified+published warning, got: {warnings}"

    def test_effective_date_before_announced_warns(self, schema):
        """effective_date earlier than announced_date generates a warning."""
        record = {
            "record_id": "test-record-2026",
            "firm_name": "Test Firm",
            "expansion_type": "new_office",
            "practice_areas": ["Tax"],
            "country": "US",
            "announced_date": "2026-06-01",
            "effective_date": "2026-01-01",
            "source_url": "https://example.com",
            "source_type": "firm_press_release",
            "confidence": "confirmed",
            "status": "draft",
            "created_at": "2026-01-01T00:00:00Z",
            "last_modified": "2026-01-01T00:00:00Z",
            "schema_version": "1.0.0",
        }
        errors, warnings = validate_record(record, schema, Path("test"))
        assert errors == [], f"Unexpected errors: {errors}"
        assert any("effective_date" in w for w in warnings), \
            f"Expected effective_date warning, got: {warnings}"

    def test_notes_over_500_chars_warns(self, schema):
        """Notes longer than 500 characters generates a warning."""
        record = {
            "record_id": "test-record-2026",
            "firm_name": "Test Firm",
            "expansion_type": "new_office",
            "practice_areas": ["Tax"],
            "country": "US",
            "announced_date": "2026-01-01",
            "source_url": "https://example.com",
            "source_type": "firm_press_release",
            "confidence": "confirmed",
            "status": "draft",
            "notes": "x" * 501,
            "created_at": "2026-01-01T00:00:00Z",
            "last_modified": "2026-01-01T00:00:00Z",
            "schema_version": "1.0.0",
        }
        errors, warnings = validate_record(record, schema, Path("test"))
        assert errors == [], f"Unexpected errors: {errors}"
        assert any("notes" in w for w in warnings), f"Expected notes warning, got: {warnings}"

    def test_high_confidence_weak_source_warns(self, schema):
        """confirmed confidence + job_posting source generates a warning."""
        record = {
            "record_id": "test-record-2026",
            "firm_name": "Test Firm",
            "expansion_type": "new_office",
            "practice_areas": ["Tax"],
            "country": "US",
            "announced_date": "2026-01-01",
            "source_url": "https://example.com",
            "source_type": "job_posting",
            "confidence": "confirmed",
            "status": "draft",
            "created_at": "2026-01-01T00:00:00Z",
            "last_modified": "2026-01-01T00:00:00Z",
            "schema_version": "1.0.0",
        }
        errors, warnings = validate_record(record, schema, Path("test"))
        assert errors == [], f"Unexpected errors: {errors}"
        assert any("weak source" in w or "job_posting" in w for w in warnings), \
            f"Expected weak-source warning, got: {warnings}"

    def test_missing_city_warns(self, schema):
        """Country set but city missing generates a warning."""
        record = {
            "record_id": "test-record-2026",
            "firm_name": "Test Firm",
            "expansion_type": "new_office",
            "practice_areas": ["Tax"],
            "country": "US",
            "announced_date": "2026-01-01",
            "source_url": "https://example.com",
            "source_type": "firm_press_release",
            "confidence": "confirmed",
            "status": "draft",
            "created_at": "2026-01-01T00:00:00Z",
            "last_modified": "2026-01-01T00:00:00Z",
            "schema_version": "1.0.0",
        }
        errors, warnings = validate_record(record, schema, Path("test"))
        assert errors == [], f"Unexpected errors: {errors}"
        assert any("city" in w for w in warnings), f"Expected city warning, got: {warnings}"


# ── Edge case tests ───────────────────────────────────────────────────────────

class TestEdgeCases:
    def test_nonascii_city_and_firm_name(self, schema):
        """Non-ASCII city (Zürich) and firm name (Müller) are valid."""
        path = FIXTURES_EDGE / "edge_nonascii_long_effective.yaml"
        record, err = load_record(path)
        assert err is None, f"Load error: {err}"
        errors, warnings = validate_record(record, schema, path)
        assert errors == [], f"Unexpected errors for non-ASCII record: {errors}"

    def test_archived_old_draft(self, schema):
        """An old announced_date with archived status does not generate errors."""
        path = FIXTURES_EDGE / "edge_draft_to_archived.yaml"
        record, err = load_record(path)
        assert err is None, f"Load error: {err}"
        errors, warnings = validate_record(record, schema, path)
        assert errors == [], f"Unexpected errors for archived record: {errors}"

    def test_duplicate_record_id_detection(self, schema):
        """Duplicate record_ids across files are flagged as errors."""
        # Use two files with same record_id
        file1 = FIXTURES_VALID / "valid_complete.yaml"
        # Create a temp file with same record_id
        import tempfile, yaml as _yaml
        record, _ = load_record(file1)
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False, encoding="utf-8"
        ) as tmp:
            _yaml.dump(record, tmp, allow_unicode=True)
            tmp_path = tmp.name

        try:
            summary = run_validation([str(file1), tmp_path], output_json=False)
            # Should have a duplicate error somewhere
            all_errors = [e for r in summary["results"] for e in r["errors"]]
            assert any("duplicate" in e.lower() or "record_id" in e.lower() for e in all_errors), \
                f"Expected duplicate error, got: {all_errors}"
        finally:
            import os
            os.unlink(tmp_path)

    def test_sao_paulo_city_preserved(self, schema):
        """São Paulo city name is preserved in YAML round-trip."""
        record = {
            "record_id": "test-sao-paulo-2026",
            "firm_name": "Test Firm",
            "expansion_type": "new_office",
            "practice_areas": ["Corporate & M&A"],
            "country": "BR",
            "city": "São Paulo",
            "announced_date": "2026-01-01",
            "source_url": "https://example.com",
            "source_type": "firm_press_release",
            "confidence": "high",
            "status": "verified",
            "created_at": "2026-01-01T00:00:00Z",
            "last_modified": "2026-01-01T00:00:00Z",
            "schema_version": "1.0.0",
        }
        errors, warnings = validate_record(record, schema, Path("test"))
        assert errors == [], f"Unexpected errors for São Paulo record: {errors}"


# ── run_validation integration tests ─────────────────────────────────────────

class TestRunValidation:
    def test_json_output_structure(self):
        """run_validation returns correct summary structure."""
        summary = run_validation(
            [str(FIXTURES_VALID / "valid_complete.yaml")],
            output_json=False,
        )
        assert "total" in summary
        assert "passed" in summary
        assert "failed" in summary
        assert "results" in summary
        assert summary["total"] == 1

    def test_valid_dir_all_pass(self):
        """Valid fixtures directory produces zero failures."""
        summary = run_validation([str(FIXTURES_VALID)], output_json=False)
        assert summary["failed"] == 0

    def test_invalid_dir_has_failures(self):
        """Invalid fixtures directory produces at least one failure."""
        summary = run_validation([str(FIXTURES_INVALID)], output_json=False)
        assert summary["failed"] > 0
