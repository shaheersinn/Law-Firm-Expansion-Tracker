"""
tests/test_normalize.py — Unit tests for scripts/normalize.py

Tests cover:
  - Practice area normalization (abbreviations, multi-area splits, canonical pass-through)
  - Country normalization (informal names, alternate codes)
  - General normalization (whitespace trimming, diacritics preserved)
  - Ambiguous inputs flagged for review
"""

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from normalize import (
    normalize_practice_area,
    normalize_country,
    normalize_whitespace,
    normalize_record,
    CANONICAL_PRACTICE_AREAS,
)


# ── Practice area normalization ───────────────────────────────────────────────

class TestNormalizePracticeArea:
    def test_ip_normalizes_to_intellectual_property(self):
        result, ambiguous = normalize_practice_area("IP")
        assert result == ["Intellectual Property"]
        assert not ambiguous

    def test_ma_normalizes_to_corporate_ma(self):
        result, ambiguous = normalize_practice_area("M&A")
        assert result == ["Corporate & M&A"]
        assert not ambiguous

    def test_privacy_cybersecurity_splits_into_two(self):
        result, ambiguous = normalize_practice_area("Privacy & Cybersecurity")
        assert "Data Privacy & Protection" in result
        assert "Technology & Cybersecurity" in result
        assert not ambiguous

    def test_labor_normalizes_to_employment_labor(self):
        result, ambiguous = normalize_practice_area("Labor")
        assert result == ["Employment & Labor"]
        assert not ambiguous

    def test_labour_normalizes_to_employment_labor(self):
        """British spelling 'Labour' normalizes to the canonical form."""
        result, ambiguous = normalize_practice_area("Labour")
        assert result == ["Employment & Labor"]
        assert not ambiguous

    def test_competition_normalizes_to_antitrust(self):
        result, ambiguous = normalize_practice_area("Competition")
        assert result == ["Antitrust & Competition"]
        assert not ambiguous

    def test_canonical_value_passes_through(self):
        """A value already in canonical form is returned unchanged."""
        for area in CANONICAL_PRACTICE_AREAS:
            result, ambiguous = normalize_practice_area(area)
            assert result == [area], f"Canonical area {area!r} was not passed through"
            assert not ambiguous

    def test_unknown_area_flagged_as_ambiguous(self):
        result, ambiguous = normalize_practice_area("Space Law")
        assert ambiguous, "Unknown area should be flagged as ambiguous"
        assert result == ["Space Law"]  # returned unchanged

    def test_whitespace_trimmed_before_lookup(self):
        result, ambiguous = normalize_practice_area("  IP  ")
        assert result == ["Intellectual Property"]
        assert not ambiguous

    def test_case_insensitive_lookup(self):
        result, ambiguous = normalize_practice_area("ip")
        assert result == ["Intellectual Property"]
        assert not ambiguous

    def test_privacy_only_normalizes(self):
        result, ambiguous = normalize_practice_area("Privacy")
        assert result == ["Data Privacy & Protection"]
        assert not ambiguous

    def test_data_privacy_cybersecurity_full(self):
        result, ambiguous = normalize_practice_area("Data Privacy & Cybersecurity")
        assert "Data Privacy & Protection" in result
        assert "Technology & Cybersecurity" in result
        assert not ambiguous


# ── Country normalization ─────────────────────────────────────────────────────

class TestNormalizeCountry:
    def test_uk_normalizes_to_gb(self):
        result, ambiguous = normalize_country("UK")
        assert result == "GB"
        assert not ambiguous

    def test_united_kingdom_normalizes_to_gb(self):
        result, ambiguous = normalize_country("United Kingdom")
        assert result == "GB"
        assert not ambiguous

    def test_usa_normalizes_to_us(self):
        result, ambiguous = normalize_country("USA")
        assert result == "US"
        assert not ambiguous

    def test_us_already_canonical(self):
        result, ambiguous = normalize_country("US")
        assert result == "US"
        assert not ambiguous

    def test_united_states_normalizes_to_us(self):
        result, ambiguous = normalize_country("United States")
        assert result == "US"
        assert not ambiguous

    def test_germany_normalizes_to_de(self):
        result, ambiguous = normalize_country("Germany")
        assert result == "DE"
        assert not ambiguous

    def test_canada_normalizes_to_ca(self):
        result, ambiguous = normalize_country("Canada")
        assert result == "CA"
        assert not ambiguous

    def test_valid_iso_code_passes_through(self):
        """A valid ISO alpha-2 code is returned unchanged."""
        for code in ["CA", "GB", "DE", "US", "JP", "AU", "BR", "SG"]:
            result, ambiguous = normalize_country(code)
            assert result == code, f"ISO code {code!r} was not passed through"
            assert not ambiguous

    def test_unknown_country_flagged_as_ambiguous(self):
        result, ambiguous = normalize_country("Narnia")
        assert ambiguous, "Unknown country should be flagged as ambiguous"

    def test_case_insensitive_lookup(self):
        result, ambiguous = normalize_country("germany")
        assert result == "DE"
        assert not ambiguous

    def test_singapore_normalizes(self):
        result, ambiguous = normalize_country("Singapore")
        assert result == "SG"
        assert not ambiguous

    def test_uae_normalizes(self):
        result, ambiguous = normalize_country("UAE")
        assert result == "AE"
        assert not ambiguous


# ── Whitespace normalization ──────────────────────────────────────────────────

class TestNormalizeWhitespace:
    def test_leading_trailing_whitespace_trimmed(self):
        assert normalize_whitespace("  hello  ") == "hello"

    def test_internal_spaces_collapsed(self):
        assert normalize_whitespace("hello   world") == "hello world"

    def test_tabs_normalized(self):
        assert normalize_whitespace("hello\tworld") == "hello world"

    def test_empty_string_returns_empty(self):
        assert normalize_whitespace("") == ""

    def test_diacritics_preserved(self):
        """Diacritics in names like Müller or São Paulo are not changed."""
        assert normalize_whitespace("Müller & Partners") == "Müller & Partners"
        assert normalize_whitespace("São Paulo") == "São Paulo"
        assert normalize_whitespace("Zürich") == "Zürich"


# ── normalize_record tests ────────────────────────────────────────────────────

class TestNormalizeRecord:
    def _base_record(self, **overrides) -> dict:
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
        record.update(overrides)
        return record

    def test_firm_name_whitespace_trimmed(self):
        record = self._base_record(firm_name="  Test Firm  ")
        normalized, changes, flags = normalize_record(record)
        assert normalized["firm_name"] == "Test Firm"
        assert any("firm_name" in c for c in changes)

    def test_country_normalized(self):
        record = self._base_record(country="UK")
        normalized, changes, flags = normalize_record(record)
        assert normalized["country"] == "GB"
        assert any("country" in c for c in changes)

    def test_practice_area_normalized(self):
        record = self._base_record(practice_areas=["IP", "M&A"])
        normalized, changes, flags = normalize_record(record)
        assert "Intellectual Property" in normalized["practice_areas"]
        assert "Corporate & M&A" in normalized["practice_areas"]

    def test_privacy_cybersecurity_splits(self):
        record = self._base_record(practice_areas=["Privacy & Cybersecurity"])
        normalized, changes, flags = normalize_record(record)
        assert "Data Privacy & Protection" in normalized["practice_areas"]
        assert "Technology & Cybersecurity" in normalized["practice_areas"]

    def test_canonical_practice_areas_unchanged(self):
        record = self._base_record(practice_areas=["Corporate & M&A", "Tax"])
        normalized, changes, flags = normalize_record(record)
        assert normalized["practice_areas"] == ["Corporate & M&A", "Tax"]
        assert not any("practice_areas" in c for c in changes)

    def test_unknown_practice_area_flagged(self):
        record = self._base_record(practice_areas=["Space Law"])
        normalized, changes, flags = normalize_record(record)
        assert any("practice_areas" in f for f in flags)

    def test_unknown_country_flagged(self):
        record = self._base_record(country="Narnia")
        normalized, changes, flags = normalize_record(record)
        assert any("country" in f for f in flags)

    def test_diacritics_in_city_preserved(self):
        record = self._base_record(city="São Paulo")
        normalized, changes, flags = normalize_record(record)
        assert normalized["city"] == "São Paulo"

    def test_diacritics_in_firm_name_preserved(self):
        record = self._base_record(firm_name="Müller & Partners LLP")
        normalized, changes, flags = normalize_record(record)
        assert normalized["firm_name"] == "Müller & Partners LLP"

    def test_duplicate_practice_areas_deduplicated(self):
        """Split practice areas don't create duplicates if same area appears twice."""
        record = self._base_record(
            practice_areas=["Data Privacy & Protection", "Privacy"]
        )
        normalized, changes, flags = normalize_record(record)
        areas = normalized["practice_areas"]
        assert areas.count("Data Privacy & Protection") == 1
