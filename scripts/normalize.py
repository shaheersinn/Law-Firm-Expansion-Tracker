#!/usr/bin/env python3
"""
normalize.py — Normalize raw field values to controlled vocabulary.

Usage:
    python scripts/normalize.py --practice-area "IP"
    python scripts/normalize.py --country "UK"
    python scripts/normalize.py --file data/firms/2026/my-record.yaml

Normalization Rules
-------------------
Practice Areas:
  "IP"                         -> "Intellectual Property"
  "M&A"                        -> "Corporate & M&A"
  "Privacy"                    -> "Data Privacy & Protection"
  "Privacy & Cybersecurity"    -> ["Technology & Cybersecurity", "Data Privacy & Protection"]
  "Labor"                      -> "Employment & Labor"
  "Competition"                -> "Antitrust & Competition"

Countries:
  "UK"            -> "GB"
  "USA" / "US"    -> "US"
  "United States" -> "US"
  "Germany"       -> "DE"
  "France"        -> "FR"
  "Canada"        -> "CA"
  "Australia"     -> "AU"
  "Japan"         -> "JP"
  "China"         -> "CN"
  "India"         -> "IN"
  "Brazil"        -> "BR"
  "Singapore"     -> "SG"
  "UAE"           -> "AE"
  "South Africa"  -> "ZA"

General:
  - Trim whitespace
  - Normalize repeated spaces
  - Preserve diacritics in firm_name and city
"""

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML is required. Install with: pip install pyyaml", file=sys.stderr)
    sys.exit(2)

# ── Practice area normalization map ─────────────────────────────────────────
# Maps raw/informal input to one or more canonical practice area strings.
PRACTICE_AREA_MAP: dict[str, list[str]] = {
    # Abbreviations
    "ip":                           ["Intellectual Property"],
    "m&a":                          ["Corporate & M&A"],
    "ma":                           ["Corporate & M&A"],
    "mergers and acquisitions":     ["Corporate & M&A"],
    "mergers & acquisitions":       ["Corporate & M&A"],
    "corporate":                    ["Corporate & M&A"],
    "privacy":                      ["Data Privacy & Protection"],
    "data privacy":                 ["Data Privacy & Protection"],
    "cybersecurity":                ["Technology & Cybersecurity"],
    "cyber":                        ["Technology & Cybersecurity"],
    "privacy & cybersecurity":      ["Data Privacy & Protection", "Technology & Cybersecurity"],
    "data privacy & cybersecurity": ["Data Privacy & Protection", "Technology & Cybersecurity"],
    "privacy and cybersecurity":    ["Data Privacy & Protection", "Technology & Cybersecurity"],
    "labor":                        ["Employment & Labor"],
    "labour":                       ["Employment & Labor"],
    "employment":                   ["Employment & Labor"],
    "employment & labour":          ["Employment & Labor"],
    "employment and labour":        ["Employment & Labor"],
    "employment & labor":           ["Employment & Labor"],
    "competition":                  ["Antitrust & Competition"],
    "antitrust":                    ["Antitrust & Competition"],
    "white collar":                 ["White Collar & Investigations"],
    "investigations":               ["White Collar & Investigations"],
    "healthcare":                   ["Healthcare & Life Sciences"],
    "life sciences":                ["Healthcare & Life Sciences"],
    "healthcare & life science":    ["Healthcare & Life Sciences"],
    "energy":                       ["Energy & Natural Resources"],
    "natural resources":            ["Energy & Natural Resources"],
    "energy & resources":           ["Energy & Natural Resources"],
    "infrastructure":               ["Infrastructure & Projects"],
    "projects":                     ["Infrastructure & Projects"],
    "restructuring":                ["Restructuring & Insolvency"],
    "insolvency":                   ["Restructuring & Insolvency"],
    "bankruptcy":                   ["Restructuring & Insolvency"],
    "environment":                  ["Environmental"],
    "environmental law":            ["Environmental"],
    "public law":                   ["Public Law & Government Affairs"],
    "government affairs":           ["Public Law & Government Affairs"],
    "regulatory":                   ["Regulatory & Compliance"],
    "compliance":                   ["Regulatory & Compliance"],
    "banking":                      ["Banking & Finance"],
    "finance":                      ["Banking & Finance"],
    "banking and finance":          ["Banking & Finance"],
    "banking & finance":            ["Banking & Finance"],
    "real estate":                  ["Real Estate"],
    "property":                     ["Real Estate"],
    "tax":                          ["Tax"],
    "taxation":                     ["Tax"],
    "immigration":                  ["Immigration"],
    "family":                       ["Family & Private Client"],
    "private client":               ["Family & Private Client"],
    "family & private client":      ["Family & Private Client"],
    "arbitration":                  ["Arbitration & Mediation"],
    "mediation":                    ["Arbitration & Mediation"],
    "dispute resolution":           ["Litigation & Dispute Resolution"],
    "litigation":                   ["Litigation & Dispute Resolution"],
    "trade":                        ["Trade & Customs"],
    "customs":                      ["Trade & Customs"],
    "technology":                   ["Technology & Cybersecurity"],
    "tech":                         ["Technology & Cybersecurity"],
    "capital markets":              ["Capital Markets"],
    "private equity":               ["Private Equity"],
    "intellectual property":        ["Intellectual Property"],
}

# Canonical practice areas (pass-through)
CANONICAL_PRACTICE_AREAS = {
    "Corporate & M&A",
    "Private Equity",
    "Capital Markets",
    "Banking & Finance",
    "Real Estate",
    "Litigation & Dispute Resolution",
    "Arbitration & Mediation",
    "Intellectual Property",
    "Technology & Cybersecurity",
    "Data Privacy & Protection",
    "Employment & Labor",
    "Tax",
    "Regulatory & Compliance",
    "Antitrust & Competition",
    "White Collar & Investigations",
    "Healthcare & Life Sciences",
    "Energy & Natural Resources",
    "Environmental",
    "Infrastructure & Projects",
    "Restructuring & Insolvency",
    "Immigration",
    "Family & Private Client",
    "Public Law & Government Affairs",
    "Trade & Customs",
}

# ── Country normalization map ────────────────────────────────────────────────
COUNTRY_MAP: dict[str, str] = {
    "uk":             "GB",
    "great britain":  "GB",
    "britain":        "GB",
    "england":        "GB",
    "united kingdom": "GB",
    "usa":            "US",
    "united states":  "US",
    "united states of america": "US",
    "u.s.":           "US",
    "u.s.a.":         "US",
    "germany":        "DE",
    "deutschland":    "DE",
    "france":         "FR",
    "canada":         "CA",
    "australia":      "AU",
    "japan":          "JP",
    "china":          "CN",
    "india":          "IN",
    "brazil":         "BR",
    "brasil":         "BR",
    "singapore":      "SG",
    "uae":            "AE",
    "united arab emirates": "AE",
    "south africa":   "ZA",
    "netherlands":    "NL",
    "the netherlands": "NL",
    "hong kong":      "HK",
    "spain":          "ES",
    "italy":          "IT",
    "sweden":         "SE",
    "norway":         "NO",
    "denmark":        "DK",
    "finland":        "FI",
    "belgium":        "BE",
    "switzerland":    "CH",
    "austria":        "AT",
    "poland":         "PL",
    "mexico":         "MX",
    "ireland":        "IE",
    "new zealand":    "NZ",
    "south korea":    "KR",
    "korea":          "KR",
    "russia":         "RU",
    "turkey":         "TR",
    "saudi arabia":   "SA",
    "egypt":          "EG",
    "nigeria":        "NG",
    "kenya":          "KE",
    "malaysia":       "MY",
    "indonesia":      "ID",
    "thailand":       "TH",
    "vietnam":        "VN",
    "philippines":    "PH",
    "pakistan":       "PK",
    "bangladesh":     "BD",
    "sri lanka":      "LK",
    "israel":         "IL",
    "luxembourg":     "LU",
    "portugal":       "PT",
    "czech republic": "CZ",
    "czechia":        "CZ",
    "hungary":        "HU",
    "romania":        "RO",
    "ukraine":        "UA",
    "greece":         "GR",
    "croatia":        "HR",
    "serbia":         "RS",
    "slovenia":       "SI",
    "slovakia":       "SK",
    "bulgaria":       "BG",
    "estonia":        "EE",
    "latvia":         "LV",
    "lithuania":      "LT",
    "qatar":          "QA",
    "kuwait":         "KW",
    "bahrain":        "BH",
    "oman":           "OM",
    "jordan":         "JO",
    "lebanon":        "LB",
    "argentina":      "AR",
    "colombia":       "CO",
    "chile":          "CL",
    "peru":           "PE",
    "venezuela":      "VE",
    "ecuador":        "EC",
    "panama":         "PA",
    "costa rica":     "CR",
    "taiwan":         "TW",
    "myanmar":        "MM",
    "cambodia":       "KH",
    "laos":           "LA",
    "ethiopia":       "ET",
    "ghana":          "GH",
    "tanzania":       "TZ",
    "uganda":         "UG",
    "zimbabwe":       "ZW",
    "zambia":         "ZM",
    "mozambique":     "MZ",
    "morocco":        "MA",
    "algeria":        "DZ",
    "tunisia":        "TN",
    "senegal":        "SN",
    "cameroon":       "CM",
    "ivory coast":    "CI",
    "cote d'ivoire":  "CI",
    "democratic republic of the congo": "CD",
    "drc":            "CD",
}


def normalize_whitespace(value: str) -> str:
    """Trim and collapse internal whitespace."""
    return re.sub(r"\s+", " ", value.strip())


def normalize_practice_area(raw: str) -> tuple[list[str], bool]:
    """
    Normalize a single raw practice area string.

    Returns:
        (canonical_list, is_ambiguous)
        is_ambiguous=True means we flagged it for manual review.
    """
    cleaned = normalize_whitespace(raw)

    # Pass-through if already canonical
    if cleaned in CANONICAL_PRACTICE_AREAS:
        return [cleaned], False

    key = cleaned.lower()
    if key in PRACTICE_AREA_MAP:
        return PRACTICE_AREA_MAP[key], False

    # Not found — flag for review
    return [cleaned], True


def normalize_country(raw: str) -> tuple[str, bool]:
    """
    Normalize a country string to ISO 3166-1 alpha-2.

    Returns:
        (normalized_code, is_ambiguous)
        is_ambiguous=True means we flagged it for manual review.
    """
    cleaned = normalize_whitespace(raw)

    # Try case-insensitive lookup first (catches "UK" -> "GB", "USA" -> "US", etc.)
    key = cleaned.lower()
    if key in COUNTRY_MAP:
        return COUNTRY_MAP[key], False

    # Already a valid 2-letter uppercase code (not in the remapping table)
    if re.match(r"^[A-Z]{2}$", cleaned):
        return cleaned, False

    # Unknown — flag for review
    return cleaned, True


def normalize_record(record: dict) -> tuple[dict, list[str], list[str]]:
    """
    Normalize a single expansion record in-place.

    Returns:
        (normalized_record, changes_made, flags_for_review)
    """
    changes: list[str] = []
    flags: list[str] = []

    # firm_name: preserve diacritics, just trim whitespace
    if "firm_name" in record and isinstance(record["firm_name"], str):
        original = record["firm_name"]
        normalized = normalize_whitespace(original)
        if normalized != original:
            record["firm_name"] = normalized
            changes.append(f"firm_name: trimmed whitespace")

    # city: preserve diacritics, just trim whitespace
    if "city" in record and isinstance(record["city"], str):
        original = record["city"]
        normalized = normalize_whitespace(original)
        if normalized != original:
            record["city"] = normalized
            changes.append(f"city: trimmed whitespace")

    # country normalization
    if "country" in record and isinstance(record["country"], str):
        original = record["country"]
        normalized, ambiguous = normalize_country(original)
        if normalized != original:
            record["country"] = normalized
            changes.append(f"country: {original!r} -> {normalized!r}")
        if ambiguous:
            flags.append(
                f"country: {original!r} could not be normalized — manual review required"
            )

    # practice_areas normalization
    if "practice_areas" in record and isinstance(record["practice_areas"], list):
        new_areas: list[str] = []
        seen: set[str] = set()
        for raw_area in record["practice_areas"]:
            if not isinstance(raw_area, str):
                flags.append(f"practice_areas: non-string value {raw_area!r} skipped")
                continue
            normalized_list, ambiguous = normalize_practice_area(raw_area)
            for area in normalized_list:
                if area not in seen:
                    seen.add(area)
                    new_areas.append(area)
                    if area != raw_area:
                        changes.append(f"practice_areas: {raw_area!r} -> {area!r}")
            if ambiguous:
                flags.append(
                    f"practice_areas: {raw_area!r} could not be normalized — manual review required"
                )
        record["practice_areas"] = new_areas

    # notes: trim whitespace, collapse internal spaces
    if "notes" in record and isinstance(record["notes"], str):
        original = record["notes"]
        normalized = normalize_whitespace(original)
        if normalized != original:
            record["notes"] = normalized
            changes.append("notes: whitespace normalized")

    return record, changes, flags


def normalize_file(path: Path) -> tuple[dict, list[str], list[str]]:
    """Load, normalize, and return the record with change log."""
    with open(path, encoding="utf-8") as f:
        record = yaml.safe_load(f)
    return normalize_record(record)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Normalize Law Firm Expansion Tracker YAML record fields.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--practice-area", metavar="VALUE", help="Normalize a single practice area value")
    group.add_argument("--country", metavar="VALUE", help="Normalize a single country value")
    group.add_argument("--file", metavar="PATH", help="Normalize all fields in a YAML record file (prints normalized YAML)")
    args = parser.parse_args()

    if args.practice_area:
        result, ambiguous = normalize_practice_area(args.practice_area)
        print(json.dumps({"input": args.practice_area, "normalized": result, "ambiguous": ambiguous}))
        return 1 if ambiguous else 0

    if args.country:
        result, ambiguous = normalize_country(args.country)
        print(json.dumps({"input": args.country, "normalized": result, "ambiguous": ambiguous}))
        return 1 if ambiguous else 0

    if args.file:
        path = Path(args.file)
        if not path.exists():
            print(f"ERROR: File not found: {path}", file=sys.stderr)
            return 2
        record, changes, flags = normalize_file(path)
        print(yaml.dump(record, allow_unicode=True, sort_keys=False, default_flow_style=False))
        if changes:
            print(f"\n# Changes made: {len(changes)}", file=sys.stderr)
            for c in changes:
                print(f"#   {c}", file=sys.stderr)
        if flags:
            print(f"\n# Manual review required: {len(flags)}", file=sys.stderr)
            for f in flags:
                print(f"#   {f}", file=sys.stderr)
        return 1 if flags else 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
