# Schema Changelog

## [1.0.0] — 2026-03-11

### Added
- Initial schema release for Law Firm Expansion Tracker.
- Required fields: `record_id`, `firm_name`, `expansion_type`, `practice_areas`,
  `country`, `announced_date`, `source_url`, `source_type`, `confidence`,
  `status`, `created_at`, `last_modified`, `schema_version`.
- Optional fields: `region`, `city`, `effective_date`, `headcount`,
  `related_records`, `tags`, `notes`, `created_by`.
- Controlled vocabulary enums for `expansion_type`, `practice_areas`,
  `source_type`, `confidence`, `status`.
- `record_id` pattern: `^[a-z0-9][a-z0-9-]*[a-z0-9]$`.
- `country` pattern: ISO 3166-1 alpha-2 (two uppercase letters).
- `source_url` must begin with `http://` or `https://`.
- `announced_date` and `effective_date` must match `YYYY-MM-DD`.
- `created_at` and `last_modified` must match ISO 8601 UTC datetime.
- `notes` capped at 1000 characters in schema (500 recommended).
- `headcount` minimum value 1.
- `related_records` items must match `record_id` pattern.
- `additionalProperties: false` — unknown fields rejected.

### Design Decisions
- One record per expansion event (not one per city for multi-city announcements
  unless modelled as parent + child records).
- `practice_areas` is an array to support multi-practice events.
- Draft-07 JSON Schema for broad validator compatibility.
- Schema version embedded in each record as `schema_version` for migration tracing.
