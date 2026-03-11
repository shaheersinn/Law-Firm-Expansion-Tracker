# Taxonomy Reference — Law Firm Expansion Tracker

All controlled vocabulary used in expansion records.
Source of truth: `data/taxonomy/`

---

## Practice Areas

File: `data/taxonomy/practice-areas.txt`

These are the only valid values for the `practice_areas` field.

| Canonical Value |
|---|
| Corporate & M&A |
| Private Equity |
| Capital Markets |
| Banking & Finance |
| Real Estate |
| Litigation & Dispute Resolution |
| Arbitration & Mediation |
| Intellectual Property |
| Technology & Cybersecurity |
| Data Privacy & Protection |
| Employment & Labor |
| Tax |
| Regulatory & Compliance |
| Antitrust & Competition |
| White Collar & Investigations |
| Healthcare & Life Sciences |
| Energy & Natural Resources |
| Environmental |
| Infrastructure & Projects |
| Restructuring & Insolvency |
| Immigration |
| Family & Private Client |
| Public Law & Government Affairs |
| Trade & Customs |

### Common Normalizations

| Raw Input | Canonical Value(s) |
|---|---|
| IP | Intellectual Property |
| M&A | Corporate & M&A |
| Privacy | Data Privacy & Protection |
| Privacy & Cybersecurity | Data Privacy & Protection + Technology & Cybersecurity |
| Data Privacy & Cybersecurity | Data Privacy & Protection + Technology & Cybersecurity |
| Labor | Employment & Labor |
| Labour | Employment & Labor |
| Competition | Antitrust & Competition |
| Antitrust | Antitrust & Competition |
| White Collar | White Collar & Investigations |
| Healthcare | Healthcare & Life Sciences |
| Energy | Energy & Natural Resources |
| Restructuring | Restructuring & Insolvency |
| Insolvency | Restructuring & Insolvency |
| Bankruptcy | Restructuring & Insolvency |

Use `python scripts/normalize.py --practice-area "IP"` to check normalization.

---

## Expansion Types

File: `data/taxonomy/expansion-types.txt`

| Value | Description |
|---|---|
| `new_office` | Opening a new physical office in a new location |
| `new_practice_group` | Launching a new practice group |
| `merger` | Firm-level or group-level merger/combination |
| `lateral_hire_group` | Hiring a group of lawyers from another firm |
| `office_relocation` | Moving an existing office to a new address |
| `office_expansion` | Expanding an existing office (space, headcount) |
| `strategic_alliance` | Formal alliance without full merger |
| `new_jurisdiction_license` | Obtaining a licence to practice in a new jurisdiction |
| `department_restructure` | Internal restructuring of a practice group or department |

---

## Confidence Levels

File: `data/taxonomy/confidence-levels.txt`

| Value | When to use |
|---|---|
| `confirmed` | Official firm announcement or primary authoritative disclosure |
| `high` | Strong third-party reporting from reputable legal industry sources |
| `medium` | Credible signal with corroboration but not fully primary |
| `low` | Weak or indirect evidence, including job postings without confirmation |
| `unverified` | Insufficient confidence — manual review required |

**Note:** Records with `confidence: unverified` should not be published without explicit approval.

---

## Source Types

File: `data/taxonomy/source-types.txt`

| Value | Description |
|---|---|
| `firm_press_release` | Official press release from the firm itself |
| `legal_directory` | Chambers, Legal 500, Martindale, or similar |
| `legal_news` | Law360, The Lawyer, The American Lawyer, etc. |
| `court_filing` | Public court filing |
| `job_posting` | Job posting (weak evidence) |
| `industry_report` | Research report from an industry analyst |
| `social_media` | LinkedIn, Twitter/X, or similar (weak evidence) |
| `other` | Other source not in the list |

**Weak source types:** `job_posting`, `social_media`, `other` — records using these as the primary source should have `confidence: low` or lower.

---

## Countries

File: `data/taxonomy/countries.txt`

Use **ISO 3166-1 alpha-2** codes (two uppercase letters).

| Common Input | ISO Code |
|---|---|
| UK, United Kingdom, Britain | GB |
| USA, United States | US |
| Germany, Deutschland | DE |
| France | FR |
| Canada | CA |
| Australia | AU |
| Japan | JP |
| China | CN |
| India | IN |
| Brazil, Brasil | BR |
| Singapore | SG |
| UAE, United Arab Emirates | AE |
| South Africa | ZA |
| Netherlands | NL |
| Hong Kong | HK |

For the complete list of valid codes, see `data/taxonomy/countries.txt`.

Use `python scripts/normalize.py --country "UK"` to check normalization.

---

## Status Values

| Value | Meaning |
|---|---|
| `draft` | Being entered, not yet reviewed |
| `under_review` | Submitted for human review |
| `verified` | Reviewed and confirmed accurate |
| `published` | Publicly visible in reports/dashboard |
| `archived` | No longer active or relevant |

See [Contributor Guide — Status Transitions](contributor-guide.md#status-transitions) for allowed transitions.
