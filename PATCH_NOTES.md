# Law Firm Tracker — Patch Notes & Integration Guide
# Generated: 2026-03-10 — Based on run log analysis (run ID: 22891995498)

## ERRORS FOUND IN LOGS

### 🔴 CRITICAL — Git push 403 (dashboard never deployed)
```
remote: Permission to shaheersinn/Law-Firm-Expansion-Tracker.git denied to github-actions[bot].
fatal: unable to access '...': The requested URL returned error: 403
##[error]Process completed with exit code 128.
```
**Root cause:** GITHUB_TOKEN had `Contents: read` only. Bot cannot push.
**Fix:** Added `permissions: contents: write` to tracker.yml.
**File:** `.github/workflows/tracker.yml` (replace entirely)

---

### 🟡 SYSTEMIC — All 32 RSS feeds returned 0 signals
Every firm logged: `[FirmName] RSS total: 0 signal(s)`
**Root cause (most likely):** Feed URLs have drifted / returned 304 / behind
cloudflare protection. Silent failure — errors swallowed without logging.
**Fix:** Run `python -m utils.rss_diagnostics` to triage which feeds are broken.
**File:** `utils/rss_diagnostics.py` (new)

---

### 🟡 SYSTEMIC — Only 1 new signal out of 728 scraper runs
Pattern: `N signals (0 new)` for nearly every scraper/firm combo.
**Root cause:** Dedup window (21 days) is correctly suppressing prior signals,
but the scraper sources are returning identical content to prior runs — no
genuinely new content is appearing.
**Fix:** Run `python -m utils.dedup_audit` to measure suppression rate per
scraper. If suppression is 100% across all scrapers, the problem is that
sources haven't published new content, not a code bug.
**File:** `utils/dedup_audit.py` (new)

---

### 🟡 CONFIG — CANLII_API_KEY is empty
Log shows: `CANLII_API_KEY: ` (blank)
**Fix:** Add `CANLII_API_KEY` to GitHub Actions secrets.
Until then, CanLIIScraper silently no-ops.

---

### 🟡 BLIND SPOT — LateralTrackScraper = 0 for all 26 firms
The #1 priority scraper found nothing. No warning was raised.
**Fix 1:** `utils/scraper_health.py` — raises Telegram alert after consecutive zeros.
**Fix 2:** `scrapers/lateral_boost.py` — augmented lateral detection with
           confidence scoring + ZSA + people page scraping.

---

## ENHANCEMENTS INCLUDED

### 1. Scraper Health Monitor (`utils/scraper_health.py`)
- Tracks consecutive zero-signal runs per scraper × firm pair
- Raises Telegram warnings when high-value scrapers go silent
- Generates daily health summary in digest
- Regression detection: alerts when a previously active scraper goes dark

**Integration in `main.py`:**
```python
from utils.scraper_health import record_run, build_health_summary

# After each scraper run:
warning = record_run(scraper_name, firm.key, total_signals, new_signals)
if warning:
    notifier.send(warning)

# In digest:
health_summary = build_health_summary()
```

---

### 2. Signal Velocity Engine (`utils/signal_velocity.py`)
- 7-day rolling velocity per firm (signals/day)
- Momentum delta: velocity(this week) − velocity(last week)
- Multi-scraper burst detection: ≥3 scrapers firing on same firm in 72h
- Cross-firm sector trends: ≥4 firms active in same practice area

**Integration in `main.py` digest:**
```python
from utils.signal_velocity import build_velocity_digest
digest_text += "\n\n" + build_velocity_digest(top_n=8)
```

---

### 3. RSS Diagnostics (`utils/rss_diagnostics.py`)
- Checks all 32 RSS feeds: HTTP status, entry count, recency
- Classifies each feed as: ok / stale / empty / error
- Prints actionable recommendations with replacement URL suggestions
- Run standalone: `python -m utils.rss_diagnostics`
- Add as a weekly GitHub Actions job to catch feed drift early

**Add to workflow (weekly, Sunday 09:00 UTC):**
```yaml
  rss_health:
    name: RSS Feed Health Check
    runs-on: ubuntu-latest
    if: github.event_name == 'schedule' && github.event.schedule == '0 9 * * 0'
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install -r requirements.txt
      - run: python -m utils.rss_diagnostics
```

---

### 4. Dedup Audit (`utils/dedup_audit.py`)
- Measures suppression rate per scraper (how many signals are deduped away)
- Age distribution of suppressed signals
- Near-duplicate URL detection (catches utm_source drift)
- CSV export for manual inspection

---

### 5. Lateral Boost (`scrapers/lateral_boost.py`)
- Augments LateralTrackScraper with confidence scoring (0.0–1.0)
- Scrapes firm people/news pages for hire-specific vocabulary
- Enhanced ZSA placement scanner with firm-name targeting
- Scores by: senior title (+0.30), named rival (+0.25), lateral verb (+0.35)
- Only emits signals with confidence ≥ 0.25

---

## DEPLOYMENT CHECKLIST

1. [ ] Replace `.github/workflows/tracker.yml` with patched version
       → This fixes the 403 error immediately on next run

2. [ ] Add `CANLII_API_KEY` to GitHub Actions secrets
       → Settings → Secrets and variables → Actions

3. [ ] Copy `utils/` files into repo
       → `scraper_health.py`, `signal_velocity.py`, `rss_diagnostics.py`, `dedup_audit.py`

4. [ ] Copy `scrapers/lateral_boost.py` into repo's scrapers directory

5. [ ] Run RSS diagnostics locally or in CI:
       `python -m utils.rss_diagnostics`
       → This will tell you which of the 32 feeds are broken/stale

6. [ ] Wire `record_run()` into main.py's scraper loop (see integration code above)

7. [ ] Wire `build_velocity_digest()` into Telegram digest

8. [ ] Run dedup audit to confirm suppression is expected:
       `python -m utils.dedup_audit`
