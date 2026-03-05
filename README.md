# 🏛 Canadian Law Firm Expansion Tracker

Tracks 26 Canadian law firms across **15 scrapers** and **17 practice departments** — surfacing lateral hires, court records, rankings, regulatory filings, and bar leadership changes before they become public knowledge.

Runs automatically on GitHub Actions. Sends instant Telegram alerts on Tier 1 signals, and a ranked weekly digest every Sunday.

---

## Architecture

### 15 Scrapers

| Scraper | Sources | Signal Type | Weight |
|---|---|---|---|
| `RSSFeedScraper` | 15 RSS feeds (Canadian Lawyer, Law Times, Globe, Bloomberg…) | `lateral_hire`, `press_release` | 1.5–2.0 |
| `PressScraper` | Firm news pages, Canadian Lawyer, Law Times, The Lawyer's Daily | `lateral_hire`, `press_release` | 1.5–3.0 |
| `LinkedInScraper` | Company feed, Google-cached profiles | `lateral_hire` | 2.0–3.5 |
| `JobsScraper` | Firm careers, Indeed, LinkedIn jobs | `job_posting` | 2.0 |
| `PublicationsScraper` | Firm insights, Lexology, Mondaq | `publication` | 1.0 |
| `WebsiteScraper` | Practice area pages + change detection | `practice_page` | 2.5 |
| `CanLIIScraper` | CanLII REST API — 25 courts & tribunals | `court_record` | 2.5 |
| `SedarScraper` | SEDAR+ securities filings | `court_record` | 2.5–5.0 |
| `GovTrackScraper` | Canada Gazette, Competition Bureau, CRTC, OSC, NEB, Privacy Commissioner, OSFI, IRCC | `court_record` | 2.5–3.0 |
| `LobbyistScraper` | Federal Lobbyist Registry | `court_record` | 3.0 |
| `ChambersScraper` | Chambers Canada, Legal 500 Canada | `ranking` | 3.0 |
| `AwardsScraper` | Best Lawyers, Benchmark Canada, Lexpert, Who's Who Legal, Precedent | `ranking` | 2.5–3.0 |
| `LawSchoolScraper` | Ultra Vires, lawrecruits.com, GreatStudentJobs, firm student pages | `recruit_posting` | 2.0 |
| `BarAssociationScraper` | CBA (25 sections), OBA, LSO, Advocates' Society, CCCA, ACC | `bar_leadership`, `bar_speaking` | 1.5–3.5 |
| `ConferenceScraper` | LSO CPD, OBA Institute, Osgoode PD, PDAC, IAPP, Canadian Institute | `bar_speaking`, `bar_sponsorship` | 1.5–2.5 |

### Signal Confidence Tiers

```
TIER 1 (weight 3.0–3.5)  bar_leadership · ranking · lateral_hire
  Firm made a financial or reputational commitment.

TIER 2 (weight 2.0–2.5)  court_record · practice_page · job_posting · recruit_posting
  Observable, verifiable activity.

TIER 3 (weight 1.0–1.5)  press_release · bar_speaking · publication
  Early leading indicator — needs corroboration.
```

### Spike Detection

Uses **z-score analysis** over a 4-week rolling baseline:
- z-score ≥ 1.5 → flagged as significant spike
- No prior history + score ≥ 3.5 → flagged as new signal

### 17 Practice Departments

Corporate/M&A · Private Equity · Capital Markets · Litigation · Restructuring ·
Real Estate · Tax · Employment · IP · Data Privacy · ESG · Energy ·
Financial Services · Competition · Healthcare · Immigration · Infrastructure

---

## Quick Start

### 1. Create GitHub repo

```bash
gh repo create law-firm-expansion-tracker --private
git clone https://github.com/YOUR_USERNAME/law-firm-expansion-tracker.git
cd law-firm-expansion-tracker
```

### 2. Copy all files into the repo

```
law-firm-expansion-tracker/
├── main.py
├── config.py
├── firms.py
├── requirements.txt
├── .env.example
├── .gitignore
├── scrapers/          (15 scrapers + base.py + __init__.py)
├── classifier/        (department.py + taxonomy.py + __init__.py)
├── analysis/          (signals.py + __init__.py)
├── database/          (db.py + __init__.py)
├── alerts/            (notifier.py + __init__.py)
├── dashboard/         (generator.py + __init__.py)
└── .github/workflows/ (tracker.yml)
```

### 3. Add GitHub Secrets

**Repo → Settings → Secrets and variables → Actions**

| Secret Name | Value |
|---|---|
| `TELEGRAM_BOT_TOKEN` | Your bot token from @BotFather |
| `TELEGRAM_CHAT_ID` | Your chat/channel ID |

### 4. Enable GitHub Pages

**Repo → Settings → Pages → Source: Deploy from branch → Branch: `main` → Folder: `/docs`**

Dashboard will be live at: `https://YOUR_USERNAME.github.io/law-firm-expansion-tracker/`

### 5. Push and test

```bash
git add .
git commit -m "Initial commit"
git push origin main
```

Then: **Actions → Run workflow → firm: osler** to test a single firm.

---

## Local Development

```bash
cp .env.example .env
# fill in TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID

pip install -r requirements.txt

# Test single firm
python main.py --firm osler

# Regenerate dashboard from existing data
python main.py --dashboard

# Send digest from existing data
python main.py --digest
```

---

## Cron Schedule

| Time | Action |
|---|---|
| Daily 07:00 UTC | Full collection — all 15 scrapers × 26 firms |
| Sunday 09:00 UTC | Weekly ranked digest via Telegram |
| On demand | Manual dispatch from Actions tab |

---

## Telegram Output

**Instant alert** (fires within 24h of Tier 1 signal):
```
🚨 New Expansion Signal

🏛 Osler, Hoskin & Harcourt LLP
🔒 Department: Data Privacy & Cybersecurity
📌 Type: 🏅 Bar Leadership
📝 [CBA] Partner elected Chair, Privacy Section
🔑 Keywords: privacy, chair, section
🔗 Source

🖥 View Full Dashboard →
```

**Sunday digest** (ranked by expansion score):
```
📊 Law Firm Expansion Tracker
Week of March 09, 2026
🖥 Open Live Dashboard →
──────────────────────────────
7 expansion signal(s) across 4 firm(s)

1. 🏛 Osler, Hoskin & Harcourt LLP
   🔒 Data Privacy & Cybersecurity 🔥
   Score: 18.4 ↑ 2.3× baseline
   Signals: 6 (3 bar-lead, 2 rankings, 1 job)
   • 🏅 Bar Leadership: Partner elected Chair...
   • 🏆 Ranking: Chambers Band 1 (new entry)...

2. 🏛 McCarthy Tétrault LLP
   🌿 ESG & Regulatory
   Score: 12.1 ↑ 1.9× baseline
   ...
```

---

## Adding Firms

Edit `firms.py` and add an entry to the `FIRMS` list:

```python
{
    "id":           "unique_id",
    "name":         "Full Firm Name LLP",
    "short":        "Short Name",
    "website":      "https://www.firmname.com",
    "careers_url":  "https://www.firmname.com/careers",
    "news_url":     "https://www.firmname.com/news",
    "linkedin_slug":"firm-linkedin-slug",
    "hq":           "City",
    "alt_names":    ["Alt Name", "Abbreviation"],
},
```

## Tuning the Classifier

Edit `classifier/taxonomy.py` to add keywords and phrases per department.
Phrase matches (multi-word) get a **2.5× boost** over single-word keywords.
