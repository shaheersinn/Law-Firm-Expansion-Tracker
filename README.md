# 🏛 Enhanced Calgary Law Firm Hiring Tracker v2.0

> **From "Are you hiring?" to "I saw your SEDAR+ filing for TransAlta — I'm a securities lawyer available Monday."**

A five-strategy, real-time intelligence system that surfaces hiring
opportunities at Calgary law firms *before they're posted*.

---

## Five Intelligence Strategies

| # | Strategy | Trigger | Weight | Urgency |
|---|----------|---------|--------|---------|
| 1 | **Follow the Work** | CanLII ABQB appearance spike (z ≥ 1.5) | 4.0 | This week |
| 2 | **Follow the Money** | SEDAR+ major deal, firm named as counsel | 4.5–5.0 | **Same day** |
| 3 | **Empty Chair** | LinkedIn associate changes employer | 4.5 | **Same day** |
| 4 | **Hireback Vacuum** | LSA directory: students not retained post-articles | 4.0–5.0 | Within 3 days |
| 5 | **Spillage Graph** | Mega-deal + BigLaw conflict → boutique overflow | 5.0 | **Same day** |

---

## Architecture

```
law_tracker/
├── config_calgary.py          # 30 firms, weights, API keys
├── main_enhanced.py           # Orchestrator CLI
├── requirements.txt
├── .env.example
│
├── signals/
│   ├── canlii_litigation.py   # Strategy 1: CanLII API + z-score spike
│   ├── sedar_corporate.py     # Strategy 2: SEDAR+ RSS + PDF counsel extraction
│   ├── linkedin_turnover.py   # Strategy 3: Proxycurl associate roster + departure detect
│   ├── lsa_hireback.py        # Strategy 4: LSA directory + retention gap
│   └── spillage_graph.py      # Strategy 5: Network graph + deal monitor
│
├── scoring/
│   └── aggregator.py          # Time-decay scoring, corroboration boost, leaderboard
│
├── outreach/
│   └── generator.py           # Signal-aware personalized email drafts
│
├── alerts/
│   └── notifier.py            # Telegram (Tier-1 instant) + SendGrid (weekly digest)
│
├── intelligence/
│   ├── decision_engine.py     # Picks the best action each morning
│   ├── reply_coach.py         # Coaches replies and interview prep
│   └── custom_agents.py       # Multi-agent analysis: scout, momentum, strategy, risk
│
├── database/
│   └── db.py                  # SQLite schema (all 5 strategies)
│
└── .github/workflows/
    └── tracker.yml            # 4 cron jobs + manual dispatch
```

---

## Scoring Engine

```
firm_score = Σ ( weight × e^(-0.1×days_ago) × corroboration_boost × tier_mult )

corroboration_boost = 1.30 if ≥2 independent strategies fire
tier_mult           = 1.20 for boutiques, 1.10 for mid-size, 1.0 for BigLaw
```

Signals decay exponentially so a 7-day-old alert is worth ~50% of a fresh one.

---

## Spillage Graph

The **spillage graph** maps which Calgary boutiques most frequently appear as
*opposing counsel* to BigLaw firms in ABQB decisions and SEDAR+ transactions.
It is built automatically as CanLII and SEDAR data is ingested.

When a mega-deal is announced:
1. Identify which BigLaw firm holds the retainer (via conflict radar)
2. Look up their top 3 boutique co-appellants from the graph
3. Fire a same-day alert: email those boutiques **before** they post the job

```
Blakes ─────── Bennett Jones (38 co-appearances)
      ╲─────── BDP           (24 co-appearances)  ← alert this one today
      ╲─────── Field Law      (19 co-appearances)
```

---

## Setup

### 1. Clone and configure

```bash
git clone https://github.com/YOUR_USERNAME/law-firm-tracker-enhanced.git
cd law-firm-tracker-enhanced
cp .env.example .env
# Fill in API keys
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Initialise DB

```bash
python main_enhanced.py --init-db
```

### 4. Bootstrap LinkedIn roster (run once)

```bash
python main_enhanced.py --build-roster
```

### 5. GitHub Secrets

Add these in **Repo → Settings → Secrets → Actions**:

| Secret | Source |
|--------|--------|
| `CANLII_API_KEY` | Register at canlii.org |
| `PROXYCURL_API_KEY` | nubela.co/proxycurl |
| `SENDGRID_API_KEY` | sendgrid.com |
| `TELEGRAM_BOT_TOKEN` | @BotFather on Telegram |
| `TELEGRAM_CHAT_ID` | Your chat ID |
| `ALERT_EMAIL_FROM` | Your verified sender |
| `ALERT_EMAIL_TO` | Your inbox |

---

## CLI Usage

```bash
# Run all 5 strategies + scoring + alerts
python main_enhanced.py --all

# Run individual strategies
python main_enhanced.py --strategy canlii
python main_enhanced.py --strategy sedar
python main_enhanced.py --strategy linkedin
python main_enhanced.py --strategy lsa
python main_enhanced.py --strategy spillage

# Analysis
python main_enhanced.py --leaderboard     # ranked opportunity scores
python main_enhanced.py --graph           # spillage graph + conflict radar
python main_enhanced.py --outreach        # print personalized email drafts
python main_v5.py --agents                # multi-agent intelligence report
python main_v5.py --agents --agents-top 8 --agents-days 21

# Alerts
python main_enhanced.py --digest          # send weekly email digest
```

### Custom Agent Swarm

The tracker now supports a deterministic multi-agent layer that reviews the
same signal store from four perspectives:

1. **Signal Scout** — finds corroborated firms and practice-area hotspots.
2. **Momentum Analyst** — spots firms whose signal volume is accelerating.
3. **Opportunity Strategist** — merges leaderboard rank with predicted demand.
4. **Risk Guard** — warns when a recommendation is stale, thinly supported, or
   already saturated with outreach.

Running `python main_v5.py --agents` prints a single markdown briefing, writes a
JSON artifact to `reports/agent_swarm_report.json`, and persists the run in the
SQLite database for later review.

---

## Cron Schedule

| Time (UTC) | Job |
|-----------|-----|
| Daily 07:00 | All 5 strategies + scoring + Telegram alerts |
| Sunday 05:00 | LinkedIn departure check |
| Sunday 09:00 | Weekly email digest + outreach plan |
| Sept 1, Oct 1 | LSA hireback vacuum |

---

## Sample Output

```
══════════════════════════════════════════════════════════════════════════
🏛  CALGARY LAW FIRM HIRING OPPORTUNITY LEADERBOARD
    Generated: 2026-03-20 07:14 UTC
══════════════════════════════════════════════════════════════════════════

 1. Burnet, Duckworth & Palmer LLP  [MID]
    Score: 28.4  |  🚨 Same-Day  |  Signals: 7  ✅ CORROBORATED
    Strategies: corporate · litigation · spillage
    Top signal: BigLaw spillage → BDP: $2,100M deal (Cenovus/SEDAR+)

 2. Field Law  [MID]
    Score: 19.1  |  ⚡ This Week  |  Signals: 4  ✅ CORROBORATED
    Strategies: litigation · turnover
    Top signal: Empty chair at Field Law: J. Smith departed → Cenovus in-house

 3. Bennett Jones LLP  [BIG]
    Score: 14.7  |  🚨 Same-Day  |  Signals: 3
    Strategies: corporate · spillage
    Top signal: SEDAR+ deal: ARC Resources — prospectus ($880M)
```

---

## Sample Outreach Email (Strategy 2)

```
Subject: First-Year Associate — Securities Background — Re: ARC Resources Prospectus

Dear Hiring Partner,

I noticed that Bennett Jones LLP was named as counsel on the recent 
prospectus for ARC Resources on SEDAR+ (deal value: ~$880M). 
Transactions of this scale typically require substantial junior due 
diligence and document review support.

My background is in Canadian securities regulation and M&A, and I am 
available to start on short notice.

[Your Name]
```

---

## Data Sources & Compliance

| Source | Access Method | Notes |
|--------|--------------|-------|
| CanLII | Official REST API | Rate-limited to 1 req/sec; API key required |
| SEDAR+ | Public RSS feed | Official public feed |
| LSA Directory | Public HTML | Publicly available lawyer lookup |
| LinkedIn | Proxycurl API | Uses public profile data per hiQ v. LinkedIn |
| Google News | Public RSS | No authentication required |

**CanLII note**: The tracker uses only the official CanLII API and strictly
respects rate limits. CanLII actively enforces against unauthorized bulk scraping.

---

## Adding More Firms

Edit `config_calgary.py` → `CALGARY_FIRMS` list. Each firm needs:
- `id`, `name`, `aliases`, `linkedin_slug`, `tier`, `focus`

## Tuning Weights

Edit `SIGNAL_WEIGHTS` in `config_calgary.py`.
