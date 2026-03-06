# Setup & Deployment

## Requirements
- Python 3.11+
- Two GitHub Secrets: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`

## Local

```bash
git clone https://github.com/YOUR_USERNAME/law-firm-tracker
cd law-firm-tracker
pip install -r requirements.txt
cp .env.example .env   # fill in your Telegram credentials

python main.py           # collect + analyse
python main.py --evolve  # run learning evolution
python main.py --digest  # send weekly digest
```

## GitHub Actions

The workflow (`.github/workflows/tracker.yml`) runs automatically:

| Schedule | Action |
|---|---|
| Daily 07:00 UTC | Full signal collection |
| Hourly (first 48 h) | Learning evolution — bootstrap phase, α = 0.40 |
| Daily (after 48 h) | Learning evolution — stable phase, α = 0.15 |
| Sunday 09:00 UTC | Weekly digest via Telegram |

Add `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` under  
**Settings → Secrets and variables → Actions**.

## Learning system

| Phase | Trigger | Alpha |
|---|---|---|
| Bootstrap | First 48 h | 0.40 |
| Stable | After 48 h | 0.15 |

Reports land in `docs/learning_report.json` and `docs/learning_history.jsonl`.
