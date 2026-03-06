# Setup & Deployment

## Requirements
- Python 3.11+
- `pip install -r requirements.txt`

## Local setup
```bash
cp .env.example .env
# Edit .env — add TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID at minimum
python main.py              # collect + send combined Telegram digest
python main.py --evolve     # run learning cycle only
python main.py --digest     # send digest from existing DB data
python main.py --firm osler # single-firm test run
```

## GitHub Secrets (Settings → Secrets and variables → Actions)

| Secret | Required | Description |
|--------|----------|-------------|
| `TELEGRAM_BOT_TOKEN` | ✅ | From @BotFather |
| `TELEGRAM_CHAT_ID` | ✅ | Your channel or chat ID |
| `DASHBOARD_URL` | ✅ | URL to your deployed dashboard — appears in every Telegram alert |
| `CANLII_API_KEY` | Optional | Free key from api.canlii.org — enables Canadian court signals |

### Getting DASHBOARD_URL
Set this to your GitHub Pages URL: `https://yourusername.github.io/law-firm-tracker/`  
Enable Pages: repo Settings → Pages → Source: `main` branch, `/docs` folder.

## Telegram alert format
Every run sends **one combined message** containing:
- New signals collected (count + type breakdown)
- Top expansion alerts ranked by score with source links
- 📈 Dashboard link
- 📋 GitHub Actions run log link

## Schedule
- **Daily at 7 AM UTC** — full collect run across all firms
- Digest is sent at the end of every collect run (not just Sundays)

## Signal lookback window
By default, only signals from the **past 21 days** are accepted.  
Older content is filtered out automatically.  
Override with: `SIGNAL_LOOKBACK_DAYS=14` (or any number) in Secrets.

## Bugs fixed in this version
- Rate-limit waits capped at 5s (was 30s × multiple URLs = job timeout/cancellation)
- CanLII scraper skips silently when no API key is set (was 401 spam on every firm)
- `osgoode.yorku.ca` removed (persistent SSL cert failure)
- `lawrecruits.com` removed (persistent connection timeout)
- `ccca-caj.ca` removed (DNS dead — domain no longer resolves)
- Per-signal Telegram blasts removed (was sending 10+ messages per run)
- Date filter added: articles older than 21 days are dropped at parse time
