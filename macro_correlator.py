"""
signals/advanced/macro_correlator.py
──────────────────────────────────────
Signal 6 — Macro Demand Correlator

The insight: Calgary legal hiring lags energy commodity prices by ~60-90 days.
When WTI crosses $80/barrel AND the TSX Energy Index shows a 3-month uptrend,
M&A activity accelerates and law firms hire. This gives you a ~60-day PREDICTIVE
window BEFORE any SEDAR+ filing or CanLII appearance even exists.

Sources (all free/public):
  • Yahoo Finance API (yfinance) — WTI, TSX Energy, AECO proxy, S&P/TSX
  • Bank of Canada valet API — CAD/USD (affects deal feasibility)
  • Statistics Canada — Canadian M&A transaction volume (quarterly)
  • Natural Resources Canada — crude oil production data

Signals fired:
  • macro_demand_surge     (w=3.5) — WTI trend + TSX energy both bullish 60+ days
  • macro_demand_collapse  (w=2.0) — commodity crash = firms may freeze hiring
  • macro_ma_wave_incoming (w=4.0) — M&A index points to deal wave in 60-90 days

BUG FIX (v5.2):
  Previously _fire_ma_wave and _fire_demand_surge looped over all 30 firms and
  inserted one DB row per firm with identical titles. Since dedup hashes on
  firm_id + signal_type + title, every firm passed dedup → 14 Telegram messages
  for a single market event.

  Fix: both methods now insert a SINGLE market-wide signal (firm_id="market").
  The description lists the top affected firms. One alert. One DB row.
  Firm-level scores still get updated because scoring/aggregator.py reads all
  signals including firm_id="market" tagged with practice_area="energy".
"""

import logging, time, json
from datetime import datetime, date, timedelta
from typing import Optional
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from database.db import insert_signal

log = logging.getLogger(__name__)

try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False
    log.warning("[Macro] yfinance not installed — using fallback HTTP fetch")

import requests

# ── Tickers ────────────────────────────────────────────────────────────────────
WTI_TICKER        = "CL=F"     # WTI Crude Futures
TSX_ENERGY_TICKER = "XEG.TO"   # iShares S&P/TSX Capped Energy ETF
TSX_COMP_TICKER   = "^GSPTSE"  # S&P/TSX Composite
NATGAS_TICKER     = "NG=F"     # Henry Hub Natural Gas (AECO proxy)
CAD_TICKER        = "CAD=X"    # USD/CAD

# ── Thresholds ─────────────────────────────────────────────────────────────────
WTI_BULLISH_THRESHOLD = 80.0    # $/barrel
WTI_TREND_DAYS        = 21      # 21-day SMA must be rising
TSX_ENERGY_TREND_DAYS = 63      # 3-month trend
MA_WAVE_SCORE_MIN     = 3       # need 3 of 7 bullish signals for MA_WAVE alert

# ── Bank of Canada Valet API ───────────────────────────────────────────────────
BOC_BASE = "https://www.bankofcanada.ca/valet"

def fetch_boc_rate(series: str = "FXCADUSD", days: int = 30) -> Optional[float]:
    """Fetch CAD/USD from Bank of Canada public API."""
    start = (date.today() - timedelta(days=days)).isoformat()
    url   = f"{BOC_BASE}/observations/{series}/json?start_date={start}"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        obs = resp.json().get("observations", [])
        if obs:
            val = obs[-1].get(series, {}).get("v")
            return float(val) if val else None
    except Exception as e:
        log.debug("[Macro] BoC fetch failed: %s", e)
    return None

# ── Price fetch (yfinance or fallback) ────────────────────────────────────────

def _fetch_prices_yf(ticker: str, days: int = 90) -> list:
    if not YFINANCE_AVAILABLE:
        return []
    try:
        tk   = yf.Ticker(ticker)
        hist = tk.history(period=f"{days}d", interval="1d")
        return hist["Close"].dropna().tolist()
    except Exception as e:
        log.debug("[Macro] yfinance error for %s: %s", ticker, e)
        return []

def _fetch_prices_http(symbol: str) -> list:
    """Fallback: Yahoo Finance JSON API (unofficial but public)."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"interval": "1d", "range": "3mo"}
    try:
        resp = requests.get(url, params=params, timeout=12,
                            headers={"User-Agent": "Mozilla/5.0"})
        data = resp.json()
        closes = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        return [c for c in closes if c is not None]
    except Exception as e:
        log.debug("[Macro] HTTP price fetch failed for %s: %s", symbol, e)
        return []

def get_prices(ticker: str, days: int = 90) -> list:
    prices = _fetch_prices_yf(ticker, days)
    if not prices:
        prices = _fetch_prices_http(ticker)
    return prices

# ── Trend analysis ─────────────────────────────────────────────────────────────

def sma(prices: list, window: int) -> float:
    if len(prices) < window:
        return sum(prices) / len(prices) if prices else 0.0
    return sum(prices[-window:]) / window

def is_uptrend(prices: list, short_w: int = 10, long_w: int = 30) -> bool:
    if len(prices) < long_w:
        return False
    return sma(prices, short_w) > sma(prices, long_w)

def pct_change(prices: list, days: int = 21) -> float:
    if len(prices) < days + 1:
        return 0.0
    base = prices[-(days + 1)]
    return ((prices[-1] - base) / base) * 100 if base else 0.0

# ── Main correlator ───────────────────────────────────────────────────────────

class MacroCorrelator:
    """
    Tracks commodity and equity macro signals and fires predictive hiring signals
    60-90 days before deal flow materialises in SEDAR+ / CanLII.
    """

    def __init__(self):
        self.new_signals: list = []

    def run(self) -> list:
        log.info("[Macro] Fetching commodity & equity data…")

        wti    = get_prices(WTI_TICKER,        days=90)
        tsx_en = get_prices(TSX_ENERGY_TICKER, days=90)
        natgas = get_prices(NATGAS_TICKER,     days=90)
        cadx   = fetch_boc_rate("FXCADUSD")

        if not wti:
            log.warning("[Macro] No WTI data — skipping macro correlator")
            return []

        # ── Evaluate each signal dimension ─────────────────────────────────
        wti_price    = wti[-1] if wti else 0
        wti_up       = is_uptrend(wti, short_w=10, long_w=30)
        wti_3mo_chg  = pct_change(wti, days=min(63, len(wti)-1))
        tsx_en_up    = is_uptrend(tsx_en, short_w=10, long_w=30) if tsx_en else False
        tsx_en_chg   = pct_change(tsx_en, days=min(63, len(tsx_en)-1)) if tsx_en else 0
        natgas_up    = is_uptrend(natgas, short_w=10, long_w=21) if natgas else False
        cad_weak     = (cadx < 0.73) if cadx else False

        bullish_count = sum([
            wti_price > WTI_BULLISH_THRESHOLD,
            wti_up,
            wti_3mo_chg > 10,
            tsx_en_up,
            tsx_en_chg > 8,
            natgas_up,
            cad_weak,
        ])

        log.info(
            "[Macro] WTI=%.1f wti_up=%s tsxen_up=%s natgas_up=%s "
            "cad_weak=%s bullish_count=%d",
            wti_price, wti_up, tsx_en_up, natgas_up, cad_weak, bullish_count
        )

        self._store_snapshot(wti_price, wti_3mo_chg, tsx_en_chg, bullish_count)

        # ── Fire signals ───────────────────────────────────────────────────
        if bullish_count >= MA_WAVE_SCORE_MIN:
            self._fire_ma_wave(wti_price, wti_3mo_chg, tsx_en_chg, bullish_count)

        if bullish_count >= 5:
            self._fire_demand_surge(wti_price, bullish_count)

        if wti_price < 65 and wti_3mo_chg < -15:
            self._fire_demand_collapse(wti_price, wti_3mo_chg)

        log.info("[Macro] Done. %d macro signals.", len(self.new_signals))
        return self.new_signals

    def _fire_ma_wave(self, wti: float, wti_chg: float,
                      tsx_chg: float, bullish: int):
        """
        FIX: was looping over all 30 firms → 14 identical DB rows → 14 Telegram
        messages per run for a single market event.

        Now fires ONE market-wide signal (firm_id="market"). The description
        lists all affected boutique/mid-tier firms. One alert in Telegram.
        """
        from config_calgary import CALGARY_FIRMS
        affected = [
            f["name"] for f in CALGARY_FIRMS
            if f["tier"] in ("mid", "boutique")
            and ("energy" in f.get("focus", []) or "corporate" in f.get("focus", []))
        ]
        firms_str = ", ".join(affected[:8])
        if len(affected) > 8:
            firms_str += f" (+{len(affected)-8} more)"

        desc = (
            f"MACRO LEADING INDICATOR: {bullish}/7 bullish signals active. "
            f"WTI=${wti:.1f}/bbl ({wti_chg:+.1f}% 3-month), "
            f"TSX Energy {tsx_chg:+.1f}% 3-month. "
            f"Historical pattern: Calgary M&A/energy deal flow typically spikes "
            f"60–90 days after sustained commodity bull runs. "
            f"Target now: {firms_str}."
        )
        is_new = insert_signal(
            firm_id="market",
            signal_type="macro_ma_wave_incoming",
            weight=4.0,
            title=f"M&A Wave Incoming (60-90 day window): WTI=${wti:.1f}, {bullish}/7 bullish",
            description=desc,
            source_url="https://finance.yahoo.com/quote/CL=F",
            practice_area="energy",
            raw_data={"wti": wti, "wti_3mo_chg": wti_chg, "tsx_chg": tsx_chg,
                      "bullish_count": bullish, "affected_firms": affected},
        )
        if is_new:
            self.new_signals.append({
                "firm_id": "market",
                "signal_type": "macro_ma_wave_incoming",
                "weight": 4.0,
                "title": f"M&A Wave Incoming: WTI=${wti:.1f}, {bullish}/7 bullish",
                "description": desc,
                "practice_area": "energy",
            })

    def _fire_demand_surge(self, wti: float, bullish: int):
        """
        FIX: same per-firm loop bug as _fire_ma_wave.
        Now fires one market-wide signal listing all energy firms.
        """
        from config_calgary import CALGARY_FIRMS
        energy_firms = [f["name"] for f in CALGARY_FIRMS
                        if "energy" in f.get("focus", [])]
        firms_str = ", ".join(energy_firms[:8])
        if len(energy_firms) > 8:
            firms_str += f" (+{len(energy_firms)-8} more)"

        is_new = insert_signal(
            firm_id="market",
            signal_type="macro_demand_surge",
            weight=3.5,
            title=f"Macro Demand Surge: {bullish}/7 bullish — energy legal market hot",
            description=(
                f"5+ macro bullish signals firing. Energy legal market entering peak demand. "
                f"WTI=${wti:.1f}. Outreach to energy law firms highly time-sensitive. "
                f"Firms: {firms_str}."
            ),
            source_url="https://finance.yahoo.com",
            practice_area="energy",
            raw_data={"wti": wti, "bullish": bullish, "energy_firms": energy_firms},
        )
        if is_new:
            self.new_signals.append({
                "firm_id": "market",
                "signal_type": "macro_demand_surge",
                "weight": 3.5,
                "title": f"Macro Demand Surge: {bullish}/7 bullish — energy legal market hot",
                "practice_area": "energy",
            })

    def _fire_demand_collapse(self, wti: float, chg: float):
        """Commodity crash → firms freeze hiring. One market-wide signal."""
        from config_calgary import CALGARY_FIRMS
        energy_firms = [f["name"] for f in CALGARY_FIRMS
                        if "energy" in f.get("focus", [])]
        insert_signal(
            firm_id="market",
            signal_type="macro_demand_collapse",
            weight=-2.0,   # NEGATIVE weight — depresses firm score
            title=f"Macro Caution: WTI=${wti:.1f} ({chg:+.1f}% 3-month)",
            description=(
                f"Commodity downturn — energy firms likely to freeze junior hiring. "
                f"Affected: {', '.join(energy_firms[:8])}."
            ),
            source_url="https://finance.yahoo.com",
            practice_area="energy",
            raw_data={"wti": wti, "chg": chg},
        )

    def _store_snapshot(self, wti, wti_chg, tsx_chg, bullish):
        from database.db import get_conn
        conn = get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS macro_snapshots (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                snapped_at  TEXT NOT NULL DEFAULT (datetime('now')),
                wti_price   REAL,
                wti_3mo_chg REAL,
                tsx_en_chg  REAL,
                bullish_count INTEGER
            )""")
        conn.execute(
            "INSERT INTO macro_snapshots (wti_price, wti_3mo_chg, tsx_en_chg, bullish_count) "
            "VALUES (?,?,?,?)", (wti, wti_chg, tsx_chg, bullish)
        )
        conn.commit()
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    mc = MacroCorrelator()
    sigs = mc.run()
    for s in sigs:
        print(f"  [{s['signal_type']}] {s['firm_id']}: {s['title']}")
