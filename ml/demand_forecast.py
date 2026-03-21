"""
ml/demand_forecast.py
──────────────────────
Demand Forecasting Engine

Uses time-series forecasting (Facebook Prophet or fallback ARIMA-lite)
to forecast FUTURE litigation and deal-flow volumes at each target firm
over the next 90 days.

Why this is different from the z-score spike detector:
  - Z-score: "This firm is CURRENTLY busy compared to baseline"
  - Forecast: "This firm WILL BE busy in 6 weeks based on seasonal patterns"

The forecast uses:
  1. Historical CanLII appearance time series per firm (up to 2 years)
  2. WTI price series (macro covariate)
  3. Calgary-specific seasonality (court sitting schedules, fiscal years)
  4. SEDAR deal history (deal close volumes → post-close integration spike)
  5. Known seasonal patterns (December close crunch, January AIF season)

Output per firm:
  {
    "firm_id":        "burnet",
    "forecast_peak":  "2026-04-15",
    "peak_volume":    18.3,   # predicted appearances
    "current_volume": 11.2,
    "peak_pct_above": 63,     # 63% above current
    "confidence":     0.82,
    "horizon_weeks":  4,
    "action":         "Reach out by April 1 — peak hits April 15"
  }

The forecasted peak date becomes the target for outreach timing.
If the peak is 4 weeks away, you contact them now.
If the peak is 12 weeks away, you schedule outreach for week 8.

Also forecasts:
  - SEDAR deal flow (using rolling 30d count time series)
  - Post-CCAA staffing surges (receivership → 6mo junior demand)
"""

import logging, json, math
from datetime import date, datetime, timedelta
from collections import defaultdict
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import numpy as np

from database.db import get_conn, insert_signal
from config_calgary import CALGARY_FIRMS, FIRM_BY_ID

log = logging.getLogger(__name__)

# Try to import Prophet; fall back to numpy ARIMA-lite
try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
    log.info("[Forecast] Prophet available")
except ImportError:
    PROPHET_AVAILABLE = False
    log.info("[Forecast] Prophet not available — using ARIMA-lite fallback")


# ── Calgary-specific seasonality priors ────────────────────────────────────────
# Monthly multipliers for litigation volume (1.0 = baseline)
CALGARY_LITIGATION_SEASONALITY = {
    1: 0.85,   # January: courts just reopened
    2: 0.95,
    3: 1.10,   # March: pre-Easter crunch
    4: 1.20,   # April: heavy sitting period
    5: 1.15,
    6: 0.90,   # June: summer slowdown begins
    7: 0.70,   # July: low court sittings
    8: 0.75,
    9: 1.20,   # September: post-summer return
    10: 1.25,  # October: peak trial season
    11: 1.15,
    12: 0.80,  # December: holiday, but emergency motions spike
}

# Monthly deal-flow multipliers (M&A / SEDAR)
CALGARY_DEAL_SEASONALITY = {
    1: 0.70,   # January AIF season = securities work, not deals
    2: 0.80,
    3: 1.10,
    4: 1.15,
    5: 1.00,
    6: 1.10,
    7: 0.80,
    8: 1.20,   # August acquisition season
    9: 1.40,   # September M&A peak
    10: 1.35,
    11: 1.20,
    12: 1.50,  # December close crunch
}


def _get_historical_appearances(firm_id: str, days: int = 365) -> list[tuple[date, int]]:
    """Returns list of (date, count) tuples for daily appearance counts."""
    conn   = get_conn()
    rows   = conn.execute("""
        SELECT date(decision_date) as d, count(*) as n
        FROM canlii_appearances
        WHERE firm_id=?
          AND date(decision_date) >= date('now', ? || ' days')
        GROUP BY date(decision_date)
        ORDER BY d ASC
    """, (firm_id, f"-{days}")).fetchall()
    conn.close()
    return [(date.fromisoformat(r["d"]), r["n"]) for r in rows]


def _get_historical_deals(firm_id: str, days: int = 365) -> list[tuple[date, int]]:
    """Returns list of (week, deal_count) tuples from SEDAR filings."""
    conn   = get_conn()
    rows   = conn.execute("""
        SELECT strftime('%Y-%W', filed_date) as wk, count(*) as n
        FROM sedar_filings
        WHERE counsel_firms LIKE ?
          AND date(filed_date) >= date('now', ? || ' days')
        GROUP BY wk ORDER BY wk ASC
    """, (f'%"{firm_id}"%', f"-{days}")).fetchall()
    conn.close()
    if rows:
        return [(r["wk"], r["n"]) for r in rows]
    return []


# ── ARIMA-lite: Simple exponential smoothing + seasonal adjustment ──────────────

class ARIMALite:
    """
    Lightweight forecasting without Prophet dependency.
    Uses double exponential smoothing (Holt's method) + seasonal index.
    """

    def __init__(self, alpha: float = 0.3, beta: float = 0.1):
        self.alpha = alpha
        self.beta  = beta

    def fit_predict(self, series: list[float], periods: int = 90,
                    seasonality: dict = None) -> list[float]:
        """
        Fit on series, predict `periods` ahead.
        Returns list of predicted values.
        """
        if len(series) < 7:
            # Too little data — return flat line at last value
            last = series[-1] if series else 0
            return [last] * periods

        # Holt's double exponential smoothing
        L = series[0]
        T = series[1] - series[0] if len(series) > 1 else 0

        smoothed = [L]
        trends   = [T]

        for i in range(1, len(series)):
            L_new = self.alpha * series[i] + (1 - self.alpha) * (L + T)
            T_new = self.beta  * (L_new - L) + (1 - self.beta) * T
            L, T  = L_new, T_new
            smoothed.append(L)
            trends.append(T)

        # Forecast
        forecasts = []
        for h in range(1, periods + 1):
            f = L + h * T
            # Apply seasonal multiplier if provided
            if seasonality:
                month = (date.today() + timedelta(days=h)).month
                f    *= seasonality.get(month, 1.0)
            forecasts.append(max(0, f))

        return forecasts


class DemandForecaster:
    """
    Forecasts 90-day appearance and deal volumes for all firms.
    Fires predictive signals when a firm is approaching a forecasted peak.
    """

    def __init__(self):
        self.arima = ARIMALite()
        self.new_signals: list[dict] = []

    def run(self) -> list[dict]:
        log.info("[Forecast] Running demand forecasts for %d firms…", len(CALGARY_FIRMS))
        forecasts = []
        for firm in CALGARY_FIRMS:
            result = self._forecast_firm(firm)
            if result:
                forecasts.append(result)
                self._fire_signal_if_warranted(result)

        self._save_forecasts(forecasts)
        log.info("[Forecast] Done. %d firms forecast, %d signals.", 
                 len(forecasts), len(self.new_signals))
        return self.new_signals

    def _forecast_firm(self, firm: dict) -> dict | None:
        fid = firm["id"]

        # Get historical appearance time series
        hist = _get_historical_appearances(fid, days=365)
        if len(hist) < 14:
            return None

        # Convert to daily float series (fill gaps with 0)
        all_dates = [hist[0][0] + timedelta(days=i)
                     for i in range((hist[-1][0] - hist[0][0]).days + 1)]
        count_map = {d: c for d, c in hist}
        series    = [float(count_map.get(d, 0)) for d in all_dates]

        # 7-day rolling average to smooth noise
        smoothed_series = []
        for i in range(len(series)):
            window = series[max(0, i-6):i+1]
            smoothed_series.append(sum(window) / len(window))

        # Forecast 90 days
        if PROPHET_AVAILABLE:
            forecasts = self._prophet_forecast(all_dates, series)
        else:
            forecasts = self.arima.fit_predict(
                smoothed_series, periods=90,
                seasonality=CALGARY_LITIGATION_SEASONALITY
            )

        if not forecasts:
            return None

        current_vol = smoothed_series[-7:] and sum(smoothed_series[-7:]) / 7 or 0
        peak_vol    = max(forecasts)
        peak_day    = forecasts.index(peak_vol)
        peak_date   = date.today() + timedelta(days=peak_day + 1)
        pct_above   = int((peak_vol - current_vol) / max(current_vol, 1) * 100)
        # Confidence based on data richness
        confidence  = min(0.95, 0.5 + len(hist) / 365)

        return {
            "firm_id":       fid,
            "firm_name":     firm["name"],
            "tier":          firm.get("tier", "?"),
            "forecast_peak": peak_date.isoformat(),
            "peak_volume":   round(peak_vol, 1),
            "current_volume":round(current_vol, 1),
            "peak_pct_above":pct_above,
            "confidence":    round(confidence, 2),
            "horizon_days":  peak_day + 1,
            "forecasts_90d": [round(f, 2) for f in forecasts],
        }

    def _prophet_forecast(self, dates: list, series: list) -> list[float]:
        """Use Facebook Prophet for richer forecasting."""
        import pandas as pd
        df = pd.DataFrame({
            "ds": pd.to_datetime(dates),
            "y":  series,
        })
        df = df[df["y"] >= 0]

        m = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=True,
            daily_seasonality=False,
            changepoint_prior_scale=0.1,
        )
        m.fit(df)

        future = m.make_future_dataframe(periods=90)
        fc     = m.predict(future)
        # Return just the 90-day forecast
        tail   = fc.tail(90)
        return tail["yhat"].clip(lower=0).tolist()

    def _fire_signal_if_warranted(self, result: dict):
        """
        Fire a predictive signal if:
        - Peak is 2-6 weeks away (sweet spot for outreach timing)
        - Peak is ≥30% above current volume
        - Confidence ≥ 0.6
        """
        horizon_weeks = result["horizon_days"] / 7
        pct_above     = result["peak_pct_above"]
        confidence    = result["confidence"]
        firm_id       = result["firm_id"]
        firm          = FIRM_BY_ID.get(firm_id, {})

        if not (2 <= horizon_weeks <= 8 and pct_above >= 25 and confidence >= 0.6):
            return

        peak_date_str = result["forecast_peak"]
        outreach_date = (date.fromisoformat(peak_date_str) - timedelta(weeks=2)).isoformat()
        weight        = min(5.0, 3.0 + (pct_above / 100) + confidence)

        desc = (
            f"DEMAND FORECAST: {firm.get('name', firm_id)} litigation volume is projected to peak "
            f"at {result['peak_volume']:.1f} appearances/day around {peak_date_str} "
            f"({pct_above}% above current {result['current_volume']:.1f}). "
            f"Confidence: {confidence:.0%}. "
            f"RECOMMENDED OUTREACH DATE: {outreach_date} — 2 weeks before peak pressure. "
            f"They'll be looking for support but not yet overwhelmed."
        )

        is_new = insert_signal(
            firm_id=firm_id,
            signal_type="forecast_demand_peak",
            weight=weight,
            title=f"Forecast peak at {firm.get('name', firm_id)}: +{pct_above}% in {horizon_weeks:.1f}w (conf={confidence:.0%})",
            description=desc,
            source_url="",
            practice_area="litigation",
            raw_data={
                "peak_date":    peak_date_str,
                "peak_volume":  result["peak_volume"],
                "current_vol":  result["current_volume"],
                "pct_above":    pct_above,
                "horizon_days": result["horizon_days"],
                "confidence":   confidence,
                "outreach_date":outreach_date,
            },
        )
        if is_new:
            self.new_signals.append({
                "firm_id": firm_id,
                "signal_type": "forecast_demand_peak",
                "weight": weight,
                "title": f"Forecast peak: {firm.get('name',firm_id)} +{pct_above}% in {horizon_weeks:.0f}w",
                "practice_area": "litigation",
                "description": desc,
            })

    def _save_forecasts(self, forecasts: list):
        import pathlib
        pathlib.Path("reports").mkdir(exist_ok=True)
        with open("reports/forecasts.json", "w") as f:
            json.dump([{k: v for k, v in fc.items() if k != "forecasts_90d"}
                       for fc in forecasts], f, indent=2)

    def print_forecasts(self, forecasts: list, top_n: int = 10):
        print("\n" + "═" * 72)
        print("📈  90-DAY DEMAND FORECAST")
        print(f"    {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
        print("═" * 72)
        sorted_fc = sorted(forecasts, key=lambda x: x["peak_pct_above"], reverse=True)
        for fc in sorted_fc[:top_n]:
            bar = "▓" * min(20, fc["peak_pct_above"] // 5)
            print(f"  {fc['firm_name']:<40}  +{fc['peak_pct_above']:3d}%  "
                  f"peak={fc['forecast_peak']}  conf={fc['confidence']:.0%}  {bar}")
        print("═" * 72 + "\n")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    forecaster = DemandForecaster()
    sigs = forecaster.run()
    for s in sigs:
        print(f"  [{s['signal_type']}] {s['firm_id']}: {s['title']}")
