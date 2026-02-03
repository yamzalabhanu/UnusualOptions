# technicals_daily.py
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import httpx


@dataclass
class DailyBar:
    t: int
    o: float
    h: float
    l: float
    c: float
    v: float


def _ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    if period <= 1:
        return values[:]
    alpha = 2.0 / (period + 1.0)
    out: List[float] = []
    cur = values[0]
    out.append(cur)
    for v in values[1:]:
        cur = alpha * v + (1 - alpha) * cur
        out.append(cur)
    return out


def _anchored_vwap_daily(bars: List[DailyBar], anchor_idx: int) -> float:
    if not bars:
        return 0.0
    anchor_idx = max(0, min(anchor_idx, len(bars) - 1))
    num = 0.0
    den = 0.0
    for b in bars[anchor_idx:]:
        tp = (b.h + b.l + b.c) / 3.0
        vol = max(0.0, b.v)
        num += tp * vol
        den += vol
    if den <= 0:
        return (bars[-1].h + bars[-1].l + bars[-1].c) / 3.0
    return num / den


def _pick_swing_high_low(bars: List[DailyBar], lookback: int = 60) -> Tuple[float, float, int, int]:
    window = bars[-lookback:] if len(bars) > lookback else bars
    lows = [b.l for b in window]
    highs = [b.h for b in window]
    lo = min(lows)
    hi = max(highs)
    lo_idx = len(bars) - len(window) + lows.index(lo)
    hi_idx = len(bars) - len(window) + highs.index(hi)
    return lo, hi, lo_idx, hi_idx


def _fib_levels(low: float, high: float) -> Dict[str, float]:
    diff = high - low
    if diff <= 0:
        return {"0.382": low, "0.5": low, "0.618": low, "0.786": low}
    return {
        "0.382": high - 0.382 * diff,
        "0.5": high - 0.5 * diff,
        "0.618": high - 0.618 * diff,
        "0.786": high - 0.786 * diff,
    }


def _nearest_level(price: float, levels: Dict[str, float]) -> Dict[str, float]:
    best_k = None
    best_d = None
    best_p = None
    for k, p in levels.items():
        d = abs(price - p)
        if best_d is None or d < best_d:
            best_d = d
            best_k = k
            best_p = p
    dist_pct = (best_d / price) * 100.0 if price else 0.0
    return {"level": float(best_k), "price": float(best_p), "dist_pct": float(dist_pct)}


async def fetch_daily_polygon(symbol: str, api_key: str, lookback_days: int = 240) -> List[DailyBar]:
    # Daily aggregates from Polygon
    end = datetime.now(timezone.utc).date()
    start = end.fromordinal(end.toordinal() - lookback_days)

    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start.isoformat()}/{end.isoformat()}"
    params = {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": api_key}

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        j = r.json()

    rows = j.get("results") or []
    bars: List[DailyBar] = []
    for x in rows:
        try:
            bars.append(
                DailyBar(
                    t=int(x["t"]),
                    o=float(x["o"]),
                    h=float(x["h"]),
                    l=float(x["l"]),
                    c=float(x["c"]),
                    v=float(x.get("v", 0.0) or 0.0),
                )
            )
        except Exception:
            continue

    return bars


async def compute_daily_context(
    symbol: str,
    api_key: str,
    ema_periods: Tuple[int, int, int] = (9, 21, 50),
    swing_lookback: int = 60,
    avwap_anchor_lookback: int = 60,
) -> Dict[str, object]:
    bars = await fetch_daily_polygon(symbol, api_key, lookback_days=240)
    if len(bars) < max(ema_periods) + 5:
        raise ValueError(f"not enough daily bars for {symbol}: {len(bars)}")

    closes = [b.c for b in bars]
    ema9 = _ema(closes, ema_periods[0])
    ema21 = _ema(closes, ema_periods[1])
    ema50 = _ema(closes, ema_periods[2])

    last = float(closes[-1])
    ema9_v = float(ema9[-1])
    ema21_v = float(ema21[-1])
    ema50_v = float(ema50[-1])
    ema21_slope = float(ema21[-1] - ema21[-2])

    anchor_idx = max(0, len(bars) - avwap_anchor_lookback)
    avwap = float(_anchored_vwap_daily(bars, anchor_idx))

    swing_low, swing_high, low_idx, high_idx = _pick_swing_high_low(bars, lookback=swing_lookback)
    fibs = _fib_levels(swing_low, swing_high)
    near = _nearest_level(last, fibs)

    # Simple daily bias
    price_above_ema50 = last > ema50_v
    ema21_above_ema50 = ema21_v > ema50_v
    ema9_above_ema21 = ema9_v > ema21_v
    ema21_up = ema21_slope > 0
    price_above_avwap = last > avwap

    if price_above_ema50 and ema21_above_ema50 and ema21_up:
        bias = "bullish"
    elif (not price_above_ema50) and (not ema21_above_ema50) and (not ema21_up):
        bias = "bearish"
    else:
        bias = "neutral"

    asof = datetime.fromtimestamp(bars[-1].t / 1000.0, tz=timezone.utc).isoformat()

    return {
        "symbol": symbol,
        "asof": asof,
        "last_close": last,
        "ema": {"ema9": ema9_v, "ema21": ema21_v, "ema50": ema50_v, "ema21_slope": ema21_slope},
        "avwap": {"value": avwap, "anchor": f"{avwap_anchor_lookback}d_back"},
        "fib": {
            "swing_low": float(swing_low),
            "swing_high": float(swing_high),
            "levels": {k: float(v) for k, v in fibs.items()},
            "nearest": near,
        },
        "flags": {
            "price_above_ema50": price_above_ema50,
            "ema21_above_ema50": ema21_above_ema50,
            "ema9_above_ema21": ema9_above_ema21,
            "ema21_slope_up": ema21_up,
            "price_above_avwap": price_above_avwap,
        },
        "trend_bias": bias,
    }
