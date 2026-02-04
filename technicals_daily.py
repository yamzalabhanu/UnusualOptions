# technicals_daily.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional, Any

import asyncio
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
    cur = float(values[0])
    out.append(cur)
    for v in values[1:]:
        cur = alpha * float(v) + (1 - alpha) * cur
        out.append(cur)
    return out


def _anchored_vwap_daily(bars: List[DailyBar], anchor_idx: int) -> float:
    """
    Anchored VWAP from anchor_idx -> end using typical price * volume.
    """
    if not bars:
        return 0.0
    anchor_idx = max(0, min(int(anchor_idx), len(bars) - 1))
    num = 0.0
    den = 0.0
    for b in bars[anchor_idx:]:
        tp = (b.h + b.l + b.c) / 3.0
        vol = max(0.0, float(b.v))
        num += tp * vol
        den += vol
    if den <= 0:
        last = bars[-1]
        return (last.h + last.l + last.c) / 3.0
    return num / den


def _pick_swing_high_low(bars: List[DailyBar], lookback: int = 60) -> Tuple[float, float, int, int]:
    """
    Returns (swing_low, swing_high, low_index_in_bars, high_index_in_bars)
    over the last `lookback` bars (or fewer if not enough).
    """
    if not bars:
        return 0.0, 0.0, 0, 0

    lookback = max(1, int(lookback))
    window = bars[-lookback:] if len(bars) > lookback else bars

    lows = [float(b.l) for b in window]
    highs = [float(b.h) for b in window]

    lo = min(lows)
    hi = max(highs)

    lo_idx = len(bars) - len(window) + lows.index(lo)
    hi_idx = len(bars) - len(window) + highs.index(hi)
    return float(lo), float(hi), int(lo_idx), int(hi_idx)


def _fib_levels(low: float, high: float) -> Dict[str, float]:
    diff = float(high) - float(low)
    if diff <= 0:
        base = float(low)
        return {"0.382": base, "0.5": base, "0.618": base, "0.786": base}
    high = float(high)
    return {
        "0.382": high - 0.382 * diff,
        "0.5": high - 0.5 * diff,
        "0.618": high - 0.618 * diff,
        "0.786": high - 0.786 * diff,
    }


def _nearest_level(price: float, levels: Dict[str, float]) -> Dict[str, Any]:
    """
    Pick the nearest fib level by absolute distance to `price`.

    IMPORTANT: "level" is returned as a STRING key like "0.382" (not float),
    so upstream can display it safely. "price" and "dist_pct" are numeric.
    """
    best_k: Optional[str] = None
    best_d: Optional[float] = None
    best_p: Optional[float] = None

    price = float(price or 0.0)

    for k, p in levels.items():
        try:
            p = float(p)
        except Exception:
            continue
        d = abs(price - p)
        if best_d is None or d < best_d:
            best_d = d
            best_k = str(k)
            best_p = p

    if best_k is None or best_d is None or best_p is None:
        return {"level": "n/a", "price": 0.0, "dist_pct": 0.0}

    dist_pct = (best_d / price) * 100.0 if price else 0.0
    return {"level": best_k, "price": float(best_p), "dist_pct": float(dist_pct)}


def _pct_dist(a: float, b: float) -> float:
    # percent distance from a to b relative to a (a is "price-like")
    a = float(a or 0.0)
    b = float(b or 0.0)
    if a == 0:
        return 0.0
    return (a - b) / a * 100.0


async def fetch_daily_polygon(
    symbol: str,
    api_key: str,
    lookback_days: int = 240,
    client: Optional[httpx.AsyncClient] = None,
    retries: int = 2,
) -> List[DailyBar]:
    """
    Fetch Polygon daily aggregates.
    If client is provided, it is reused (recommended).
    """
    symbol = (symbol or "").upper().strip()
    if not symbol:
        return []

    end = datetime.now(timezone.utc).date()
    start = end.fromordinal(end.toordinal() - int(lookback_days))

    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start.isoformat()}/{end.isoformat()}"
    params = {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": api_key}

    async def _do_get(c: httpx.AsyncClient) -> dict:
        r = await c.get(url, params=params, timeout=20)
        r.raise_for_status()
        return r.json()

    last_err: Optional[Exception] = None
    for attempt in range(int(retries) + 1):
        try:
            if client is not None:
                j = await _do_get(client)
            else:
                async with httpx.AsyncClient(timeout=20) as c:
                    j = await _do_get(c)

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
        except Exception as e:
            last_err = e
            await asyncio.sleep(0.25 * (2**attempt))

    raise last_err or RuntimeError("fetch_daily_polygon failed")


async def compute_daily_context(
    symbol: str,
    api_key: str,
    ema_periods: Tuple[int, int, int] = (9, 21, 50),
    swing_lookback: int = 60,
    avwap_anchor_lookback: int = 60,
    client: Optional[httpx.AsyncClient] = None,
) -> Dict[str, object]:
    bars = await fetch_daily_polygon(symbol, api_key, lookback_days=240, client=client)
    if len(bars) < max(int(p) for p in ema_periods) + 5:
        raise ValueError(f"not enough daily bars for {symbol}: {len(bars)}")

    closes = [float(b.c) for b in bars]

    ema9 = _ema(closes, int(ema_periods[0]))
    ema21 = _ema(closes, int(ema_periods[1]))
    ema50 = _ema(closes, int(ema_periods[2]))

    last = float(closes[-1])
    ema9_v = float(ema9[-1])
    ema21_v = float(ema21[-1])
    ema50_v = float(ema50[-1])

    ema21_slope = float(ema21[-1] - ema21[-2])
    trend_strength_pct = (abs(ema21_slope) / last * 100.0) if last else 0.0

    anchor_idx = max(0, len(bars) - int(avwap_anchor_lookback))
    avwap = float(_anchored_vwap_daily(bars, anchor_idx))

    swing_low, swing_high, _, _ = _pick_swing_high_low(bars, lookback=int(swing_lookback))
    fibs = _fib_levels(swing_low, swing_high)
    near = _nearest_level(last, fibs)

    # daily bias (with a "flat trend -> neutral" rule)
    price_above_ema50 = last > ema50_v
    ema21_above_ema50 = ema21_v > ema50_v
    ema9_above_ema21 = ema9_v > ema21_v
    ema21_up = ema21_slope > 0
    price_above_avwap = last > avwap

    # if trend is extremely flat, avoid declaring bullish/bearish
    if trend_strength_pct < 0.05:
        bias = "neutral"
    else:
        if price_above_ema50 and ema21_above_ema50 and ema21_up:
            bias = "bullish"
        elif (not price_above_ema50) and (not ema21_above_ema50) and (not ema21_up):
            bias = "bearish"
        else:
            bias = "neutral"

    dist_to_ema21_pct = _pct_dist(last, ema21_v)   # + means price above EMA21, - means below
    dist_to_avwap_pct = _pct_dist(last, avwap)     # + means above AVWAP, - means below

    asof = datetime.fromtimestamp(bars[-1].t / 1000.0, tz=timezone.utc).isoformat()

    return {
        "symbol": (symbol or "").upper(),
        "asof": asof,
        "last_close": last,
        "ema": {
            "ema9": ema9_v,
            "ema21": ema21_v,
            "ema50": ema50_v,
            "ema21_slope": ema21_slope,
            "trend_strength_pct": float(trend_strength_pct),
        },
        "avwap": {"value": avwap, "anchor": f"{int(avwap_anchor_lookback)}d_back"},
        "fib": {
            "swing_low": float(swing_low),
            "swing_high": float(swing_high),
            "levels": {k: float(v) for k, v in fibs.items()},
            "nearest": near,
        },
        "dist": {
            "dist_to_ema21_pct": float(dist_to_ema21_pct),
            "dist_to_avwap_pct": float(dist_to_avwap_pct),
        },
        "flags": {
            "price_above_ema50": bool(price_above_ema50),
            "ema21_above_ema50": bool(ema21_above_ema50),
            "ema9_above_ema21": bool(ema9_above_ema21),
            "ema21_slope_up": bool(ema21_up),
            "price_above_avwap": bool(price_above_avwap),
        },
        "trend_bias": bias,
    }
