# alert_formatter.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

def _as_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

def _as_int(x: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if x is None:
            return default
        return int(float(x))
    except Exception:
        return default

def _money(x: Any) -> str:
    v = _as_float(x, None)
    if v is None:
        return "n/a"
    return f"${v:,.0f}"

def _pct(x: Any) -> str:
    v = _as_float(x, None)
    if v is None:
        return "n/a"
    return f"{v:.2f}%"

def _fmt2(x: Any) -> str:
    v = _as_float(x, None)
    return "n/a" if v is None else f"{v:.2f}"

def _ratio(a: Optional[int], b: Optional[int]) -> Optional[float]:
    if a is None or b is None or b == 0:
        return None
    return a / float(b)

def _ask_pct(ask: Optional[int], bid: Optional[int]) -> Optional[float]:
    if ask is None or bid is None:
        return None
    tot = ask + bid
    if tot <= 0:
        return None
    return ask / float(tot)

def _parse_time(ts: Any) -> str:
    s = str(ts or "").strip()
    if not s:
        return "unknown"
    return s

def format_quick_summary(alert: Dict[str, Any]) -> str:
    """
    Human-readable summary using ONLY fields you already computed.
    No GPT. Telegram plain text.
    """
    kind = (alert.get("kind") or "ALERT").upper()   # FLOW / CHAIN
    ticker = (alert.get("ticker") or "UNK").upper()
    opt_type = (alert.get("opt_type") or "UNK").upper()

    contract = alert.get("contract") or "n/a"
    expiry = alert.get("expiry") or alert.get("exp") or "unknown"
    strike = alert.get("strike")
    strike_s = "unknown" if strike is None else str(int(float(strike))) if str(strike).replace(".","",1).isdigit() else str(strike)

    dte = alert.get("dte")
    dte_s = "unknown" if dte is None else str(dte)

    premium = alert.get("premium")
    prem_label = alert.get("premium_label") or ""
    prem_s = _money(premium)
    if prem_label:
        prem_s += f" ({prem_label})"

    vol = _as_int(alert.get("vol"), 0) or 0
    oi = _as_int(alert.get("oi"), 0) or 0
    oi_chg = alert.get("oi_chg")
    voloi = alert.get("voloi")

    ask_vol = _as_int(alert.get("ask_vol"), None)
    bid_vol = _as_int(alert.get("bid_vol"), None)
    sweeps = _as_int(alert.get("sweeps"), None)
    multileg = _as_int(alert.get("multileg"), None)

    askp = _ask_pct(ask_vol, bid_vol)
    askp_s = "n/a" if askp is None else f"{askp:.2f}"

    strength10 = alert.get("strength10")
    score100 = alert.get("score")
    tier = alert.get("tier") or "n/a"

    horizon = alert.get("horizon") or "unknown"

    # contexts
    market_bias = alert.get("market_bias") or "MIXED"
    tide_line = alert.get("market_tide_line") or ""
    dp_line = alert.get("darkpool_line") or ""

    daily = alert.get("daily") or {}
    daily_bias = daily.get("bias") or daily.get("trend_bias") or "unknown"
    ema9 = daily.get("ema9")
    ema21 = daily.get("ema21")
    ema50 = daily.get("ema50")
    avwap = daily.get("avwap")
    fib_near = daily.get("fib_near")
    fib_dist = daily.get("fib_dist_pct")

    created_at = _parse_time(alert.get("time") or alert.get("created_at"))

    # interpretation (conservative)
    meaning_bits = []
    if oi_chg is not None:
        try:
            if int(oi_chg) >= 1500:
                meaning_bits.append("fresh positioning (OI up)")
            elif int(oi_chg) <= -1500:
                meaning_bits.append("possible closing/roll (OI down)")
        except Exception:
            pass

    if askp is not None:
        # for PUT: ask-heavy = urgency/defensive
        if askp >= 0.65:
            meaning_bits.append("aggressive prints (ask-heavy)")
        elif 0.42 <= askp <= 0.58:
            meaning_bits.append("mixed prints")

    if multileg is not None and vol > 0:
        mr = multileg / float(vol)
        if mr >= 0.45:
            meaning_bits.append("complex/hedge-heavy (multi-leg)")

    if sweeps is not None:
        if sweeps >= 1500:
            meaning_bits.append("heavy sweeps")

    meaning = "; ".join(meaning_bits) if meaning_bits else "high interest flow; direction unclear"

    # bias lines
    near_term = "bearish" if opt_type == "PUT" else "bullish" if opt_type == "CALL" else "unknown"
    # adjust if daily bias conflicts
    if str(daily_bias).lower() == "neutral":
        longer_term = "unknown"
    elif str(daily_bias).lower() == "bullish":
        longer_term = "bullish"
    elif str(daily_bias).lower() == "bearish":
        longer_term = "bearish"
    else:
        longer_term = "unknown"

    # Build message (short)
    lines = []
    lines.append(f"{ticker} {('Chain' if kind=='CHAIN' else 'Flow')} — Quick Summary")
    lines.append("")
    lines.append(f"Trade: {expiry} {strike_s} {opt_type} (DTE {dte_s}, {horizon})")
    lines.append(f"Size: {prem_s}")

    type_bits = []
    type_bits.append(f"Vol {vol:,} vs OI {oi:,}")
    if voloi is not None:
        try:
            type_bits.append(f"Vol/OI {float(voloi):.2f}")
        except Exception:
            pass
    if oi_chg is not None:
        type_bits.append(f"OIΔ {oi_chg}")
    if askp is not None:
        type_bits.append(f"ask% {askp_s}")
    if sweeps is not None:
        type_bits.append(f"sweeps {sweeps:,}")
    if multileg is not None:
        type_bits.append(f"multi {multileg:,}")
    if market_bias:
        type_bits.append(f"MarketTide {market_bias}")

    lines.append("Type: " + " | ".join(type_bits))
    lines.append(f"Meaning: {meaning}.")
    lines.append("")
    lines.append("Why it matters")
    lines.append(f"- Tier: {tier} | Strength {strength10}/10 | Score {score100}/100" if score100 is not None else f"- Tier: {tier} | Strength {strength10}/10")
    lines.append(f"- Daily bias: {daily_bias} (EMA9/21/50 { _fmt2(ema9) }/{ _fmt2(ema21) }/{ _fmt2(ema50) }, AVWAP { _fmt2(avwap) }, Fib {fib_near} @ {_pct(fib_dist)})")
    if dp_line:
        lines.append(f"- Darkpool: {dp_line}")
    elif alert.get("darkpool_top"):
        lines.append(f"- Darkpool: {_money(alert.get('darkpool_top'))}")

    lines.append("")
    lines.append(f"Bias for {ticker}")
    lines.append(f"- Near-term: {near_term}")
    lines.append(f"- Longer-term: {longer_term}")
    lines.append("")
    lines.append("Practical takeaway (general, not financial advice)")
    lines.append("- Treat as a watchlist setup; confirm with price action + OI follow-through.")
    lines.append("- If IV is elevated, consider defined-risk structures (spreads).")
    lines.append("")
    # strength label
    strength_label = "very strong" if (strength10 or 0) >= 9 else "strong" if (strength10 or 0) >= 8 else "medium" if (strength10 or 0) >= 6 else "weak"
    lines.append(f"Signal strength: {strength_label} ({strength10}/10) | Time: {created_at}")

    return "\n".join(lines)
